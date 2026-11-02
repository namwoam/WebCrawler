"""All admission, task lease and completion transitions are atomic on one Valkey primary."""
ENQUEUE = r'''
local job = cjson.decode(ARGV[1])
local jk = 'poc:job:' .. job.id
if redis.call('EXISTS', jk) == 1 then return 0 end
redis.call('HSET', jk, 'status','pending','total',#job.tasks,'completed',0,'succeeded',0,'failed',0,'html_files',0,'html_bytes',0,'created_at',job.created_at,'expires',job.expires)
local function add(t)
  redis.call('RPUSH','poc:q:'..t.group,cjson.encode(t))
  redis.call('HSET','poc:states',t.id,'pending')
  redis.call('ZADD','poc:ready','NX',0,t.group)
end
for _,t in ipairs(job.tasks) do
  if redis.call('HSETNX','poc:origins:v2',t.origin_id,cjson.encode({state='pending'})) == 1 then
    add({id='robots:v2:'..t.origin_id,kind='robots',url=t.origin..'/robots.txt',origin=t.origin,origin_id=t.origin_id,group=t.group,interval=t.interval,concurrency=t.concurrency,attempt=0})
  end
  add(t)
end
redis.call('INCRBY','poc:pending',#job.tasks)
return #job.tasks
'''

CLAIM = r'''
local tm=redis.call('TIME'); local now=tonumber(tm[1])+tonumber(tm[2])/1000000
local count=tonumber(ARGV[1]); local result={}; local wait=0.25
local groups=cjson.decode(ARGV[5] or '[]'); local maximum=tonumber(ARGV[6] or '30')
local owned={};local scores={};local domain_count=0
for _,g in ipairs(groups) do owned[g]=true;domain_count=domain_count+1 end
if #groups>0 then
 local existing=redis.call('ZMSCORE','poc:ready',unpack(groups))
 for i,g in ipairs(groups) do if existing[i] then scores[g]=tonumber(existing[i]) end end
end
local function take()
 if domain_count<maximum then
  local first=redis.call('ZPOPMIN','poc:ready',1)
  if #first>0 then scores[first[1]]=nil end
  return first
 end
 local best=nil;local due=math.huge
 for g,score in pairs(scores) do if score<due then best=g;due=score end end
 if not best then return {} end
 redis.call('ZREM','poc:ready',best);scores[best]=nil
 return {best,tostring(due)}
end
local function schedule(g,score,nx)
 if nx then redis.call('ZADD','poc:ready','NX',score,g) else redis.call('ZADD','poc:ready',score,g) end
 if owned[g] then scores[g]=tonumber(redis.call('ZSCORE','poc:ready',g)) end
end
for i=1,256 do
  if #result>=count then break end
  local first=take()
  if #first==0 then break end
  local g=first[1]; local due=tonumber(first[2])
  if due>now then schedule(g,due,false);wait=math.min(0.25,due-now);break end
  local hk='poc:host:'..g; local net='poc:net:'..g
  redis.call('ZREMRANGEBYSCORE',net,'-inf',now)
  local raw=redis.call('LPOP','poc:q:'..g)
  if raw then
    local t=cjson.decode(raw)
    local state=redis.call('HGET','poc:states',t.id)
    local js=t.job_id and redis.call('HGET','poc:job:'..t.job_id,'status') or 'running'
    local expiry=t.job_id and tonumber(redis.call('HGET','poc:job:'..t.job_id,'expires') or '0') or now+1
    if state=='pending' and js~='cancelled' and expiry>now then
      local origin=cjson.decode(redis.call('HGET','poc:origins:v2',t.origin_id) or '{"state":"pending"}')
      local blocked=t.kind~='robots' and origin.state=='pending'
      local delayed=tonumber(t.not_before or '0')>now
      local next_at=tonumber(redis.call('HGET',hk,'next') or '0')
      local skip=t.kind~='robots' and origin.state=='denied'
      if blocked or delayed or (not skip and (next_at>now or redis.call('ZCARD',net)>=t.concurrency)) then
        redis.call((blocked or delayed) and 'RPUSH' or 'LPUSH','poc:q:'..g,raw)
        schedule(g,math.max(now+0.03,next_at),false)
      else
        local token=ARGV[2]..':'..tostring(i)
        t.token=token;t.worker=ARGV[3];t.attempt=(t.attempt or 0)+1;t.skip=skip;t.network_started=nil
        t.granted_at=now;t.network_deadline=now+180;t.lease_deadline=now+180
        if not skip then
          redis.call('ZADD',net,t.network_deadline,token)
        end
        redis.call('HSET','poc:states',t.id,'leased:'..token)
        redis.call('HSET','poc:leases',token,cjson.encode(t))
        redis.call('ZADD','poc:lease_deadlines',t.lease_deadline,token)
        if t.job_id then
          if redis.call('HSETNX','poc:job:'..t.job_id,'started_at',ARGV[4])==1 then redis.call('HSET','poc:job:'..t.job_id,'status','running') end
        end
        if not owned[g] then owned[g]=true;domain_count=domain_count+1 end
        table.insert(result,t)
      end
    elseif state=='pending' then
      redis.call('HSET','poc:states',t.id,'cancelled')
      if t.job_id then redis.call('DECR','poc:pending') end
    end
    if redis.call('LLEN','poc:q:'..g)>0 and redis.call('ZSCORE','poc:ready',g)==false then schedule(g,now,false) end
  end
end
return cjson.encode({tasks=result,wait=wait})
'''

START_NETWORK = r'''
local tm=redis.call('TIME'); local now=tonumber(tm[1])+tonumber(tm[2])/1000000
local raw=redis.call('HGET','poc:leases',ARGV[1])
if not raw then return cjson.encode({status='expired'}) end
local t=cjson.decode(raw)
if redis.call('HGET','poc:states',t.id)~='leased:'..ARGV[1] then
 return cjson.encode({status='expired'})
end
if t.job_id then
 local jk='poc:job:'..t.job_id
 if redis.call('HGET',jk,'status')=='cancelled' or tonumber(redis.call('HGET',jk,'expires') or '0')<=now then
  return cjson.encode({status='cancelled'})
 end
end
local deadline=math.min(t.network_deadline,t.lease_deadline)
local permit=redis.call('ZSCORE','poc:net:'..t.group,ARGV[1])
if not permit or math.min(deadline,tonumber(permit))-now<20 then
 return cjson.encode({status='expired'})
end
-- A token authorizes at most one HTTP start. Retries must obtain a new lease.
if t.network_started then return cjson.encode({status='expired'}) end
local hk='poc:host:'..t.group
local next_at=tonumber(redis.call('HGET',hk,'next') or '0')
if next_at>now then return cjson.encode({status='wait',wait=next_at-now}) end
redis.call('HSET',hk,'next',now+t.interval)
t.network_started=now
redis.call('HSET','poc:leases',ARGV[1],cjson.encode(t))
return cjson.encode({status='started',remaining=math.min(deadline,tonumber(permit))-now})
'''

ACK = r'''
local raw=redis.call('HGET','poc:leases',ARGV[1]); if not raw then return 0 end
local t=cjson.decode(raw)
if redis.call('HGET','poc:states',t.id)~='leased:'..ARGV[1] then return 0 end
redis.call('HSET','poc:states',t.id,'done')
redis.call('HDEL','poc:leases',ARGV[1]);redis.call('ZREM','poc:lease_deadlines',ARGV[1]);redis.call('ZREM','poc:net:'..t.group,ARGV[1])
if t.job_id then redis.call('DECR','poc:pending') end
if t.job_id and redis.call('HGET','poc:job:'..t.job_id,'status')~='cancelled' then
 local jk='poc:job:'..t.job_id
 local done=redis.call('HINCRBY',jk,'completed',1)
 redis.call('HINCRBY',jk,ARGV[2]=='1' and 'succeeded' or 'failed',1)
 redis.call('HINCRBY',jk,'html_files',ARGV[3]=='1' and 1 or 0)
 redis.call('HINCRBY',jk,'html_bytes',ARGV[4])
 redis.call('INCRBY','poc:html_bytes',ARGV[4])
 redis.call('HINCRBY','poc:stats',ARGV[2]=='1' and 'completed_success' or 'completed_error',1)
 if done==tonumber(redis.call('HGET',jk,'total')) then redis.call('HSET',jk,'status','completed','completed_at',ARGV[5]) end
end
return 1
'''

REQUEUE = r'''
local raw=redis.call('HGET','poc:leases',ARGV[1]); if not raw then return 0 end
local old=cjson.decode(raw); local t=cjson.decode(ARGV[2]);t.not_before=tonumber(ARGV[3])
if redis.call('HGET','poc:states',old.id)~='leased:'..ARGV[1] then return 0 end
redis.call('ZREM','poc:net:'..old.group,ARGV[1]);redis.call('HDEL','poc:leases',ARGV[1]);redis.call('ZREM','poc:lease_deadlines',ARGV[1])
redis.call('HSET','poc:states',t.id,'pending')
if t.kind~='robots' and redis.call('HSETNX','poc:origins:v2',t.origin_id,cjson.encode({state='pending'}))==1 then
 local r={id='robots:v2:'..t.origin_id,kind='robots',url=t.origin..'/robots.txt',origin=t.origin,origin_id=t.origin_id,group=t.group,interval=t.interval,concurrency=t.concurrency,attempt=0}
 redis.call('RPUSH','poc:q:'..t.group,cjson.encode(r));redis.call('HSET','poc:states',r.id,'pending')
end
redis.call('RPUSH','poc:q:'..t.group,cjson.encode(t))
redis.call('ZADD','poc:ready','NX',ARGV[3],t.group)
return 1
'''

RECOVER = r'''
local tm=redis.call('TIME'); local now=tonumber(tm[1])+tonumber(tm[2])/1000000
local expired=redis.call('ZRANGEBYSCORE','poc:lease_deadlines','-inf',now,'LIMIT',0,128)
for _,token in ipairs(expired) do
 local raw=redis.call('HGET','poc:leases',token)
 if raw then
  local t=cjson.decode(raw)
  if redis.call('HGET','poc:states',t.id)=='leased:'..token then
   redis.call('HSET','poc:states',t.id,'pending');redis.call('RPUSH','poc:q:'..t.group,cjson.encode(t));redis.call('ZADD','poc:ready','NX',now,t.group)
   redis.call('HINCRBY','poc:stats','recovered_leases',1)
  end
  redis.call('ZREM','poc:net:'..t.group,token)
 end
 redis.call('HDEL','poc:leases',token);redis.call('ZREM','poc:lease_deadlines',token)
end
return #expired
'''

CANCEL = r'''
local key='poc:job:'..ARGV[1]
if redis.call('EXISTS',key)==0 then return 0 end
if redis.call('HGET',key,'status')=='cancelled' then return 0 end
redis.call('HSET',key,'status','cancelled')
redis.call('DECRBY','poc:html_bytes',tonumber(redis.call('HGET',key,'html_bytes') or '0'))
return 1
'''
