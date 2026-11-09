"""All admission, task lease and completion transitions are atomic on one Valkey primary."""
from .retention import STATE_RETENTION, JOB_RETENTION_GRACE, IDLE_HOST_TTL, MAX_PENDING_URLS

_ROBOTS_HELPERS = r'''
local origins='poc:origins:v3'
local body_prefix='poc:robots:body:'
local function ensure_robots(t)
 local raw=redis.call('HGET',origins,t.origin_id)
 local origin=raw and cjson.decode(raw) or nil
 if not origin or (origin.state~='pending' and (not origin.generation or redis.call('EXISTS',body_prefix..t.origin_id..':'..origin.generation)==0)) then
  local generation=tostring(redis.call('INCR','poc:robots:generation'))
  local tm=redis.call('TIME');local admitted_at=t.ready_at or (tonumber(tm[1])+tonumber(tm[2])/1000000)
  origin={state='pending',generation=generation,group=t.group,ready_at=admitted_at}
  redis.call('HSET',origins,t.origin_id,cjson.encode(origin))
  local r={id='robots:v3:'..t.origin_id..':'..generation,kind='robots',url=t.origin..'/robots.txt',origin=t.origin,origin_id=t.origin_id,group=t.group,interval=t.interval,concurrency=t.concurrency,attempt=0,robots_generation=generation}
  redis.call('LPUSH','poc:q:'..t.group,cjson.encode(r))
  redis.call('HSET','poc:states',r.id,'pending')
  local tm=redis.call('TIME');local ready_at=tonumber(tm[1])+tonumber(tm[2])/1000000
  redis.call('ZADD','poc:ready','NX',ready_at,t.group)
  redis.call('HINCRBY','poc:robots:stats','refetches',1)
 end
 return origin
end
'''

_RETENTION_HELPERS = r'''
local function terminal_state(id,value)
 redis.call('HSET','poc:states',id,value)
 redis.call('HEXPIRE','poc:states',@STATE_TTL@,'FIELDS',1,id)
end
'''.replace('@STATE_TTL@',str(STATE_RETENTION))

ENQUEUE = _ROBOTS_HELPERS + r'''
local job = cjson.decode(ARGV[1])
local tm=redis.call('TIME');local ready_at=tonumber(tm[1])+tonumber(tm[2])/1000000
local jk = 'poc:job:' .. job.id
if redis.call('EXISTS', jk) == 1 then return 0 end
if tonumber(redis.call('GET','poc:pending') or '0')+#job.tasks>tonumber(ARGV[2] or '@MAX_PENDING@') then return -1 end
redis.call('HSET', jk, 'status','pending','total',#job.tasks,'completed',0,'succeeded',0,'failed',0,'html_files',0,'html_bytes',0,'created_at',job.created_at,'expires',job.expires)
local function add(t)
  redis.call('RPUSH','poc:q:'..t.group,cjson.encode(t))
  redis.call('HSET','poc:states',t.id,'pending')
  redis.call('ZADD','poc:ready','NX',ready_at,t.group)
end
for _,t in ipairs(job.tasks) do
  t.ready_at=ready_at
  ensure_robots(t)
  add(t)
end
redis.call('INCRBY','poc:pending',#job.tasks)
return #job.tasks
'''.replace('@MAX_PENDING@',str(MAX_PENDING_URLS))

CLAIM = _ROBOTS_HELPERS + _RETENTION_HELPERS + r'''
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
for i=1,64 do
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
      local origin=t.kind=='robots' and {} or ensure_robots(t)
      if t.kind~='robots' then t.robots_generation=origin.generation end
      local blocked=t.kind~='robots' and origin.state=='pending'
      local delayed=tonumber(t.not_before or '0')>now
      local next_at=tonumber(redis.call('HGET',hk,'next') or '0')
      local skip=t.kind~='robots' and origin.state=='denied'
      if blocked or delayed or (not skip and (next_at>now or redis.call('ZCARD',net)>=t.concurrency)) then
        redis.call((blocked or delayed) and 'RPUSH' or 'LPUSH','poc:q:'..g,raw)
        local check_at=now+0.03
        if blocked then check_at=now+1 elseif delayed then check_at=tonumber(t.not_before) end
        schedule(g,math.max(check_at,next_at),false)
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
      terminal_state(t.id,'cancelled')
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
redis.call('EXPIRE',hk,math.max(@HOST_TTL@,math.ceil(t.interval+180)))
t.network_started=now
redis.call('HSET','poc:leases',ARGV[1],cjson.encode(t))
return cjson.encode({status='started',remaining=math.min(deadline,tonumber(permit))-now})
'''.replace('@HOST_TTL@',str(IDLE_HOST_TTL))

ACK = _RETENTION_HELPERS + r'''
local raw=redis.call('HGET','poc:leases',ARGV[1]); if not raw then return 0 end
local t=cjson.decode(raw)
if redis.call('HGET','poc:states',t.id)~='leased:'..ARGV[1] then return 0 end
terminal_state(t.id,'done')
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

REQUEUE = _ROBOTS_HELPERS + r'''
local raw=redis.call('HGET','poc:leases',ARGV[1]); if not raw then return 0 end
local old=cjson.decode(raw); local t=cjson.decode(ARGV[2]);t.not_before=tonumber(ARGV[3])
if redis.call('HGET','poc:states',old.id)~='leased:'..ARGV[1] then return 0 end
redis.call('ZREM','poc:net:'..old.group,ARGV[1]);redis.call('HDEL','poc:leases',ARGV[1]);redis.call('ZREM','poc:lease_deadlines',ARGV[1])
redis.call('HSET','poc:states',t.id,'pending')
if t.kind~='robots' then ensure_robots(t) end
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
redis.call('EXPIRE',key,@JOB_GRACE@)
return 1
'''.replace('@JOB_GRACE@',str(JOB_RETENTION_GRACE))

_CACHE_HELPERS = r'''
local prefix='poc:robots:body:'
local function drop_body(key,reason,keep_metadata)
 if string.sub(key,1,#prefix)~=prefix then return redis.error_reply('Invalid robots cache key') end
 local size=tonumber(redis.call('HGET','poc:robots:sizes',key) or '0')
 redis.call('UNLINK',key)
 redis.call('HDEL','poc:robots:sizes',key)
 redis.call('ZREM','poc:robots:lru',key)
 redis.call('ZREM','poc:robots:expiry',key)
 if not keep_metadata then
  local origin,generation=string.match(string.sub(key,#prefix+1),'^([^:]+):([^:]+)$')
  if origin then
   local raw=redis.call('HGET','poc:origins:v3',origin)
   local state=raw and cjson.decode(raw) or nil
   if state and state.state=='ready' and state.generation==generation then redis.call('HDEL','poc:origins:v3',origin) end
  end
 end
 if size>0 then
  redis.call('DECRBY','poc:robots:bytes',size)
  redis.call('HINCRBY','poc:robots:stats',reason,1)
 end
end
'''

ROBOTS_STORE = _CACHE_HELPERS + r'''
local raw=redis.call('HGET','poc:leases',ARGV[1]); if not raw then return 0 end
local t=cjson.decode(raw)
if t.kind~='robots' or redis.call('HGET','poc:states',t.id)~='leased:'..ARGV[1] then return 0 end
local raw_origin=redis.call('HGET','poc:origins:v3',t.origin_id)
local origin=raw_origin and cjson.decode(raw_origin) or nil
if not origin or origin.generation~=t.robots_generation then return 0 end
local size=#ARGV[2];local budget=tonumber(ARGV[4]);local entries=tonumber(ARGV[5])
if size>budget then return redis.error_reply('Robots response exceeds cache byte budget') end
local key=prefix..t.origin_id..':'..t.robots_generation
if redis.call('HEXISTS','poc:robots:sizes',key)==1 then drop_body(key,'replaced',true) end
local trimmed=0
while tonumber(redis.call('GET','poc:robots:bytes') or '0')+size>budget or redis.call('ZCARD','poc:robots:lru')>=entries do
 if trimmed>=128 then return 2 end
 local victim=redis.call('ZRANGE','poc:robots:lru',0,0)
 if #victim==0 then return redis.error_reply('Robots cache accounting is inconsistent') end
 drop_body(victim[1],'evicted');trimmed=trimmed+1
end
local tm=redis.call('TIME');local now=tonumber(tm[1])+tonumber(tm[2])/1000000
local ttl=tonumber(ARGV[3])
redis.call('SET',key,ARGV[2],'EX',ttl)
redis.call('HSET','poc:robots:sizes',key,size)
redis.call('INCRBY','poc:robots:bytes',size)
redis.call('ZADD','poc:robots:lru',now,key)
redis.call('ZADD','poc:robots:expiry',now+ttl,key)
local group=origin.group or t.group
redis.call('HSET','poc:origins:v3',t.origin_id,cjson.encode({state='ready',generation=t.robots_generation,group=group}))
redis.call('HEXPIRE','poc:origins:v3',ttl,'FIELDS',1,t.origin_id)
if redis.call('LLEN','poc:q:'..group)>0 then redis.call('ZADD','poc:ready','LT',origin.ready_at or now,group) end
redis.call('HINCRBY','poc:robots:stats','stored',1)
return 1
'''

ROBOTS_GET = _CACHE_HELPERS + r'''
local key=prefix..ARGV[1]..':'..ARGV[2]
local value=redis.call('GET',key)
if value then
 local tm=redis.call('TIME');local now=tonumber(tm[1])+tonumber(tm[2])/1000000
 redis.call('ZADD','poc:robots:lru',now,key)
 redis.call('HINCRBY','poc:robots:stats','hits',1)
else
 drop_body(key,'expired')
 redis.call('HINCRBY','poc:robots:stats','misses',1)
end
return value
'''

ROBOTS_INVALIDATE = _CACHE_HELPERS + r'''
local key=prefix..ARGV[1]..':'..ARGV[2]
if redis.call('GET',key)==ARGV[3] then drop_body(key,'corrupt');return 1 end
return 0
'''

MAINTAIN = _CACHE_HELPERS + r'''
local tm=redis.call('TIME');local now=tonumber(tm[1])+tonumber(tm[2])/1000000
local expired=redis.call('ZRANGEBYSCORE','poc:robots:expiry','-inf',now,'LIMIT',0,128)
for _,key in ipairs(expired) do drop_body(key,'expired') end
-- Upgrade old terminal tombstones in bounded batches. Pending/leased states
-- have no TTL; neither this sweep nor cache eviction may remove ownership.
local cursor=redis.call('GET','poc:retention:states-cursor') or '0'
local page=redis.call('HSCAN','poc:states',cursor,'COUNT',128)
redis.call('SET','poc:retention:states-cursor',page[1])
for i=1,#page[2],2 do
 local id=page[2][i];local value=page[2][i+1]
 if value=='done' or value=='cancelled' then
  redis.call('HEXPIRE','poc:states',@STATE_TTL@,'NX','FIELDS',1,id)
  local job=string.match(id,'^(%x+):%d+$')
  if job then
   local key='poc:job:'..job
   if redis.call('HGET',key,'status')=='cancelled' and redis.call('TTL',key)==-1 then redis.call('EXPIRE',key,@JOB_GRACE@) end
  end
 end
end
cursor=redis.call('GET','poc:retention:hosts-cursor') or '0'
page=redis.call('SCAN',cursor,'MATCH','poc:host:*','COUNT',128)
redis.call('SET','poc:retention:hosts-cursor',page[1])
for _,key in ipairs(page[2]) do
 if redis.call('TTL',key)==-1 then redis.call('EXPIRE',key,@HOST_TTL@) end
end
return #expired
'''.replace('@STATE_TTL@',str(STATE_RETENTION)).replace('@JOB_GRACE@',str(JOB_RETENTION_GRACE)).replace('@HOST_TTL@',str(IDLE_HOST_TTL))
