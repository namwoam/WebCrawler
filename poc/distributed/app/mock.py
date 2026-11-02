"""Controlled external HTTP target used before live URL benchmarks."""
import asyncio
from collections import Counter
import time
from aiohttp import web

active=Counter()
peak=Counter()
events=[]
attempts=Counter()

async def handle(request):
    if request.path=='/__stats':
        return web.json_response(dict(events=events,peak=dict(peak),active=dict(active)))
    host=request.host
    active[host]+=1;peak[host]=max(peak[host],active[host])
    key=(host,request.path);attempts[key]+=1
    events.append(dict(host=host,path=request.path,start=time.time(),worker=request.headers.get('X-Crawler-Worker'),node=request.headers.get('X-Crawler-Node'),headers=dict(request.headers)))
    event=events[-1]
    try:
        if request.path=='/robots.txt':
            if host.startswith('robots403.'):
                return web.Response(status=403,text='access denied')
            if host.startswith('robots429.'):
                return web.Response(status=429,headers={'Retry-After':'86400'},text='rate limited')
            if host.startswith('robots500.'):
                return web.Response(status=500,text='server unavailable')
            if host.startswith('robotsrules403.'):
                return web.Response(status=403,text='User-agent: *\nDisallow: /denied\n')
            return web.Response(text='User-agent: *\nDisallow: /denied\nDisallow: /*.pdf$\nAllow: /public/*.pdf$\nCrawl-delay: 86400\n')
        await asyncio.sleep(float(request.query.get('delay','0.1')))
        if request.path=='/throttle' and attempts[key]==1:
            return web.Response(status=429,headers={'Retry-After':'2'})
        if request.path=='/refresh':
            return web.Response(text='<html><meta http-equiv="refresh" content="0;url=/html"></html>',content_type='text/html')
        if request.path.startswith('/chain/'):
            step=int(request.path.rsplit('/',1)[1])
            return web.Response(status=302,headers={'Location':f'/chain/{step+1}' if step<7 else '/retry503'})
        if request.path=='/retry503' and attempts[key]==1:
            return web.Response(status=503,text='temporary')
        if request.path=='/redirect':
            return web.Response(status=302,headers={'Location':'http://target.poc.test:8081/html'})
        return web.Response(text='<html><title>crawler mock</title>ok</html>',content_type='text/html')
    finally:
        active[host]-=1;event['end']=time.time()

app=web.Application();app.router.add_route('*','/{path:.*}',handle)
web.run_app(app,host='0.0.0.0',port=8081)
