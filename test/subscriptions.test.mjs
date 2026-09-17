import test from 'node:test';
import assert from 'node:assert/strict';
import { loadModuleUnderTest } from './helpers/harness.mjs';
const { maintainSubscription, MESSAGE_EVENT }=await loadModuleUnderTest();
const now=1800000000,space='spaces/TESTSPACE123',topic='projects/example/topics/chat';
const sub=(patch={})=>({name:'subscriptions/sub-1',targetResource:`//chat.googleapis.com/${space}`,
  notificationEndpoint:{pubsubTopic:topic},eventTypes:[MESSAGE_EVENT],payloadOptions:{includeResource:true},state:'ACTIVE',expireTime:new Date((now+14400)*1000).toISOString(),...patch});
const options=request=>({space,topic,token:'events-token',inspectionToken:'chat-token',bufferSeconds:1800,now,request});

test('create unwraps an operation and records actual subscription expiry',async()=>{
  const requests=[];
  const result=await maintainSubscription(options(async(url,opts)=>{requests.push({url,opts});
    return opts?.method==='POST'?{status:200,data:{name:'operations/create-1',done:true,response:sub()}}:{status:200,data:{subscriptions:[]}};
  }));
  assert.equal(result.name,'subscriptions/sub-1');assert.equal(result.expiresAt,now+14400);
  assert.equal(new URL(requests[0].url).searchParams.get('filter'),`event_types:"${MESSAGE_EVENT}" AND target_resource="//chat.googleapis.com/${space}"`);
  assert.equal(requests[0].opts.headers.Authorization,'Bearer chat-token');assert.equal(requests[1].opts.headers.Authorization,'Bearer events-token');
  assert.equal(requests[1].opts.body.ttl,'14400s');assert.equal(requests[1].opts.body.payloadOptions.includeResource,true);
});

test('existing subscription renewal uses PATCH and polls the returned operation',async()=>{
  const requests=[];
  const result=await maintainSubscription({...options(async(url,opts)=>{requests.push({url,opts});
    if(url.includes('operations/')) return {status:200,data:{done:true,response:sub()}};
    if(opts?.method==='PATCH') return {status:200,data:{name:'operations/update-1'}};
    return {status:200,data:sub({expireTime:new Date((now+50)*1000).toISOString()})};
  }),current:{name:'subscriptions/sub-1'}});
  assert.equal(requests[1].opts.method,'PATCH');assert.ok(requests[1].url.endsWith('?updateMask=ttl'));
  assert.equal(requests[2].opts.headers.Authorization,'Bearer chat-token');
  assert.deepEqual(requests[1].opts.body,{name:'subscriptions/sub-1',ttl:'14400s'});assert.equal(result.expiresAt,now+14400);
});

test('discovery paginates and matches space, destination and event type',async()=>{
  let count=0;
  const result=await maintainSubscription(options(async(url)=>{count++;
    if(count===1)return {status:200,data:{subscriptions:[sub({notificationEndpoint:{pubsubTopic:'wrong'}})],nextPageToken:'next page'}};
    assert.ok(url.includes('pageToken=next%20page'));return {status:200,data:{subscriptions:[sub()]}};
  }));assert.equal(result.name,'subscriptions/sub-1');assert.equal(count,2);
});

test('conflict rediscovers, suspended state reactivates and stale state gets recreated',async()=>{
  let lists=0,posts=0;
  const result=await maintainSubscription({...options(async(url,opts)=>{
    if(url.endsWith(':reactivate')) return {status:200,data:{done:true,response:sub()}};
    if(opts?.method==='POST'){posts++;return {status:409,data:{}};}
    if(url.includes('?filter=')){lists++;return {status:200,data:{subscriptions:lists===1?[]:[sub({state:'SUSPENDED'})]}};}
    return {status:404,data:{}};
  }),current:{name:'subscriptions/deleted'}});
  assert.equal(result.name,'subscriptions/sub-1');assert.equal(posts,1);assert.equal(lists,2);
});

test('API failures and malformed/mismatched operations never produce saved success',async()=>{
  for(const response of [
    {status:403,data:{error:{message:'SECRET'}}},
    {status:200,data:{name:'operations/bad',done:true,error:{message:'SECRET'}}},
    {status:200,data:{name:'operations/bad',done:true}},
    {status:200,data:{done:true,response:sub({targetResource:'//chat.googleapis.com/spaces/OTHER'})}},
    {status:200,data:{name:'http://unsafe/operation'}}
  ]) {
    await assert.rejects(()=>maintainSubscription(options(async(_,opts)=>opts?.method==='POST'?response:{status:200,data:{subscriptions:[]}})),e=>!e.message.includes('SECRET'));
  }
});
