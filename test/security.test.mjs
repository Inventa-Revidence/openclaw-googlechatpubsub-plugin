import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdirSync, writeFileSync, symlinkSync, linkSync, statSync, readFileSync, renameSync } from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { GoogleAuth } from 'google-auth-library';
import { loadModuleUnderTest, makeTempDir, startMockChat, MODULE_PATH } from './helpers/harness.mjs';
const m = await loadModuleUnderTest();
const space = 'spaces/TESTSPACE123';
const baseConfig = () => ({auth:{mode:'service-account'},projectId:'example-project',topicId:'topic',subscriptionId:'sub',
  allowFrom:['users/123'],bindings:[{space,agents:[{agentId:'main',alwaysListen:true}]}]});
function event(overrides={}, type=m.MESSAGE_EVENT) {
  return {ackId:'ack-1',message:{attributes:{'ce-type':type},data:Buffer.from(JSON.stringify({message:{
    name:`${space}/messages/msg1`,space:{name:space},sender:{type:'HUMAN',name:'users/123'},text:'private-message',...overrides
  }})).toString('base64')}};
}
function runtime(dispatch) {
  return {config:{},runtime:{channel:{reply:{finalizeInboundContext:x=>x,dispatchReplyWithBufferedBlockDispatcher:dispatch},
    session:{recordSessionMetaFromInbound:async()=>{}}}}};
}

test('admission denies unknown senders/spaces/types and separates command authority', () => {
  const config=baseConfig(); m.__configure({config}); m.__resetDedup();
  assert.equal(m.admittedJobs(event()).length,1);
  assert.equal(m.isCommandAuthorized(space,'users/123'),false);
  for(const msg of [event({sender:{type:'HUMAN',name:'users/456'}}),event({sender:{type:'BOT',name:'users/123'}}),
    event({space:{name:'spaces/OTHER'}}),event({name:'spaces/OTHER/messages/m'}),event({thread:{name:'spaces/OTHER/threads/t'}}),
    event({},'google.workspace.chat.message.v1.updated'), event({text:'x'.repeat(32769)})]) assert.deepEqual(m.admittedJobs(msg),[]);
  config.commandAllowFrom=['users/456']; assert.equal(m.isCommandAuthorized(space,'users/456'),false);
  config.commandAllowFrom=['users/123']; assert.equal(m.isCommandAuthorized(space,'users/123'),true);
  config.allowAllSpaceMembers=true; assert.equal(m.isSenderAllowed(space,'users/456'),true);
  assert.equal(m.isSenderAllowed(space,'users/someone@example.com'),false);
  config.bindings[0].allowAllSpaceMembers=false; config.bindings[0].allowFrom=[];
  assert.equal(m.isSenderAllowed(space,'users/123'),false);
});

test('configuration fails closed without admission and on app-only unsupported features',()=>{
  const c=baseConfig(); m.__configure({config:c}); assert.doesNotThrow(m.validateConfig);
  delete c.allowFrom; assert.throws(m.validateConfig,/allowFrom/);
  c.allowFrom=['users/123']; c.media={outboundEnabled:true}; assert.throws(m.validateConfig,/user-only/);
  c.media={}; c.reactionsEnabled=true; assert.throws(m.validateConfig,/user-only/);
  c.reactionsEnabled=false; c.allowFrom=['email@example.com']; assert.throws(m.validateConfig,/numeric/);
  c.allowFrom=['users/123']; c.projectId='../escape'; assert.throws(m.validateConfig,/identifier/);
});

test('all outbound paths reject unbound spaces, wrong threads and disabled media',async()=>{
  m.__configure({config:baseConfig()});
  assert.throws(()=>m.assertDestination('spaces/OTHER'),/destination/i);
  assert.throws(()=>m.assertDestination(space,'spaces/OTHER/threads/t'),/resource/);
  await assert.rejects(()=>m.resolveOutboundMedia('/etc/passwd'),/disabled/);
  await assert.rejects(()=>m.sendMediaMessages({space,mediaRefs:['/etc/passwd'],token:'x'}),/disabled/);
  await assert.rejects(()=>m.uploadAttachment({space,buffer:Buffer.from('x'),filename:'a',mimeType:'text/plain',token:'x'}),/disabled/);
});

test('filesystem guard rejects traversal, symlinks, hardlinks and oversize reads',async t=>{
  const tmp=makeTempDir('safe-files'); t.after(()=>tmp.cleanup());
  const root=path.join(tmp.dir,'allowed'); mkdirSync(root); writeFileSync(path.join(root,'ok.txt'),'hello');
  const secret=tmp.file('secret','do-not-upload'); symlinkSync(secret,path.join(root,'link')); linkSync(secret,path.join(root,'hard'));
  assert.equal((await m.readMediaFile(path.join(root,'ok.txt'),[root],5)).toString(),'hello');
  for(const name of ['../secret','link','hard']) await assert.rejects(()=>m.readMediaFile(path.join(root,name),[root],100));
  await assert.rejects(()=>m.readMediaFile(path.join(root,'ok.txt'),[root],4));
});

test('bounded response rejects claimed and streamed oversize data',async()=>{
  await assert.rejects(()=>m.readBoundedResponse(new Response('abc',{headers:{'content-length':'999999999'}}),8),/byte limit/);
  let cancelled=false;
  const stream=new ReadableStream({pull(c){c.enqueue(new Uint8Array(8));},cancel(){cancelled=true;}});
  await assert.rejects(()=>m.readBoundedResponse(new Response(stream),10),/byte limit/);
  assert.equal(cancelled,true);
});

test('real SSRF guard rejects HTTP, local targets, DNS rebinding and unsafe redirects',async t=>{
  t.after(()=>{delete globalThis.__TEST_GUARD_OVERRIDES__;});
  let requests=0;
  globalThis.__TEST_GUARD_OVERRIDES__={lookupFn:async()=>[{address:'127.0.0.1',family:4}],fetchImpl:async()=>{requests++;return new Response('secret');}};
  for(const url of ['http://example.org/a','https://127.0.0.1/a','https://169.254.169.254/latest/meta-data',
    'https://[::1]/a','https://example.org/rebound','https://user:pass@example.org/a']) await assert.rejects(()=>m.fetchMedia(url,100));
  assert.equal(requests,0);
  globalThis.__TEST_GUARD_OVERRIDES__={lookupFn:async()=>[{address:'93.184.216.34',family:4}],fetchImpl:async()=>{
    requests++;return new Response(null,{status:302,headers:{location:'https://169.254.169.254/credentials'}});
  }};
  await assert.rejects(()=>m.fetchMedia('https://public.example/file',100)); assert.equal(requests,1);
});

test('cross-origin media redirects strip bearer credentials and pin DNS',async t=>{
  t.after(()=>{delete globalThis.__TEST_GUARD_OVERRIDES__;});
  const requests=[];
  globalThis.__TEST_GUARD_OVERRIDES__={lookupFn:async()=>[{address:'93.184.216.34',family:4}],fetchImpl:async(url,init)=>{
    requests.push({url:String(url),init});
    return requests.length===1 ? new Response(null,{status:302,headers:{location:'https://cdn.example/file'}}) : new Response('file');
  }};
  assert.equal((await m.fetchMedia('https://public.example/file',20,{Authorization:'Bearer SECRET'})).buffer.toString(),'file');
  assert.equal(new Headers(requests[0].init.headers).get('authorization'),'Bearer SECRET');
  assert.equal(new Headers(requests[1].init.headers).get('authorization'),null);
  assert.ok(requests.every(r=>r.init.dispatcher),'transport must use pinned dispatchers');
});

test('service account caches separate scoped clients and coalesces token refreshes',async t=>{
  const original=GoogleAuth.prototype.getAccessToken; t.after(()=>{GoogleAuth.prototype.getAccessToken=original;});
  const seen=[]; let finish;
  GoogleAuth.prototype.getAccessToken=function(){seen.push(this); return new Promise(r=>{finish=r;});};
  const provider=new m.ServiceAccountTokens('/unused/test-key.json');
  const a=provider.token('pubsub'),b=provider.token('pubsub'); assert.equal(a,b); assert.equal(seen.length,1);
  finish('token-pubsub'); assert.equal(await a,'token-pubsub');
  const chat=provider.token('chat'); finish('token-chat'); await chat;
  const events=provider.token('events'); finish('token-events'); await events;
  assert.deepEqual(seen.map(c=>c.scopes),[ [m.AUTH_SCOPES.pubsub],[m.AUTH_SCOPES.chat],[m.AUTH_SCOPES.events] ]);
  assert.equal(new Set(seen).size,3);
  m.__configure({config:baseConfig()}); await assert.rejects(m.getOAuthToken,/user-oauth/);
});

test('durable admission survives a separate process; interrupted runs are quarantined',async t=>{
  const tmp=makeTempDir('inbox'); t.after(()=>tmp.cleanup()); const p=path.join(tmp.dir,'inbox.json');
  const inbox=new m.DurableInbox(p,2); inbox.admit([{id:'one',payload:{text:'private'}},{id:'two',payload:{text:'second'}}]);
  inbox.update('one',{state:'running',attempts:1});
  assert.equal(statSync(p).mode & 0o777,0o600);
  const output=execFileSync(process.execPath,['--input-type=module','-e',`import {DurableInbox} from ${JSON.stringify(MODULE_PATH)}; console.log(JSON.stringify(new DurableInbox(${JSON.stringify(p)},2).snapshot()));`],{encoding:'utf8'});
  const recovered=JSON.parse(output); assert.equal(recovered[0].state,'failed'); assert.equal(recovered[1].state,'pending');
  const reopened=new m.DurableInbox(p,2); reopened.admit([{id:'one',payload:{text:'duplicate'}}]);
  assert.equal(reopened.snapshot().length,2); assert.throws(()=>reopened.admit([{id:'three',payload:{}}]),/full/);
  reopened.update('two',{state:'done'}); assert.equal(reopened.snapshot()[1].payload,undefined);
  const before=reopened.snapshot(); renameSync(tmp.dir,tmp.dir+'-moved');
  try {assert.throws(()=>reopened.admit([{id:'three',payload:{}}]));assert.deepEqual(reopened.snapshot(),before);}
  finally {renameSync(tmp.dir+'-moved',tmp.dir);}
});

test('a second process cannot acquire an active inbox lock',async t=>{
  const tmp=makeTempDir('inbox-lock'); t.after(()=>tmp.cleanup()); const p=path.join(tmp.dir,'inbox.json');
  const release=await m.lockInbox(p);
  try {
    execFileSync(process.execPath,['--input-type=module','-e',`import {lockInbox} from ${JSON.stringify(MODULE_PATH)}; try {await lockInbox(${JSON.stringify(p)});process.exit(9);} catch {process.exit(0);}`]);
  } finally {await release();}
  const again=await m.lockInbox(p);await again();
});

test('poll commits every routed job before ACK, deduplicates redelivery and keeps commands denied',async t=>{
  const tmp=makeTempDir('poll');t.after(()=>tmp.cleanup());const p=path.join(tmp.dir,'inbox.json');
  const inbox=new m.DurableInbox(p);const contexts=[]; const logs=[];
  const config=baseConfig();config.bindings[0].agents.push({agentId:'second',alwaysListen:true});
  const host=await startMockChat(req=>{
    if(req.url.endsWith(':pull')) return {body:{receivedMessages:[event()]}};
    if(req.url.endsWith(':acknowledge')) {
      const saved=JSON.parse(readFileSync(p,'utf8'));assert.equal(saved.records.length,2);
      return {body:{}};
    }
    throw Error('unexpected endpoint');
  });t.after(()=>host.close());globalThis.__PUBSUB_BASE__=host.base;
  m.__resetDedup();m.__configure({config,inbox,stopping:false,tokens:{token:async p=>{assert.equal(p,'pubsub');return 'test';}},
    logger:Object.fromEntries(['info','error','warn','debug'].map(k=>[k,s=>logs.push(s)])),
    api:runtime(async p=>{contexts.push(p.ctx);p.replyOptions.onAgentRunTerminalOutcome('completed');})});
  await m.pollOnce();await m.__waitWorker();await m.pollOnce();await m.__waitWorker();
  assert.equal(contexts.length,2);assert.ok(contexts.every(c=>c.CommandAuthorized===false));
  assert.ok(inbox.snapshot().every(r=>r.state==='done'));
  assert.ok(!logs.join('\n').includes('private-message'));m.__configure({stopping:true});
});

test('storage failure prevents ACK; dispatcher failures never become successful tombstones',async t=>{
  const tmp=makeTempDir('poll-fail');t.after(()=>tmp.cleanup());let acks=0;
  const host=await startMockChat(req=>req.url.endsWith(':pull')?{body:{receivedMessages:[event()]}}:(acks++,{body:{}}));
  t.after(()=>host.close());globalThis.__PUBSUB_BASE__=host.base; m.__resetDedup();
  m.__configure({config:baseConfig(),stopping:false,inbox:{admit(){throw Error('disk full');},ready(){return undefined;}},tokens:{token:async()=> 'test'}});
  await m.pollOnce();await m.__waitWorker();assert.equal(acks,0);
  const inbox=new m.DurableInbox(path.join(tmp.dir,'inbox.json'));
  m.__configure({inbox,api:runtime(async p=>{p.dispatcherOptions.onError(Error('SECRET raw response'),{kind:'final'});})});
  await m.pollOnce();await m.__waitWorker();assert.equal(acks,1);assert.equal(inbox.snapshot()[0].state,'failed');
  assert.equal(inbox.ready(),undefined);m.__configure({stopping:true});
});

test('nonoverlapping polling and cooperative timeout wait for the active run to settle',async t=>{
  const tmp=makeTempDir('poll-cancel');t.after(()=>tmp.cleanup());let pulls=0, releasePull;
  const host=await startMockChat(async req=>{
    if(req.url.endsWith(':pull')) {pulls++;await new Promise(r=>{releasePull=r;});return {body:{receivedMessages:[event()]}};}
    return {body:{}};
  });t.after(()=>host.close());globalThis.__PUBSUB_BASE__=host.base; m.__resetDedup();
  const config=baseConfig();config.agentTimeoutSeconds=0.02; // Bypass startup minimum only to test cancellation quickly.
  const inbox=new m.DurableInbox(path.join(tmp.dir,'inbox.json'));let started,settle;let runs=0;
  const runStarted=new Promise(r=>{started=r;});
  m.__configure({config,inbox,stopping:false,api:runtime(async p=>{runs++;started();await new Promise(r=>{settle=r;});assert.equal(p.replyOptions.abortSignal.aborted,true);})});
  const first=m.pollOnce();while(!releasePull) await new Promise(r=>setTimeout(r,5));
  await m.pollOnce();assert.equal(pulls,1);releasePull();await first;await runStarted;
  await new Promise(r=>setTimeout(r,35));m.startInboxWorker();assert.equal(runs,1);assert.equal(inbox.snapshot()[0].state,'running');
  settle();await m.__waitWorker();assert.equal(inbox.snapshot()[0].state,'failed');m.__configure({stopping:true});
});

test('safe error classifications never include response bodies, URLs or tokens',()=>{
  const err=Object.assign(Error('https://private/?token=SECRET user message'),{code:'EAUTH'});
  assert.equal(m.safeError(err),'Error (EAUTH)');assert.equal(m.safeError({name:'SECRET',code:'secret value'}),'Error');
});

test('timeout remains active after response headers while a media body stalls',async t=>{
  t.after(()=>{delete globalThis.__TEST_GUARD_OVERRIDES__;});
  let cancelled=false;
  globalThis.__TEST_GUARD_OVERRIDES__={lookupFn:async()=>[{address:'93.184.216.34',family:4}],fetchImpl:async(_,init)=>{
    return new Response(new ReadableStream({start(c){
      c.enqueue(new Uint8Array([1]));
      init.signal.addEventListener('abort',()=>{cancelled=true;c.error(new DOMException('Aborted','AbortError'));},{once:true});
    }}));
  }};
  await assert.rejects(()=>m.fetchMedia('https://public.example/stalled?token=SECRET',100,{},25),/Aborted/);assert.equal(cancelled,true);
});

test('registered action and adapter callbacks enforce destination and media policy',async()=>{
  let channel;
  m.default({config:{},logger:{info(){},warn(){},error(){},debug(){}},registerChannel(c){channel=c;},registerService(){}});
  m.__configure({config:baseConfig(),tokens:{token(){throw Error('policy denial must precede auth');}}});
  for(const action of ['send','upload-file','react','reactions']) {
    const result=await channel.handleAction({action,params:{target:'spaces/OTHER',text:'test',filePath:'/etc/passwd'}});
    assert.equal(result.ok,false);
  }
  assert.equal((await channel.outbound.sendText({target:'spaces/OTHER',text:'test'})).ok,false);
  assert.equal((await channel.outbound.sendText({target:space,text:'test',threadId:'spaces/OTHER/threads/t'})).ok,false);
  let read=false;
  assert.equal((await channel.outbound.sendMedia({target:space,mediaUrl:'/etc/passwd',mediaReadFile:async()=>{read=true;return Buffer.from('secret');}})).ok,false);
  assert.equal(read,false);
});

test('service-account startup, renewal discovery, pull and text reply need no user token file',async t=>{
  const tmp=makeTempDir('app-start');t.after(()=>tmp.cleanup());
  const key=path.join(tmp.dir,'key.json');writeFileSync(key,JSON.stringify({type:'service_account',client_email:'test@example.iam.gserviceaccount.com',private_key:'synthetic-not-a-key'}),{mode:0o600});
  const config={...baseConfig(),enabled:true,serviceAccountFile:key};
  const originalFetch=globalThis.fetch,originalAuth=GoogleAuth.prototype.getAccessToken;
  const calls=[],scopes=[];let service,delivered=false;
  t.after(async()=>{await service?.stop();globalThis.fetch=originalFetch;GoogleAuth.prototype.getAccessToken=originalAuth;});
  GoogleAuth.prototype.getAccessToken=async function(){scopes.push(this.scopes[0]);return 'token-'+this.scopes[0].split('/').pop();};
  globalThis.__GCHAT_BASE__='https://chat.example';globalThis.__PUBSUB_BASE__='https://pubsub.example';
  globalThis.fetch=async(url,init)=>{
    const u=String(url);calls.push({u,init});
    if(u.includes('workspaceevents.googleapis.com')) return Response.json({subscriptions:[{
      name:'subscriptions/app-sub',targetResource:`//chat.googleapis.com/${space}`,notificationEndpoint:{pubsubTopic:'projects/example-project/topics/topic'},
      eventTypes:[m.MESSAGE_EVENT],payloadOptions:{includeResource:true},state:'ACTIVE',expireTime:new Date(Date.now()+4*3600000).toISOString()
    }]});
    if(u.endsWith(':pull'))return Response.json({receivedMessages:delivered?[]:[event()]});
    if(u.endsWith(':acknowledge'))return Response.json({});
    if(u.endsWith('/messages')){delivered=true;return Response.json({name:`${space}/messages/bot-reply`,sender:{type:'BOT',name:'users/999'}});}
    throw Error('unexpected test endpoint');
  };
  const api=runtime(async p=>{assert.equal(p.ctx.CommandAuthorized,false);await p.dispatcherOptions.deliver({text:'safe reply'},{kind:'final'});});
  api.config={channels:{googlechatpubsub:config}};api.runtime.state={resolveStateDir:()=>tmp.dir};
  api.logger={info(){},error(){},warn(){},debug(){}};api.registerChannel=()=>{};api.registerService=s=>{service=s;};
  m.default(api);await service.start();await m.pollOnce();await m.__waitWorker();await service.stop();
  assert.equal(delivered,true);assert.deepEqual([...new Set(scopes)].sort(),Object.values(m.AUTH_SCOPES).sort());
  assert.equal(calls.find(c=>c.u.includes('workspaceevents')).init.headers.Authorization,'Bearer token-chat.bot');
  assert.equal(calls.find(c=>c.u.endsWith(':pull')).init.headers.Authorization,'Bearer token-pubsub');
  assert.equal(calls.find(c=>c.u.endsWith('/messages')).init.headers.Authorization,'Bearer token-chat.bot');
  assert.ok(calls.every(c=>!c.u.includes('oauth2.googleapis.com')));
});

test('cross-agent failure waits for other active runs before releasing ownership',async()=>{
  const config=baseConfig();config.bindings[0].agents.push({agentId:'a',mentionKeyword:'alpha'},{agentId:'b',mentionKeyword:'beta'});
  m.__configure({config});m.__resetDedup();assert.equal(m.admittedJobs(event({text:'alpha beta'})).length,3);
  let finishB,startedB;const started=new Promise(r=>{startedB=r;});
  m.__configure({config,api:runtime(async p=>{
    if(p.ctx.AccountId==='a')throw Error('first run failed');
    if(p.ctx.AccountId==='b'){startedB();await new Promise(r=>{finishB=r;});}
  })});
  let settled=false;
  const run=m.crossAgentDispatch({replyText:'alpha beta',sourceAgentId:'main',space,spaceDisplayName:space,senderName:'main',
    messageName:`${space}/messages/test`,threadName:'',replyInThread:false,threadSessionIsolation:false,chainKey:'test-cross-agent'})
    .then(()=>{settled=true;throw Error('should fail');},()=>{settled=true;});
  await started;await new Promise(r=>setImmediate(r));assert.equal(settled,false);
  finishB();await run;assert.equal(settled,true);
});

test('user OAuth refresh is serialized and atomically persists only the rotated refresh token',async t=>{
  const tmp=makeTempDir('oauth-refresh');t.after(()=>tmp.cleanup());
  const tokensFile=tmp.file('tokens.json',JSON.stringify({refresh_token:'SYNTHETIC_OLD',access_token:'STALE'}));
  const c={...baseConfig(),auth:{mode:'user-oauth'},oauth:{clientId:'synthetic-client',clientSecret:'SYNTHETIC_SECRET',tokensFile}};
  m.__configure({config:c});const original=globalThis.fetch;t.after(()=>{globalThis.fetch=original;});let count=0;
  globalThis.fetch=async(url,init)=>{count++;assert.equal(String(url),'https://oauth2.googleapis.com/token');
    assert.equal(new URLSearchParams(init.body).get('grant_type'),'refresh_token');
    await new Promise(r=>setTimeout(r,10));return Response.json({access_token:'SYNTHETIC_ACCESS',expires_in:3600,refresh_token:'SYNTHETIC_ROTATED'});
  };
  assert.deepEqual(await Promise.all([m.getOAuthToken(),m.getOAuthToken(),m.getOAuthToken()]),Array(3).fill('SYNTHETIC_ACCESS'));
  assert.equal(await m.getOAuthToken(),'SYNTHETIC_ACCESS');assert.equal(count,1);
  assert.deepEqual(JSON.parse(readFileSync(tokensFile,'utf8')),{refresh_token:'SYNTHETIC_ROTATED'});
  assert.equal(statSync(tokensFile).mode & 0o777,0o600);
});
