import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { loadModuleUnderTest } from './helpers/harness.mjs';
const m = await loadModuleUnderTest();
const SPACE = 'spaces/TESTSPACE123';
const THREAD = `${SPACE}/threads/incoming`;
const STATUS = `${SPACE}/messages/status`;
const drain = async () => { for (let i = 0; i < 30; i++) await Promise.resolve(); };
const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return {promise, resolve}; };
function clock(t) {
  t.mock.timers.enable({apis:['setTimeout','Date']});
  return async (ms = 5000) => { t.mock.timers.tick(ms); await drain(); };
}
function activity(t, options = {}) {
  const edits = [], errors = [];
  const progress = new m.ActivityProgress({intervalMs:5000,maxRecent:3,includeDetails:true,
    update:async text=>{edits.push(text);},onError:e=>errors.push(e),...options});
  t.after(()=>progress.stop());
  return {progress,edits,errors};
}

test('bursts coalesce, paired tool/command events count once and recent activities are bounded', async t=>{
  const tick=clock(t), {progress:p,edits}=activity(t);
  for(let i=0;i<8;i++) {
    assert.equal(p.onToolStart({toolCallId:`cmd-${i}`,name:'exec',phase:'start',args:{title:`Action ${i}`,command:'RAW_COMMAND'}}),false);
    p.onItemEvent({toolCallId:`cmd-${i}`,itemId:`tool:cmd-${i}`,kind:'tool',name:'exec',phase:'start',status:'running'});
    p.onItemEvent({toolCallId:`cmd-${i}`,itemId:`command:cmd-${i}`,kind:'command',name:'exec',phase:'end',status:'completed'});
  }
  for(let i=0;i<2;i++) p.onToolStart({toolCallId:`search-${i}`,name:'memory_search',phase:'start',args:{query:`Conference query ${i}`}});
  await tick(4999);assert.equal(edits.length,0);
  await tick(1);assert.equal(edits.length,1);
  assert.match(edits[0],/Commands ×8 · Memory Search ×2/);
  assert.match(edits[0],/✓ Commands — Action 7/);
  assert.match(edits[0],/Memory Search — Conference query 1/);
  assert.equal(edits[0].split('\n').length,5);assert.doesNotMatch(edits[0],/Action 6|RAW_COMMAND/);
  assert.equal(p.onToolStart({toolCallId:'search-1',name:'memory_search',phase:'update'}),true);
  await tick(100000);assert.equal(edits.length,1,'unchanged events and idle time do not send edits');
});

test('details are opt-out and item metadata, raw commands, results, reasoning and mentions stay out',async t=>{
  const tick=clock(t), {progress:p,edits}=activity(t);
  p.onToolStart({toolCallId:'a',name:'exec',args:{command:'COMMAND_SECRET',code:'CODE_SECRET'}});
  p.onItemEvent({toolCallId:'a',kind:'command',name:'exec',title:'ITEM_SECRET',meta:'META_SECRET',summary:'SUMMARY_SECRET',progressText:'OUTPUT_SECRET'});
  p.onItemEvent({itemId:'reasoning',kind:'reasoning',title:'REASONING_SECRET'});
  p.onToolStart({toolCallId:'b',name:'memory_search',args:{query:'Conference <users/all> password="SENSITIVE_VALUE" Bearer BEARER_SECRET https://host/private?key=URL_SECRET'}});
  p.onToolStart({toolCallId:'c',name:'exec',args:{title:'Inspect status'}});
  await tick();
  assert.doesNotMatch(edits.join(''),/COMMAND_SECRET|CODE_SECRET|ITEM_SECRET|META_SECRET|SUMMARY_SECRET|OUTPUT_SECRET|REASONING_SECRET|SENSITIVE_VALUE|BEARER_SECRET|URL_SECRET|<users\/all>/);
  assert.match(edits[0],/Conference/);assert.match(edits[0],/\[redacted\]/);
  const hidden=activity(t,{includeDetails:false});
  hidden.progress.onToolStart({toolCallId:'b',name:'memory_search',args:{query:'PRIVATE_QUERY',title:'PRIVATE_TITLE'}});
  await tick();assert.match(hidden.edits[0],/Memory Search ×1/);assert.doesNotMatch(hidden.edits[0],/PRIVATE/);
});

test('bounded titles and unknown tool groups keep the draft within the Chat text limit',async t=>{
  const tick=clock(t), {progress:p,edits}=activity(t,{maxRecent:8});
  for(let i=0;i<40;i++) p.onToolStart({toolCallId:`id-${i}`,name:`custom_tool_${i}`,args:{title:'long '.repeat(1000)}});
  await tick();assert.ok(edits[0].length<4000);assert.match(edits[0],/Other Tools ×32/);
});

test('completion/failure update status without increasing invocation counts',async t=>{
  const tick=clock(t), {progress:p,edits}=activity(t);
  p.onToolStart({itemId:'tool:one',name:'exec',phase:'start'});
  await tick();assert.match(edits[0],/… Commands/);
  p.onItemEvent({itemId:'command:one',kind:'command',name:'exec',phase:'end',status:'failed'});
  await tick();assert.match(edits[1],/Commands ×1\n! Commands/);
  p.onItemEvent({kind:'tool',name:'exec',phase:'start'});
  p.onToolStart({name:'exec',phase:'update'});
  await tick();assert.equal(edits.length,2,'uncorrelated updates cannot create duplicate counts');
  p.onItemEvent({toolCallId:'blocked',kind:'tool',name:'exec',phase:'end',status:'blocked'});
  await tick();assert.match(edits[2],/Commands ×2\n! Commands\n! Commands/);
});

test('stop waits for a pending edit and discards queued edits',async t=>{
  const tick=clock(t), pending=deferred(), edits=[];
  const {progress:p}=activity(t,{update:async text=>{edits.push(text);await pending.promise;}});
  p.onToolStart({toolCallId:'a',name:'exec'});await tick();
  p.onToolStart({toolCallId:'b',name:'memory_search'});await tick(30000);assert.equal(edits.length,1);
  let stopped=false;const stopping=p.stop().then(()=>{stopped=true;});await drain();assert.equal(stopped,false);
  pending.resolve();await stopping;await tick(30000);assert.equal(edits.length,1);
  assert.equal(p.onToolStart({toolCallId:'c',name:'exec'}),false);
});

test('updates arriving during an edit coalesce after the minimum interval',async t=>{
  const tick=clock(t), pending=deferred(), edits=[];
  const {progress:p}=activity(t,{update:async text=>{edits.push(text);if(edits.length===1) await pending.promise;}});
  p.onToolStart({toolCallId:'a',name:'exec'});await tick();
  p.onToolStart({toolCallId:'b',name:'memory_search'});await tick(30000);assert.equal(edits.length,1);
  pending.resolve();await drain();await tick(4999);assert.equal(edits.length,1);
  await tick(1);assert.equal(edits.length,2);assert.match(edits[1],/Memory Search ×1/);
});

test('abort cancels timers and edit failure stops further activity attempts',async t=>{
  const tick=clock(t), abort=new AbortController(), aborted=activity(t,{signal:abort.signal});
  aborted.progress.onToolStart({toolCallId:'a',name:'exec'});abort.abort();await tick();assert.equal(aborted.edits.length,0);
  let calls=0;const failed=activity(t,{update:async()=>{calls++;throw new Error('fixture error');}});
  failed.progress.onToolStart({toolCallId:'a',name:'exec'});await tick();
  failed.progress.onToolStart({toolCallId:'b',name:'exec'});await tick(100000);
  assert.equal(calls,1);assert.equal(failed.errors.length,1);
});

function pipeline(t, {progress={enabled:true}, threaded=false, typing=false, respond} = {}) {
  const originalFetch=globalThis.fetch, requests=[], config={
    projectId:'project',topicId:'topic',subscriptionId:'sub',auth:{mode:'service-account'},
    allowFrom:['users/123'],silentReply:!typing,progress,
    bindings:[{space:SPACE,replyInThread:threaded,agents:[{agentId:'main',alwaysListen:true}]}],
  };
  const abort=new AbortController();
  globalThis.__GCHAT_BASE__='https://chat-fixture.example';
  globalThis.fetch=async(url,init={})=>{
    assert.ok(String(url).startsWith(globalThis.__GCHAT_BASE__));
    const r={url:new URL(url),method:init.method,body:typeof init.body==='string'?JSON.parse(init.body):undefined};
    requests.push(r);
    const result=await respond?.(r,requests.length);
    return result || Response.json({name:r.method==='PATCH'?r.url.pathname.slice(4):STATUS,thread:{name:`${SPACE}/threads/status`}});
  };
  let dispatch;
  const api={config:{channels:{googlechatpubsub:config}},runtime:{channel:{
    session:{recordSessionMetaFromInbound:async()=>{}},reply:{finalizeInboundContext:c=>c,
      dispatchReplyWithBufferedBlockDispatcher:async p=>dispatch(p)},
  }}};
  m.__configure({config,api,abort,tokens:{token:async()=> 'synthetic-token'}});
  t.after(()=>{globalThis.fetch=originalFetch;m.__configure({abort:undefined});});
  return {requests,abort,config,run:fn=>{dispatch=fn;return m.processMessageInPipeline({agentId:'main',space:SPACE,spaceDisplayName:'Fixture',senderId:'users/123',senderName:'Test user',text:'Question',messageName:`${SPACE}/messages/input`,threadName:THREAD,replyInThread:threaded,threadSessionIsolation:threaded});}};
}

for(const threaded of [false,true]) test(`pipeline uses one ${threaded?'thread':'space'} status, SDK activity hooks and final edit`,async t=>{
  const tick=clock(t), f=pipeline(t,{threaded});
  await f.run(async({replyOptions:o,dispatcherOptions:d})=>{
    for(const flag of ['suppressDefaultToolProgressMessages','suppressToolProgressMessages','allowToolLifecycleWhenProgressHidden']) assert.equal(o[flag],true);
    assert.equal(o.commentaryPayloadsEnabled,false);assert.equal(o.reasoningPayloadsEnabled,false);
    assert.equal(o.onReasoningStream,undefined);
    o.onToolStart({toolCallId:'a',name:'exec',phase:'start',args:{title:'Inspect invite exports'}});
    o.onItemEvent({toolCallId:'a',kind:'tool',name:'exec',phase:'end',status:'completed'});
    await tick();assert.equal(f.requests.length,2);assert.match(f.requests[1].body.text,/✓ Commands — Inspect invite exports/);
    await d.deliver({text:'Final answer'},{kind:'final'});
    o.onToolStart({toolCallId:'b',name:'exec'});await tick(30000);
  });
  assert.deepEqual(f.requests.map(r=>r.method),['POST','PATCH','PATCH']);
  assert.equal(f.requests[0].body.thread?.name,threaded?THREAD:undefined);
  assert.equal(f.requests[0].url.searchParams.get('messageReplyOption'),threaded?'REPLY_MESSAGE_OR_FAIL':null);
  assert.equal(f.requests.at(-1).body.text,'Final answer');
  for(const r of f.requests.slice(1)) assert.equal(r.url.pathname,`/v1/${STATUS}`);
  assert.ok(m.processedMsgIds.has(STATUS),'placeholder self echoes are ignored');
});

test('final answer waits for a slow activity PATCH; no activity overwrites it',async t=>{
  const tick=clock(t), pending=deferred();
  const f=pipeline(t,{respond:async r=>{if(r.method==='PATCH' && r.body.text.includes('Commands ×')) await pending.promise;}});
  await f.run(async({replyOptions:o,dispatcherOptions:d})=>{
    o.onToolStart({toolCallId:'a',name:'exec'});await tick();assert.equal(f.requests.length,2);
    const final=d.deliver({text:'Final answer'},{kind:'final'});await drain();assert.equal(f.requests.length,2);
    pending.resolve();await final;await tick(30000);
  });
  assert.equal(f.requests.length,3);assert.equal(f.requests.at(-1).body.text,'Final answer');
});

test('failed activity PATCH is nonfatal and never creates replacement status posts',async t=>{
  const tick=clock(t), f=pipeline(t,{respond:r=>r.method==='PATCH' && r.body.text.includes('Commands ×') ? Response.json({}, {status:429}):undefined});
  await f.run(async({replyOptions:o,dispatcherOptions:d})=>{
    o.onToolStart({toolCallId:'a',name:'exec'});await tick();
    o.onToolStart({toolCallId:'b',name:'exec'});await tick(30000);
    await d.deliver({text:'Final answer'},{kind:'final'});
  });
  assert.deepEqual(f.requests.map(r=>r.method),['POST','PATCH','PATCH']);assert.equal(f.requests.at(-1).body.text,'Final answer');
});

test('abort during an edit waits for that edit before deleting the status',async t=>{
  const tick=clock(t), pending=deferred();
  const f=pipeline(t,{respond:async r=>{if(r.method==='PATCH') await pending.promise;}});
  let settled=false;
  const run=f.run(async({replyOptions:o})=>{
    o.onToolStart({toolCallId:'a',name:'exec'});await tick();f.abort.abort();
  }).then(()=>{settled=true;});
  await drain();assert.equal(settled,false);assert.deepEqual(f.requests.map(r=>r.method),['POST','PATCH']);
  pending.resolve();await run;await tick(30000);
  assert.deepEqual(f.requests.map(r=>r.method),['POST','PATCH','DELETE']);
});

test('failed initial status POST still permits normal final delivery without activity retries',async t=>{
  const tick=clock(t), f=pipeline(t,{respond:(r,n)=>n===1?Response.json({}, {status:503}):undefined});
  await f.run(async({replyOptions:o,dispatcherOptions:d})=>{
    assert.equal(o.onToolStart({toolCallId:'a',name:'exec'}),false);await tick();
    await d.deliver({text:'Final answer'},{kind:'final'});
  });
  assert.deepEqual(f.requests.map(r=>r.method),['POST','POST']);
});

test('deleted status falls back to a correctly routed final POST',async t=>{
  const f=pipeline(t,{respond:r=>r.method==='PATCH'?Response.json({}, {status:404}):undefined});
  await f.run(async({dispatcherOptions:d})=>d.deliver({text:'Final answer'},{kind:'final'}));
  assert.deepEqual(f.requests.map(r=>r.method),['POST','PATCH','POST','DELETE']);
  assert.equal(f.requests[2].body.text,'Final answer');assert.equal(f.requests[2].body.thread,undefined);
});

for(const outcome of ['silent','throw','reported-error','abort']) test(`pipeline ${outcome} cleans status and pending timers`,async t=>{
  const tick=clock(t), f=pipeline(t);
  const run=f.run(async({replyOptions:o,dispatcherOptions:d})=>{
    o.onToolStart({toolCallId:'a',name:'exec'});
    if(outcome==='throw') throw new Error('synthetic failure');
    if(outcome==='reported-error') d.onError(new Error('synthetic failure'),{kind:'final'});
    if(outcome==='abort') {f.abort.abort();await tick();}
  });
  if(outcome==='throw'||outcome==='reported-error') await assert.rejects(run); else await run;
  await tick(30000);assert.deepEqual(f.requests.map(r=>r.method),['POST','DELETE']);
});

test('disabled progress preserves silent replies',async t=>{
  const f=pipeline(t,{progress:{enabled:false}});
  await f.run(async({replyOptions:o,dispatcherOptions:d})=>{assert.equal(o.onToolStart,undefined);await d.deliver({text:'Final'},{kind:'final'});});
  assert.deepEqual(f.requests.map(r=>r.method),['POST']);assert.equal(f.requests[0].body.text,'Final');
});

test('progress settings validate and match in both schemas',()=>{
  const manifest=JSON.parse(readFileSync(new URL('../openclaw.plugin.json',import.meta.url)));
  assert.deepEqual(manifest.configSchema,manifest.channelConfigs.googlechatpubsub.schema);
  const base={projectId:'project',topicId:'topic',subscriptionId:'sub',allowFrom:['users/123'],bindings:[{space:SPACE,agents:[{agentId:'main'}]}]};
  for(const progress of [{enabled:'yes'},{includeDetails:1},{updateIntervalSeconds:1},{updateIntervalSeconds:31},{maxRecentActivities:0},{maxRecentActivities:9}]) {
    m.__configure({config:{...base,progress}});assert.throws(()=>m.validateConfig());
  }
  m.__configure({config:{...base,progress:{enabled:true,includeDetails:true,updateIntervalSeconds:5,maxRecentActivities:3}}});
  assert.doesNotThrow(()=>m.validateConfig());
});
