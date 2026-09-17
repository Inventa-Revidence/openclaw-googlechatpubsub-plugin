import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { loadModuleUnderTest, makeTempDir } from './helpers/harness.mjs';
const m = await loadModuleUnderTest();
const SPACE = 'spaces/TESTSPACE123';
const INCOMING_THREAD = `${SPACE}/threads/incoming`;
const OWN_THREAD = `${SPACE}/threads/bot-placeholder`;

function fixture(t, { threaded = false, isolated = threaded, typing = false, progress = false, media = false, failPatch = false, failAttachment = false } = {}) {
  const tmp = makeTempDir('reply-mode');
  const file = tmp.file('attachment.txt', 'synthetic attachment');
  const tokensFile = tmp.file('tokens.json', JSON.stringify({refresh_token:'SYNTHETIC_REFRESH'}));
  const config = {
    auth: {mode: media ? 'user-oauth' : 'service-account'},
    ...(media && {oauth:{clientId:'synthetic',clientSecret:'synthetic',tokensFile}}),
    allowFrom:['users/123'], silentReply:!typing,
    progress:{enabled:progress},
    media:{outboundEnabled:media,localRoots:[tmp.dir]},
    bindings:[{space:SPACE,replyInThread:threaded,threadSessionIsolation:isolated,agents:[{agentId:'main',alwaysListen:true}]}],
  };
  const originalFetch = globalThis.fetch;
  const requests = [], contexts = [];
  let sequence = 0, channel;
  globalThis.__GCHAT_BASE__ = 'https://chat-fixture.example';
  globalThis.fetch = async (url, init = {}) => {
    if (String(url) === 'https://oauth2.googleapis.com/token') return Response.json({access_token:'synthetic-oauth',expires_in:3600});
    assert.ok(String(url).startsWith(globalThis.__GCHAT_BASE__), 'unexpected network request');
    const request = {url:new URL(url),method:init.method || 'GET',body:typeof init.body === 'string' ? JSON.parse(init.body) : undefined};
    requests.push(request);
    if (request.url.pathname.includes('/attachments:upload')) return Response.json({attachmentDataRef:{attachmentUploadToken:'synthetic-upload'}});
    if (failPatch && request.method === 'PATCH') return Response.json({}, {status:500});
    if (failAttachment && request.body?.attachment) return Response.json({}, {status:500});
    return Response.json({name:request.method === 'PATCH' ? request.url.pathname.slice(4) : `${SPACE}/messages/bot-${++sequence}`,thread:{name:OWN_THREAD}});
  };
  let payloads = [];
  const api = {
    config:{channels:{googlechatpubsub:config}}, logger:{info(){},warn(){},error(){},debug(){}},
    registerChannel(c){channel=c;},registerService(){},
    runtime:{channel:{
      session:{recordSessionMetaFromInbound:async()=>{}},
      reply:{finalizeInboundContext:c=>c,dispatchReplyWithBufferedBlockDispatcher:async p=>{
        contexts.push(p.ctx);
        for (const payload of payloads) await p.dispatcherOptions.deliver(payload,{kind:'final'});
      }},
    }},
  };
  m.default(api);
  m.__configure({config,api,tokens:{token:async()=> 'synthetic-bot'},stopping:true});
  m.__resetDedup();
  t.after(()=>{globalThis.fetch=originalFetch;m.__configure({stopping:true});tmp.cleanup();});
  const input = {
    agentId:'main',space:SPACE,spaceDisplayName:SPACE,senderId:'users/123',senderName:'Test user',
    text:'Question',messageName:`${SPACE}/messages/user-message`,threadName:INCOMING_THREAD,
    replyInThread:threaded,threadSessionIsolation:isolated,
  };
  return {config,requests,contexts,file,channel,tmp,input,
    setPayloads(values){payloads=values;},
    run(values){payloads=values;return m.processMessageInPipeline(input);},
  };
}
function messagePosts(f) {return f.requests.filter(r=>r.method==='POST' && r.url.pathname.endsWith('/messages'));}
function assertSpacePosts(f) {
  const posts=messagePosts(f);assert.ok(posts.length);
  for(const r of posts) {
    assert.equal(r.body.thread,undefined);
    assert.equal(r.url.searchParams.has('messageReplyOption'),false);
    assert.equal(r.url.searchParams.has('threadKey'),false);
  }
}

test('unthreaded replies ignore incoming thread metadata and omit reply routing from context',async t=>{
  const f=fixture(t);
  await f.run([{text:'Answer',replyToId:INCOMING_THREAD}]);
  assertSpacePosts(f);assert.equal(messagePosts(f).length,1);
  assert.equal(f.contexts[0].ReplyToId,undefined);assert.equal(f.contexts[0].ReplyToIdFull,undefined);
  assert.ok(!f.contexts[0].SessionKey.includes(':thread:'));
});

test('threaded replies preserve the original thread and thread session',async t=>{
  const f=fixture(t,{threaded:true});await f.run([{text:'Answer'}]);
  const [request]=messagePosts(f);assert.equal(request.body.thread.name,INCOMING_THREAD);
  assert.equal(request.url.searchParams.get('messageReplyOption'),'REPLY_MESSAGE_OR_FAIL');
  assert.equal(f.contexts[0].ReplyToId,INCOMING_THREAD);assert.ok(f.contexts[0].SessionKey.includes(':thread:incoming'));
});

test('session isolation remains independent of space-level reply placement',async t=>{
  const f=fixture(t,{isolated:true});await f.run([{text:'Answer'}]);
  assertSpacePosts(f);assert.ok(f.contexts[0].SessionKey.includes(':thread:incoming'));
});

test('typing placeholder and later text chunks never reuse the bot-created thread',async t=>{
  const f=fixture(t,{typing:true});await f.run([{text:'x'.repeat(8500)}]);
  assertSpacePosts(f);assert.equal(messagePosts(f).length,3);
  assert.equal(messagePosts(f)[0].body.text,'_typing..._');
  const patches=f.requests.filter(r=>r.method==='PATCH');assert.equal(patches.length,1);
  assert.equal(patches[0].body.text.length,4000);
});

test('typing PATCH failure falls back to a space-level message',async t=>{
  const f=fixture(t,{typing:true,failPatch:true});await f.run([{text:'Answer'}]);
  assertSpacePosts(f);assert.equal(messagePosts(f).length,2);
  assert.equal(messagePosts(f)[1].body.text,'Answer');
});

test('pipeline attachments post in the space after retiring a typing placeholder',async t=>{
  const f=fixture(t,{typing:true,media:true});await f.run([{text:'Caption',mediaUrl:f.file}]);
  assertSpacePosts(f);assert.ok(f.requests.some(r=>r.method==='DELETE'));
  const attachment=messagePosts(f).find(r=>r.body.attachment);assert.ok(attachment);assert.equal(attachment.body.text,'Caption');
});

test('activity placeholder is retired before a space-level attachment reply',async t=>{
  const f=fixture(t,{progress:true,media:true});await f.run([{text:'Caption',mediaUrl:f.file}]);
  assertSpacePosts(f);assert.match(messagePosts(f)[0].body.text,/OpenClaw is working/);
  const deletion=f.requests.findIndex(r=>r.method==='DELETE');
  const attachment=f.requests.findIndex(r=>r.body?.attachment);
  assert.ok(deletion>0 && attachment>deletion);
  assert.equal(f.requests.filter(r=>r.method==='PATCH').length,0);
});

test('attachment failure keeps the caption fallback in the space',async t=>{
  const f=fixture(t,{media:true,failAttachment:true});
  await assert.rejects(()=>f.run([{text:'Caption',mediaUrl:f.file}]),/media deliveries failed/);
  assertSpacePosts(f);assert.equal(messagePosts(f).at(-1).body.text,'Caption');
  assert.equal(messagePosts(f).at(-1).body.attachment,undefined);
});

for(const threaded of [false,true]) {
  test(`message actions and outbound adapters respect replyInThread=${threaded}`,async t=>{
    const f=fixture(t,{threaded,media:true});
    assert.equal((await f.channel.handleAction({action:'send',params:{target:SPACE,message:'Action',threadId:INCOMING_THREAD}})).ok,true);
    assert.equal((await f.channel.outbound.sendText({target:SPACE,text:'Adapter',replyTo:INCOMING_THREAD})).ok,true);
    assert.equal((await f.channel.handleAction({action:'upload-file',params:{target:SPACE,path:f.file,replyTo:INCOMING_THREAD}})).ok,true);
    assert.equal((await f.channel.outbound.sendMedia({target:SPACE,mediaUrl:f.file,threadId:INCOMING_THREAD})).ok,true);
    assert.equal(messagePosts(f).length,4);
    if(threaded) for(const r of messagePosts(f)) {
      assert.equal(r.body.thread.name,INCOMING_THREAD);
      assert.equal(r.url.searchParams.get('messageReplyOption'),'REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD');
    } else assertSpacePosts(f);
  });
}

test('space-level mode still rejects thread context belonging to a different space',async t=>{
  const f=fixture(t,{media:true});const other='spaces/OTHER/threads/thread';
  assert.equal((await f.channel.handleAction({action:'send',params:{target:SPACE,message:'No',threadId:other}})).ok,false);
  assert.equal((await f.channel.outbound.sendText({target:SPACE,text:'No',replyTo:other})).ok,false);
  assert.equal((await f.channel.outbound.sendMedia({target:SPACE,mediaUrl:f.file,threadId:other})).ok,false);
  assert.equal(f.requests.length,0);
});

test('pending jobs adopt the current space-level reply and session settings',async t=>{
  const f=fixture(t);const inbox=new m.DurableInbox(path.join(f.tmp.dir,'inbox.json'));
  inbox.admit([{id:'old-job',payload:{...f.input,agent:{agentId:'main'},replyInThread:true,threadSessionIsolation:true}}]);
  f.setPayloads([{text:'Recovered answer'}]);m.__configure({inbox,stopping:false});
  m.startInboxWorker();await m.__waitWorker();
  assert.equal(inbox.snapshot()[0].state,'done');assertSpacePosts(f);
  assert.equal(f.contexts[0].ReplyToId,undefined);assert.ok(!f.contexts[0].SessionKey.includes(':thread:'));
});
