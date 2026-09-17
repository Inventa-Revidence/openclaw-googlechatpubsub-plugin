import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync, mkdtempSync, mkdirSync, rmSync } from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
const parent=path.resolve('.test-tmp');mkdirSync(parent,{recursive:true});
const temp=mkdtempSync(path.join(parent,'pack-'));
try {
  const [pack]=JSON.parse(execFileSync('npm',['pack','--ignore-scripts','--json','--pack-destination',temp],{encoding:'utf8'}));
  assert.deepEqual(pack.files.map(f=>f.path).sort(),['LICENSE','README.md','dist/index.js','openclaw.plugin.json','package.json']);
  execFileSync('tar',['-xzf',path.join(temp,pack.filename),'-C',temp]);
  const root=path.join(temp,'package');
  assert.deepEqual(readFileSync(path.join(root,'dist/index.js')),readFileSync('dist/index.js'));
  const manifest=JSON.parse(readFileSync(path.join(root,'openclaw.plugin.json'),'utf8'));
  assert.deepEqual(manifest.configSchema,manifest.channelConfigs.googlechatpubsub.schema);
  const {default:register}=await import(pathToFileURL(path.join(root,'dist/index.js')).href);
  let channel,service;
  register({config:{},logger:{info(){},warn(){},error(){},debug(){}},registerChannel(c){channel=c;},registerService(s){service=s;}});
  assert.equal(channel.id,'googlechatpubsub');assert.equal(service.id,'googlechatpubsub-listener');
  await service.start();await service.stop();
  console.log('Packaged module: file allowlist, runtime bytes, mirrored schemas, SDK import and disabled service registration passed.');
} finally {rmSync(temp,{recursive:true,force:true});}
