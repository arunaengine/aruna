// Tests the actual ARCitect Electron UI with native Aruna Git and LFS.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import {spawn, execFileSync} from 'node:child_process';
import {pathToFileURL, fileURLToPath} from 'node:url';

const root = process.argv[2];
const workspace = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const arcitect = path.resolve(process.env.ARUNA_ARCITECT);
const {_electron} = await import(pathToFileURL(path.join(arcitect, 'node_modules/playwright/index.mjs')));
const globalConfig = path.join(root, 'arcitect.gitconfig');
const env = {...process.env, GIT_CONFIG_GLOBAL: globalConfig};
env.XDG_CONFIG_HOME = path.join(root, 'arcitect-config');
const ports = path.join(root, 'arcitect-ports.cjs');
await fs.writeFile(ports, "const net=require('node:net'); const listen=net.Server.prototype.listen; net.Server.prototype.listen=function(...args){if(args[0]===7890||args[0]===7891)args[0]=0;return listen.apply(this,args);};\n");
env.NODE_OPTIONS = `--require=${ports}`;
const stamp = new Date().toISOString();
env.GIT_AUTHOR_DATE = stamp;
env.GIT_COMMITTER_DATE = stamp;
delete env.GIT_ASKPASS;
delete env.ELECTRON_RUN_AS_NODE;
const config = (key, value) => execFileSync('git', ['config', '--file', globalConfig, key, value]);
for (const key of ['user.name', 'user.email', 'user.signingkey', 'commit.gpgsign', 'core.hooksPath']) {
  config(key, execFileSync('git', ['config', '--get', key], {cwd: workspace}).toString().trim());
}
const helper = path.join(root, 'arcitect-credential');
await fs.writeFile(helper, '#!/bin/sh\nif [ "$1" = get ]; then printf "username=aruna\\npassword=%s\\n\\n" "$ARUNA_TOKEN"; fi\n', {mode: 0o700});
config('credential.helper', `!${helper}`);
execFileSync('git', ['lfs', 'install', '--file', globalConfig, '--skip-repo'], {env});
const resources = path.join(arcitect, 'node_modules/electron/dist/resources');
for (const file of ['ARCitect.json', 'DataHubs.json']) {
  await fs.copyFile(path.join(arcitect, 'resources', file), path.join(resources, file));
}
const display = spawn(process.env.ARUNA_XVFB || path.join(workspace, '.claude/tools/usr/bin/Xvfb'),
  ['-displayfd', '3', '-screen', '0', '1280x900x24', '-nolisten', 'tcp'], {stdio: ['ignore', 'ignore', 'pipe', 'pipe']});
const displayNumber = await new Promise((resolve, reject) => {
  display.once('error', reject);
  display.once('exit', code => reject(new Error(`Xvfb exited: ${code}`)));
  display.stdio[3].once('data', data => resolve(data.toString().trim()));
});
env.DISPLAY = `:${displayNumber}`;
let app;
try {
  app = await _electron.launch({
    executablePath: path.join(arcitect, 'node_modules/electron/dist/electron'),
    args: [arcitect, `--user-data-dir=${path.join(root, 'arcitect-profile')}`],
    cwd: arcitect, env, timeout: 120000,
  });
  const page = await app.firstWindow();
  assert.ok((await app.evaluate(({app}) => app.getPath('userData'))).startsWith(root));
  page.setDefaultTimeout(120000);
  await page.waitForFunction(() => Boolean(window.ipc));
  const git = async (cwd, args) => {
    const result = await page.evaluate(({cwd, args}) => window.ipc.invoke('GitService.run', {cwd, args, silent: true}), {cwd, args});
    assert.equal(result[0], true, `ARCitect Git command failed: ${args[0]}`);
    return result[1].trim();
  };
  const clone = path.join(root, 'arcitect-clone');
  await git(root, ['clone', process.env.ARUNA_GIT_URL, clone]);
  await git(clone, ['lfs', 'pull']);
  console.log('PASS: actual ARCitect GitService cloned and fetched LFS from native Aruna');

  const hooks = path.join(clone, '.git/hooks');
  const originalHooks = execFileSync('git', ['config', '--get', 'core.hooksPath'], {cwd: workspace}).toString().trim();
  for (const file of await fs.readdir(originalHooks)) {
    if (file === 'pre-push') continue;
    await fs.symlink(path.join(originalHooks, file), path.join(hooks, file));
  }
  const prePush = `#!${process.env.ARUNA_ARC_PYTHON}\nimport subprocess,sys\ndata=sys.stdin.buffer.read()\nresult=subprocess.run([${JSON.stringify(path.join(originalHooks, 'pre-push'))},*sys.argv[1:]],input=data)\nif result.returncode: sys.exit(result.returncode)\nsys.exit(subprocess.run(['git','lfs','pre-push',*sys.argv[1:]],input=data).returncode)\n`;
  await fs.writeFile(path.join(hooks, 'pre-push'), prePush, {mode: 0o700});
  await git(clone, ['config', 'core.hooksPath', hooks]);
  await app.evaluate(({dialog}, directory) => {
    dialog.showOpenDialog = async () => ({canceled: false, filePaths: [directory]});
  }, clone);
  await page.getByRole('button', {name: 'Open ARC', exact: true}).click();
  await page.waitForFunction(() => [...document.querySelectorAll('.q-item')].some(item =>
    item.textContent.trim().includes('Commit') && !item.getAttribute('style')?.includes('opacity')));
  const payload = path.join(clone, 'assays/assay/dataset/measurements.bin');
  const content = Buffer.from('ARCitect native Aruna payload\n'.repeat(8192));
  await fs.writeFile(payload, content);
  const previous = await git(clone, ['rev-parse', 'HEAD']);
  await page.getByText('Commit', {exact: true}).first().click();
  const email = page.getByRole('textbox', {name: 'eMail', exact: true});
  await email.waitFor();
  await page.waitForFunction(input => Boolean(input.value), await email.elementHandle());
  const message = page.getByRole('textbox', {name: 'Commit Message', exact: true});
  await message.fill('test: synchronize ARCitect with Aruna');
  await message.press('Tab');
  assert.equal(await message.inputValue(), 'test: synchronize ARCitect with Aruna');
  await page.getByRole('button', {name: 'Commit', exact: true}).last().click();
  await page.getByRole('button', {name: /^ok$/i}).last().click();
  const current = await git(clone, ['rev-parse', 'HEAD']);
  assert.notEqual(current, previous, 'ARCitect did not create the requested commit');
  assert.equal(await git(clone, ['log', '-1', '--format=%G?']), 'G');
  await page.getByText('DataHUB Sync', {exact: true}).first().click();
  await page.getByText('origin', {exact: true}).last().click();
  await page.getByRole('button', {name: 'Push', exact: true}).click();
  await page.getByRole('button', {name: /^ok$/i}).last().click();
  assert.ok((await git(clone, ['ls-remote', 'origin', 'refs/heads/main'])).startsWith(current));
  const restored = path.join(root, 'arcitect-restored');
  await git(root, ['clone', process.env.ARUNA_GIT_URL, restored]);
  await git(restored, ['lfs', 'pull']);
  assert.deepEqual(await fs.readFile(path.join(restored, 'assays/assay/dataset/measurements.bin')), content);
  await page.screenshot({path: path.join(workspace, '.claude/arcitect-native.png')});
  console.log('PASS: patched ARCitect UI opened the ARC, made a signed commit and pushed with native LFS');
} catch (error) {
  if (app) {
    await (await app.firstWindow()).screenshot({path: path.join(workspace, '.claude/arcitect-failure.png')}).catch(() => {});
  }
  throw error;
} finally {
  if (app) await app.close();
  display.kill();
}
