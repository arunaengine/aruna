// Registry admin page. The token stays in sessionStorage; all realm text is set as text.
'use strict';

const TOKEN_KEY = 'aruna-registry-admin-token';
const THEME_KEY = 'aruna-registry-theme';
const $ = (id) => document.getElementById(id);
let realms = [];

function applyTheme(theme) {
  document.documentElement.dataset.theme = theme;
}

function initialTheme() {
  const stored = localStorage.getItem(THEME_KEY);
  if (stored === 'light' || stored === 'dark') return stored;
  return matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (key === 'class') node.className = value;
    else if (key.startsWith('on')) node.addEventListener(key.slice(2), value);
    else if (value !== false && value !== undefined) node.setAttribute(key, value === true ? '' : value);
  }
  for (const child of children.flat()) {
    if (child === null || child === undefined) continue;
    node.append(typeof child === 'string' ? document.createTextNode(child) : child);
  }
  return node;
}

function showMessage(text, info = false) {
  const box = $('message');
  box.textContent = text;
  box.className = info ? 'message info' : 'message';
  box.hidden = !text;
}

async function api(path, options = {}) {
  const token = sessionStorage.getItem(TOKEN_KEY);
  const headers = { Authorization: `Bearer ${token}` };
  if (options.body) headers['Content-Type'] = 'application/json';
  const response = await fetch(path, { ...options, headers, cache: 'no-store' });
  if (response.status === 401) {
    signOut('The admin token was not accepted.');
    throw new Error('unauthorized');
  }
  if (!response.ok) throw new Error((await response.text()) || `HTTP ${response.status}`);
  return response.json();
}

function signOut(text = '') {
  sessionStorage.removeItem(TOKEN_KEY);
  realms = [];
  $('list').replaceChildren();
  $('realms').hidden = true;
  $('signout').hidden = true;
  $('who').textContent = '';
  $('signin').hidden = false;
  showMessage(text);
}

async function signIn() {
  try {
    const admin = await api('/v1/admin/whoami');
    $('who').textContent = `Signed in with ${admin.subject}`;
    $('signin').hidden = true;
    $('signout').hidden = false;
    $('realms').hidden = false;
    showMessage('');
    await load();
  } catch (error) {
    if (error.message !== 'unauthorized') signOut(`Sign-in failed: ${error.message}`);
  }
}

async function load() {
  try {
    realms = await api('/v1/admin/realms');
    render();
  } catch (error) {
    if (error.message !== 'unauthorized') showMessage(`Loading realms failed: ${error.message}`);
  }
}

function time(seconds) {
  return new Date(seconds * 1000).toLocaleString();
}

function statusOrder(realm) {
  return { approved: 0, listed: 1, hidden: 2, retired: 3 }[realm.admin.status] ?? 4;
}

function render() {
  const showAll = $('show-all').checked;
  const shown = realms
    .filter((realm) => showAll || (realm.leased && ['listed', 'approved'].includes(realm.admin.status)))
    .sort((a, b) => statusOrder(a) - statusOrder(b) || b.received_at - a.received_at);
  const list = $('list');
  if (shown.length === 0) {
    list.replaceChildren(el('p', { class: 'muted' }, 'No realms to show.'));
    return;
  }
  list.replaceChildren(...shown.map(realmCard));
}

async function save(realm, change, confirmText) {
  if (confirmText && !confirm(confirmText)) return;
  const next = { ...realm.admin, ...change };
  try {
    realm.admin = await api(`/v1/admin/realms/${encodeURIComponent(realm.realm_id)}`, {
      method: 'PUT',
      body: JSON.stringify(next),
    });
    showMessage('Saved.', true);
    render();
  } catch (error) {
    if (error.message !== 'unauthorized') showMessage(`Change refused: ${error.message}`);
  }
}

function statusActions(realm) {
  const status = realm.admin.status;
  if (status === 'retired') return [el('span', { class: 'muted' }, 'Retired realms keep their status.')];
  const name = realm.names.at(-1)?.name ?? realm.realm_id;
  const actions = [];
  if (status === 'approved') actions.push(el('button', { type: 'button', onclick: () => save(realm, { status: 'listed' }) }, 'Unapprove'));
  else actions.push(el('button', { type: 'button', class: 'primary', onclick: () => save(realm, { status: 'approved' }) }, 'Approve'));
  if (status === 'hidden') actions.push(el('button', { type: 'button', onclick: () => save(realm, { status: 'listed' }) }, 'Unhide'));
  else actions.push(el('button', { type: 'button', onclick: () => save(realm, { status: 'hidden' }) }, 'Hide'));
  actions.push(el('button', {
    type: 'button',
    class: 'danger',
    onclick: () => save(realm, { status: 'retired' }, `Retire "${name}"? This cannot be undone.`),
  }, 'Retire'));
  return actions;
}

function editForm(realm) {
  const retired = realm.admin.status === 'retired';
  const namespace = el('input', { type: 'text', value: realm.admin.namespace ?? '', placeholder: 'none', disabled: retired });
  const note = el('textarea', {}, realm.admin.note);
  const form = el('form', { class: 'edit', onsubmit: (event) => {
    event.preventDefault();
    save(realm, { namespace: namespace.value.trim() || null, note: note.value });
  } },
  el('div', {}, el('label', {}, 'w3id namespace (aruna/…)'), namespace),
  el('div', {}, el('label', {}, 'Admin note'), note),
  el('button', { type: 'submit' }, 'Save'));
  return form;
}

function auditDetails(realm) {
  const body = el('div', {}, el('p', { class: 'muted' }, 'Loading…'));
  const details = el('details', { ontoggle: async () => {
    if (!details.open) return;
    try {
      const records = await api(`/v1/admin/realms/${encodeURIComponent(realm.realm_id)}/audit`);
      if (records.length === 0) {
        body.replaceChildren(el('p', { class: 'muted' }, 'No admin changes yet.'));
        return;
      }
      body.replaceChildren(el('table', {},
        el('tr', {}, el('th', {}, 'When'), el('th', {}, 'Who'), el('th', {}, 'Change')),
        records.slice().reverse().map((record) => el('tr', {},
          el('td', {}, time(record.at)),
          el('td', {}, record.actor),
          el('td', {}, describeChange(record.before, record.after))))));
    } catch (error) {
      body.replaceChildren(el('p', { class: 'muted' }, `Audit unavailable: ${error.message}`));
    }
  } }, el('summary', {}, 'Audit trail'), body);
  return details;
}

function describeChange(before, after) {
  const parts = [];
  if (before.status !== after.status) parts.push(`status ${before.status} → ${after.status}`);
  if ((before.namespace ?? '') !== (after.namespace ?? '')) parts.push(`namespace ${before.namespace ?? 'none'} → ${after.namespace ?? 'none'}`);
  if (before.note !== after.note) parts.push('note changed');
  return parts.join(', ') || 'no change';
}

// Realm URLs are self-described, so only http(s) targets become links.
function link(url) {
  if (!/^https?:\/\//.test(url)) return url;
  return el('a', { href: url, rel: 'noopener noreferrer', target: '_blank' }, url);
}

function realmCard(realm) {
  const descriptor = realm.registration.payload.descriptor.payload;
  const kpis = realm.registration.payload.kpis;
  const previous = realm.names
    .slice(0, -1)
    .map((record, index) => ({ name: record.name, until: realm.names[index + 1].since }))
    .reverse();
  const verification = realm.verified
    ? el('span', { class: 'badge ok' }, 'verified')
    : el('span', { class: 'badge bad' }, 'unverified');
  const count = (value) => (value === null || value === undefined ? 'unknown' : String(value));
  return el('article', { class: 'card' },
    el('div', { class: 'realm-head' },
      el('h3', {}, descriptor.name),
      el('span', { class: `badge ${realm.admin.status}` }, realm.admin.status),
      verification,
      realm.leased ? null : el('span', { class: 'badge warn' }, 'lease expired'),
      realm.stale ? el('span', { class: 'badge warn' }, 'stale KPIs') : null),
    el('dl', { class: 'facts' },
      el('dt', {}, 'Realm id'), el('dd', { class: 'mono' }, realm.realm_id),
      previous.length ? [el('dt', {}, 'Previous names'),
        el('dd', {}, previous.map((record) => `${record.name} (until ${time(record.until)})`).join(', '))] : null,
      descriptor.description ? [el('dt', {}, 'Description'), el('dd', {}, descriptor.description)] : null,
      el('dt', {}, 'API'), el('dd', {}, link(descriptor.api_url)),
      el('dt', {}, 'Portal'), el('dd', {}, link(descriptor.portal_url)),
      descriptor.contact ? [el('dt', {}, 'Contact'), el('dd', {}, descriptor.contact)] : null,
      descriptor.login_providers?.length ? [el('dt', {}, 'Login providers'),
        el('dd', {}, descriptor.login_providers.map((provider) => `${provider.id} (${provider.issuer})`).join(', '))] : null,
      el('dt', {}, 'Last registration'), el('dd', {}, time(realm.received_at)),
      el('dt', {}, 'Datasets / groups / nodes'),
      el('dd', {}, `${count(kpis.live_datasets)} / ${count(kpis.groups)} / ${count(kpis.nodes_configured)}`),
      realm.problems.length ? [el('dt', {}, 'Verification'),
        el('dd', {}, el('ul', { class: 'problems' }, realm.problems.map((problem) => el('li', {}, problem))))] : null),
    el('div', { class: 'actions' }, statusActions(realm)),
    editForm(realm),
    auditDetails(realm),
    el('details', {}, el('summary', {}, 'Signed self-description'),
      el('pre', {}, JSON.stringify(realm.registration, null, 2))));
}

document.addEventListener('DOMContentLoaded', () => {
  applyTheme(initialTheme());
  $('theme').addEventListener('click', () => {
    const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
    localStorage.setItem(THEME_KEY, next);
    applyTheme(next);
  });
  $('signin-form').addEventListener('submit', (event) => {
    event.preventDefault();
    sessionStorage.setItem(TOKEN_KEY, $('token').value.trim());
    $('token').value = '';
    signIn();
  });
  $('signout').addEventListener('click', () => signOut());
  $('reload').addEventListener('click', load);
  $('show-all').addEventListener('change', render);
  if (sessionStorage.getItem(TOKEN_KEY)) signIn();
  else signOut();
});
