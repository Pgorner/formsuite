/* persistence.js — single-source-of-truth for active DOCX
   --------------------------------------------------------
   Responsibilities
   • Keep ONE canonical “active doc” signal across tabs (localStorage + BroadcastChannel)
   • Store FileSystemFileHandle in IndexedDB
   • Maintain a working copy of bytes in OPFS (Origin Private File System)
   • Persist per-doc state (schema/values/tagMap) in localStorage
   • Provide safe, atomic helpers (locks + coalescers) used by pages
*/

(() => {
  // ===== Keys & channels (canonical + legacy for BWC) =====
  const LS_KEYS = {
    CANON_ACTIVE: 'FS_ACTIVE_DOC_META', // { docId, name }  <- canonical
    LEGACY_ACTIVE: 'FS_CURRENT_DOC_META', // legacy mirror
    INTERNAL_ACTIVE: 'FS_active_doc_v1',  // older internal
    STATE_PREFIX: 'FS_state_',            // per docId -> { schema, values, ... }
  };
  const ACTIVE_KEYS_READ_ORDER = [
    LS_KEYS.CANON_ACTIVE,
    LS_KEYS.LEGACY_ACTIVE,
    LS_KEYS.INTERNAL_ACTIVE,
  ];

  const BC = {
    CANON: 'fs-active-doc',  // {active:set|active:updated|active:clear}
    LEGACY: 'form-suite-doc' // {doc-switched|doc-updated|doc-cleared}
  };

  const DB_NAME = 'formsuite_v1';
  const DB_STORE_HANDLES = 'handles';

  // ===== Utils =====
  const uuid = () =>
    (crypto?.randomUUID?.() || ('doc-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 8)));

  const readJSON = (k) => { try { return JSON.parse(localStorage.getItem(k) || 'null'); } catch { return null; } };
  const writeJSON = (k, v) => { try { (v == null) ? localStorage.removeItem(k) : localStorage.setItem(k, JSON.stringify(v)); } catch {} };

  // ===== IndexedDB for FileSystemFileHandle =====
  function openDB() {
    return new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, 1);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains(DB_STORE_HANDLES)) {
          db.createObjectStore(DB_STORE_HANDLES, { keyPath: 'docId' });
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  async function idbPutHandle(docId, handle) {
    try {
      const db = await openDB();
      await new Promise((res, rej) => {
        const tx = db.transaction(DB_STORE_HANDLES, 'readwrite');
        tx.objectStore(DB_STORE_HANDLES).put({ docId, handle });
        tx.oncomplete = res; tx.onerror = () => rej(tx.error);
      });
    } catch {}
  }
  async function idbGetHandle(docId) {
    try {
      const db = await openDB();
      return await new Promise((res, rej) => {
        const tx = db.transaction(DB_STORE_HANDLES, 'readonly');
        const req = tx.objectStore(DB_STORE_HANDLES).get(docId);
        req.onsuccess = () => res(req.result ? req.result.handle : null);
        req.onerror = () => rej(req.error);
      });
    } catch { return null; }
  }
  async function idbEachHandle(cb) {
    try {
      const db = await openDB();
      await new Promise((res, rej) => {
        const tx = db.transaction(DB_STORE_HANDLES, 'readonly');
        const store = tx.objectStore(DB_STORE_HANDLES);
        const req = store.openCursor();
        req.onsuccess = async () => {
          const cursor = req.result;
          if (cursor) { try { await cb(cursor.value.docId, cursor.value.handle); } catch {} cursor.continue(); }
          else res();
        };
        req.onerror = () => rej(req.error);
      });
    } catch {}
  }

  // ===== OPFS working copy =====
  async function opfsRoot() {
    try { return await navigator.storage.getDirectory(); } catch { return null; }
  }
  async function opfsPut(docId, bytes) {
    try {
      const root = await opfsRoot(); if (!root) return false;
      const fh = await root.getFileHandle(`${docId}.docx`, { create: true });
      const w = await fh.createWritable();
      await w.write(bytes);
      await w.close();
      return true;
    } catch { return false; }
  }
  async function opfsGet(docId) {
    try {
      const root = await opfsRoot(); if (!root) return null;
      const fh = await root.getFileHandle(`${docId}.docx`, { create: false });
      const f = await fh.getFile();
      return await f.arrayBuffer();
    } catch { return null; }
  }

  // ===== State (schema/values/etc.) =====
  async function saveState(docId, patch) {
    if (!docId) return null;
    const key = LS_KEYS.STATE_PREFIX + docId;
    const cur = readJSON(key) || {};
    const next = { ...cur, ...(patch || {}) };
    writeJSON(key, next);
    return next;
  }
  async function loadState(docId) {
    if (!docId) return null;
    return readJSON(LS_KEYS.STATE_PREFIX + docId) || null;
  }
  function getLastKnownValues(docId) {
    try { return (readJSON(LS_KEYS.STATE_PREFIX + docId) || {}).values || {}; }
    catch { return {}; }
  }

  // ===== Permissions =====
  async function ensurePermission(handle, mode = 'readwrite') {
    if (!handle) return 'denied';
    try {
      let q = await handle.queryPermission?.({ mode });
      if (q === 'granted') return 'granted';
      if (q === 'prompt') {
        q = await handle.requestPermission?.({ mode }) || q;
      }
      return q || 'denied';
    } catch { return 'denied'; }
  }

  async function regrantAll() {
    let ok = 0, fail = 0;
    await idbEachHandle(async (_docId, h) => {
      const p = await ensurePermission(h, 'readwrite');
      if (p === 'granted') ok++; else fail++;
    });
    return { ok, fail };
  }

  // ===== Active doc single-source-of-truth (BC + LS) =====
  const bcLegacy = ('BroadcastChannel' in window) ? new BroadcastChannel(BC.LEGACY) : null;
  const bcCanon  = ('BroadcastChannel' in window) ? new BroadcastChannel(BC.CANON)  : null;

  let _active = null; // { docId, name }

  // Small debug helper
  function dbg(where, data) {
    try {
      console.log('%c[Persist]', 'color:#2563eb;font-weight:600', where, data || '');
    } catch {}
  }

  function readActiveFromStorage() {
    for (const k of ACTIVE_KEYS_READ_ORDER) {
      const v = readJSON(k);
      if (v?.docId) return v;
    }
    return null;
  }

  function writeActiveToStorage(metaOrNull) {
    // Always write canonical + legacy mirrors
    writeJSON(LS_KEYS.CANON_ACTIVE,  metaOrNull);
    writeJSON(LS_KEYS.LEGACY_ACTIVE, metaOrNull);
    writeJSON(LS_KEYS.INTERNAL_ACTIVE, metaOrNull);
  }

  function getActiveDocMeta() {
    if (_active?.docId) return _active;
    const ls = readActiveFromStorage();
    if (ls?.docId) _active = ls;
    return _active;
  }

  async function setActiveDoc(metaOrNull) {
    if (!metaOrNull || !metaOrNull.docId) {
      _active = null;
      writeActiveToStorage(null);
      try { bcCanon?.postMessage({ type: 'active:clear', ts: Date.now() }); } catch {}
      try { bcLegacy?.postMessage({ type: 'doc-cleared', ts: Date.now() }); } catch {}
      return null;
    }
    const next = { docId: metaOrNull.docId, name: metaOrNull.name || 'document' };
    _active = next;
    writeActiveToStorage(next);
    // Broadcast on both channels
    const payload = { docId: next.docId, name: next.name, ts: Date.now() };
    try { bcCanon?.postMessage({ type: 'active:set', ...payload }); } catch {}
    try { bcLegacy?.postMessage({ type: 'doc-switched', ...payload }); } catch {}
    return next;
  }

  function broadcastDocUpdated(docId, name) {
    const payload = { docId, name, ts: Date.now() };
    try { bcCanon?.postMessage({ type: 'active:updated', ...payload }); } catch {}
    try { bcLegacy?.postMessage({ type: 'doc-updated',  ...payload }); } catch {}
  }

  // Keep in-memory cache aligned if other tabs write canonical or legacy keys
  window.addEventListener('storage', (e) => {
    if (!e.key) return;
    if (ACTIVE_KEYS_READ_ORDER.includes(e.key)) {
      try { _active = e.newValue ? JSON.parse(e.newValue) : null; } catch { _active = null; }
    }
  });

  // ===== Concurrency helpers =====
  const __locks = new Map(); // docId -> Promise chain
  async function withDocLock(docId, fn) {
    if (!docId) return fn();
    const prev = __locks.get(docId) || Promise.resolve();
    let release;
    const gate = new Promise(r => (release = r));
    __locks.set(docId, prev.then(() => gate));
    try {
      return await fn();
    } finally {
      release();
      if (__locks.get(docId) === gate) __locks.delete(docId);
    }
  }

  const __coals = new Map(); // key -> timeout
  function coalesce(key, fn, delay = 120) {
    clearTimeout(__coals.get(key));
    const t = setTimeout(() => { __coals.delete(key); try { fn(); } catch {} }, delay);
    __coals.set(key, t);
  }

  // ===== Public bytes I/O (with safe fallbacks) =====
  async function getBytes(docId) {
    if (!docId) return null;
    // 1) OPFS working copy
    const opfs = await opfsGet(docId);
    if (opfs) return opfs;
    // 2) Fallback: read directly from handle, then refresh OPFS
    const h = await idbGetHandle(docId);
    if (h?.getFile) {
      try {
        const f = await h.getFile();
        const ab = await f.arrayBuffer();
        try { await opfsPut(docId, new Uint8Array(ab)); } catch {}
        return ab;
      } catch { /* ignore */ }
    }
    return null;
  }

  async function getCurrentDocBytes() {
    const meta = getActiveDocMeta();
    if (!meta?.docId) return null;
    return await getBytes(meta.docId);
  }

  async function putBytes(docId, bytes, { broadcast = true } = {}) {
    if (!docId) return false;
    const ok = await opfsPut(docId, bytes);
    if (ok && broadcast) {
      const meta = getActiveDocMeta();
      broadcastDocUpdated(docId, meta?.name);
    }
    return ok;
  }

  // ===== High-level “set current” APIs =====
  // Keep the SAME docId if one already exists; otherwise mint a new one.
  async function setCurrentDoc({ bytes, handle, name }) {
    const prev = readActiveFromStorage();
    const docId = prev?.docId || uuid();
    const meta = { docId, name: (name || prev?.name || 'document').replace(/\.docx$/i,'') };

    // Persist handle & bytes (best-effort)
    if (handle) await idbPutHandle(docId, handle);
    if (bytes)  await opfsPut(docId, bytes);

    // Ensure there is at least an empty state container
    if (!await loadState(docId)) await saveState(docId, { schema: null, values: {} });

    await setActiveDoc(meta); // BC + LS (both canonical + legacy)
    // Opportunistic hydrate after switching/setting current
    try { coalesce('hydrate:'+docId, () => hydrateFromDocxIfEmpty(docId)); } catch {}
    return meta;
  }

  // Convenience: when caller only has bytes + plain meta
  async function setCurrentDocFromBytes(bytes, meta = {}) {
    return await setCurrentDoc({ bytes, handle: meta.handle, name: meta.name });
  }

  // ===== Public API surface =====
  const api = {
    // canonical storage key (pages listen for it)
    ACTIVE_DOC_KEY: LS_KEYS.CANON_ACTIVE,

    // Active doc (single source of truth)
    getActiveDocMeta,
    setActiveDoc,
    clearActiveDoc: async () => setActiveDoc(null),

    // Back-compat (names some pages call)
    getCurrentDocMeta: getActiveDocMeta,
    getActiveDoc: async () => getActiveDocMeta(),
    getState: loadState,
    getActiveDocMetaSync: getActiveDocMeta,

    // Meta/bytes
    setCurrentDoc,
    setCurrentDocFromBytes,
    getHandle: idbGetHandle,
    getBytes,
    putBytes,
    getCurrentDocBytes,

    // State
    saveState,
    loadState,
    getLastKnownValues,

    // Permissions
    ensurePermission,
    regrantAll,

    // Concurrency helpers
    withDocLock,
    coalesce,
    // Restore
    hydrateFromDocxIfEmpty,
  };

  // Utils bundle (optional)
  const utils = (function(){
    function sanitizeValues(schema, vals) {
      const out = {};
      const fields = Array.isArray(schema?.fields) ? schema.fields : [];
      for (const f of fields) {
        const id = f.id; let v = vals?.[id];
        if (v == null) continue;

        if (f.type === 'multichoice') {
          let arr = Array.isArray(v) ? v.slice() : (typeof v === 'string' ? v.split(',') : []);
          arr = arr.map(x => String(x).trim()).filter(Boolean);
          if (Array.isArray(f.options) && f.options.length) {
            const allowed = new Set(f.options.map(o => String(o?.value ?? o)));
            arr = arr.filter(x => allowed.has(String(x)));
          }
          if (arr.length) out[id] = arr;
          continue;
        }

        if (f.type === 'select') {
          let s = Array.isArray(v) ? String(v[0] ?? '') : String(v ?? '');
          if (Array.isArray(f.options) && f.options.length) {
            const allowed = new Set(f.options.map(o => String(o?.value ?? o)));
            if (!allowed.has(s)) s = '';
          }
          if (s !== '' || f.required) out[id] = s;
          continue;
        }

        if (f.type === 'number') {
          if (v === '' || v == null) continue;
          const num = (typeof v === 'number') ? v : Number(String(v).replace(',', '.'));
          if (Number.isFinite(num)) out[id] = num;
          continue;
        }

        if (f.type === 'date') { out[id] = String(v ?? ''); continue; }

        if (f.type === 'datediff') {
          const d = v;
          if (d && typeof d === 'object') {
            const outObj = {
              days: Number(d.days ?? 0),
              months: Number(d.months ?? 0),
              years: Number(d.years ?? 0),
              formatted: String(d.formatted ?? '')
            };
            if (outObj.formatted) out[id] = outObj;
          } else if (Number.isFinite(Number(d))) {
            const n = Number(d);
            out[id] = { days: n, months: 0, years: 0, formatted: `${n}-0-0 (${n})` };
          }
          continue;
        }

        if (f.type === 'address') {
          if (typeof v === 'string') {
            const s = v.trim();
            if (s || f.required) out[id] = s ? { formatted: s } : { formatted: '' };
          } else if (v && typeof v === 'object') {
            const o = {
              formatted: String(v.formatted || ''),
              street: String(v.street || ''),
              houseNumber: String(v.houseNumber || ''),
              postcode: String(v.postcode || ''),
              city: String(v.city || ''),
              country: String(v.country || ''),
              lat: (v.lat ?? null),
              lon: (v.lon ?? null)
            };
            if (o.formatted || f.required) out[id] = o;
          }
          continue;
        }

        if (f.type === 'table') {
          const cols = Array.isArray(f.columns) ? f.columns : [];
          const colIds = cols.map(c => c.id);
          const arr = Array.isArray(v) ? v : [];
          const cleaned = arr.map(row => {
            const o = {};
            for (const cid of colIds) { let cell = row?.[cid]; if (cell == null) cell = ''; o[cid] = String(cell); }
            return o;
          }).filter(r => Object.values(r).some(val => String(val).trim() !== ''));
          const min = Math.max(0, parseInt(f.minRows || 0, 10));
          while (cleaned.length < min) {
            const empty = {}; colIds.forEach(cid => empty[cid] = ''); cleaned.push(empty);
          }
          if (cleaned.length) out[id] = cleaned;
          continue;
        }

        const s = String(v ?? '');
        if (s !== '' || f.required) out[id] = s;
      }
      return out;
    }
    function sanitizeTagMap(tagMap, validIds) {
      const out = {};
      for (const [tag, fid] of Object.entries(tagMap || {})) {
        if (validIds.has(fid)) out[tag] = fid;
      }
      return out;
    }
    function emptyRow(field){ const r={}; (field.columns||[]).forEach(c=>r[c.id]=''); return r; }
    return { sanitizeValues, sanitizeTagMap, emptyRow };
  })();

  // ===== Payload hydration (centralized) =====
  async function ensureJSZip(){
    if (window.JSZip) return;
    await new Promise((res, rej) => {
      const s = document.createElement('script');
      s.src = 'https://cdn.jsdelivr.net/npm/jszip@3.10.1/dist/jszip.min.js';
      s.onload = res; s.onerror = rej; document.head.appendChild(s);
    });
  }

  const PAYLOAD_KEY = 'CRONOS_PAYLOAD';

  async function readPayloadFromDocx(bytes){
    try {
      await ensureJSZip();
      const zip = await window.JSZip.loadAsync(bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes));
      const settings = zip.file('word/settings.xml');
      if (settings) {
        const xmlText = await settings.async('string');
        const parser = new DOMParser();
        const xml = parser.parseFromString(xmlText, 'application/xml');
        const W_NS = 'http://schemas.openxmlformats.org/wordprocessingml/2006/main';
        const docVars = xml.getElementsByTagNameNS(W_NS, 'docVars')[0];
        if (docVars) {
          const vars = docVars.getElementsByTagNameNS(W_NS, 'docVar');
          for (let i = 0; i < vars.length; i++) {
            const dv = vars[i];
            const name = dv.getAttributeNS(W_NS, 'name') || dv.getAttribute('w:name') || dv.getAttribute('name');
            if (name === PAYLOAD_KEY) {
              const val = dv.getAttributeNS(W_NS, 'val') || dv.getAttribute('w:val') || dv.getAttribute('val') || '';
              return val || null;
            }
          }
        }
      }
      const custom = zip.file('docProps/custom.xml');
      if (custom) {
        const xmlText = await custom.async('string');
        const parser = new DOMParser();
        const xml = parser.parseFromString(xmlText, 'application/xml');
        const props = xml.getElementsByTagName('property');
        for (let i = 0; i < props.length; i++) {
          const p = props[i];
          const nm = p.getAttribute('name');
          if (nm === PAYLOAD_KEY) {
            const child = p.firstElementChild;
            return child && child.textContent ? child.textContent : null;
          }
        }
      }
    } catch (e) { dbg('readPayloadFromDocx error', e); }
    return null;
  }

  let __hydrating = false;
  async function hydrateFromDocxIfEmpty(docId) {
    if (!docId || __hydrating) return false;
    try {
      const st = await loadState(docId);
      const hasSchema = Array.isArray(st?.schema?.fields) && st.schema.fields.length > 0;
      if (hasSchema) return false;
      dbg('hydrate:begin', { docId });
      let bytes = await getBytes(docId);
      if (!bytes) bytes = await getCurrentDocBytes();
      if (!bytes) { dbg('hydrate:no-bytes', { docId }); return false; }
      const raw = await readPayloadFromDocx(bytes);
      if (!raw) { dbg('hydrate:no-payload', { docId }); return false; }
      let payload = null;
      try { payload = JSON.parse(raw); } catch { payload = null; }
      if (!payload || !Array.isArray(payload.fields) || !payload.fields.length) { dbg('hydrate:invalid', { docId }); return false; }
      const nextSchema = { title: payload.title || 'Form', fields: payload.fields };
      const cleanValues = (window.formSuiteUtils?.sanitizeValues ? window.formSuiteUtils.sanitizeValues(nextSchema, payload.values || {}) : (payload.values || {}));
      const tagMap = payload.tagMap || {};
      const rules  = Array.isArray(payload.rules) ? payload.rules : [];
      await saveState(docId, { schema: nextSchema, values: cleanValues, tagMap, rules, schemaUpdatedAt: new Date().toISOString() });
      // Notify other tabs/pages
      try { bcCanon?.postMessage({ type: 'schema-updated', docId, ts: Date.now() }); } catch {}
      try { bcLegacy?.postMessage({ type: 'schema-updated', docId, ts: Date.now() }); } catch {}
      try { const meta = getActiveDocMeta(); broadcastDocUpdated(docId, meta?.name); } catch {}
      dbg('hydrate:done', { docId, fields: nextSchema.fields.length });
      return true;
    } catch (e) { dbg('hydrate:error', e); return false; }
  }

  // Trigger hydration on key events across tabs
  try {
    bcCanon?.addEventListener('message', (ev) => {
      const m = ev?.data || {};
      if ((m.type === 'active:set' || m.type === 'active:updated') && m.docId) {
        coalesce('hydrate:'+m.docId, () => hydrateFromDocxIfEmpty(m.docId));
      }
    });
  } catch {}

  window.addEventListener('focus', () => {
    const meta = getActiveDocMeta();
    if (meta?.docId) coalesce('hydrate:'+meta.docId, () => hydrateFromDocxIfEmpty(meta.docId));
  });

  // Expose
  window.formSuitePersist = api;
  window.formSuiteUtils = Object.freeze(utils);
})();

/* --------------------------------------------------------------------------
 * Active Document Guard (global)
 * - Auto-mounts on every page that includes persistence.js
 * - Detects lost DOCX (no bytes/handle/permission) and blocks UI with a modal
 * - Offers: Open/Upload DOCX…, Try again, or Clear workspace
 * -------------------------------------------------------------------------- */
(function () {
  const ACTIVE_LS_KEY = 'FS_ACTIVE_DOC_META';
  const hasFSAccess = typeof window.showOpenFilePicker === 'function'; // feature detection
  const bcLegacy = ('BroadcastChannel' in window) ? new BroadcastChannel('form-suite-doc') : null;
  const bcCanon  = ('BroadcastChannel' in window) ? new BroadcastChannel('fs-active-doc') : null;

  // Minimal console tag
  const tag = (m) => ["%c[DocGuard]", "color:#6b7280;font-weight:600", m];

  // Safe JSON parse
  const jget = (k) => {
    try { const v = localStorage.getItem(k); return v ? JSON.parse(v) : null; }
    catch { return null; }
  };

  // Broadcast helpers
  function broadcastActive(meta) {
    try { localStorage.setItem(ACTIVE_LS_KEY, JSON.stringify({ docId: meta?.docId || null, name: meta?.name || null })); } catch {}
    try { bcCanon?.postMessage({ type: 'active:set', docId: meta?.docId || null, name: meta?.name || null, ts: Date.now() }); } catch {}
    try { bcLegacy?.postMessage({ type: 'doc-switched', docId: meta?.docId || null, name: meta?.name || null, ts: Date.now() }); } catch {}
  }
  function broadcastClear() {
    try { localStorage.removeItem(ACTIVE_LS_KEY); } catch {}
    try { bcCanon?.postMessage({ type: 'active:clear', ts: Date.now() }); } catch {}
    try { bcLegacy?.postMessage({ type: 'doc-cleared', ts: Date.now() }); } catch {}
  }
  function broadcastUpdated(meta) {
    try { bcCanon?.postMessage({ type: 'active:updated', docId: meta?.docId || null, name: meta?.name || null, ts: Date.now() }); } catch {}
    try { bcLegacy?.postMessage({ type: 'doc-updated', docId: meta?.docId || null, name: meta?.name || null, ts: Date.now() }); } catch {}
  }

  // Small DOM helper
  function el(tag, attrs = {}, children = []) {
    const n = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) {
      if (k === 'style' && v && typeof v === 'object') Object.assign(n.style, v);
      else if (k.startsWith('on') && typeof v === 'function') n.addEventListener(k.slice(2), v);
      else if (v != null) n.setAttribute(k, v);
    }
    for (const c of (Array.isArray(children) ? children : [children])) {
      if (c == null) continue;
      n.appendChild(typeof c === 'string' ? document.createTextNode(c) : c);
    }
    return n;
  }

  // Create the modal overlay once
  let overlay = null;
  let visible = false;

  function ensureOverlay() {
    if (overlay) return overlay;

    const css = `
.fs-docguard-overlay{position:fixed;inset:0;background:rgba(17,24,39,.66);backdrop-filter:saturate(120%) blur(2px);z-index:999999}
.fs-docguard-card{position:fixed;inset:auto;left:50%;top:12%;transform:translateX(-50%);
  width:min(720px,92vw);background:var(--card,#fff);color:var(--ink,#111827);border:1px solid var(--border,#e5e7eb);
  border-radius:12px;box-shadow:0 10px 30px rgba(0,0,0,.2);padding:16px;display:grid;gap:12px;z-index:1000000}
.fs-docguard-row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.fs-docguard-actions{display:flex;gap:8px;justify-content:flex-end}
.fs-docguard-btn{padding:8px 12px;border-radius:8px;border:1px solid var(--border-strong,#d1d5db);background:var(--card,#fff);cursor:pointer;color:var(--ink,#111827)}
.fs-docguard-btn.primary{background:var(--accent,#2563eb);color:var(--accent-ink,#fff);border-color:transparent}
.fs-docguard-btn.ghost{background:var(--ghost-bg,#f3f4f6);color:var(--ghost-ink,#111827)}
.fs-docguard-btn.danger{background:#ef4444;color:#fff;border-color:transparent}
.fs-docguard-badge{display:inline-flex;align-items:center;gap:6px;padding:2px 8px;border-radius:999px;border:1px solid var(--border,#e5e7eb);background:var(--ghost-bg,#f3f4f6);font-size:.85rem}
`;
    const style = el('style', {}, css);

    const title = el('div', { class: 'fs-docguard-row' }, [
      el('strong', {}, 'Document connection lost'),
      el('span', { class: 'fs-docguard-badge', id: 'fsdgDocName' }, '(no name)'),
    ]);

    const body = el('div', {}, [
      'The active DOCX is no longer available (bytes/handle/permission not accessible). ',
      'To keep things safe, editing is paused until you reconnect or clear the workspace.',
    ]);

    const status = el('div', { id: 'fsdgStatus', style: { color: 'var(--muted,#6b7280)', fontSize: '.9rem' } }, '');

    const btnOpenLabel = hasFSAccess ? 'Open DOCX…' : 'Upload DOCX…';
    const btnOpen  = el('button', { class: 'fs-docguard-btn primary', id: 'fsdgOpen' }, btnOpenLabel);
    const btnRetry = el('button', { class: 'fs-docguard-btn ghost',   id: 'fsdgRetry' }, 'Try again');
    const btnClear = el('button', { class: 'fs-docguard-btn danger',  id: 'fsdgClear' }, 'Clear workspace');

    const actions = el('div', { class: 'fs-docguard-actions' }, [btnClear, btnRetry, btnOpen]);
    const card = el('div', { class: 'fs-docguard-card' }, [title, body, status, actions]);
    overlay = el('div', { class: 'fs-docguard-overlay', style: { display: 'none' } }, [style, card]);
    document.documentElement.appendChild(overlay);

    // ---- cross-browser DOCX picker ----
    async function pickDocxFile() {
      // Preferred: File System Access API
      if (hasFSAccess) {
        const [handle] = await window.showOpenFilePicker({
          multiple: false,
          excludeAcceptAllOption: true,
          types: [{
            description: 'Word document',
            accept: { 'application/vnd.openxmlformats-officedocument.wordprocessingml.document': ['.docx', '.docm'] }
          }]
        });
        const f = await handle.getFile();
        const bytes = new Uint8Array(await f.arrayBuffer());
        return { file: f, bytes, handle };
      }

      // Fallback: classic file input (works on Safari/Firefox/iOS)
      return new Promise((resolve, reject) => {
        const input = document.createElement('input');
        input.type = 'file';
        input.accept = '.docx,.docm,application/vnd.openxmlformats-officedocument.wordprocessingml.document';
        input.style.position = 'fixed';
        input.style.inset = '-9999px';
        document.body.appendChild(input);
        input.addEventListener('change', async () => {
          const f = input.files && input.files[0];
          input.remove();
          if (!f) { reject(new DOMException('Abort', 'AbortError')); return; }
          try {
            const bytes = new Uint8Array(await f.arrayBuffer());
            resolve({ file: f, bytes, handle: null });
          } catch (e) { reject(e); }
        }, { once: true });
        // Kick off picker; if user cancels, no 'change' — input is auto-removed after 60s.
        setTimeout(() => { try { input.click(); } catch {} }, 0);
        setTimeout(() => { if (document.body.contains(input)) input.remove(); }, 60000);
      });
    }

    // Wire actions
    btnOpen.addEventListener('click', async () => {
      setStatus('Opening picker…');
      try {
        const { file, bytes, handle } = await pickDocxFile();
        const nameNoExt = (file?.name || 'document').replace(/\.docx$/i, '');

        // Persist into our storage layer
        let meta;
        if (window.formSuitePersist?.setCurrentDoc) {
          meta = await window.formSuitePersist.setCurrentDoc({ bytes, handle, name: nameNoExt + '.docx' });
        } else if (window.formSuitePersist?.setCurrentDocFromBytes) {
          meta = await window.formSuitePersist.setCurrentDocFromBytes(bytes, { name: nameNoExt + '.docx', handle });
        } else {
          meta = { docId: 'inline-' + Date.now(), name: nameNoExt + '.docx' };
        }

        // Seed bytes and broadcast so all tabs rehydrate
        try { await window.formSuitePersist?.putBytes?.(meta.docId, bytes); } catch {}
        broadcastActive(meta);
        broadcastUpdated(meta);

        hide();
      } catch (e) {
        const msg = (e && (e.name === 'AbortError' || e.code === e.ABORT_ERR)) ? 'Open canceled.' : 'Open failed.';
        setStatus(msg);
        console.warn('[DocGuard] open failed', e);
      }
    });

    btnRetry.addEventListener('click', async () => {
      setStatus('Rechecking access…');
      try {
        const ok = await checkAccess(true);
        setStatus(ok ? 'Reconnected.' : 'Still not available.');
        if (ok) hide();
      } catch {
        setStatus('Retry failed.');
      }
    });

    btnClear.addEventListener('click', async () => {
      try {
        // Best-effort clear; pages will reset themselves
        const meta = jget(ACTIVE_LS_KEY);
        if (meta?.docId) {
          try { await window.formSuitePersist?.saveState?.(meta.docId, {}); } catch {}
          try { await window.formSuitePersist?.putBytes?.(meta.docId, new Uint8Array()); } catch {}
        }
      } finally {
        broadcastClear();
        hide();
      }
    });

    function setStatus(s) { status.textContent = s || ''; }
    overlay._setStatus = setStatus;
    overlay._setDocName = (n) => {
      const badge = overlay.querySelector('#fsdgDocName');
      if (badge) badge.textContent = n || '(no name)';
    };

    return overlay;
  }

  function show(name) {
    const ov = ensureOverlay();
    ov._setDocName(name || jget(ACTIVE_LS_KEY)?.name || '(no name)');
    ov.style.display = 'block';
    visible = true;
    document.documentElement.style.overflow = 'hidden'; // lock scroll behind
  }
  function hide() {
    if (!overlay) return;
    overlay.style.display = 'none';
    visible = false;
    document.documentElement.style.overflow = '';
  }
  function setStatus(s) { ensureOverlay()._setStatus?.(s); }

  // Access checker: returns true if we can read bytes (or will be able to imminently)
  async function checkAccess(tryHandleFallback = false) {
    const meta = jget(ACTIVE_LS_KEY);
    const docId = meta?.docId;
    if (!docId) return false;

    try {
      // 1) Try OPFS bytes
      let bytes = await window.formSuitePersist?.getBytes?.(docId);
      if (bytes?.byteLength) return true;

      // 2) Try currentDocBytes mirror
      bytes = await window.formSuitePersist?.getCurrentDocBytes?.();
      if (bytes?.byteLength) return true;

      // 3) Optional: try handle→file path
      if (tryHandleFallback) {
        const h = await window.formSuitePersist?.getHandle?.(docId);
        if (h?.getFile) {
          try {
            const p = await h.queryPermission?.({ mode: 'read' });
            if (p !== 'granted') await h.requestPermission?.({ mode: 'read' });
          } catch {}
          const f = await h.getFile();
          const ab = await f.arrayBuffer();
          if (ab?.byteLength) {
            try { await window.formSuitePersist?.putBytes?.(docId, new Uint8Array(ab)); } catch {}
            return true;
          }
        }
      }
    } catch (e) {
      console.warn(...tag('checkAccess error'), e);
    }
    return false;
  }

  // Detection strategy
  async function evaluateAndMaybeShow(source) {
    const meta = jget(ACTIVE_LS_KEY);
    const hasDoc = !!meta?.docId;
    if (!hasDoc) {
      show('(no document)');
      setStatus('No active document.');
      console.log(...tag(`lost (no meta) via ${source}`));
      return;
    }
    const ok = await checkAccess(false);
    if (!ok) {
      show(meta?.name || '(no name)');
      setStatus('Lost access to current DOCX.');
      console.log(...tag(`lost (no access) via ${source}`));
    } else if (visible) {
      hide();
    }
  }

  // Broadcast listeners
  bcCanon?.addEventListener('message', (ev) => {
    const m = ev?.data || {};
    if (m.type === 'active:clear') {
      show('(cleared)'); setStatus('Workspace cleared.');
    }
    if (m.type === 'active:set')      evaluateAndMaybeShow('bcCanon active:set');
    if (m.type === 'active:updated')  evaluateAndMaybeShow('bcCanon active:updated');
  });
  bcLegacy?.addEventListener('message', (ev) => {
    const m = ev?.data || {};
    if (m.type === 'doc-cleared') {
      show('(cleared)'); setStatus('Workspace cleared.');
    }
    if (m.type === 'doc-switched' || m.type === 'doc-updated') {
      evaluateAndMaybeShow('bcLegacy ' + m.type);
    }
  });

  // Storage listener (cross-tab)
  window.addEventListener('storage', (e) => {
    if (e.key === ACTIVE_LS_KEY) evaluateAndMaybeShow('storage');
  });

  // Foreground triggers
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') evaluateAndMaybeShow('visibilitychange');
  });
  window.addEventListener('focus', () => evaluateAndMaybeShow('focus'));

  // Light heartbeat (only if not visible)
  setInterval(() => { if (!visible) evaluateAndMaybeShow('heartbeat'); }, 25000);

  // Initial mount
  window.addEventListener('DOMContentLoaded', () => { evaluateAndMaybeShow('DOMContentLoaded'); });

  // Expose (optional) API
  window.fsDocGuard = {
    show, hide,
    ping: () => evaluateAndMaybeShow('manual'),
    open: () => ensureOverlay().querySelector('#fsdgOpen')?.click(),
    clear: () => ensureOverlay().querySelector('#fsdgClear')?.click(),
  };
})();
