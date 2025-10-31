/* persistence.js — single-source-of-truth for active DOCX
   --------------------------------------------------------
   Responsibilities
   • Keep ONE canonical “active doc” signal across tabs (localStorage + BroadcastChannel)
   • Store FileSystemFileHandle in IndexedDB
   • Maintain a working copy of bytes in OPFS (Origin Private File System)
   • Persist per-doc state (schema/values/tagMap/rules/…) in localStorage
   • Provide safe, atomic helpers (locks + coalescers) used by pages
   • Broadcast payload patches across tabs + snapshot/rehydration

   IMPORTANT (Oct 2025):
   • Content-addressed identity: docId = "<basename>#<sha256:12>"
     - Different file contents → different docId (even if filename is identical)
     - Everything is strictly keyed by docId
   • Rules are handled like values/tagMap:
     - Patch into workspace state
     - Mirror into payload.CRONOS_PAYLOAD
     - No immediate DOCX embedding during edit (export handles it)
*/

(() => {
  // ===== Keys & channels (canonical + legacy for BWC) =====
  const LS_KEYS = {
    CANON_ACTIVE: 'FS_ACTIVE_DOC_META', // { docId, name }  <- canonical
    LEGACY_ACTIVE: 'FS_CURRENT_DOC_META', // legacy mirror
    INTERNAL_ACTIVE: 'FS_active_doc_v1',  // older internal
    STATE_PREFIX: 'FS_state_',            // per docId -> { schema, values, tagMap, rules, fieldRules, payload, ... }
  };
  const ACTIVE_KEYS_READ_ORDER = [
    LS_KEYS.CANON_ACTIVE,
    LS_KEYS.LEGACY_ACTIVE,
    LS_KEYS.INTERNAL_ACTIVE,
  ];

  const BC = {
    CANON:  'fs-active-doc',   // {active:set|active:updated|active:clear}
    LEGACY: 'form-suite-doc',  // {doc-switched|doc-updated|doc-cleared}
    PAYLOAD:'fs-payload-v1',   // {t:'payload'|'request-snapshot'|'snapshot', ...}
  };

  const DB_NAME = 'formsuite_v1';
  const DB_STORE_HANDLES = 'handles';
  const PAYLOAD_KEY = 'CRONOS_PAYLOAD';

  // ===== Utils =====
  const readJSON = (k) => { try { return JSON.parse(localStorage.getItem(k) || 'null'); } catch { return null; } };
  const writeJSON = (k, v) => { try { (v == null) ? localStorage.removeItem(k) : localStorage.setItem(k, JSON.stringify(v)); } catch {} };
  const isPlainObject = (o) => !!o && typeof o === 'object' && !Array.isArray(o);
  const _safeArr = (v) => Array.isArray(v) ? v.slice() : (v == null ? [] : [v]);
  const _nowIso = () => { try { return new Date().toISOString(); } catch { return ''; } };
  const debounce = (fn, ms) => { let t; return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); }; };

  function deepMerge(a, b) {
    if (Array.isArray(b)) return b.slice(); // arrays REPLACE (by design)
    if (!isPlainObject(b)) return (b === undefined) ? a : b;
    const out = isPlainObject(a) ? { ...a } : {};
    for (const [k, v] of Object.entries(b)) {
      out[k] = deepMerge(a?.[k], v);
    }
    return out;
  }
  function isEmptyState(st) {
    if (!st || typeof st !== 'object') return true;
    const keys = Object.keys(st).filter(k => !k.startsWith('__'));
    return keys.length === 0;
  }

  // --- Content-addressing helpers (stable doc identity) ---
  async function sha256Hex(bytes) {
    const ab = (bytes instanceof ArrayBuffer) ? bytes
           : (bytes && bytes.buffer instanceof ArrayBuffer) ? bytes.buffer
           : new Uint8Array(bytes || []).buffer;
    const d = await crypto.subtle.digest('SHA-256', ab);
    const u8 = new Uint8Array(d);
    let out = '';
    for (let i = 0; i < u8.length; i++) out += u8[i].toString(16).padStart(2, '0');
    return out;
  }
  function splitNameAndExt(name) {
    const m = /^(.*?)(\.(docx|docm|dotx|dotm))?$/i.exec(name || 'document.docx');
    return { base: (m && m[1]) || 'document', ext: (m && m[2]) || '.docx' };
  }
  async function computeDocIdFromBytes(bytes, fileName) {
    const hex = await sha256Hex(bytes);
    const { base } = splitNameAndExt(fileName || 'document.docx');
    return `${base}#${hex.slice(0,12)}`; // short, collision-resistant id
  }

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
    let rows = [];
    try {
      const db = await openDB();
      rows = await new Promise((res, rej) => {
        const items = [];
        const tx = db.transaction(DB_STORE_HANDLES, 'readonly');
        const store = tx.objectStore(DB_STORE_HANDLES);
        const req = store.openCursor();
        req.onsuccess = () => { const c = req.result; if (c) { items.push(c.value); c.continue(); } };
        req.onerror  = () => rej(req.error);
        tx.oncomplete= () => res(items);
        tx.onerror   = () => rej(tx.error);
      });
    } catch { rows = []; }
    for (const { docId, handle } of rows) {
      try { await cb(docId, handle); } catch {}
    }
  }

  // ===== OPFS working copy =====
  async function opfsRoot()         { try { return await navigator.storage.getDirectory(); } catch { return null; } }
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

  // ===== Cross-tab plumbing (active/payload) =====
  const bcLegacy  = ('BroadcastChannel' in window) ? new BroadcastChannel(BC.LEGACY)  : null;
  const bcCanon   = ('BroadcastChannel' in window) ? new BroadcastChannel(BC.CANON)   : null;
  const bcPayload = ('BroadcastChannel' in window) ? new BroadcastChannel(BC.PAYLOAD) : null;

  const TAB = (crypto?.randomUUID?.() || (Date.now() + '_' + Math.random().toString(16).slice(2)));

  const listeners = {
    active:  new Set(), // fn(meta)
    payload: new Set(), // fn({docId, patch, v, from})
  };
  function subscribe(kind, fn) { listeners[kind]?.add(fn); return () => listeners[kind]?.delete(fn); }
  function emit(kind, data)    { listeners[kind]?.forEach(fn => { try { fn(data); } catch {} }); }

  // ===== State (schema/values/etc.) =====
  function keyFor(docId)    { return LS_KEYS.STATE_PREFIX + docId; }
  function getState(docId)  { if (!docId) return {}; return readJSON(keyFor(docId)) || {}; }

  // ---- payload builder + mirror ----
  function buildPayloadFromState(state) {
    const schema  = state?.schema || {};
    const fields  = Array.isArray(schema.fields) ? schema.fields : [];
    const values  = state?.values || {};
    const tagMap  = state?.tagMap || {};
    const rules   = _safeArr(state?.rules);
    const fRules  = _safeArr(state?.fieldRules);
    const headingsFlat = Array.isArray(state?.headingsFlat)
      ? state.headingsFlat
      : Array.isArray(state?.headings) ? state.headings
      : [];
    const headingsTree = Array.isArray(state?.headingsTree) ? state.headingsTree : [];
    return {
      title: schema?.title || 'Form',
      fields,
      values,
      tagMap,
      rules,
      fieldRules: fRules,
      headingsFlat,
      headingsTree,
      updatedAt: _nowIso(),
    };
  }
  function mirrorPayloadOnState(state) {
    const payload = buildPayloadFromState(state);
    if (!state.payload) state.payload = {};
    state.payload[PAYLOAD_KEY] = payload;   // canonical nested
    state[PAYLOAD_KEY]        = payload;    // flat mirror (old code paths)
    state.cronos_payload      = payload;    // another flat mirror (BWC)
    return state;
  }

  // Save & broadcast a partial patch to the per-doc state
  function setState(docId, patch) {
    if (!docId || !patch || typeof patch !== 'object') return getState(docId);
    const prev = getState(docId);

    // Normalize rule arrays if present (empty arrays are allowed for deletions)
    const normalizedPatch = { ...patch };
    if ('rules' in normalizedPatch)      normalizedPatch.rules = _safeArr(normalizedPatch.rules);
    if ('fieldRules' in normalizedPatch) normalizedPatch.fieldRules = _safeArr(normalizedPatch.fieldRules);

    // Merge shallowly then ensure mirrors
    const next = deepMerge(prev, normalizedPatch);

    // Ensure structural defaults
    if (!isPlainObject(next.schema))     next.schema = {};
    if (!isPlainObject(next.values))     next.values = {};
    if (!isPlainObject(next.tagMap))     next.tagMap = {};
    if (!Array.isArray(next.rules))      next.rules = [];
    if (!Array.isArray(next.fieldRules)) next.fieldRules = [];

    mirrorPayloadOnState(next);

    next.__v = (next.__v|0) + 1;
    writeJSON(keyFor(docId), next);

    // Broadcast to peers & same-tab listeners
    try { bcPayload?.postMessage({ t: 'payload', docId, patch: normalizedPatch, v: next.__v, from: TAB }); } catch {}
    emit('payload', { docId, patch: normalizedPatch, v: next.__v, from: TAB });

    return next;
  }

  // Back-compat alias with the previous signature (async)
  async function saveState(docId, obj) { return setState(docId, obj); }
  async function loadState(docId)      { return getState(docId); }
  function getLastKnownValues(docId)   { try { return (getState(docId) || {}).values || {}; } catch { return {}; } }

  // ===== Permissions =====
  async function ensurePermission(handle, mode = 'readwrite') {
    if (!handle) return 'denied';
    try {
      let q = await handle.queryPermission?.({ mode });
      if (q === 'granted') return 'granted';
      if (q === 'prompt') q = await handle.requestPermission?.({ mode }) || q;
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
  let _active = null; // { docId, name }
  function dbg(where, data) { try { console.log('%c[Persist]', 'color:#2563eb;font-weight:600', where, data || ''); } catch {} }

  function readActiveFromStorage() {
    for (const k of ACTIVE_KEYS_READ_ORDER) {
      const v = readJSON(k);
      if (v?.docId) return v;
    }
    return null;
  }
  function writeActiveToStorage(metaOrNull) {
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
      emit('active', null);
      return null;
    }
    const next = { docId: metaOrNull.docId, name: metaOrNull.name || 'document.docx' };
    _active = next;
    writeActiveToStorage(next);
    const payload = { docId: next.docId, name: next.name, ts: Date.now() };
    try { bcCanon?.postMessage({ type: 'active:set', ...payload }); } catch {}
    try { bcLegacy?.postMessage({ type: 'doc-switched', ...payload }); } catch {}
    emit('active', next);
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
      emit('active', _active);
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
    try { return await fn(); }
    finally {
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

  // ===== JSZip helpers for DOCX payload IO (used by Export/Save flows) =====
  async function ensureJSZip(){
    if (window.JSZip) return;
    await new Promise((res, rej) => {
      const s = document.createElement('script');
      s.src = 'https://cdn.jsdelivr.net/npm/jszip@3.10.1/dist/jszip.min.js';
      s.onload = res; s.onerror = rej; document.head.appendChild(s);
    });
  }
  async function readPayloadFromDocx(bytes){
    try {
      await ensureJSZip();
      const zip = await window.JSZip.loadAsync(bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes));
      // Try word/settings.xml (docVars)
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
              if (val) return val;
            }
          }
        }
      }
      // Fallback: docProps/custom.xml custom property
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
  async function writeDocVarSettings(bytes, key, jsonValue) {
    // NOTE: kept for Export flows. We do NOT auto-embed while editing rules anymore.
    await ensureJSZip();
    const u8  = (bytes instanceof Uint8Array) ? bytes : new Uint8Array(bytes);
    const zip = await window.JSZip.loadAsync(u8);

    // Ensure word/settings.xml exists
    let settings = zip.file('word/settings.xml');
    let xmlText  = settings ? await settings.async('string') : null;
    if (!xmlText) {
      xmlText = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:settings xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006"
            xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"
            mc:Ignorable="w14 w15 w16se w16cid w16">
  <w:docVars/>
</w:settings>`;
    }
    const parser = new DOMParser();
    const xml    = parser.parseFromString(xmlText, 'application/xml');
    const W_NS   = 'http://schemas.openxmlformats.org/wordprocessingml/2006/main';

    let settingsEl = xml.getElementsByTagNameNS(W_NS, 'settings')[0];
    if (!settingsEl) {
      const se = xml.createElementNS(W_NS, 'w:settings');
      xml.appendChild(se);
      settingsEl = se;
    }
    let docVars = xml.getElementsByTagNameNS(W_NS, 'docVars')[0];
    if (!docVars) {
      docVars = xml.createElementNS(W_NS, 'w:docVars');
      settingsEl.appendChild(docVars);
    }

    // Find or create the docVar
    let found = null;
    const vars = docVars.getElementsByTagNameNS(W_NS, 'docVar');
    for (let i = 0; i < vars.length; i++) {
      const dv = vars[i];
      const name = dv.getAttributeNS(W_NS, 'name') || dv.getAttribute('w:name') || dv.getAttribute('name');
      if (name === key) { found = dv; break; }
    }
    if (!found) {
      found = xml.createElementNS(W_NS, 'w:docVar');
      found.setAttributeNS(W_NS, 'w:name', key);
      docVars.appendChild(found);
    }
    found.setAttributeNS(W_NS, 'w:name', key);
    found.setAttributeNS(W_NS, 'w:val', jsonValue);
    found.setAttribute('name', key);
    found.setAttribute('val', jsonValue);

    const serializer = new XMLSerializer();
    const outXml     = serializer.serializeToString(xml);
    zip.file('word/settings.xml', outXml);

    const newBuf = await zip.generateAsync({ type: 'uint8array' });
    return newBuf.buffer;
  }

  // ===== High-level “set current” APIs (CONTENT-ADDRESSED) =====
  /**
   * Set the active document from bytes/handle/name.
   * Always computes docId = "<basename>#<sha256:12>" from BYTES if available.
   * If only a handle is provided, this attempts to read its bytes for hashing.
   */
  async function setCurrentDoc({ bytes, handle, name }) {
    // 1) Normalize name and obtain bytes for hashing
    const fileName = name || 'document.docx';
    let theBytes = bytes;
    if (!theBytes && handle?.getFile) {
      try { const f = await handle.getFile(); theBytes = await f.arrayBuffer(); } catch {}
    }
    if (!theBytes) throw new Error('setCurrentDoc: bytes or readable handle required to compute content id');

    // 2) Compute content-addressed docId
    const docId = await computeDocIdFromBytes(theBytes, fileName);
    const meta  = { docId, name: fileName };

    // 3) Persist handle & bytes (best-effort)
    if (handle) await idbPutHandle(docId, handle);
    await opfsPut(docId, theBytes);

    // 4) Ensure minimal state and content fingerprint
    const fingerprint = await sha256Hex(theBytes);
    const existing = await loadState(docId);
    if (!existing || isEmptyState(existing)) {
      await saveState(docId, { schema: null, values: {}, rules: [], fieldRules: [], fingerprint });
    } else if (existing.fingerprint !== fingerprint) {
      await saveState(docId, { fingerprint });
    }

    // 5) Flip active & try to hydrate schema/payload from DOCX if none present
    await setActiveDoc(meta); // BC + LS
    try { await hydrateFromDocxIfEmpty(docId); } catch {}
    return meta;
  }
  async function setCurrentDocFromBytes(bytes, meta = {}) {
    // Accept meta.name for basename, meta.handle optional
    const fileName = meta?.name || 'document.docx';
    return await setCurrentDoc({ bytes, handle: meta.handle, name: fileName });
  }

  // ===== Utils bundle used by pages (values/tagMap cleaning) =====
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
          const cols   = Array.isArray(f.columns) ? f.columns : [];
          const colIds = cols.map(c => c.id);
          const arr    = Array.isArray(v) ? v : [];
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
  let __hydrating = false;
  async function hydrateFromDocxIfEmpty(docId) {
    if (!docId || __hydrating) return false;
    try {
      const st = await loadState(docId);
      const hasSchema = Array.isArray(st?.schema?.fields) && st.schema.fields.length > 0;
      if (hasSchema) return false;
      __hydrating = true;
      dbg('hydrate:begin', { docId });
      let bytes = await getBytes(docId);
      if (!bytes) bytes = await getCurrentDocBytes();
      if (!bytes) { dbg('hydrate:no-bytes', { docId }); return false; }
      const raw = await readPayloadFromDocx(bytes);
      if (!raw) { dbg('hydrate:no-payload', { docId }); return false; }

      // Parse payload JSON from DOCX
      let payload = null;
      try { payload = JSON.parse(raw); } catch { payload = null; }
      if (!payload || !Array.isArray(payload.fields) || !payload.fields.length) {
        dbg('hydrate:invalid', { docId }); return false;
      }

      const nextSchema  = { title: payload.title || 'Form', fields: payload.fields };
      const cleanValues = (window.formSuiteUtils?.sanitizeValues
        ? window.formSuiteUtils.sanitizeValues(nextSchema, payload.values || {})
        : (payload.values || {}));
      const tagMap     = payload.tagMap || {};
      const rules      = Array.isArray(payload.rules) ? payload.rules : [];
      const fieldRules = Array.isArray(payload.fieldRules) ? payload.fieldRules : [];

      const canonical = {
        title: nextSchema.title,
        fields: nextSchema.fields,
        values: cleanValues,
        tagMap,
        rules,
        fieldRules,
        updatedAt: new Date().toISOString()
      };

      await saveState(docId, {
        schema: nextSchema,
        values: cleanValues,
        tagMap,
        rules,
        fieldRules,
        payload: { [PAYLOAD_KEY]: canonical },
        [PAYLOAD_KEY]: canonical,
        cronos_payload: canonical,
        schemaUpdatedAt: new Date().toISOString()
      });

      try { bcCanon?.postMessage({ type: 'schema-updated', docId, ts: Date.now() }); } catch {}
      try { bcLegacy?.postMessage({ type: 'schema-updated', docId, ts: Date.now() }); } catch {}
      try { const meta = getActiveDocMeta(); broadcastDocUpdated(docId, meta?.name); } catch {}
      dbg('hydrate:done', { docId, fields: nextSchema.fields.length, rules: rules.length, fieldRules: fieldRules.length });
      return true;
    } catch (e) { dbg('hydrate:error', e); return false; }
    finally { __hydrating = false; }
  }

  // Trigger hydration on key events across tabs
  try {
    bcCanon?.addEventListener('message', (ev) => {
      const m = ev?.data || {};
      if ((m.type === 'active:set' || m.type === 'active:updated') && m.docId) {
        coalesce('hydrate:'+m.docId, () => hydrateFromDocxIfEmpty(m.docId));
      }
      if (m.type === 'active:set') emit('active', { docId: m.docId, name: m.name });
      if (m.type === 'active:clear') emit('active', null);
    });
  } catch {}
  window.addEventListener('focus', () => {
    const meta = getActiveDocMeta();
    if (meta?.docId) coalesce('hydrate:'+meta.docId, () => hydrateFromDocxIfEmpty(meta.docId));
  });

  // ===== Cross-tab payload channel =====
  bcPayload && (bcPayload.onmessage = (ev) => {
    const msg = ev.data || {};
    if (msg.from === TAB) return;
    if (msg.t === 'payload') {
      emit('payload', msg);
    } else if (msg.t === 'request-snapshot') {
      const st = getState(msg.docId);
      if (!isEmptyState(st)) {
        try { bcPayload.postMessage({ t:'snapshot', docId: msg.docId, state: st, from:TAB }); } catch {}
      }
    } else if (msg.t === 'snapshot') {
      const cur = getState(msg.docId);
      if (isEmptyState(cur)) {
        writeJSON(keyFor(msg.docId), msg.state);
        emit('payload', { docId: msg.docId, patch: msg.state, v: (msg.state.__v|0), from:'remote-snapshot' });
      }
    }
  });

  // ===== RULES helpers (PATCH & OVERWRITE) — no embed here =====
  function _dedupeByJSON(arr) {
    const seen = new Set(); const out = [];
    for (const x of (Array.isArray(arr) ? arr : [])) {
      const k = JSON.stringify(x ?? null);
      if (!seen.has(k)) { seen.add(k); out.push(x); }
    }
    return out;
  }
  function patchRules(docId, { headingRules, fieldRules }, { replace = false } = {}) {
    const st = getState(docId);
    const wsHeading = Array.isArray(st.rules) ? st.rules : [];
    const wsField   = Array.isArray(st.fieldRules) ? st.fieldRules : [];
    const inHeading = _safeArr(headingRules);
    const inField   = _safeArr(fieldRules);

    const nextHeading = replace ? inHeading : _dedupeByJSON(wsHeading.concat(inHeading));
    const nextField   = replace ? inField   : _dedupeByJSON(wsField.concat(inField));

    return setState(docId, { rules: nextHeading, fieldRules: nextField });
  }

  // In-window implementation (no self-recursion on window.formSuitePersist.*)
  async function overwriteRules(docId, patch) {
    const nextRules      = Array.isArray(patch?.rules) ? patch.rules : [];
    const nextFieldRules = Array.isArray(patch?.fieldRules) ? patch.fieldRules : [];

    const cur = (await loadState(docId)) || {};
    const basePayload =
        (cur?.payload?.CRONOS_PAYLOAD)
    || (cur?.CRONOS_PAYLOAD)
    || (cur?.cronos_payload)
    || {};

    const canonical = {
      ...basePayload,
      rules: nextRules,
      fieldRules: nextFieldRules,
      updatedAt: new Date().toISOString()
    };

    const next = {
      rules: nextRules,
      fieldRules: nextFieldRules,
      payload: { CRONOS_PAYLOAD: canonical },
      CRONOS_PAYLOAD: canonical,
      cronos_payload: canonical
    };

    // Prefer setState to retain broadcast semantics
    await setState(docId, next);

    // Nudge active channels (some listeners only react to active:* messages)
    try { new BroadcastChannel('fs-active-doc').postMessage({ type: 'rules-updated', docId, ts: Date.now(), from: 'persist' }); } catch {}
  }

  function getRules(docId) {
    const st = getState(docId);
    return { rules: Array.isArray(st.rules) ? st.rules : [], fieldRules: Array.isArray(st.fieldRules) ? st.fieldRules : [] };
  }

  // (Legacy) persistRules signature kept for callers but it ONLY writes to workspace + mirrors now.
  async function persistRules(docId, rules, fieldRules /*, { embed } */) {
    if (!docId) throw new Error('persistRules: missing docId');
    return saveState(docId, { rules: _safeArr(rules), fieldRules: _safeArr(fieldRules) });
  }

  // ---- Hydration helper hook used by some pages (noop-safe) ----
  async function ensureHydrated() {
    // Best-effort: if there's an active doc with no schema, try to hydrate from DOCX
    const meta = getActiveDocMeta();
    if (meta?.docId) { try { await hydrateFromDocxIfEmpty(meta.docId); } catch {} }
    return true;
  }

  // ===== Public API surface =====
  const api = {
    // canonical storage key (pages listen for it)
    ACTIVE_DOC_KEY: LS_KEYS.CANON_ACTIVE,
    PAYLOAD_KEY,

    // Active doc (single source of truth)
    getActiveDocMeta,
    setActiveDoc,
    clearActiveDoc: async () => setActiveDoc(null),

    // Back-compat aliases
    getCurrentDocMeta: getActiveDocMeta,
    getActiveDoc: async () => getActiveDocMeta(),
    getActiveDocMetaSync: getActiveDocMeta,

    // State
    getState,
    loadState,
    saveState,
    setState,
    getLastKnownValues,

    // Rules API (new)
    patchRules,
    overwriteRules,
    getRules,
    persistRules, // legacy-friendly: writes to state only (no embed)

    // DOCX bytes/handles
    setCurrentDoc,
    setCurrentDocFromBytes,
    getHandle: idbGetHandle,
    getBytes,
    putBytes,
    getCurrentDocBytes,

    // Permissions
    ensurePermission,
    regrantAll,

    // Concurrency helpers
    withDocLock,
    coalesce,

    // DOCX payload helpers (used by Export/Save flows)
    hydrateFromDocxIfEmpty,
    readPayloadFromDocx,
    writeDocVarSettings,

    // Optional init hook used by some pages
    ensureHydrated,
  };

  // Expose
  window.formSuitePersist = Object.freeze(api);
  window.formSuiteUtils   = Object.freeze(utils);
})();

/* --------------------------------------------------------------------------
 * Active Document Guard (global)
 * - Auto-mounts on every page that includes persistence.js
 * - Detects lost DOCX (no bytes/handle/permission) and blocks UI with a modal
 * - Offers: Open/Upload DOCX…, Try again, or Clear workspace
 * -------------------------------------------------------------------------- */
(function () {
  const ACTIVE_LS_KEY = 'FS_ACTIVE_DOC_META';
  const hasFSAccess   = typeof window.showOpenFilePicker === 'function'; // feature detection
  const bcLegacy      = ('BroadcastChannel' in window) ? new BroadcastChannel('form-suite-doc') : null;
  const bcCanon       = ('BroadcastChannel' in window) ? new BroadcastChannel('fs-active-doc') : null;

  const tag  = (m) => ["%c[DocGuard]", "color:#6b7280;font-weight:600", m];
  const jget = (k) => { try { const v = localStorage.getItem(k); return v ? JSON.parse(v) : null; } catch { return null; } };

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
    try { bcLegacy?.postMessage({ type: 'doc-updated',  docId: meta?.docId || null, name: meta?.name || null, ts: Date.now() }); } catch {}
  }

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

    async function pickDocxFile() {
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
        setTimeout(() => { try { input.click(); } catch {} }, 0);
        setTimeout(() => { if (document.body.contains(input)) input.remove(); }, 60000);
      });
    }

    btnOpen.addEventListener('click', async () => {
      setStatus('Opening picker…');
      try {
        const { file, bytes, handle } = await pickDocxFile();
        const name = file?.name || 'document.docx';
        let meta;
        if (window.formSuitePersist?.setCurrentDoc) {
          meta = await window.formSuitePersist.setCurrentDoc({ bytes, handle, name });
        } else if (window.formSuitePersist?.setCurrentDocFromBytes) {
          meta = await window.formSuitePersist.setCurrentDocFromBytes(bytes, { name, handle });
        } else {
          // last-ditch: content-address here (rare path)
          const hex = await (async () => {
            const d = await crypto.subtle.digest('SHA-256', bytes.buffer || bytes);
            const u8 = new Uint8Array(d); let s=''; for (let i=0;i<u8.length;i++) s+=u8[i].toString(16).padStart(2,'0'); return s;
          })();
          const base = (name || 'document.docx').replace(/\.(docx|docm|dotx|dotm)$/i,'');
          meta = { docId: `${base}#${hex.slice(0,12)}`, name };
        }
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
    document.documentElement.style.overflow = 'hidden';
  }
  function hide() {
    if (!overlay) return;
    overlay.style.display = 'none';
    visible = false;
    document.documentElement.style.overflow = '';
  }
  function setStatus(s) { ensureOverlay()._setStatus?.(s); }

  async function checkAccess(tryHandleFallback = false) {
    const meta = jget(ACTIVE_LS_KEY);
    const docId = meta?.docId;
    if (!docId) return false;

    try {
      let bytes = await window.formSuitePersist?.getBytes?.(docId);
      if (bytes?.byteLength) return true;

      bytes = await window.formSuitePersist?.getCurrentDocBytes?.();
      if (bytes?.byteLength) return true;

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

  async function evaluateAndMaybeShow(source) {
    const meta = jget(ACTIVE_LS_KEY);
    const hasDoc = !!meta?.docId;
    if (!hasDoc) {
      show('(no document)');
      setStatus('No active document.');
      return;
    }
    const ok = await checkAccess(false);
    if (!ok) {
      show(meta?.name || '(no name)');
      setStatus('Lost access to current DOCX.');
    } else if (visible) {
      hide();
    }
  }

  bcCanon?.addEventListener('message', (ev) => {
    const m = ev?.data || {};
    if (m.type === 'active:clear') { show('(cleared)'); setStatus('Workspace cleared.'); }
    if (m.type === 'active:set')      evaluateAndMaybeShow('bcCanon active:set');
    if (m.type === 'active:updated')  evaluateAndMaybeShow('bcCanon active:updated');
  });
  bcLegacy?.addEventListener('message', (ev) => {
    const m = ev?.data || {};
    if (m.type === 'doc-cleared') { show('(cleared)'); setStatus('Workspace cleared.'); }
    if (m.type === 'doc-switched' || m.type === 'doc-updated') {
      evaluateAndMaybeShow('bcLegacy ' + m.type);
    }
  });

  window.addEventListener('storage', (e) => {
    if (e.key === ACTIVE_LS_KEY) evaluateAndMaybeShow('storage');
  });

  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') evaluateAndMaybeShow('visibilitychange');
  });
  window.addEventListener('focus', () => evaluateAndMaybeShow('focus'));
  setInterval(() => { if (!visible) evaluateAndMaybeShow('heartbeat'); }, 25000);
  window.addEventListener('DOMContentLoaded', () => { evaluateAndMaybeShow('DOMContentLoaded'); });

  window.fsDocGuard = {
    show, hide,
    ping: () => evaluateAndMaybeShow('manual'),
    open: () => ensureOverlay().querySelector('#fsdgOpen')?.click(),
    clear: () => ensureOverlay().querySelector('#fsdgClear')?.click(),
  };
})();
