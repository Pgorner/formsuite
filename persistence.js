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

  // Expose
  window.formSuitePersist = api;
  window.formSuiteUtils = Object.freeze(utils);
})();
