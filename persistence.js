/* ============================================================================
 * Form Suite — persistence.js (CANONICAL / HARDENED)
 * ----------------------------------------------------------------------------
 * Design goals:
 *  1) One coordination layer: window.formSuitePersist
 *  2) One broadcast mechanism: BroadcastChannel("fs-active-doc")
 *  3) One payload mirror: state.payload.CRONOS_PAYLOAD (ONLY; derived here)
 *  4) Bytes persistence is robust:
 *      - Primary: OPFS working copy (safe filename; no illegal chars)
 *      - Secondary: IndexedDB bytes store (structured-clone ArrayBuffer)
 *      - Tertiary: FileSystemHandle (optional; for rehydration)
 *  5) Identity is stable:
 *      - Prefer embedded DOC GUID in DOCX (docVar/custom prop)
 *        BUT: detect collisions (copied templates) and mint a new docId
 *      - Else: handle mapping (IDB) if same file
 *      - Else: fingerprint map (LS) for exact bytes
 *      - Else: mint new fsdoc:UUID
 *  6) Debuggability:
 *      - Every critical path logs: OPFS, IDB, handle, permissions, state
 *      - Includes explicit “why we missed” logs for bytes
 *
 * Dependencies (optional):
 *  - docx-core.js:
 *      window.readDocVarSettings / window.readDocVarCustom
 *      window.writeDocVarSettings / window.writeDocVarCustom
 * ----------------------------------------------------------------------------
 * Public API: window.formSuitePersist
 * ============================================================================
 */
(() => {
  // ===========================================================================
  // DEBUG / TRACE
  // ===========================================================================
  const DEBUG = {
    on: true,
    seq: 0,
    // Flip these to reduce noise
    verbose: true,
    logStatePayload: true,
    logStorageOps: true
  };

  const _t = () => new Date().toISOString().slice(11, 23);
  const _tag = (name) => `%c[Persist ${_t()} #${++DEBUG.seq}] ${name}`;
  const _style = 'color:#2563eb;font-weight:700';

  function TRACE(name, details) {
    const label = `${name} :: ${_t()} :: #${DEBUG.seq + 1}`;
    if (DEBUG.on) {
      try { console.groupCollapsed(_tag(name), _style, details ?? ''); } catch {}
      try { console.time(label); } catch {}
    }
    let ended = false;
    const api = {
      step(msg, data) { if (!DEBUG.on) return; try { console.log(_tag(`  ↳ ${msg}`), _style, data ?? ''); } catch {} },
      info(msg, data) { if (!DEBUG.on) return; try { console.log(_tag(`  • ${msg}`), _style, data ?? ''); } catch {} },
      warn(msg, data) { if (!DEBUG.on) return; try { console.warn(_tag(`  ⚠ ${msg}`), _style, data ?? ''); } catch {} },
      error(msg, err) { if (!DEBUG.on) return; try { console.error(_tag(`  ✖ ${msg}`), _style, err); } catch {} },
      end(extra) {
        if (!DEBUG.on || ended) return;
        ended = true;
        if (extra !== undefined) { try { console.log(_tag('done'), _style, extra); } catch {} }
        try { console.timeEnd(label); } catch {}
        try { console.groupEnd(); } catch {}
      }
    };
    return api;
  }

  window.addEventListener('error', (e) => {
    if (!DEBUG.on) return;
    try {
      console.error(_tag('window.error'), _style, {
        message: e.message, filename: e.filename, lineno: e.lineno, colno: e.colno, error: e.error
      });
    } catch {}
  });

  window.addEventListener('unhandledrejection', (e) => {
    if (!DEBUG.on) return;
    try { console.error(_tag('window.unhandledrejection'), _style, e.reason); } catch {}
  });

  // ===========================================================================
  // CONSTANTS (CANONICAL)
  // ===========================================================================
  const LS_KEYS = {
    ACTIVE_META: 'FS_ACTIVE_DOC_META',          // { docId, name }
    STATE_PREFIX: 'FS_state_',                  // per docId -> state JSON
    FP_MAP: 'FS_DOC_FINGERPRINT_MAP_V2',        // { [sha256]: { docId, ts } }
  };

  const BC_NAME = 'fs-active-doc';
  const bc = ('BroadcastChannel' in window) ? new BroadcastChannel(BC_NAME) : null;

  const DB_NAME = 'formsuite_v2';
  const DB_VER = 2;
  const DB_STORE_HANDLES = 'handles'; // { docId, handle }
  const DB_STORE_BYTES = 'bytes';     // { docId, ab, ts, len }
  const DB_STORE_META = 'meta';       // { k, v } future-proof

  const PAYLOAD_KEY = 'CRONOS_PAYLOAD';
  const DOC_GUID_KEY = 'FS_DOC_GUID';
  const STABLE_DOCID_PREFIX = 'fsdoc:';
  const OPFS_EXT = '.docx';

  const TAB_ID = (() => {
    try { return crypto?.randomUUID?.() || (Date.now() + '_' + Math.random().toString(16).slice(2)); }
    catch { return (Date.now() + '_' + Math.random().toString(16).slice(2)); }
  })();

  // ===========================================================================
  // BASIC UTILS
  // ===========================================================================
  const isPlainObject = (o) => !!o && typeof o === 'object' && !Array.isArray(o);
  const _nowIso = () => { try { return new Date().toISOString(); } catch { return ''; } };
  const _safeArr = (v) => Array.isArray(v) ? v.slice() : (v == null ? [] : [v]);

  function readJSON(key) {
    try {
      const raw = localStorage.getItem(key);
      if (!raw) return null;
      return JSON.parse(raw);
    } catch {
      return null;
    }
  }
  function writeJSON(key, value) {
    try {
      if (value == null) localStorage.removeItem(key);
      else localStorage.setItem(key, JSON.stringify(value));
    } catch {}
  }

  // Deep merge (objects merge, arrays replace)
  function deepMerge(a, b) {
    if (Array.isArray(b)) return b.slice();
    if (!isPlainObject(b)) return (b === undefined) ? a : b;
    const out = isPlainObject(a) ? { ...a } : {};
    for (const [k, v] of Object.entries(b)) out[k] = deepMerge(a?.[k], v);
    return out;
  }

  function isEmptyState(st) {
    if (!st || typeof st !== 'object') return true;
    const keys = Object.keys(st).filter(k => !String(k).startsWith('__'));
    return keys.length === 0;
  }

  function keyForState(docId) {
    return LS_KEYS.STATE_PREFIX + String(docId || '');
  }

  function normalizeStableDocId(raw) {
    if (!raw) return null;
    const s = String(raw).trim();
    if (!s) return null;
    if (s.startsWith(STABLE_DOCID_PREFIX)) return s;
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(s)) {
      return STABLE_DOCID_PREFIX + s;
    }
    return null;
  }

  function makeStableDocId() {
    try { if (crypto?.randomUUID) return STABLE_DOCID_PREFIX + crypto.randomUUID(); } catch {}
    const u8 = new Uint8Array(16);
    try { crypto.getRandomValues(u8); } catch { for (let i = 0; i < 16; i++) u8[i] = Math.floor(Math.random() * 256); }
    u8[6] = (u8[6] & 0x0f) | 0x40;
    u8[8] = (u8[8] & 0x3f) | 0x80;
    const hex = Array.from(u8).map(b => b.toString(16).padStart(2, '0')).join('');
    return STABLE_DOCID_PREFIX + (
      hex.slice(0, 8) + '-' + hex.slice(8, 12) + '-' + hex.slice(12, 16) + '-' + hex.slice(16, 20) + '-' + hex.slice(20)
    );
  }

  async function sha256Hex(bufOrU8) {
    const tr = TRACE('sha256Hex', { type: bufOrU8?.constructor?.name, len: bufOrU8?.byteLength || bufOrU8?.length });
    try {
      const ab = (bufOrU8 instanceof ArrayBuffer)
        ? bufOrU8
        : (bufOrU8?.buffer instanceof ArrayBuffer)
          ? bufOrU8.buffer
          : new Uint8Array(bufOrU8 || []).buffer;

      const d = await crypto.subtle.digest('SHA-256', ab);
      const hex = [...new Uint8Array(d)].map(b => b.toString(16).padStart(2, '0')).join('');
      tr.end({ hex12: hex.slice(0, 12) });
      return hex;
    } catch (e) {
      tr.error('hash failed', e);
      tr.end();
      return '(hash-error)';
    }
  }

  // ===========================================================================
  // FINGERPRINT MAP (LS utility)
  // ===========================================================================
  function _readFpMap() {
    try {
      const raw = localStorage.getItem(LS_KEYS.FP_MAP);
      const obj = raw ? JSON.parse(raw) : {};
      return (obj && typeof obj === 'object') ? obj : {};
    } catch {
      return {};
    }
  }
  function _writeFpMap(map) {
    try { localStorage.setItem(LS_KEYS.FP_MAP, JSON.stringify(map || {})); } catch {}
  }
  function getMappedDocIdForFingerprint(fpHex) {
    if (!fpHex || fpHex === '(hash-error)') return null;
    const m = _readFpMap();
    const v = m[fpHex];
    const id = (typeof v === 'string') ? v : (v && typeof v === 'object') ? v.docId : null;
    return normalizeStableDocId(id);
  }
  function setMappedDocIdForFingerprint(fpHex, docId) {
    const norm = normalizeStableDocId(docId);
    if (!fpHex || fpHex === '(hash-error)' || !norm) return;
    const m = _readFpMap();
    m[fpHex] = { docId: norm, ts: Date.now() };

    // Bound growth (best effort)
    try {
      const entries = Object.entries(m);
      if (entries.length > 350) {
        entries.sort((a, b) => ((a[1]?.ts || 0) - (b[1]?.ts || 0)));
        const keep = entries.slice(-250);
        const nm = {};
        for (const [k, v] of keep) nm[k] = v;
        _writeFpMap(nm);
        return;
      }
    } catch {}

    _writeFpMap(m);
  }

  // ===========================================================================
  // IDB (handles + bytes)
  // ===========================================================================
  function openDB() {
    return new Promise((resolve, reject) => {
      const tr = TRACE('openDB', { DB_NAME, DB_VER });
      const req = indexedDB.open(DB_NAME, DB_VER);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains(DB_STORE_HANDLES)) {
          db.createObjectStore(DB_STORE_HANDLES, { keyPath: 'docId' });
        }
        if (!db.objectStoreNames.contains(DB_STORE_BYTES)) {
          db.createObjectStore(DB_STORE_BYTES, { keyPath: 'docId' });
        }
        if (!db.objectStoreNames.contains(DB_STORE_META)) {
          db.createObjectStore(DB_STORE_META, { keyPath: 'k' });
        }
      };
      req.onsuccess = () => { tr.end('ok'); resolve(req.result); };
      req.onerror = () => { tr.error('idb open failed', req.error); tr.end(); reject(req.error); };
    });
  }

  async function idbPut(store, keyObj) {
    const tr = TRACE('idbPut', { store, key: keyObj?.docId ?? keyObj?.k ?? '(unknown)' });
    try {
      const db = await openDB();
      await new Promise((res, rej) => {
        const tx = db.transaction(store, 'readwrite');
        tx.objectStore(store).put(keyObj);
        tx.oncomplete = res;
        tx.onerror = () => rej(tx.error);
      });
      tr.end('stored');
      return true;
    } catch (e) {
      tr.warn('idbPut failed', e);
      tr.end('fail');
      return false;
    }
  }

  async function idbGet(store, key) {
    const tr = TRACE('idbGet', { store, key });
    try {
      const db = await openDB();
      const v = await new Promise((res, rej) => {
        const tx = db.transaction(store, 'readonly');
        const req = tx.objectStore(store).get(key);
        req.onsuccess = () => res(req.result || null);
        req.onerror = () => rej(req.error);
      });
      tr.end({ ok: !!v });
      return v;
    } catch (e) {
      tr.warn('idbGet failed', e);
      tr.end('fail');
      return null;
    }
  }

  async function idbPutHandle(docId, handle) {
    const tr = TRACE('idbPutHandle', { docId, hasHandle: !!handle });
    if (!docId || !handle) { tr.end('skip'); return false; }
    return idbPut(DB_STORE_HANDLES, { docId, handle, ts: Date.now() });
  }

  async function idbGetHandle(docId) {
    const tr = TRACE('idbGetHandle', { docId });
    if (!docId) { tr.end('no docId'); return null; }
    const rec = await idbGet(DB_STORE_HANDLES, docId);
    const h = rec?.handle || null;
    tr.end({ hasHandle: !!h, kind: h?.kind || null });
    return h;
  }

  async function idbPutBytes(docId, ab) {
    const tr = TRACE('idbPutBytes', { docId, len: ab?.byteLength || 0 });
    if (!docId || !ab) { tr.end('skip'); return false; }
    return idbPut(DB_STORE_BYTES, { docId, ab, len: ab.byteLength, ts: Date.now() });
  }

  async function idbGetBytes(docId) {
    const tr = TRACE('idbGetBytes', { docId });
    if (!docId) { tr.end('no docId'); return null; }
    const rec = await idbGet(DB_STORE_BYTES, docId);
    const ab = rec?.ab || null;
    tr.end({ ok: !!ab, len: ab?.byteLength || 0, ts: rec?.ts || null });
    return ab;
  }

  async function findDocIdByHandle(handle) {
    const tr = TRACE('findDocIdByHandle', { hasHandle: !!handle });
    try {
      if (!handle || typeof handle.isSameEntry !== 'function') { tr.end('unsupported'); return null; }
      const db = await openDB();
      const recs = await new Promise((res, rej) => {
        const items = [];
        const tx = db.transaction(DB_STORE_HANDLES, 'readonly');
        const store = tx.objectStore(DB_STORE_HANDLES);
        const req = store.openCursor();
        req.onsuccess = () => {
          const c = req.result;
          if (c) { items.push(c.value); c.continue(); }
        };
        req.onerror = () => rej(req.error);
        tx.oncomplete = () => res(items);
        tx.onerror = () => rej(tx.error);
      });

      for (const r of recs) {
        const h = r?.handle;
        if (!h || typeof h.isSameEntry !== 'function') continue;
        try {
          if (await h.isSameEntry(handle)) {
            tr.end({ docId: r.docId });
            return r.docId;
          }
        } catch {}
      }
      tr.end('no match');
      return null;
    } catch (e) {
      tr.warn('find by handle failed', e);
      tr.end('fail');
      return null;
    }
  }

  // ===========================================================================
  // OPFS (working copy) — SAFE FILENAMES
  // ===========================================================================
  function opfsFileNameForDocId(docId) {
    // OPFS filenames should be conservative: avoid ":" and any odd characters.
    const safe = String(docId || '')
      .replace(/^fsdoc:/i, 'fsdoc_')
      .replace(/[^a-z0-9._-]/gi, '_');
    return `${safe}${OPFS_EXT}`;
  }

  async function opfsRoot() {
    const tr = TRACE('opfsRoot');
    try {
      const root = await navigator.storage.getDirectory();
      tr.end({ ok: !!root });
      return root;
    } catch (e) {
      tr.warn('no OPFS root', e);
      tr.end({ ok: false });
      return null;
    }
  }

  async function opfsPut(docId, bytes) {
    const tr = TRACE('opfsPut', { docId, len: bytes?.byteLength || bytes?.length });
    try {
      const root = await opfsRoot();
      if (!root) { tr.end('no root'); return false; }

      const u8 = (bytes instanceof Uint8Array) ? bytes : new Uint8Array(bytes || []);
      const fname = opfsFileNameForDocId(docId);
      tr.step('fname', fname);

      const fh = await root.getFileHandle(fname, { create: true });
      const w = await fh.createWritable();
      await w.write(u8);
      await w.close();

      tr.end({ ok: true, len: u8.byteLength, fname });
      return true;
    } catch (e) {
      tr.warn('opfs put failed', e);
      tr.end({ ok: false });
      return false;
    }
  }

  async function opfsGet(docId) {
    const tr = TRACE('opfsGet', { docId });
    try {
      const root = await opfsRoot();
      if (!root) { tr.end('no root'); return null; }

      const fname = opfsFileNameForDocId(docId);
      tr.step('fname', fname);

      const fh = await root.getFileHandle(fname, { create: false });
      const f = await fh.getFile();
      const ab = await f.arrayBuffer();

      tr.end({ ok: true, len: ab?.byteLength || 0, fname });
      return ab || null;
    } catch (e) {
      tr.end('miss');
      return null;
    }
  }

  // ===========================================================================
  // PERMISSIONS (HANDLE OPTIONAL)
  // ===========================================================================
  async function ensurePermission(handle, mode = 'readwrite') {
    const tr = TRACE('ensurePermission', { mode, hasHandle: !!handle });
    try {
      if (!handle) { tr.end('no handle'); return 'denied'; }
      let q = await handle.queryPermission?.({ mode });
      if (q === 'granted') { tr.end('granted'); return 'granted'; }
      if (q === 'prompt') q = await handle.requestPermission?.({ mode }) || q;
      tr.end({ result: q || 'denied' });
      return q || 'denied';
    } catch (e) {
      tr.warn('permission check failed', e);
      tr.end('denied');
      return 'denied';
    }
  }

  // ===========================================================================
  // ACTIVE DOC (single source of truth)
  // ===========================================================================
  let _active = null; // { docId, name }
  const activeListeners = new Set();
  const stateListeners = new Set();

  function emitActive(meta) { for (const fn of activeListeners) { try { fn(meta); } catch {} } }
  function emitState(msg) { for (const fn of stateListeners) { try { fn(msg); } catch {} } }

  function getActiveDocMeta() {
    if (_active?.docId) return _active;
    const ls = readJSON(LS_KEYS.ACTIVE_META);
    if (ls?.docId) _active = ls;
    return _active;
  }

  function _writeActive(metaOrNull) {
    const tr = TRACE('_writeActive', metaOrNull);
    try {
      _active = metaOrNull;
      writeJSON(LS_KEYS.ACTIVE_META, metaOrNull);
      tr.end('written');
    } catch (e) {
      tr.warn('write active failed', e);
      tr.end('fail');
    }
  }

  async function setActiveDoc(metaOrNull, { broadcast = true } = {}) {
    const tr = TRACE('setActiveDoc', metaOrNull);
    try {
      if (!metaOrNull?.docId) {
        _writeActive(null);
        if (broadcast) {
          try { bc?.postMessage({ type: 'active:clear', from: TAB_ID, ts: Date.now() }); } catch {}
        }
        emitActive(null);
        tr.end('cleared');
        return null;
      }

      const next = { docId: metaOrNull.docId, name: metaOrNull.name || 'document.docx' };
      _writeActive(next);

      if (broadcast) {
        try { bc?.postMessage({ type: 'active:set', docId: next.docId, name: next.name, from: TAB_ID, ts: Date.now() }); } catch {}
      }
      emitActive(next);
      tr.end(next);
      return next;
    } catch (e) {
      tr.error('setActiveDoc failed', e);
      tr.end('fail');
      return null;
    }
  }

  function broadcastActiveUpdated(docId, name) {
    const tr = TRACE('broadcastActiveUpdated', { docId, name });
    try {
      bc?.postMessage({ type: 'active:updated', docId, name, from: TAB_ID, ts: Date.now() });
      tr.end('sent');
    } catch (e) {
      tr.warn('broadcast updated failed', e);
      tr.end('fail');
    }
  }

  window.addEventListener('storage', (e) => {
    if (e.key !== LS_KEYS.ACTIVE_META) return;
    const tr = TRACE('storage:ACTIVE_META', { hasNew: !!e.newValue });
    try {
      const meta = e.newValue ? JSON.parse(e.newValue) : null;
      _active = meta;
      emitActive(_active);
      tr.end(meta);
    } catch (err) {
      tr.warn('parse active meta failed', err);
      tr.end('fail');
    }
  });

  bc?.addEventListener('message', (ev) => {
    const m = ev?.data || {};
    if (!m || typeof m !== 'object') return;
    if (m.from === TAB_ID) return;

    const tr = TRACE('BC:message', m);
    try {
      if (m.type === 'active:set') {
        _active = { docId: m.docId, name: m.name || 'document.docx' };
        writeJSON(LS_KEYS.ACTIVE_META, _active);
        emitActive(_active);
      } else if (m.type === 'active:clear') {
        _active = null;
        writeJSON(LS_KEYS.ACTIVE_META, null);
        emitActive(null);
      } else if (m.type === 'active:updated') {
        emitState({ docId: m.docId, kind: 'bytes-updated', from: m.from, ts: m.ts });
      } else if (m.type === 'state:patched') {
        emitState({ docId: m.docId, kind: 'state-patched', patch: m.patch || {}, from: m.from, ts: m.ts });
      } else if (m.type === 'rules-updated') {
        emitState({ docId: m.docId, kind: 'rules-updated', from: m.from, ts: m.ts });
      }
      tr.end('handled');
    } catch (e) {
      tr.warn('BC handler failed', e);
      tr.end('fail');
    }
  });

  // ===========================================================================
  // STATE + CANONICAL PAYLOAD MIRROR
  // ===========================================================================
  function buildCanonicalPayloadFromState(state) {
    const schema = isPlainObject(state?.schema) ? state.schema : {};
    const fields = Array.isArray(schema.fields) ? schema.fields : [];
    return {
      title: schema.title || 'Form',
      fields,
      values: isPlainObject(state?.values) ? state.values : {},
      tagMap: isPlainObject(state?.tagMap) ? state.tagMap : {},
      rules: Array.isArray(state?.rules) ? state.rules : [],
      fieldRules: Array.isArray(state?.fieldRules) ? state.fieldRules : [],
      updatedAt: _nowIso()
    };
  }

  function normalizeState(docId, stIn) {
    const tr = TRACE('normalizeState', { docId });
    try {
      const st = isPlainObject(stIn) ? stIn : {};
      if (!isPlainObject(st.schema)) st.schema = {};
      if (!isPlainObject(st.values)) st.values = {};
      if (!isPlainObject(st.tagMap)) st.tagMap = {};
      if (!Array.isArray(st.rules)) st.rules = [];
      if (!Array.isArray(st.fieldRules)) st.fieldRules = [];
      if (!isPlainObject(st.payload)) st.payload = {};

      st.payload[PAYLOAD_KEY] = buildCanonicalPayloadFromState(st);
      st.__v = (st.__v | 0) + 1;

      if (DEBUG.logStatePayload) {
        try {
          console.log('[DBG persist.normalizeState]', {
            docId,
            v: st.__v,
            rulesLen: st.rules.length,
            fieldRulesLen: st.fieldRules.length,
            payloadRulesLen: st.payload?.[PAYLOAD_KEY]?.rules?.length ?? null
          });
        } catch {}
      }

      tr.end({ v: st.__v });
      return st;
    } catch (e) {
      tr.warn('normalizeState failed', e);
      tr.end('fail');
      return {
        schema: {},
        values: {},
        tagMap: {},
        rules: [],
        fieldRules: [],
        payload: { [PAYLOAD_KEY]: { title: 'Form', fields: [], values: {}, tagMap: {}, rules: [], fieldRules: [], updatedAt: _nowIso() } },
        __v: 1
      };
    }
  }

  function getState(docId) {
    const tr = TRACE('getState', { docId });
    try {
      if (!docId) { tr.end('no docId'); return {}; }
      const st = readJSON(keyForState(docId)) || {};
      const out = normalizeState(docId, st);
      tr.end({ v: out.__v, keys: Object.keys(out || {}).length });
      return out;
    } catch (e) {
      tr.warn('getState failed', e);
      tr.end('fail');
      return {};
    }
  }

  function setState(docId, patch, { broadcast = true } = {}) {
    const tr = TRACE('setState', { docId, patchKeys: Object.keys(patch || {}) });
    try {
      if (!docId) { tr.end('no docId'); return {}; }
      if (!patch || typeof patch !== 'object') { tr.end('no patch'); return getState(docId); }

      const prev = readJSON(keyForState(docId)) || {};
      const normalizedPatch = { ...patch };
      if ('rules' in normalizedPatch) normalizedPatch.rules = _safeArr(normalizedPatch.rules);
      if ('fieldRules' in normalizedPatch) normalizedPatch.fieldRules = _safeArr(normalizedPatch.fieldRules);

      const merged = deepMerge(prev, normalizedPatch);
      const next = normalizeState(docId, merged);

      writeJSON(keyForState(docId), next);

      if (DEBUG.on && DEBUG.logStorageOps) {
        try { console.log('[DBG persist.setState]', { docId, v: next.__v }); } catch {}
      }

      if (broadcast) {
        try { bc?.postMessage({ type: 'state:patched', docId, patch: normalizedPatch, from: TAB_ID, ts: Date.now() }); } catch {}
      }
      emitState({ docId, kind: 'state-patched', patch: normalizedPatch, from: TAB_ID, ts: Date.now() });

      tr.end({ v: next.__v });
      return next;
    } catch (e) {
      tr.error('setState failed', e);
      tr.end('fail');
      return {};
    }
  }

  async function saveState(docId, patch, opts) {
    const tr = TRACE('saveState', { docId });
    try {
      const out = setState(docId, patch, opts);
      tr.end({ v: out?.__v });
      return out;
    } catch (e) {
      tr.error('saveState failed', e);
      tr.end('fail');
      return {};
    }
  }

  async function loadState(docId) {
    const tr = TRACE('loadState', { docId });
    try {
      const st = getState(docId);
      if (DEBUG.on) {
        try {
          console.log('[DBG persist.loadState]', {
            docId, v: st.__v,
            rulesLen: st.rules?.length ?? null,
            fieldRulesLen: st.fieldRules?.length ?? null,
            hasPayload: !!st?.payload?.[PAYLOAD_KEY]
          });
        } catch {}
      }
      tr.end({ v: st.__v });
      return st;
    } catch (e) {
      tr.warn('loadState failed', e);
      tr.end('fail');
      return {};
    }
  }

  function getLastKnownValues(docId) {
    try { return (getState(docId) || {}).values || {}; } catch { return {}; }
  }

  // ===========================================================================
  // DOCX GUID + PAYLOAD (optional)
  // ===========================================================================
  async function readDocVar(bytes, key) {
    const tr = TRACE('readDocVar', { key, len: bytes?.byteLength || bytes?.length });
    try {
      if (!bytes) { tr.end('no bytes'); return null; }
      if (typeof window.readDocVarSettings === 'function') {
        const v = await window.readDocVarSettings(bytes, key);
        if (v != null) { tr.end({ via: 'settings' }); return v; }
      }
      if (typeof window.readDocVarCustom === 'function') {
        const v = await window.readDocVarCustom(bytes, key);
        if (v != null) { tr.end({ via: 'custom' }); return v; }
      }
      tr.end('no reader');
      return null;
    } catch (e) {
      tr.warn('readDocVar failed', e);
      tr.end('fail');
      return null;
    }
  }

  async function writeDocVar(bytes, key, value) {
    const tr = TRACE('writeDocVar', { key, valueLen: String(value ?? '').length });
    try {
      if (!bytes) { tr.end('no bytes'); return bytes; }
      if (typeof window.writeDocVarSettings === 'function') {
        const out = await window.writeDocVarSettings(bytes, key, value);
        tr.end({ via: 'settings' });
        return out;
      }
      if (typeof window.writeDocVarCustom === 'function') {
        const out = await window.writeDocVarCustom(bytes, key, value);
        tr.end({ via: 'custom' });
        return out;
      }
      tr.end('no writer');
      return bytes;
    } catch (e) {
      tr.warn('writeDocVar failed', e);
      tr.end('fail');
      return bytes;
    }
  }

  async function readPayloadFromDocx(bytes) {
    const tr = TRACE('readPayloadFromDocx');
    try {
      const raw = await readDocVar(bytes, PAYLOAD_KEY);
      tr.end({ has: raw != null, len: raw ? String(raw).length : 0 });
      return raw;
    } catch (e) {
      tr.warn('read payload failed', e);
      tr.end('fail');
      return null;
    }
  }

  async function readDocGuidFromDocx(bytes) {
    const tr = TRACE('readDocGuidFromDocx');
    try {
      const raw = await readDocVar(bytes, DOC_GUID_KEY);
      const norm = normalizeStableDocId(raw);
      tr.end({ raw: raw || null, norm: norm || null });
      return norm;
    } catch (e) {
      tr.warn('read doc guid failed', e);
      tr.end('fail');
      return null;
    }
  }

  async function writeDocGuidToDocx(bytes, stableDocId) {
    const tr = TRACE('writeDocGuidToDocx', { stableDocId });
    try {
      const norm = normalizeStableDocId(stableDocId);
      if (!norm) { tr.end('invalid'); return bytes; }
      const raw = norm.startsWith(STABLE_DOCID_PREFIX) ? norm.slice(STABLE_DOCID_PREFIX.length) : norm;
      const out = await writeDocVar(bytes, DOC_GUID_KEY, raw);
      tr.end('written');
      return out;
    } catch (e) {
      tr.warn('write doc guid failed', e);
      tr.end('fail');
      return bytes;
    }
  }

  // ===========================================================================
  // HYDRATION (safe only if empty schema)
  // ===========================================================================
  let __hydrating = false;

  async function hydrateFromDocxIfEmpty(docId) {
    const tr = TRACE('hydrateFromDocxIfEmpty', { docId, locked: __hydrating });
    if (!docId || __hydrating) { tr.end('skip'); return false; }
    __hydrating = true;
    try {
      const st = await loadState(docId);
      const hasSchema = Array.isArray(st?.schema?.fields) && st.schema.fields.length > 0;
      if (hasSchema) { tr.end('already has schema'); return false; }

      const bytes = await getBytes(docId);
      if (!bytes) { tr.end('no bytes'); return false; }

      const raw = await readPayloadFromDocx(bytes);
      if (!raw) { tr.end('no payload in docx'); return false; }

      let payload = null;
      try { payload = JSON.parse(raw); } catch { payload = null; }
      if (!payload || !Array.isArray(payload.fields) || payload.fields.length === 0) {
        tr.end('payload invalid/empty');
        return false;
      }

      const schema = { title: payload.title || 'Form', fields: payload.fields };
      const values = isPlainObject(payload.values) ? payload.values : {};
      const tagMap = isPlainObject(payload.tagMap) ? payload.tagMap : {};
      const rules = Array.isArray(payload.rules) ? payload.rules : [];
      const fieldRules = Array.isArray(payload.fieldRules) ? payload.fieldRules : [];

      setState(docId, { schema, values, tagMap, rules, fieldRules, hydratedAt: _nowIso() }, { broadcast: true });
      tr.end({ fields: schema.fields.length, rulesLen: rules.length, fieldRulesLen: fieldRules.length });
      return true;
    } catch (e) {
      tr.error('hydrate failed', e);
      tr.end('fail');
      return false;
    } finally {
      __hydrating = false;
    }
  }

  async function ensureHydrated() {
    const tr = TRACE('ensureHydrated');
    try {
      const meta = getActiveDocMeta();
      if (!meta?.docId) { tr.end('no active'); return false; }
      const ok = await hydrateFromDocxIfEmpty(meta.docId);
      tr.end({ ok });
      return ok;
    } catch (e) {
      tr.warn('ensureHydrated failed', e);
      tr.end('fail');
      return false;
    }
  }

  // ===========================================================================
  // BYTES API (hardened): OPFS -> IDB bytes -> handle -> MISS
  // ===========================================================================
  async function getBytes(docId) {
    const tr = TRACE('getBytes', { docId });
    try {
      if (!docId) { tr.end('no docId'); return null; }

      // 1) OPFS (canonical)
      const opfs = await opfsGet(docId);
      if (opfs) { tr.end({ via: 'opfs', len: opfs.byteLength }); return opfs; }

      // 2) IDB bytes fallback
      const idb = await idbGetBytes(docId);
      if (idb) {
        // Best-effort: re-seed OPFS for faster next time
        await opfsPut(docId, new Uint8Array(idb));
        tr.end({ via: 'idb', len: idb.byteLength });
        return idb;
      }

      // 3) Handle fallback
      const h = await idbGetHandle(docId);
      if (h?.getFile) {
        try {
          const f = await h.getFile();
          const ab = await f.arrayBuffer();
          // persist everywhere best-effort
          await opfsPut(docId, new Uint8Array(ab));
          await idbPutBytes(docId, ab);
          tr.end({ via: 'handle', len: ab.byteLength });
          return ab;
        } catch (e) {
          tr.warn('handle read failed', e);
        }
      }

      // MISS (explicit why)
      tr.warn('MISS bytes', {
        docId,
        opfs: 'miss',
        idbBytes: 'miss',
        handle: h ? 'present-but-unreadable' : 'none'
      });
      tr.end('miss');
      return null;
    } catch (e) {
      tr.error('getBytes failed', e);
      tr.end('fail');
      return null;
    }
  }

  async function getCurrentDocBytes() {
    const tr = TRACE('getCurrentDocBytes');
    try {
      const meta = getActiveDocMeta();
      if (!meta?.docId) { tr.end('no active'); return null; }
      const ab = await getBytes(meta.docId);
      tr.end({ ok: !!ab, len: ab?.byteLength || 0 });
      return ab;
    } catch (e) {
      tr.warn('getCurrentDocBytes failed', e);
      tr.end('fail');
      return null;
    }
  }

  async function putBytes(docId, bytes, { broadcast = true } = {}) {
    const tr = TRACE('putBytes', { docId, len: bytes?.byteLength || bytes?.length, broadcast });
    try {
      if (!docId) { tr.end('no docId'); return false; }
      const u8 = (bytes instanceof Uint8Array) ? bytes : new Uint8Array(bytes || []);
      const ab = u8.buffer.slice(u8.byteOffset, u8.byteOffset + u8.byteLength);

      const okOpfs = await opfsPut(docId, u8);
      const okIdb = await idbPutBytes(docId, ab);

      if (broadcast) {
        const meta = getActiveDocMeta();
        broadcastActiveUpdated(docId, meta?.name);
      }

      tr.end({ ok: !!(okOpfs || okIdb), opfs: okOpfs, idb: okIdb });
      return !!(okOpfs || okIdb);
    } catch (e) {
      tr.error('putBytes failed', e);
      tr.end('fail');
      return false;
    }
  }

  // ===========================================================================
  // IDENTITY + CURRENT DOC SETUP (hardened)
  // ===========================================================================
  /**
   * setCurrentDoc({ bytes|handle, name })
   *
   * Guarantees best-effort persistence:
   *  - writes bytes to OPFS and IDB bytes store
   *  - stores handle mapping (optional)
   *  - sets active meta (LS + BC)
   *  - initializes state if missing
   *  - attempts safe hydration (if schema empty)
   *
   * IMPORTANT: copied templates can carry the same embedded FS_DOC_GUID.
   * This function detects that collision and mints a new docId so opening a
   * new file always gets a clean workspace.
   */
  async function setCurrentDoc({ bytes, handle, name }) {
    const tr = TRACE('setCurrentDoc', { hasBytes: !!bytes, hasHandle: !!handle, name });
    try {
      let fileName = name || 'document.docx';
      let ab = bytes;

      // Normalize input
      if (!ab && handle?.getFile) {
        const f = await handle.getFile();
        fileName = name || f.name || fileName;
        ab = await f.arrayBuffer();
      }
      if (!ab) throw new Error('setCurrentDoc: bytes or readable handle required');

      const u8 = (ab instanceof Uint8Array) ? ab : new Uint8Array(ab);

      // Fingerprint (of the incoming file)
      const fp = await sha256Hex(u8);
      const fromFp = getMappedDocIdForFingerprint(fp);

      // Embedded GUID (if any)
      const embedded = await readDocGuidFromDocx(u8);

      // Handle mapping (if any)
      const mappedByHandle = handle ? (await findDocIdByHandle(handle)) : null;

      // -----------------------------
      // COLLISION GUARD (core fix)
      // -----------------------------
      // If the file has an embedded GUID that already has persisted bytes/state,
      // but the incoming bytes are different (fingerprint mismatch), then this
      // is very likely a copied template (same FS_DOC_GUID). We must mint a new docId.
      let embeddedCollision = false;
      let embeddedWhy = null;

      if (embedded) {
        // 1) If this handle is known under another docId, embedded is wrong for this file
        if (mappedByHandle && normalizeStableDocId(mappedByHandle) && normalizeStableDocId(mappedByHandle) !== embedded) {
          embeddedCollision = true;
          embeddedWhy = { kind: 'handle-maps-elsewhere', mappedByHandle, embedded };
        }

        // 2) If fingerprint map says this file belongs to another docId, embedded is wrong
        if (!embeddedCollision && fromFp && fromFp !== embedded) {
          embeddedCollision = true;
          embeddedWhy = { kind: 'fingerprint-maps-elsewhere', fromFp, embedded };
        }

        // 3) If persisted bytes exist for embedded docId and hash differs => collision
        if (!embeddedCollision) {
          // Fast existence check: OPFS/IDB bytes for embedded
          const existingOpfs = await opfsGet(embedded);
          const existingIdb = existingOpfs ? null : await idbGetBytes(embedded);
          const existing = existingOpfs || existingIdb;

          if (existing && existing.byteLength) {
            const existingFp = await sha256Hex(existing);
            if (existingFp !== '(hash-error)' && fp !== '(hash-error)' && existingFp !== fp) {
              embeddedCollision = true;
              embeddedWhy = { kind: 'embedded-bytes-mismatch', embedded, fp12: fp.slice(0, 12), existingFp12: existingFp.slice(0, 12) };
            }
          } else {
            // Also treat "existing state with schema" as a hint of collision risk
            // (optional soft-signal; does not trigger alone)
          }
        }
      }

      if (embeddedCollision) {
        tr.warn('embedded GUID collision detected; minting new docId', embeddedWhy);
      }

      // Determine docId (priority: embedded if SAFE, else handle, else fp, else mint)
      let docId = null;

      // 1) embedded guid (only if NOT colliding)
      if (embedded && !embeddedCollision) docId = embedded;

      // 2) handle mapping (same file)
      if (!docId && mappedByHandle) docId = normalizeStableDocId(mappedByHandle);

      // 3) fp mapping (exact bytes)
      if (!docId && fromFp) docId = fromFp;

      // 4) mint
      if (!docId) docId = makeStableDocId();

      tr.step('identity', {
        docId,
        embedded: embedded || null,
        embeddedCollision,
        mappedByHandle: mappedByHandle || null,
        viaFp: !!fromFp
      });

      // Store handle mapping (optional)
      if (handle) {
        const okH = await idbPutHandle(docId, handle);
        tr.step('idbPutHandle', { ok: okH });
      }

      // Best-effort embed guid into working bytes:
      // - if missing OR collision OR embedded != docId, write docId back
      let workingU8 = u8;
      if (!embedded || embeddedCollision || embedded !== docId) {
        const maybe = await writeDocGuidToDocx(u8, docId);
        if (maybe && maybe !== u8) {
          workingU8 = (maybe instanceof Uint8Array) ? maybe : new Uint8Array(maybe);
        }
      }

      // Persist bytes HARD: OPFS + IDB
      const abWorking = workingU8.buffer.slice(workingU8.byteOffset, workingU8.byteOffset + workingU8.byteLength);

      const okOpfs = await opfsPut(docId, workingU8);
      const okIdb = await idbPutBytes(docId, abWorking);

      tr.step('persist bytes', { opfs: okOpfs, idb: okIdb, len: workingU8.byteLength });

      // Update fp mapping (best effort)
      try {
        setMappedDocIdForFingerprint(fp, docId);
        const fp2 = await sha256Hex(workingU8);
        if (fp2 && fp2 !== fp) setMappedDocIdForFingerprint(fp2, docId);
      } catch {}

      // Activate
      const meta = { docId, name: fileName };
      await setActiveDoc(meta, { broadcast: true });

      // Initialize state if empty
      const st = await loadState(docId);
      if (!st || isEmptyState(st) || !(Array.isArray(st?.schema?.fields))) {
        setState(docId, {
          schema: null,
          values: {},
          tagMap: {},
          rules: [],
          fieldRules: [],
          createdAt: _nowIso()
        }, { broadcast: false });
      }

      // Safe hydrate attempt (if empty schema)
      await hydrateFromDocxIfEmpty(docId);

      // Final verification: can we read bytes back now?
      const verify = await getBytes(docId);
      tr.step('verify getBytes after setCurrentDoc', {
        ok: !!verify,
        len: verify?.byteLength || 0
      });

      tr.end({ docId, name: fileName, fp12: (fp || '').slice(0, 12) });
      return meta;
    } catch (e) {
      tr.error('setCurrentDoc failed', e);
      tr.end('fail');
      throw e;
    }
  }

  async function setCurrentDocFromBytes(bytes, meta = {}) {
    return setCurrentDoc({ bytes, handle: meta.handle || null, name: meta.name || 'document.docx' });
  }

  // ===========================================================================
  // RULES APIs (storage only)
  // ===========================================================================
  function getRules(docId) {
    const st = getState(docId);
    return {
      rules: Array.isArray(st.rules) ? st.rules : [],
      fieldRules: Array.isArray(st.fieldRules) ? st.fieldRules : []
    };
  }

  async function overwriteRules(docId, { rules, fieldRules }, { broadcast = true } = {}) {
    const tr = TRACE('overwriteRules', { docId, rulesLen: rules?.length, fieldRulesLen: fieldRules?.length });
    try {
      if (!docId) throw new Error('overwriteRules: missing docId');
      const out = setState(docId, {
        rules: _safeArr(rules),
        fieldRules: _safeArr(fieldRules),
        rulesUpdatedAt: _nowIso()
      }, { broadcast });

      try { bc?.postMessage({ type: 'rules-updated', docId, from: TAB_ID, ts: Date.now() }); } catch {}

      tr.end({ v: out.__v });
      return out;
    } catch (e) {
      tr.error('overwriteRules failed', e);
      tr.end('fail');
      throw e;
    }
  }

  async function persistRules(docId, rules, fieldRules) {
    return overwriteRules(docId, { rules, fieldRules }, { broadcast: true });
  }

  // ===========================================================================
  // CONCURRENCY HELPERS
  // ===========================================================================
  const __locks = new Map(); // docId -> promise chain
  async function withDocLock(docId, fn) {
    const tr = TRACE('withDocLock', { docId });
    if (!docId) { tr.end('no docId'); return fn(); }

    const prev = __locks.get(docId) || Promise.resolve();
    let release;
    const gate = new Promise(r => (release = r));
    __locks.set(docId, prev.then(() => gate));

    try {
      const out = await fn();
      tr.end('ok');
      return out;
    } finally {
      try { release(); } catch {}
      // best-effort cleanup
      try { __locks.delete(docId); } catch {}
    }
  }

  const __coals = new Map();
  function coalesce(key, fn, delay = 120) {
    clearTimeout(__coals.get(key));
    const t = setTimeout(() => {
      __coals.delete(key);
      try { fn(); } catch {}
    }, delay);
    __coals.set(key, t);
  }

  // ===========================================================================
  // SUBSCRIPTIONS
  // ===========================================================================
  function onActiveChange(fn) { activeListeners.add(fn); return () => activeListeners.delete(fn); }
  function onStateChange(fn) { stateListeners.add(fn); return () => stateListeners.delete(fn); }

  // ===========================================================================
  // DIAGNOSTICS (public)
  // ===========================================================================
  async function diag(docId = null) {
    const tr = TRACE('diag', { docId });
    try {
      const meta = getActiveDocMeta();
      const id = docId || meta?.docId || null;

      const out = {
        tabId: TAB_ID,
        active: meta || null,
        docId: id,
        opfs: null,
        idbBytes: null,
        idbHandle: null
      };

      if (id) {
        const opfs = await opfsGet(id);
        out.opfs = { ok: !!opfs, len: opfs?.byteLength || 0, fname: opfsFileNameForDocId(id) };

        const idb = await idbGetBytes(id);
        out.idbBytes = { ok: !!idb, len: idb?.byteLength || 0 };

        const h = await idbGetHandle(id);
        out.idbHandle = { ok: !!h, kind: h?.kind || null };
      }

      tr.end(out);
      return out;
    } catch (e) {
      tr.error('diag failed', e);
      tr.end('fail');
      return null;
    }
  }

  // ===========================================================================
  // BOOTSTRAP
  // ===========================================================================
  (function boot() {
    const tr = TRACE('boot', { ua: navigator.userAgent });
    try {
      const meta = readJSON(LS_KEYS.ACTIVE_META);
      if (meta?.docId) {
        _active = meta;
        tr.step('restored active', meta);
        emitActive(meta);
      } else {
        tr.step('no active meta');
      }
    } catch (e) {
      tr.warn('boot failed', e);
    } finally {
      tr.end();
    }
  })();

  // ===========================================================================
  // PUBLIC API
  // ===========================================================================
  const api = {
    // constants
    ACTIVE_DOC_KEY: LS_KEYS.ACTIVE_META,
    BC_NAME,
    PAYLOAD_KEY,
    DOC_GUID_KEY,
    STABLE_DOCID_PREFIX,
    __TAB_ID: TAB_ID,

    // debug controls
    __DEBUG: DEBUG,
    diag,

    // active doc
    getActiveDocMeta,
    setActiveDoc,
    clearActiveDoc: () => setActiveDoc(null),

    // state
    getState,
    setState,
    loadState,
    saveState,
    getLastKnownValues,

    // rules
    getRules,
    overwriteRules,
    persistRules,

    // bytes / identity
    setCurrentDoc,
    setCurrentDocFromBytes,
    getHandle: idbGetHandle,
    ensurePermission,
    getBytes,
    getCurrentDocBytes,
    putBytes,

    // hydration
    ensureHydrated,
    hydrateFromDocxIfEmpty,
    readPayloadFromDocx,
    readDocGuidFromDocx,
    writeDocGuidToDocx,
    readDocVar,
    writeDocVar,

    // concurrency
    withDocLock,
    coalesce
  };

  window.formSuitePersist = Object.freeze(api);
})();
