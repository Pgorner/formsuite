/* ============================================================================
 * Form Suite — persistence.js (CANONICAL)
 * ----------------------------------------------------------------------------
 * Goals (enforced here):
 *  1) One coordination layer: this file (window.formSuitePersist)
 *  2) One broadcast mechanism: BroadcastChannel("fs-active-doc")
 *  3) One payload mirror: state.payload.CRONOS_PAYLOAD (ONLY)
 *  4) One byte workflow: OPFS working copy as canonical; FileSystemHandle optional
 *  5) Hydration: supported as a SAFE, best-effort utility (never overwrites local
 *     state unless the state is empty / caller explicitly applies)
 *  6) Legacy code: removed (no legacy channels, no legacy LS keys, no interceptors)
 *  7) Debug logs everywhere
 *
 * Dependencies (optional):
 *  - docx-core.js: window.readDocVarSettings/readDocVarCustom and
 *                  window.writeDocVarSettings/writeDocVarCustom (or wrappers)
 * ----------------------------------------------------------------------------
 * Public API: window.formSuitePersist
 * ============================================================================
 */
(() => {
  // =========================
  // DEBUG / TRACE
  // =========================
  const DEBUG = { on: true, seq: 0 };
  const _t = () => new Date().toISOString().slice(11, 23);
  const tag = (name) => `%c[Persist ${_t()} #${++DEBUG.seq}] ${name}`;
  const tagStyle = 'color:#2563eb;font-weight:700';

  function TRACE(name, details) {
    const label = `${name} :: ${_t()} :: #${DEBUG.seq + 1}`;
    try { console.groupCollapsed(tag(name), tagStyle, details ?? ''); } catch {}
    try { console.time(label); } catch {}
    let ended = false;
    return {
      step(msg, data) { try { console.log(tag(`  ↳ ${msg}`), tagStyle, data ?? ''); } catch {} },
      warn(msg, data) { try { console.warn(tag(`  ⚠ ${msg}`), tagStyle, data ?? ''); } catch {} },
      error(msg, err) { try { console.error(tag(`  ✖ ${msg}`), tagStyle, err); } catch {} },
      end(extra) {
        if (ended) return;
        ended = true;
        if (extra) { try { console.log(tag('done'), tagStyle, extra); } catch {} }
        try { console.timeEnd(label); } catch {}
        try { console.groupEnd(); } catch {}
      }
    };
  }

  window.addEventListener('error', (e) => {
    try {
      console.error(tag('window.error'), tagStyle, {
        message: e.message, filename: e.filename, lineno: e.lineno, colno: e.colno, error: e.error
      });
    } catch {}
  });
  window.addEventListener('unhandledrejection', (e) => {
    try { console.error(tag('window.unhandledrejection'), tagStyle, e.reason); } catch {}
  });

  // =========================
  // CONSTANTS (CANONICAL)
  // =========================
  const LS_KEYS = {
    ACTIVE_META: 'FS_ACTIVE_DOC_META', // { docId, name }
    STATE_PREFIX: 'FS_state_',         // per docId -> state
    FP_MAP: 'FS_DOC_FINGERPRINT_MAP_V1'// { [sha256]: { docId, ts } } - mapping utility (not "state")
  };

  const BC_NAME = 'fs-active-doc'; // ONLY broadcast channel
  const bc = ('BroadcastChannel' in window) ? new BroadcastChannel(BC_NAME) : null;

  const DB_NAME = 'formsuite_v1';
  const DB_STORE_HANDLES = 'handles'; // { docId, handle }

  const PAYLOAD_KEY = 'CRONOS_PAYLOAD';   // payload key inside state.payload
  const DOC_GUID_KEY = 'FS_DOC_GUID';     // stable GUID stored in DOCX docVar/custom prop
  const STABLE_DOCID_PREFIX = 'fsdoc:';   // internal stable docId prefix

  // OPFS working copy: canonical bytes store
  const OPFS_EXT = '.docx'; // we always store a .docx working copy per docId (even if docm etc)

  // Tab identity for broadcast filtering
  const TAB_ID = (() => {
    try { return crypto?.randomUUID?.() || (Date.now() + '_' + Math.random().toString(16).slice(2)); }
    catch { return (Date.now() + '_' + Math.random().toString(16).slice(2)); }
  })();

  // =========================
  // BASIC UTILS
  // =========================
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
    for (const [k, v] of Object.entries(b)) {
      out[k] = deepMerge(a?.[k], v);
    }
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

  function splitNameAndExt(fileName) {
    const m = String(fileName || 'document.docx').match(/\.(docx|docm|dotx|dotm)$/i);
    return {
      base: String(fileName || 'document.docx').replace(/\.(docx|docm|dotx|dotm)$/i, ''),
      ext:  (m ? m[1] : 'docx').toLowerCase()
    };
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
    try {
      if (crypto?.randomUUID) return STABLE_DOCID_PREFIX + crypto.randomUUID();
    } catch {}
    const u8 = new Uint8Array(16);
    try { crypto.getRandomValues(u8); } catch { for (let i = 0; i < 16; i++) u8[i] = Math.floor(Math.random() * 256); }
    u8[6] = (u8[6] & 0x0f) | 0x40;
    u8[8] = (u8[8] & 0x3f) | 0x80;
    const hex = Array.from(u8).map(b => b.toString(16).padStart(2, '0')).join('');
    return STABLE_DOCID_PREFIX + (
      hex.slice(0, 8) + '-' + hex.slice(8, 12) + '-' + hex.slice(12, 16) + '-' + hex.slice(16, 20) + '-' + hex.slice(20)
    );
  }

  // =========================
  // FINGERPRINT MAP (utility)
  // =========================
  function _readFpMap() {
    const tr = TRACE('_readFpMap');
    try {
      const raw = localStorage.getItem(LS_KEYS.FP_MAP);
      const obj = raw ? JSON.parse(raw) : {};
      const out = (obj && typeof obj === 'object') ? obj : {};
      tr.end({ entries: Object.keys(out).length });
      return out;
    } catch (e) {
      tr.warn('read fp map failed', e);
      tr.end();
      return {};
    }
  }
  function _writeFpMap(map) {
    const tr = TRACE('_writeFpMap', { entries: Object.keys(map || {}).length });
    try { localStorage.setItem(LS_KEYS.FP_MAP, JSON.stringify(map || {})); }
    catch (e) { tr.warn('write fp map failed', e); }
    finally { tr.end(); }
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

    // bounded growth: keep most recent 200 (best-effort)
    try {
      const entries = Object.entries(m);
      if (entries.length > 250) {
        entries.sort((a, b) => ((a[1]?.ts || 0) - (b[1]?.ts || 0)));
        const keep = entries.slice(-200);
        const nm = {};
        for (const [k, v] of keep) nm[k] = v;
        _writeFpMap(nm);
        return;
      }
    } catch {}

    _writeFpMap(m);
  }

  // =========================
  // INDEXEDDB HANDLE STORE
  // =========================
  function openDB() {
    return new Promise((resolve, reject) => {
      const tr = TRACE('openDB', { DB_NAME });
      const req = indexedDB.open(DB_NAME, 1);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains(DB_STORE_HANDLES)) {
          db.createObjectStore(DB_STORE_HANDLES, { keyPath: 'docId' });
        }
      };
      req.onsuccess = () => { tr.end('ok'); resolve(req.result); };
      req.onerror = () => { tr.error('idb open failed', req.error); tr.end(); reject(req.error); };
    });
  }

  async function idbPutHandle(docId, handle) {
    const tr = TRACE('idbPutHandle', { docId, hasHandle: !!handle });
    try {
      if (!docId || !handle) { tr.end('skip'); return; }
      const db = await openDB();
      await new Promise((res, rej) => {
        const tx = db.transaction(DB_STORE_HANDLES, 'readwrite');
        tx.objectStore(DB_STORE_HANDLES).put({ docId, handle });
        tx.oncomplete = res;
        tx.onerror = () => rej(tx.error);
      });
      tr.end('stored');
    } catch (e) {
      tr.warn('store handle failed', e);
      tr.end();
    }
  }

  async function idbGetHandle(docId) {
    const tr = TRACE('idbGetHandle', { docId });
    try {
      if (!docId) { tr.end('no docId'); return null; }
      const db = await openDB();
      const handle = await new Promise((res, rej) => {
        const tx = db.transaction(DB_STORE_HANDLES, 'readonly');
        const req = tx.objectStore(DB_STORE_HANDLES).get(docId);
        req.onsuccess = () => res(req.result ? req.result.handle : null);
        req.onerror = () => rej(req.error);
      });
      tr.end({ hasHandle: !!handle });
      return handle || null;
    } catch (e) {
      tr.warn('get handle failed', e);
      tr.end();
      return null;
    }
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
      tr.end();
      return null;
    }
  }

  // =========================
  // OPFS WORKING COPY (CANONICAL BYTES)
  // =========================
  async function opfsRoot() {
    try { return await navigator.storage.getDirectory(); }
    catch { return null; }
  }

  async function opfsPut(docId, bytes) {
    const tr = TRACE('opfsPut', { docId, len: bytes?.byteLength || bytes?.length });
    try {
      const root = await opfsRoot();
      if (!root) { tr.end('no root'); return false; }

      const u8 = (bytes instanceof Uint8Array) ? bytes : new Uint8Array(bytes || []);
      const fh = await root.getFileHandle(`${docId}${OPFS_EXT}`, { create: true });
      const w = await fh.createWritable();
      await w.write(u8);
      await w.close();
      tr.end({ ok: true, len: u8.byteLength });
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

      const fh = await root.getFileHandle(`${docId}${OPFS_EXT}`, { create: false });
      const f = await fh.getFile();
      const ab = await f.arrayBuffer();
      tr.end({ len: ab?.byteLength || 0 });
      return ab || null;
    } catch (e) {
      tr.end('miss');
      return null;
    }
  }

  // =========================
  // PERMISSIONS (HANDLE OPTIONAL)
  // =========================
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

  // =========================
  // ACTIVE DOC (SINGLE SOURCE OF TRUTH)
  // =========================
  let _active = null; // { docId, name }
  const activeListeners = new Set();
  const stateListeners = new Set();

  function emitActive(meta) {
    for (const fn of activeListeners) { try { fn(meta); } catch {} }
  }
  function emitState(msg) {
    for (const fn of stateListeners) { try { fn(msg); } catch {} }
  }

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
      tr.end();
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
      tr.end();
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
      tr.end();
    }
  }

  // Keep in-memory active aligned with storage changes (cross-tab)
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
      tr.end();
    }
  });

  // BC messages (single channel)
  bc?.addEventListener('message', (ev) => {
    const m = ev?.data || {};
    if (!m || typeof m !== 'object') return;
    if (m.from === TAB_ID) return;

    const tr = TRACE('BC:message', m);
    try {
      if (m.type === 'active:set') {
        _active = { docId: m.docId, name: m.name || 'document.docx' };
        writeJSON(LS_KEYS.ACTIVE_META, _active); // align storage for non-BC listeners
        emitActive(_active);
      } else if (m.type === 'active:clear') {
        _active = null;
        writeJSON(LS_KEYS.ACTIVE_META, null);
        emitActive(null);
      } else if (m.type === 'active:updated') {
        // no-op here; pages may listen + react (rehydrate UI)
        // we still forward as a "state-ish" event for convenience
        emitState({ docId: m.docId, kind: 'bytes-updated', from: m.from, ts: m.ts });
      } else if (m.type === 'state:patched') {
        emitState({ docId: m.docId, kind: 'state-patched', patch: m.patch || {}, from: m.from, ts: m.ts });
      }
      tr.end('handled');
    } catch (e) {
      tr.warn('BC handler failed', e);
      tr.end();
    }
  });

  // =========================
  // STATE + CANONICAL PAYLOAD MIRROR
  // =========================
  function buildCanonicalPayloadFromState(state) {
    // IMPORTANT: payload mirror is derived. It must be deterministic and complete.
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

      // enforce canonical mirror ONLY here
      st.payload[PAYLOAD_KEY] = buildCanonicalPayloadFromState(st);

      // version counter for lightweight change detection
      st.__v = (st.__v | 0) + 1;

      tr.end({
        v: st.__v,
        rulesLen: st.rules.length,
        fieldRulesLen: st.fieldRules.length,
        hasPayload: !!st.payload?.[PAYLOAD_KEY]
      });
      return st;
    } catch (e) {
      tr.warn('normalizeState failed', e);
      tr.end();
      return { payload: { [PAYLOAD_KEY]: { title: 'Form', fields: [], values: {}, tagMap: {}, rules: [], fieldRules: [], updatedAt: _nowIso() } }, __v: 1 };
    }
  }

  function getState(docId) {
    const tr = TRACE('getState', { docId });
    try {
      if (!docId) { tr.end('no docId'); return {}; }
      const st = readJSON(keyForState(docId)) || {};
      const out = normalizeState(docId, st); // keep invariant even if someone edited LS manually
      // IMPORTANT: We do not write back on getState (avoid chatty LS). Only on setState/saveState.
      tr.end({ keys: Object.keys(out || {}).length, v: out.__v });
      return out;
    } catch (e) {
      tr.warn('getState failed', e);
      tr.end();
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

      // enforce arrays replacing semantics; accept explicit empty arrays
      if ('rules' in normalizedPatch) normalizedPatch.rules = _safeArr(normalizedPatch.rules);
      if ('fieldRules' in normalizedPatch) normalizedPatch.fieldRules = _safeArr(normalizedPatch.fieldRules);

      // Merge then normalize (canonical mirror regenerated)
      const merged = deepMerge(prev, normalizedPatch);
      const next = normalizeState(docId, merged);

      writeJSON(keyForState(docId), next);

      try {
        console.log('[DBG persist.setState]', { docId, v: next.__v, rulesLen: next.rules.length, fieldRulesLen: next.fieldRules.length });
        console.log('[DBG persist.setState] payload.CRONOS_PAYLOAD', {
          has: !!next?.payload?.CRONOS_PAYLOAD,
          rulesLen: next?.payload?.CRONOS_PAYLOAD?.rules?.length ?? null,
          fieldRulesLen: next?.payload?.CRONOS_PAYLOAD?.fieldRules?.length ?? null
        });
      } catch {}

      if (broadcast) {
        try { bc?.postMessage({ type: 'state:patched', docId, patch: normalizedPatch, from: TAB_ID, ts: Date.now() }); } catch {}
      }
      emitState({ docId, kind: 'state-patched', patch: normalizedPatch, from: TAB_ID, ts: Date.now() });

      tr.end({ v: next.__v });
      return next;
    } catch (e) {
      tr.error('setState failed', e);
      tr.end();
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
      tr.end();
      return {};
    }
  }

  async function loadState(docId) {
    const tr = TRACE('loadState', { docId });
    try {
      const st = getState(docId);
      try {
        console.log('[DBG persist.loadState]', { docId, v: st.__v, keys: Object.keys(st || {}), rulesLen: st.rules?.length ?? null });
      } catch {}
      tr.end({ v: st.__v });
      return st;
    } catch (e) {
      tr.warn('loadState failed', e);
      tr.end();
      return {};
    }
  }

  function getLastKnownValues(docId) {
    try { return (getState(docId) || {}).values || {}; } catch { return {}; }
  }

  // =========================
  // DOCX PAYLOAD + GUID (OPTIONAL; uses docx-core.js if present)
  // =========================
  async function readDocVar(bytes, key) {
    const tr = TRACE('readDocVar', { key, len: bytes?.byteLength || bytes?.length });
    try {
      if (!bytes) { tr.end('no bytes'); return null; }
      // Prefer docx-core.js wrappers if present
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
      tr.end();
      return null;
    }
  }

  async function writeDocVar(bytes, key, value) {
    const tr = TRACE('writeDocVar', { key, valueLen: String(value ?? '').length });
    try {
      if (!bytes) { tr.end('no bytes'); return bytes; }
      // Prefer settings writer; fallback custom writer if available
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
      tr.end();
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
      tr.end();
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
      tr.end();
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
      tr.end();
      return bytes;
    }
  }

  // =========================
  // HYDRATION (SAFE BY DESIGN)
  //  - Only hydrates if state has NO schema fields (empty workspace)
  //  - Never overwrites non-empty local state
  // =========================
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
        tr.end('payload invalid/empty'); return false;
      }

      const schema = { title: payload.title || 'Form', fields: payload.fields };
      const values = isPlainObject(payload.values) ? payload.values : {};
      const tagMap = isPlainObject(payload.tagMap) ? payload.tagMap : {};
      const rules = Array.isArray(payload.rules) ? payload.rules : [];
      const fieldRules = Array.isArray(payload.fieldRules) ? payload.fieldRules : [];

      // IMPORTANT: store into canonical state; payload mirror derived automatically
      setState(docId, {
        schema,
        values,
        tagMap,
        rules,
        fieldRules,
        schemaUpdatedAt: _nowIso(),
        hydratedAt: _nowIso()
      });

      tr.end({ fields: schema.fields.length, rulesLen: rules.length, fieldRulesLen: fieldRules.length });
      return true;
    } catch (e) {
      tr.error('hydrate failed', e);
      tr.end();
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
      tr.end();
      return false;
    }
  }

  // =========================
  // BYTES API (CANONICAL)
  // =========================
  async function getBytes(docId) {
    const tr = TRACE('getBytes', { docId });
    try {
      if (!docId) { tr.end('no docId'); return null; }

      // 1) OPFS working copy is canonical
      const opfs = await opfsGet(docId);
      if (opfs) { tr.end({ via: 'opfs', len: opfs.byteLength }); return opfs; }

      // 2) Fallback: handle (if stored), then refresh OPFS
      const h = await idbGetHandle(docId);
      if (h?.getFile) {
        try {
          const f = await h.getFile();
          const ab = await f.arrayBuffer();
          await opfsPut(docId, new Uint8Array(ab));
          tr.end({ via: 'handle', len: ab.byteLength });
          return ab;
        } catch (e) {
          tr.warn('handle read failed', e);
        }
      }

      tr.end('miss');
      return null;
    } catch (e) {
      tr.error('getBytes failed', e);
      tr.end();
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
      tr.end();
      return null;
    }
  }

  async function putBytes(docId, bytes, { broadcast = true } = {}) {
    const tr = TRACE('putBytes', { docId, len: bytes?.byteLength || bytes?.length, broadcast });
    try {
      if (!docId) { tr.end('no docId'); return false; }
      const u8 = (bytes instanceof Uint8Array) ? bytes : new Uint8Array(bytes || []);
      const ok = await opfsPut(docId, u8);
      if (ok && broadcast) {
        const meta = getActiveDocMeta();
        broadcastActiveUpdated(docId, meta?.name);
      }
      tr.end({ ok });
      return ok;
    } catch (e) {
      tr.error('putBytes failed', e);
      tr.end();
      return false;
    }
  }

  // =========================
  // STABLE IDENTITY + CURRENT DOC SETUP
  // =========================
  /**
   * Identity rules (canonical):
   *  - Prefer embedded DOC_GUID_KEY in DOCX when present (portable stability)
   *  - Else, if opened via FS handle, reuse IDB mapping for that handle (stability on same machine)
   *  - Else, reuse fingerprint map if available (stability for exact bytes)
   *  - Else, mint new stable docId (fsdoc:UUID) and (best-effort) embed it into DOCX working copy
   *
   * Result:
   *  - OPFS working copy always exists for docId
   *  - Active meta set
   *  - State initialized (if empty)
   *  - Optional safe hydration attempt (schema/payload) if state is empty
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

      // Compute helpers
      const fp = await sha256Hex(u8);
      const fromFp = getMappedDocIdForFingerprint(fp);

      // Determine docId
      let docId = null;

      // 1) embedded doc guid
      const embedded = await readDocGuidFromDocx(u8);
      if (embedded) docId = embedded;

      // 2) handle mapping
      if (!docId && handle) {
        const mapped = await findDocIdByHandle(handle);
        if (mapped) docId = mapped;
      }

      // 3) fingerprint map
      if (!docId && fromFp) docId = fromFp;

      // 4) mint
      if (!docId) docId = makeStableDocId();

      // Store handle mapping (optional) and set active
      if (handle) await idbPutHandle(docId, handle);

      // Best-effort embed guid into working copy if missing (only if docx-core write support exists)
      let working = u8;
      if (!embedded) {
        const maybe = await writeDocGuidToDocx(u8, docId);
        // If writer exists, it may return a new buffer
        if (maybe && maybe !== u8) {
          working = (maybe instanceof Uint8Array) ? maybe : new Uint8Array(maybe);
        }
      }

      // Write working copy to OPFS (canonical bytes)
      await opfsPut(docId, working);

      // Update fingerprint map for both original and working bytes (best-effort)
      try {
        setMappedDocIdForFingerprint(fp, docId);
        const fp2 = await sha256Hex(working);
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

      // Safe hydration attempt if empty schema
      await hydrateFromDocxIfEmpty(docId);

      tr.end({ docId, name: fileName, fp12: fp.slice(0, 12) });
      return meta;
    } catch (e) {
      tr.error('setCurrentDoc failed', e);
      tr.end();
      throw e;
    }
  }

  async function setCurrentDocFromBytes(bytes, meta = {}) {
    return setCurrentDoc({ bytes, handle: meta.handle || null, name: meta.name || 'document.docx' });
  }

  // =========================
  // RULES APIs (CANONICAL STORAGE ONLY)
  // NOTE: normalization is handled in rules-core.js (not here)
  // =========================
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

      // Optional: nudge consumers that listen specifically for "rules-updated"
      try {
        bc?.postMessage({ type: 'rules-updated', docId, from: TAB_ID, ts: Date.now() });
      } catch {}

      tr.end({ v: out.__v });
      return out;
    } catch (e) {
      tr.error('overwriteRules failed', e);
      tr.end();
      throw e;
    }
  }

  async function persistRules(docId, rules, fieldRules) {
    // legacy-friendly name; canonical behavior: store only
    return overwriteRules(docId, { rules, fieldRules }, { broadcast: true });
  }

  // =========================
  // CONCURRENCY HELPERS
  // =========================
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
      if (__locks.get(docId) === gate) __locks.delete(docId);
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

  // =========================
  // SUBSCRIPTIONS (for pages)
  // =========================
  function onActiveChange(fn) {
    activeListeners.add(fn);
    return () => activeListeners.delete(fn);
  }
  function onStateChange(fn) {
    stateListeners.add(fn);
    return () => stateListeners.delete(fn);
  }

  // =========================
  // BOOTSTRAP (best-effort)
  // =========================
  (function boot() {
    const tr = TRACE('boot');
    try {
      const meta = readJSON(LS_KEYS.ACTIVE_META);
      if (meta?.docId) {
        _active = meta;
        tr.step('restored active', meta);
        emitActive(meta);
        // do not auto-hydrate here; pages can call ensureHydrated when ready
      } else {
        tr.step('no active meta');
      }
    } catch (e) {
      tr.warn('boot failed', e);
    } finally {
      tr.end();
    }
  })();

  // =========================
  // PUBLIC API
  // =========================
  const api = {
    // constants
    ACTIVE_DOC_KEY: LS_KEYS.ACTIVE_META,
    PAYLOAD_KEY,
    DOC_GUID_KEY,
    STABLE_DOCID_PREFIX,
    BC_NAME,

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

    // rules (storage only)
    getRules,
    overwriteRules,
    persistRules,

    // bytes + identity
    setCurrentDoc,
    setCurrentDocFromBytes,
    getHandle: idbGetHandle,
    ensurePermission,
    getBytes,
    getCurrentDocBytes,
    putBytes,

    // hydration helpers
    ensureHydrated,
    hydrateFromDocxIfEmpty,
    readPayloadFromDocx,
    readDocGuidFromDocx,
    writeDocGuidToDocx,
    readDocVar,
    writeDocVar,

    // concurrency helpers
    withDocLock,
    coalesce,

    // subscriptions
    onActiveChange,
    onStateChange,

    // debug
    __TAB_ID: TAB_ID
  };

  window.formSuitePersist = Object.freeze(api);
})();
