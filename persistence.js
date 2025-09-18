/* persistence.js — workspace, file handles, and OPFS working copy
   - Stores current doc meta in localStorage
   - Stores file handles in IndexedDB (structured clone)
   - Stores working .docx bytes in OPFS (silent read/write)
   - Exposes helpers used across pages (Extractor, Matcher, etc.)
*/

(() => {
  const LS_KEYS = {
    CURRENT: 'FS_current_doc_v1',         // {docId, name}
    STATE_PREFIX: 'FS_state_',            // per docId
  };
  const DB_NAME = 'formsuite_v1';
  const DB_STORE_HANDLES = 'handles';

  // ---------- Utilities ----------
  const uuid = () =>
    (crypto && crypto.randomUUID) ? crypto.randomUUID() :
    'doc-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 8);

  function readJSON(key) {
    try { return JSON.parse(localStorage.getItem(key) || 'null'); } catch { return null; }
  }
  function writeJSON(key, val) {
    localStorage.setItem(key, JSON.stringify(val));
  }

  // ---------- IndexedDB (for FileSystemFileHandle) ----------
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
        tx.oncomplete = () => res();
        tx.onerror = () => rej(tx.error);
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
    } catch {
      return null;
    }
  }

  // ---------- OPFS (Origin Private File System) ----------
  async function opfsRoot() {
    if (!('storage' in navigator) || !navigator.storage.getDirectory) return null;
    try { return await navigator.storage.getDirectory(); } catch { return null; }
  }

  async function putBytes(docId, bytes) {
    try {
      const root = await opfsRoot();
      if (!root) return false;
      const fh = await root.getFileHandle(`${docId}.docx`, { create: true });
      const w = await fh.createWritable();
      await w.write(bytes);
      await w.close();
      return true;
    } catch { return false; }
  }

  async function getBytes(docId) {
    try {
      const root = await opfsRoot();
      if (!root) return null;
      const fh = await root.getFileHandle(`${docId}.docx`, { create: false });
      const file = await fh.getFile();
      return await file.arrayBuffer();
    } catch { return null; }
  }

  // ---------- State (schema/values) ----------
  async function saveState(docId, patch) {
    const key = LS_KEYS.STATE_PREFIX + docId;
    const cur = readJSON(key) || {};
    const next = { ...cur, ...(patch || {}) };
    writeJSON(key, next);
    return next;
  }

  async function loadState(docId) {
    return readJSON(LS_KEYS.STATE_PREFIX + docId) || null;
  }

  // ---------- Permissions ----------
  async function ensurePermission(handle, mode = 'readwrite') {
    if (!handle) return 'denied';
    try {
      let p = await handle.queryPermission?.({ mode });
      if (p === 'prompt') {
        // Must be called from a user gesture (click)
        p = await handle.requestPermission?.({ mode });
      }
      return p || 'denied';
    } catch {
      return 'denied';
    }
  }

  // Re-grant for current docId (you usually only work on one doc)
  async function regrantAll() {
    const meta = readJSON(LS_KEYS.CURRENT);
    if (!meta?.docId) return { ok: 0, fail: 0 };
    const h = await idbGetHandle(meta.docId);
    if (!h) return { ok: 0, fail: 1 };
    const p = await ensurePermission(h, 'readwrite');
    return { ok: p === 'granted' ? 1 : 0, fail: p === 'granted' ? 0 : 1 };
  }

  // ---------- Current doc meta & setup ----------
  async function setCurrentDoc({ bytes, handle, name }) {
    const prev = readJSON(LS_KEYS.CURRENT);
    const docId = prev?.docId || uuid();

    const meta = { docId, name: name || prev?.name || 'document.docx' };
    writeJSON(LS_KEYS.CURRENT, meta);

    if (handle) await idbPutHandle(docId, handle);
    if (bytes) await putBytes(docId, bytes);

    const existing = await loadState(docId);
    if (!existing) await saveState(docId, { schema: null, values: {} });

    return meta;
  }



  function getCurrentDocMeta() {
    return readJSON(LS_KEYS.CURRENT) || null;
  }

  async function getCurrentDocBytes() {
    const meta = getCurrentDocMeta();
    if (!meta?.docId) return null;
    return await getBytes(meta.docId);
  }

  // ---------- Public API ----------
  window.formSuitePersist = {
    // meta
    setCurrentDoc,
    getCurrentDocMeta,

    // state
    saveState,
    loadState,

    // handles & permission
    getHandle: idbGetHandle,
    ensurePermission,
    regrantAll,

    // OPFS
    putBytes,
    getBytes,
    getCurrentDocBytes,
  };
})();
