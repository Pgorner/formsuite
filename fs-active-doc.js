// fs-active-doc.js
// Centralized "active document" coordination for all Form Suite tabs.

(function () {
  const ACTIVE_LS_KEY = 'FS_ACTIVE_DOC_META';
  const LEGACY_LS_KEY = 'FS_CURRENT_DOC_META';

  const hasBC = typeof BroadcastChannel !== 'undefined';
  const bcLegacy = hasBC ? new BroadcastChannel('form-suite-doc') : null;
  const bcCanon  = hasBC ? new BroadcastChannel('fs-active-doc') : null;

  const listeners = new Set();
  let cachedMeta = null;

  function trace(label, data) {
    try {
      // keep it very lightweight; you already have heavy TRACE in pages
      console.debug('[fs-active-doc]', label, data || '');
    } catch {
      /* noop */
    }
  }

  function safeParse(raw) {
    if (!raw) return null;
    try {
      return JSON.parse(raw);
    } catch {
      return null;
    }
  }

  function readFromStorage() {
    try {
      const raw =
        localStorage.getItem(ACTIVE_LS_KEY) ||
        localStorage.getItem(LEGACY_LS_KEY);
      return safeParse(raw);
    } catch {
      return null;
    }
  }

  function writeToStorage(meta) {
    try {
      if (meta) {
        const json = JSON.stringify(meta);
        localStorage.setItem(ACTIVE_LS_KEY, json);
        localStorage.setItem(LEGACY_LS_KEY, json);
      } else {
        localStorage.removeItem(ACTIVE_LS_KEY);
        localStorage.removeItem(LEGACY_LS_KEY);
      }
    } catch (e) {
      trace('writeToStorage:failed', e);
    }

    // keep formSuitePersist in sync if it exposes helpers
    try {
      if (window.formSuitePersist) {
        if (typeof window.formSuitePersist.setActiveDocMeta === 'function') {
          window.formSuitePersist.setActiveDocMeta(meta || null);
        }
      }
    } catch (e) {
      trace('formSuitePersist.setActiveDocMeta failed', e);
    }
  }

  function notifyListeners(meta) {
    cachedMeta = meta || null;
    for (const fn of Array.from(listeners)) {
      try {
        fn(cachedMeta);
      } catch (e) {
        trace('listener failed', e);
      }
    }
  }

  function emit(type, meta) {
    const m = meta || {};
    const docId = m.docId;
    const name  = m.name || m.docTitle;

    // Canonical channel: fine-grained active-* events
    const canonPayload = Object.assign({ type }, m);
    try {
      bcCanon && bcCanon.postMessage(canonPayload);
    } catch (e) {
      trace('bcCanon post failed', e);
    }

    // Legacy channel: doc-switched/doc-updated/doc-cleared
    try {
      if (!bcLegacy) return;

      if (type === 'active:set' && docId) {
        bcLegacy.postMessage({ type: 'doc-switched', docId, name });
      } else if (type === 'active:updated' && docId) {
        bcLegacy.postMessage({ type: 'doc-updated', docId, name });
      } else if (type === 'active:clear') {
        bcLegacy.postMessage({ type: 'doc-cleared' });
      }
    } catch (e) {
      trace('bcLegacy post failed', e);
    }
  }

  function readActiveDocSync() {
    // Prefer cached
    if (cachedMeta) return cachedMeta;

    // If persistence exposes a dedicated getter, use that
    try {
      if (
        window.formSuitePersist &&
        typeof window.formSuitePersist.getActiveDocMeta === 'function'
      ) {
        const meta = window.formSuitePersist.getActiveDocMeta();
        if (meta) {
          cachedMeta = meta;
          return cachedMeta;
        }
      }
    } catch (e) {
      trace('getActiveDocMeta failed', e);
    }

    // Fallback to localStorage
    cachedMeta = readFromStorage();
    return cachedMeta;
  }

  function setActiveDocMeta(meta) {
    const next = meta || null;
    trace('setActiveDocMeta', next);
    writeToStorage(next);
    emit('active:set', next);
    notifyListeners(next);
    return next;
  }

  function clearActiveDocMeta() {
    trace('clearActiveDocMeta');
    writeToStorage(null);
    emit('active:clear', null);
    notifyListeners(null);
  }

  function notifyActiveDocUpdated(patchOrMeta) {
    const current = readActiveDocSync() || {};
    const next =
      patchOrMeta && patchOrMeta.docId
        ? patchOrMeta
        : Object.assign({}, current, patchOrMeta || {});
    trace('notifyActiveDocUpdated', { from: current, to: next });
    writeToStorage(next);
    emit('active:updated', next);
    notifyListeners(next);
    return next;
  }

  function installActiveDocListener(fn) {
    if (typeof fn !== 'function') return () => {};
    listeners.add(fn);

    // Immediately notify with current meta if we have one
    try {
      const meta = readActiveDocSync();
      if (meta) fn(meta);
    } catch (e) {
      trace('installActiveDocListener initial failed', e);
    }

    return () => {
      listeners.delete(fn);
    };
  }

  function broadcastSchemaUpdated(docId) {
    const active = readActiveDocSync();
    const targetId = docId || (active && active.docId);
    if (!targetId) return;

    const msg = { type: 'rules-updated', docId: targetId, ts: Date.now() };
    trace('broadcastSchemaUpdated', msg);

    try {
      bcCanon && bcCanon.postMessage(msg);
      bcLegacy && bcLegacy.postMessage(msg);
    } catch (e) {
      trace('broadcastSchemaUpdated failed', e);
    }
  }

  function broadcastStateUpdated(docId) {
    const active = readActiveDocSync();
    const targetId = docId || (active && active.docId);
    if (!targetId) return;

    const msg = { type: 'state-updated', docId: targetId, ts: Date.now() };
    trace('broadcastStateUpdated', msg);

    try {
      bcCanon && bcCanon.postMessage(msg);
      bcLegacy && bcLegacy.postMessage(msg);
    } catch (e) {
      trace('broadcastStateUpdated failed', e);
    }
  }

  // Keep cache in sync if another tab writes localStorage directly
  window.addEventListener('storage', (e) => {
    if (e.key !== ACTIVE_LS_KEY && e.key !== LEGACY_LS_KEY) return;
    cachedMeta = null;
    const meta = readActiveDocSync();
    trace('storage event', meta);
    notifyListeners(meta);
  });

  // Public API
  window.readActiveDocSync = readActiveDocSync;
  window.setActiveDocMeta = setActiveDocMeta;
  window.clearActiveDocMeta = clearActiveDocMeta;
  window.notifyActiveDocUpdated = notifyActiveDocUpdated;
  window.installActiveDocListener = installActiveDocListener;
  window.broadcastSchemaUpdated = broadcastSchemaUpdated;
  window.broadcastStateUpdated = broadcastStateUpdated;

  // Optional bridge: expose via formSuitePersist if present
  if (window.formSuitePersist) {
    if (typeof window.formSuitePersist.getActiveDocMeta !== 'function') {
      window.formSuitePersist.getActiveDocMeta = readActiveDocSync;
    }
    if (typeof window.formSuitePersist.setActiveDocMeta !== 'function') {
      window.formSuitePersist.setActiveDocMeta = setActiveDocMeta;
    }
  }
})();
