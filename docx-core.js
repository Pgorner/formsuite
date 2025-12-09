'use strict';

// Global Pyodide handle shared across all pages
let pyLoaded = false;
let py = null;

// Load Pyodide once and register all Python DOCX helpers
async function ensurePy(forceReinit = false) {
  if (forceReinit && py && py._module) {
    pyLoaded = false;
    py = null;
  }
  if (!pyLoaded) {
    await new Promise((res, rej) => {
      if (window.loadPyodide) return res();
      const s = document.createElement('script');
      s.src = 'https://cdn.jsdelivr.net/pyodide/v0.24.1/full/pyodide.js';
      s.onload = res;
      s.onerror = rej;
      document.head.appendChild(s);
    });

    py = await loadPyodide({
      indexURL: 'https://cdn.jsdelivr.net/pyodide/v0.24.1/full/'
    });

    // IMPORTANT:
    // Copy the full Python block you already have in index.html / extractor.html
    // into this string, starting with the two import lines below and ending at
    // the final "return outbuf.getvalue()" of apply_removal_with_backup.
    await py.runPythonAsync(`
import io, zipfile, json, re
import xml.etree.ElementTree as ET

# ------------------------------------------------------------------
# PASTE HERE: everything from your existing Python helper block
# (write_docvar, read_docvar_settings, read_docvar_custom,
#  write_sdts_by_tag, inspect_export_removal_plan,
#  apply_removal_with_backup, restore_document_from_backup,
#  plus helper functions they depend on)
# ------------------------------------------------------------------
# Make sure the last line is the final "return outbuf.getvalue()"
# from apply_removal_with_backup.
`);

    pyLoaded = true;
  }
}

// ----- DOCVAR helpers -----

async function readDocVarSettings(arrayBufferOrBytes, name) {
  await ensurePy();
  const u8in = arrayBufferOrBytes instanceof Uint8Array
    ? arrayBufferOrBytes
    : new Uint8Array(arrayBufferOrBytes);
  const fn = py.globals.get('read_docvar_settings');
  const pyBytes = py.toPy(u8in);
  let pyOut;

  try {
    pyOut = fn(pyBytes, name);
  } finally {
    try { fn.destroy(); } catch {}
    try { pyBytes.destroy(); } catch {}
  }

  let txt = null;
  if (pyOut && typeof pyOut.toJs === 'function') {
    txt = pyOut.toJs({ create_proxies: false });
  } else {
    txt = pyOut ?? null;
  }
  try { pyOut?.destroy?.(); } catch {}
  return txt;
}

async function readDocVarCustom(arrayBufferOrBytes, name) {
  await ensurePy();
  const u8in = arrayBufferOrBytes instanceof Uint8Array
    ? arrayBufferOrBytes
    : new Uint8Array(arrayBufferOrBytes);
  const fn = py.globals.get('read_docvar_custom');
  const pyBytes = py.toPy(u8in);
  let pyOut;

  try {
    pyOut = fn(pyBytes, name);
  } finally {
    try { fn.destroy(); } catch {}
    try { pyBytes.destroy(); } catch {}
  }

  let txt = null;
  if (pyOut && typeof pyOut.toJs === 'function') {
    txt = pyOut.toJs({ create_proxies: false });
  } else {
    txt = pyOut ?? null;
  }
  try { pyOut?.destroy?.(); } catch {}
  return txt;
}

// canonical writer
async function writeDocVar(bytes, name, value) {
  await ensurePy();
  const fn = py.globals.get('write_docvar');
  const pyBytes = py.toPy(new Uint8Array(bytes));
  let pyOut;

  try {
    pyOut = fn(pyBytes, name, value);
  } finally {
    try { fn.destroy(); } catch {}
    try { pyBytes.destroy(); } catch {}
  }

  let u8;
  if (pyOut?.toJs) {
    u8 = pyOut.toJs({ create_proxies: false });
  } else if (pyOut?.getBuffer) {
    u8 = new Uint8Array(pyOut.getBuffer());
  } else {
    u8 = new Uint8Array([]);
  }
  try { pyOut?.destroy?.(); } catch {}
  return u8;
}

// compatibility wrapper for existing extractor usage
async function writeDocVarSettings(arrayBufferOrBytes, name, value) {
  const u8in = arrayBufferOrBytes instanceof Uint8Array
    ? arrayBufferOrBytes
    : new Uint8Array(arrayBufferOrBytes);
  return writeDocVar(u8in, name, value);
}

// ----- SDT writer (tags -> text) -----

async function writeSDTs(arrayBufferOrBytes, tagToTextObj) {
  await ensurePy();
  const u8in = arrayBufferOrBytes instanceof Uint8Array
    ? arrayBufferOrBytes
    : new Uint8Array(arrayBufferOrBytes);
  const fn = py.globals.get('write_sdts_by_tag');
  const pyBytes = py.toPy(u8in);
  const pyMap = py.toPy(JSON.stringify(tagToTextObj || {}));
  let pyOut;

  try {
    pyOut = fn(pyBytes, pyMap);
  } finally {
    try { fn.destroy(); } catch {}
    try { pyBytes.destroy(); } catch {}
    try { pyMap.destroy(); } catch {}
  }

  let u8;
  if (pyOut?.toJs) {
    u8 = pyOut.toJs({ create_proxies: false });
  } else if (pyOut?.getBuffer) {
    u8 = new Uint8Array(pyOut.getBuffer());
  } else {
    u8 = new Uint8Array([]);
  }
  try { pyOut?.destroy?.(); } catch {}
  return u8;
}

// ----- visibility map helpers -----

const serializeVisibilityMapForPython = (map) => {
  const remapped = {};
  for (const [key, value] of Object.entries(map || {})) {
    const num = Number(key);
    if (Number.isFinite(num)) {
      remapped[num] = value;
    } else {
      remapped[key] = value;
    }
  }
  return JSON.stringify(remapped);
};

// ----- removal plan / application -----

async function inspectRemovalPlan(bytesU8, visibilityMap) {
  await ensurePy();
  const fn = py.globals.get('inspect_export_removal_plan');
  const buf = bytesU8 instanceof Uint8Array
    ? bytesU8
    : new Uint8Array(bytesU8 || []);
  const pyBytes = py.toPy(buf);
  const pyMap = py.toPy(serializeVisibilityMapForPython(visibilityMap));
  let pyOut;

  try {
    pyOut = fn(pyBytes, pyMap);
  } finally {
    try { fn.destroy(); } catch {}
    try { pyBytes.destroy(); } catch {}
    try { pyMap.destroy(); } catch {}
  }

  let out = null;
  if (pyOut?.toJs) {
    out = pyOut.toJs({ create_proxies: false });
  } else {
    out = pyOut ?? null;
  }
  try { pyOut?.destroy?.(); } catch {}

  if (typeof out === 'string') {
    try {
      return JSON.parse(out);
    } catch {
      return { error: 'parse-error', raw: out };
    }
  }
  return out;
}

async function applyRemovalWithBackup(bytesU8, visibilityMap, originalBytes) {
  await ensurePy();
  const fn = py.globals.get('apply_removal_with_backup');
  const buf = bytesU8 instanceof Uint8Array
    ? bytesU8
    : new Uint8Array(bytesU8);
  const pyBytes = py.toPy(buf);
  const pyMap = py.toPy(serializeVisibilityMapForPython(visibilityMap));
  const pyPath = py.toPy('customXml/originalDocument.xml');

  const origBuf = (originalBytes instanceof Uint8Array)
    ? originalBytes
    : (originalBytes ? new Uint8Array(originalBytes) : null);

  let pyOut;
  let pyOrig = null;

  try {
    if (origBuf) {
      pyOrig = py.toPy(origBuf);
      pyOut = fn(pyBytes, pyMap, pyPath, pyOrig);
    } else {
      pyOut = fn(pyBytes, pyMap, pyPath, None);
    }
  } finally {
    try { fn.destroy(); } catch {}
    try { pyBytes.destroy(); } catch {}
    try { pyMap.destroy(); } catch {}
    try { pyPath.destroy(); } catch {}
    try { pyOrig?.destroy(); } catch {}
  }

  let u8;
  if (pyOut?.toJs) {
    u8 = pyOut.toJs({ create_proxies: false });
  } else if (pyOut?.getBuffer) {
    u8 = new Uint8Array(pyOut.getBuffer());
  } else {
    u8 = new Uint8Array([]);
  }
  try { pyOut?.destroy?.(); } catch {}
  return u8;
}

async function restoreDocxFromBackup(bytesU8, backupPath = 'customXml/originalDocument.xml') {
  await ensurePy();
  const fn = py.globals.get('restore_document_from_backup');
  const buf = bytesU8 instanceof Uint8Array
    ? bytesU8
    : new Uint8Array(bytesU8 || []);
  const pyBytes = py.toPy(buf);
  const pyPath = py.toPy(String(backupPath || 'customXml/originalDocument.xml'));
  let pyOut;

  try {
    pyOut = fn(pyBytes, pyPath);
  } finally {
    try { fn.destroy(); } catch {}
    try { pyBytes.destroy(); } catch {}
    try { pyPath.destroy(); } catch {}
  }

  let u8;
  if (pyOut?.toJs) {
    u8 = pyOut.toJs({ create_proxies: false });
  } else if (pyOut?.getBuffer) {
    u8 = new Uint8Array(pyOut.getBuffer());
  } else {
    u8 = new Uint8Array(buf);
  }
  try { pyOut?.destroy?.(); } catch {}
  return u8;
}

// Optional: export to window for convenience
if (typeof window !== 'undefined') {
  Object.assign(window, {
    ensurePy,
    readDocVarSettings,
    readDocVarCustom,
    writeDocVar,
    writeDocVarSettings,
    writeSDTs,
    serializeVisibilityMapForPython,
    inspectRemovalPlan,
    applyRemovalWithBackup,
    restoreDocxFromBackup
  });
}
