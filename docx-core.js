'use strict';

// Global Pyodide handle/state (must be declared under strict mode)
let py = null;
let pyLoaded = false;

// IMPORTANT: do NOT declare `W_NS` here because extractor.html already declares it.
// Use a unique internal constant instead.
const _DOCX_W_NS = (window.DOCX_W_NS = window.DOCX_W_NS || "http://schemas.openxmlformats.org/wordprocessingml/2006/main");

/**
 * docx-core.js (drop-in)
 *
 * Primary goal of this drop-in: restore reliable *heading discovery* without Pyodide by
 * providing a pure-JS implementation of `inspectRemovalPlan()` that extractor/rules use
 * to build the heading baseline.
 *
 * This file intentionally keeps the existing Pyodide-based helpers (read/write docvars,
 * SDT writing, removal application) so you do not lose capabilities that other pages may
 * still rely on. However, `inspectRemovalPlan()` now prefers the JSZip path and will only
 * fall back to Pyodide if JSZip is not available.
 */

// ------------------------------
// Utilities
// ------------------------------

function _hasFn(x) { return typeof x === 'function'; }

async function ensureJSZip() {
  // JSZip is included via script tag on most pages; if not present, try to load it.
  if (window.JSZip) return window.JSZip;

  // Best-effort dynamic loader (keeps compatibility if a page changes script order).
  await new Promise((res, rej) => {
    const existing = document.querySelector('script[data-fs-jszip]');
    if (existing) {
      existing.addEventListener('load', () => res(), { once: true });
      existing.addEventListener('error', () => rej(new Error('Failed to load JSZip')), { once: true });
      return;
    }
    const s = document.createElement('script');
    s.dataset.fsJszip = '1';
    s.src = 'https://cdn.jsdelivr.net/npm/jszip@3.10.1/dist/jszip.min.js';
    s.onload = () => res();
    s.onerror = () => rej(new Error('Failed to load JSZip'));
    document.head.appendChild(s);
  });

  if (!window.JSZip) throw new Error('JSZip not available after loading attempt');
  return window.JSZip;
}

function _xmlParse(xmlText) {
  const dp = new DOMParser();
  const doc = dp.parseFromString(xmlText, 'application/xml');
  // Detect parsererror in a cross-browser way
  const pe = doc.getElementsByTagName('parsererror');
  if (pe && pe.length) {
    throw new Error('XML parse error');
  }
  return doc;
}

function _getAttr(node, attrName) {
  if (!node) return null;
  // WordprocessingML uses prefixed attributes like w:val.
  // In most DOMs, getAttribute('w:val') works; also try local-name fallback.
  return node.getAttribute(attrName) ?? node.getAttribute(attrName.replace(/^.*:/, '')) ?? null;
}

function _extractParagraphText(pNode) {
  // Collect all w:t nodes under the paragraph (namespace-safe)
  const tNodes = pNode.getElementsByTagNameNS
    ? pNode.getElementsByTagNameNS(_DOCX_W_NS, 't')
    : pNode.getElementsByTagName('w:t');

  let out = '';
  for (let i = 0; i < tNodes.length; i++) {
    out += tNodes[i].textContent || '';
  }
  return out.replace(/\s+/g, ' ').trim();
}


function _stripLeadingNumber(s) {
  const str = String(s || '').trim();
  // Remove common numbering prefixes like "1.2 ", "1) ", "I. " etc.
  return str.replace(/^\s*(?:[\dIVXLCM]+(?:[\.)]|\.(?=\d)|\s+))+\s*/i, '').trim();
}


function _headingLevelFromStyleVal(styleVal) {
  if (!styleVal) return null;
  // Typical style ids: Heading1..Heading9
  const m = String(styleVal).match(/^heading\s*([1-9]\d*)$/i) || String(styleVal).match(/^Heading([1-9]\d*)$/);
  if (m) {
    const n = parseInt(m[1], 10);
    if (Number.isFinite(n) && n >= 1 && n <= 9) return n;
  }
  return null;
}

function _headingLevelFromOutline(pNode) {
  // w:pPr/w:outlineLvl w:val="0..8"
  const pPr = _firstNS(pNode, 'pPr');
  if (!pPr) return null;
  const ol = _firstNS(pPr, 'outlineLvl');
  if (!ol) return null;
  const v = _getAttr(ol, 'w:val');
  if (v == null) return null;
  const n = parseInt(String(v), 10);
  if (!Number.isFinite(n)) return null;
  // outlineLvl 0 -> Heading level 1
  const lvl = n + 1;
  if (lvl >= 1 && lvl <= 9) return lvl;
  return null;
}

function _stableHash(str) {
  // Simple deterministic 32-bit hash for stable IDs
  let h = 2166136261;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return (h >>> 0).toString(16);
}

// ------------------------------
// Pure-JS heading inspector (working without Pyodide)
// ------------------------------

/**
 * JS-first removal plan inspector used by Rules heading discovery.
 *
 * Robust heading detection strategy:
 *  - Resolve paragraph styleId via styles.xml (including basedOn inheritance)
 *  - Determine heading level primarily from style outlineLvl (w:outlineLvl)
 *  - Fallback to style name / styleId patterns (Heading 1, Überschrift 1, etc.)
 *  - Fallback to paragraph outlineLvl
 *
 * Returns the shape extractor expects: { parts: [{ headings: [{idx, level, text, id}] }] }
 */
async function inspectRemovalPlan_JS(bytesU8 /* Uint8Array */, visibilityMap /* unused here */) {
  const JSZip = await ensureJSZip();

  const input = (bytesU8 instanceof Uint8Array) ? bytesU8 : new Uint8Array(bytesU8 || []);
  // IMPORTANT: copy into a fresh Uint8Array to avoid `.buffer` offset pitfalls.
  const buf = new Uint8Array(input);

  const zip = await JSZip.loadAsync(buf);

  const docFile = zip.file('word/document.xml');
  if (!docFile) return { parts: [{ headings: [] }] };

  const stylesFile = zip.file('word/styles.xml');
  const docXml = await docFile.async('string');
  const docDom = _xmlParse(docXml);

  let styleLevelMap = new Map(); // styleId -> level (1..9)
  if (stylesFile) {
    try {
      const stylesXml = await stylesFile.async('string');
      const stylesDom = _xmlParse(stylesXml);
      styleLevelMap = _buildStyleHeadingLevelMap(stylesDom);
    } catch (e) {
      // Styles parsing is best-effort; fallback paths still work.
      styleLevelMap = new Map();
    }
  }

  const paras = Array.from(
    docDom.getElementsByTagNameNS
      ? docDom.getElementsByTagNameNS(_DOCX_W_NS, 'p')
      : docDom.getElementsByTagName('w:p')
  );

  const headings = [];

  for (let i = 0; i < paras.length; i++) {
    const p = paras[i];

    let level = null;

    // 1) Style-driven level via styles.xml
    const pPr = _firstNS(p, 'pPr') || _first(p, 'w:pPr');
    const pStyle = pPr ? (_firstNS(pPr, 'pStyle') || _first(pPr, 'w:pStyle')) : null;
    if (pStyle) {
      const styleId = _getAttr(pStyle, 'w:val');
      if (styleId) {
        level = styleLevelMap.get(styleId) ?? _headingLevelFromStyleVal(styleId);
      }
    }

    // 2) Paragraph outlineLvl fallback
    if (!level) level = _headingLevelFromOutline(p);

    if (!level) continue;

    const text = _extractParagraphText(p);
    if (!text) continue;

    const cleanText = _stripLeadingNumber(text);
    const id = `sec_${String(i).padStart(6,'0')}`;
    headings.push({ id, level, text: cleanText, rawText: text, idx: i });
  }


  // Compute section ranges: from this heading idx until before next heading of same or higher level.
  for (let h = 0; h < headings.length; h++) {
    const cur = headings[h];
    let endIdx = paras.length - 1;
    for (let j = h + 1; j < headings.length; j++) {
      const nxt = headings[j];
      if (nxt.level <= cur.level) { endIdx = nxt.idx - 1; break; }
    }
    cur.startIdx = cur.idx;
    cur.endIdx = endIdx;
  }

  return { parts: [{ name: 'word/document.xml', headings, paraCount: paras.length }] };
}

/**
 * Build a resolved map: styleId -> heading level (1..9)
 *
 * Resolution order:
 *  1) style's outlineLvl (w:outlineLvl) if present
 *  2) inferred from styleId/name patterns (Heading 1, Überschrift 1, etc.)
 *  3) inherited via basedOn
 */
function _buildStyleHeadingLevelMap(stylesDom) {
  const styles = Array.from(
    stylesDom.getElementsByTagNameNS
      ? stylesDom.getElementsByTagNameNS(_DOCX_W_NS, 'style')
      : stylesDom.getElementsByTagName('w:style')
  );

  const byId = new Map();

  for (const st of styles) {
    const type = _getAttr(st, 'w:type');
    if (type && type !== 'paragraph') continue;

    const styleId = _getAttr(st, 'w:styleId');
    if (!styleId) continue;

    const nameEl = _firstNS(st, 'name') || _first(st, 'w:name');
    const name = nameEl ? (_getAttr(nameEl, 'w:val') || '') : '';

    const basedOnEl = _firstNS(st, 'basedOn') || _first(st, 'w:basedOn');
    const basedOn = basedOnEl ? (_getAttr(basedOnEl, 'w:val') || '') : '';

    const outlineLvl = _styleOutlineLevel(st); // returns 1..9 or null

    byId.set(styleId, { styleId, name, basedOn, outlineLvl });
  }

  const resolved = new Map();
  const visiting = new Set();

  function resolve(styleId) {
    if (resolved.has(styleId)) return resolved.get(styleId);
    if (visiting.has(styleId)) return null; // cycle
    visiting.add(styleId);

    const st = byId.get(styleId);
    if (!st) { visiting.delete(styleId); return null; }

    // 1) outlineLvl in style
    if (st.outlineLvl) {
      resolved.set(styleId, st.outlineLvl);
      visiting.delete(styleId);
      return st.outlineLvl;
    }

    // 2) infer from styleId or name patterns
    const inferred = _inferHeadingLevelFromStyleToken(st.styleId, st.name);
    if (inferred) {
      resolved.set(styleId, inferred);
      visiting.delete(styleId);
      return inferred;
    }

    // 3) basedOn inheritance
    if (st.basedOn) {
      const parentLvl = resolve(st.basedOn);
      if (parentLvl) {
        resolved.set(styleId, parentLvl);
        visiting.delete(styleId);
        return parentLvl;
      }
    }

    visiting.delete(styleId);
    return null;
  }

  for (const styleId of byId.keys()) resolve(styleId);

  // Keep only resolved headings
  const out = new Map();
  for (const [styleId, lvl] of resolved.entries()) {
    if (lvl && lvl >= 1 && lvl <= 9) out.set(styleId, lvl);
  }
  return out;
}

function _styleOutlineLevel(styleEl) {
  // styleEl: <w:style>
  const pPr = _firstNS(styleEl, 'pPr') || _first(styleEl, 'w:pPr');
  if (!pPr) return null;
  const ol = _firstNS(pPr, 'outlineLvl') || _first(pPr, 'w:outlineLvl');
  if (!ol) return null;
  const v = _getAttr(ol, 'w:val');
  if (v == null) return null;
  const n = Number(v);
  if (!Number.isFinite(n)) return null;
  // Word stores outlineLvl 0..8 for heading 1..9
  const lvl = n + 1;
  if (lvl < 1 || lvl > 9) return null;
  return lvl;
}

function _inferHeadingLevelFromStyleToken(styleId, styleName) {
  const s = `${styleId || ''} ${styleName || ''}`.toLowerCase();

  // Common localized heading tokens
  const tokens = [
    'heading',         // en
    'überschrift',     // de
    'uberschrift',     // de (no umlaut)
    'ueberschrift',    // de
    'titre',           // fr
    'título',          // es/pt with accent
    'titulo',          // es/pt
    'titolo',          // it
    'rubrik',          // some templates
    'section'          // some templates
  ];

  let hit = false;
  for (const t of tokens) {
    if (s.includes(t)) { hit = true; break; }
  }
  if (!hit) return null;

  // Find first digit 1..9 in the string
  const m = s.match(/(?:^|[^0-9])([1-9])(?:[^0-9]|$)/);
  if (!m) return null;
  const lvl = Number(m[1]);
  if (lvl >= 1 && lvl <= 9) return lvl;
  return null;
}

function _firstNS(node, localName) {
  if (!node) return null;
  try {
    if (node.getElementsByTagNameNS) {
      const els = node.getElementsByTagNameNS(_DOCX_W_NS, localName);
      return (els && els.length) ? els[0] : null;
    }
  } catch {}
  const els2 = node.getElementsByTagName('w:' + localName);
  return (els2 && els2.length) ? els2[0] : null;
}

/**
 * DOM traversal helper: chained first-child lookup by tag names.
 * _first(node, 'w:pPr', 'w:pStyle') means: find first w:pPr under node, then first w:pStyle under that.
 */
function _first(node, ...tags) {
  let cur = node;
  for (const t of tags) {
    if (!cur) return null;
    const els = cur.getElementsByTagName(t);
    if (!els || !els.length) return null;
    cur = els[0];
  }
  return cur;
}

async function ensurePy(forceReinit = false) {
  if (forceReinit && py && py._module) {
    pyLoaded = false;
    py = null;
  }
  if (!pyLoaded) {
    await new Promise((res, rej) => {
      if (window.loadPyodide) return res();
      const s = document.createElement('script');
      s.src = 'https://cdn.jsdelivr.net/pyodide/v0.25.1/full/pyodide.js';
      s.onload = res;
      s.onerror = rej;
      document.head.appendChild(s);
    });

    py = await window.loadPyodide();

    // IMPORTANT:
    // Your original project may inject python helpers elsewhere.
    // This file will not crash if those helpers are missing; callers are guarded.

    pyLoaded = true;
  }
  return py;
}

// These wrappers are left intact, but guarded to avoid ".destroy is not a function" noise.
async function _callPyFn(fnName, args = []) {
  await ensurePy();
  const fn = py.globals.get(fnName);
  if (!fn) throw new Error(`Py function not found: ${fnName}`);
  try {
    return fn(...args);
  } finally {
    try { if (fn && _hasFn(fn.destroy)) fn.destroy(); } catch {}
  }
}

// ---- docVar read/write (Pyodide-backed as before) ----

async function readDocVarSettings(bytesU8, key) {
  await ensurePy();
  const fn = py.globals.get('read_docvar_settings');
  if (!fn) return null;
  const buf = bytesU8 instanceof Uint8Array ? bytesU8 : new Uint8Array(bytesU8 || []);
  const pyBytes = py.toPy(buf);
  let pyOut;
  try {
    pyOut = fn(pyBytes, key);
  } finally {
    try { if (fn && _hasFn(fn.destroy)) fn.destroy(); } catch {}
    try { if (pyBytes && _hasFn(pyBytes.destroy)) pyBytes.destroy(); } catch {}
  }
  const out = pyOut?.toJs ? pyOut.toJs() : pyOut;
  try { if (pyOut && _hasFn(pyOut.destroy)) pyOut.destroy(); } catch {}
  return out;
}

async function readDocVarCustom(bytesU8, key) {
  await ensurePy();
  const fn = py.globals.get('read_docvar_custom');
  if (!fn) return null;
  const buf = bytesU8 instanceof Uint8Array ? bytesU8 : new Uint8Array(bytesU8 || []);
  const pyBytes = py.toPy(buf);
  let pyOut;
  try {
    pyOut = fn(pyBytes, key);
  } finally {
    try { if (fn && _hasFn(fn.destroy)) fn.destroy(); } catch {}
    try { if (pyBytes && _hasFn(pyBytes.destroy)) pyBytes.destroy(); } catch {}
  }
  const out = pyOut?.toJs ? pyOut.toJs() : pyOut;
  try { if (pyOut && _hasFn(pyOut.destroy)) pyOut.destroy(); } catch {}
  return out;
}

async function writeDocVar(bytesU8, key, jsonStr) {
  await ensurePy();
  const fn = py.globals.get('write_docvar_auto');
  if (!fn) return new Uint8Array(bytesU8 instanceof Uint8Array ? bytesU8 : new Uint8Array(bytesU8 || []));
  const buf = bytesU8 instanceof Uint8Array ? bytesU8 : new Uint8Array(bytesU8 || []);
  const pyBytes = py.toPy(buf);
  let pyOut;
  try {
    pyOut = fn(pyBytes, key, jsonStr);
  } finally {
    try { if (fn && _hasFn(fn.destroy)) fn.destroy(); } catch {}
    try { if (pyBytes && _hasFn(pyBytes.destroy)) pyBytes.destroy(); } catch {}
  }
  const out = pyOut?.toJs ? pyOut.toJs() : pyOut;
  try { if (pyOut && _hasFn(pyOut.destroy)) pyOut.destroy(); } catch {}
  return out instanceof Uint8Array ? out : new Uint8Array(out || []);
}

async function writeDocVarSettings(bytesU8, key, jsonStr) {
  await ensurePy();
  const fn = py.globals.get('write_docvar_settings');
  if (!fn) return new Uint8Array(bytesU8 instanceof Uint8Array ? bytesU8 : new Uint8Array(bytesU8 || []));
  const buf = bytesU8 instanceof Uint8Array ? bytesU8 : new Uint8Array(bytesU8 || []);
  const pyBytes = py.toPy(buf);
  let pyOut;
  try {
    pyOut = fn(pyBytes, key, jsonStr);
  } finally {
    try { if (fn && _hasFn(fn.destroy)) fn.destroy(); } catch {}
    try { if (pyBytes && _hasFn(pyBytes.destroy)) pyBytes.destroy(); } catch {}
  }
  const out = pyOut?.toJs ? pyOut.toJs() : pyOut;
  try { if (pyOut && _hasFn(pyOut.destroy)) pyOut.destroy(); } catch {}
  return out instanceof Uint8Array ? out : new Uint8Array(out || []);
}

async function writeDocVarCustom(bytesU8, key, jsonStr) {
  await ensurePy();
  const fn = py.globals.get('write_docvar_custom');
  if (!fn) return new Uint8Array(bytesU8 instanceof Uint8Array ? bytesU8 : new Uint8Array(bytesU8 || []));
  const buf = bytesU8 instanceof Uint8Array ? bytesU8 : new Uint8Array(bytesU8 || []);
  const pyBytes = py.toPy(buf);
  let pyOut;
  try {
    pyOut = fn(pyBytes, key, jsonStr);
  } finally {
    try { if (fn && _hasFn(fn.destroy)) fn.destroy(); } catch {}
    try { if (pyBytes && _hasFn(pyBytes.destroy)) pyBytes.destroy(); } catch {}
  }
  const out = pyOut?.toJs ? pyOut.toJs() : pyOut;
  try { if (pyOut && _hasFn(pyOut.destroy)) pyOut.destroy(); } catch {}
  return out instanceof Uint8Array ? out : new Uint8Array(out || []);
}

// ---- SDT writing ----
async function writeSDTs(bytesU8, tagToTextMap) {
  await ensurePy();
  const fn = py.globals.get('write_sdts_by_tag');
  if (!fn) return new Uint8Array(bytesU8 instanceof Uint8Array ? bytesU8 : new Uint8Array(bytesU8 || []));
  const buf = bytesU8 instanceof Uint8Array ? bytesU8 : new Uint8Array(bytesU8 || []);
  const pyBytes = py.toPy(buf);
  const pyMap = py.toPy(tagToTextMap || {});
  let pyOut;
  try {
    pyOut = fn(pyBytes, pyMap);
  } finally {
    try { if (fn && _hasFn(fn.destroy)) fn.destroy(); } catch {}
    try { if (pyBytes && _hasFn(pyBytes.destroy)) pyBytes.destroy(); } catch {}
    try { if (pyMap && _hasFn(pyMap.destroy)) pyMap.destroy(); } catch {}
  }
  const out = pyOut?.toJs ? pyOut.toJs() : pyOut;
  try { if (pyOut && _hasFn(pyOut.destroy)) pyOut.destroy(); } catch {}
  return out instanceof Uint8Array ? out : new Uint8Array(out || []);
}

// ---- visibility map serialization (used by Py) ----
function serializeVisibilityMapForPython(map) {
  // Keep existing behavior expected by your python helpers.
  // Supports Map, plain objects, or arrays of entries.
  const normalizeValue = (value) => {
    if (value == null) return false;
    if (typeof value === 'boolean') return value;
    if (typeof value === 'string') {
      const s = value.trim().toLowerCase();
      if (!s) return false;
      if (s === 'show' || s === 'visible' || s === 'keep') return false;
      if (s === 'hide' || s === 'remove' || s === 'true') return true;
    }
    if (typeof value === 'object' && value) {
      const action = value.action || value.visibility || value.state;
      if (typeof action === 'string') {
        const s = action.trim().toLowerCase();
        if (s === 'show' || s === 'visible' || s === 'keep') return false;
        if (s === 'hide' || s === 'remove') return true;
      }
    }
    return !!value;
  };
  if (!map) return {};
  if (map instanceof Map) {
    const o = {};
    for (const [k, v] of map.entries()) o[String(k)] = normalizeValue(v);
    return o;
  }
  if (Array.isArray(map)) {
    const o = {};
    for (const [k, v] of map) o[String(k)] = normalizeValue(v);
    return o;
  }
  if (typeof map === 'object') {
    const o = {};
    for (const k of Object.keys(map)) o[String(k)] = normalizeValue(map[k]);
    return o;
  }
  return {};
}

// ---- removal plan / application ----
// IMPORTANT: `inspectRemovalPlan` is now JS-first.
async function inspectRemovalPlan(bytesU8, visibilityMap) {
  // Prefer JSZip heading inspection to keep Rules functional even when Pyodide helpers are absent.
  try {
    return await inspectRemovalPlan_JS(bytesU8, visibilityMap);
  } catch (e) {
    // If JSZip path fails, fall back to Pyodide if available.
    try {
      await ensurePy();
      const fn = py.globals.get('inspect_export_removal_plan');
      if (!fn) return { parts: [{ headings: [] }] };

      const buf = bytesU8 instanceof Uint8Array ? bytesU8 : new Uint8Array(bytesU8 || []);
      const pyBytes = py.toPy(buf);
      const pyMap = py.toPy(serializeVisibilityMapForPython(visibilityMap));
      let pyOut;

      try {
        pyOut = fn(pyBytes, pyMap);
      } finally {
        try { if (fn && _hasFn(fn.destroy)) fn.destroy(); } catch {}
        try { if (pyBytes && _hasFn(pyBytes.destroy)) pyBytes.destroy(); } catch {}
        try { if (pyMap && _hasFn(pyMap.destroy)) pyMap.destroy(); } catch {}
      }

      const out = pyOut?.toJs ? pyOut.toJs() : pyOut;
      try { if (pyOut && _hasFn(pyOut.destroy)) pyOut.destroy(); } catch {}
      return out || { parts: [{ headings: [] }] };
    } catch {
      // Ultimate fallback: no headings
      return { parts: [{ headings: [] }] };
    }
  }
}

async function applyRemovalWithBackup(bytesU8, visibilityMap, originalBytesU8) {
  await ensurePy();
  const fn = py.globals.get('apply_removal_with_backup');
  if (!fn) return new Uint8Array(bytesU8 instanceof Uint8Array ? bytesU8 : new Uint8Array(bytesU8 || []));
  const buf = bytesU8 instanceof Uint8Array ? bytesU8 : new Uint8Array(bytesU8 || []);
  const orig = originalBytesU8 instanceof Uint8Array ? originalBytesU8 : new Uint8Array(originalBytesU8 || []);
  const pyBytes = py.toPy(buf);
  const pyOrig = py.toPy(orig);
  const pyMap = py.toPy(serializeVisibilityMapForPython(visibilityMap));
  let pyOut;
  try {
    pyOut = fn(pyBytes, pyMap, pyOrig);
  } finally {
    try { if (fn && _hasFn(fn.destroy)) fn.destroy(); } catch {}
    try { if (pyBytes && _hasFn(pyBytes.destroy)) pyBytes.destroy(); } catch {}
    try { if (pyOrig && _hasFn(pyOrig.destroy)) pyOrig.destroy(); } catch {}
    try { if (pyMap && _hasFn(pyMap.destroy)) pyMap.destroy(); } catch {}
  }
  const out = pyOut?.toJs ? pyOut.toJs() : pyOut;
  try { if (pyOut && _hasFn(pyOut.destroy)) pyOut.destroy(); } catch {}
  return out instanceof Uint8Array ? out : new Uint8Array(out || []);
}

async function restoreDocxFromBackup(bytesU8) {
  await ensurePy();
  const fn = py.globals.get('restore_document_from_backup');
  if (!fn) return new Uint8Array(bytesU8 instanceof Uint8Array ? bytesU8 : new Uint8Array(bytesU8 || []));
  const buf = bytesU8 instanceof Uint8Array ? bytesU8 : new Uint8Array(bytesU8 || []);
  const pyBytes = py.toPy(buf);
  let pyOut;
  try {
    pyOut = fn(pyBytes);
  } finally {
    try { if (fn && _hasFn(fn.destroy)) fn.destroy(); } catch {}
    try { if (pyBytes && _hasFn(pyBytes.destroy)) pyBytes.destroy(); } catch {}
  }
  const out = pyOut?.toJs ? pyOut.toJs() : pyOut;
  try { if (pyOut && _hasFn(pyOut.destroy)) pyOut.destroy(); } catch {}
  return out instanceof Uint8Array ? out : new Uint8Array(out || []);
}

// ------------------------------
// Exports
// ------------------------------

if (typeof window !== 'undefined') {
  // Maintain both legacy global functions and window.docxCore namespace.
  window.docxCore = window.docxCore || {};
  Object.assign(window.docxCore, {
    ensurePy,
    ensureJSZip,
    readDocVarSettings,
    readDocVarCustom,
    writeDocVar,
    writeDocVarSettings,
    writeDocVarCustom,
    writeSDTs,
    serializeVisibilityMapForPython,
    inspectRemovalPlan,
    applyRemovalWithBackup,
    restoreDocxFromBackup
  });

  Object.assign(window, {
    ensurePy,
    readDocVarSettings,
    readDocVarCustom,
    writeDocVar,
    writeDocVarSettings,
    writeDocVarCustom,
    writeSDTs,
    serializeVisibilityMapForPython,
    inspectRemovalPlan,
    applyRemovalWithBackup,
    restoreDocxFromBackup
  });
}
