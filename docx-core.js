'use strict';

// Global Pyodide handle/state (must be declared under strict mode)
let py = null;
let pyLoaded = false;

// IMPORTANT: do NOT declare `W_NS` here because extractor.html already declares it.
// Use a unique internal constant instead.
const _DOCX_W_NS = (window.DOCX_W_NS = window.DOCX_W_NS || "http://schemas.openxmlformats.org/wordprocessingml/2006/main");

// Baseline storage (matches your working snippet)
const _BASE_PATH = 'customXml/originalDocument.xml';
const _META_PATH = 'customXml/rebuilder_meta.json';

function _hasFn(x) { return typeof x === 'function'; }

// ------------------------------
// JSZip loader
// ------------------------------
async function ensureJSZip() {
  if (window.JSZip) return window.JSZip;

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

// ------------------------------
// XML helpers
// ------------------------------
function _xmlParse(xmlText) {
  const dp = new DOMParser();
  const doc = dp.parseFromString(xmlText, 'application/xml');
  const pe = doc.getElementsByTagName('parsererror');
  if (pe && pe.length) throw new Error('XML parse error');
  return doc;
}
function _xmlSerialize(doc) {
  return new XMLSerializer().serializeToString(doc);
}

function _getAttr(node, attrName) {
  if (!node) return null;
  return node.getAttribute(attrName) ?? node.getAttribute(attrName.replace(/^.*:/, '')) ?? null;
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

function _normalizeText(s) {
  return String(s || '').replace(/\u00A0/g, ' ').replace(/\s+/g, ' ').trim();
}
function _extractParagraphText(pNode) {
  const tNodes = pNode.getElementsByTagNameNS
    ? pNode.getElementsByTagNameNS(_DOCX_W_NS, 't')
    : pNode.getElementsByTagName('w:t');

  let out = '';
  for (let i = 0; i < tNodes.length; i++) out += tNodes[i].textContent || '';
  return _normalizeText(out);
}
function _stripLeadingNumber(s) {
  const str = String(s || '').trim();
  return str.replace(/^[\s\t\u00A0]*\d+(?:[.\u00A0 \t]+\d+)*[.\u00A0 \t]*/,'').trim();
}

function _headingLevelFromStyleVal(styleVal) {
  if (!styleVal) return null;
  const m = String(styleVal).match(/^heading\s*([1-9]\d*)$/i) || String(styleVal).match(/^Heading([1-9]\d*)$/);
  if (m) {
    const n = parseInt(m[1], 10);
    if (Number.isFinite(n) && n >= 1 && n <= 9) return n;
  }
  return null;
}

function _headingLevelFromOutline(pNode) {
  const pPr = _firstNS(pNode, 'pPr');
  if (!pPr) return null;
  const ol = _firstNS(pPr, 'outlineLvl');
  if (!ol) return null;
  const v = _getAttr(ol, 'w:val');
  if (v == null) return null;
  const n = parseInt(String(v), 10);
  if (!Number.isFinite(n)) return null;
  const lvl = n + 1;
  if (lvl >= 1 && lvl <= 9) return lvl;
  return null;
}

// ------------------------------
// styles.xml -> styleId => heading level
// ------------------------------
function _styleOutlineLevel(styleEl) {
  const pPr = _firstNS(styleEl, 'pPr') || _first(styleEl, 'w:pPr');
  if (!pPr) return null;
  const ol = _firstNS(pPr, 'outlineLvl') || _first(pPr, 'w:outlineLvl');
  if (!ol) return null;
  const v = _getAttr(ol, 'w:val');
  if (v == null) return null;
  const n = Number(v);
  if (!Number.isFinite(n)) return null;
  const lvl = n + 1;
  if (lvl < 1 || lvl > 9) return null;
  return lvl;
}
function _inferHeadingLevelFromStyleToken(styleId, styleName) {
  const s = `${styleId || ''} ${styleName || ''}`.toLowerCase();
  const tokens = ['heading','überschrift','uberschrift','ueberschrift','titre','título','titulo','titolo','rubrik','section'];
  if (!tokens.some(t => s.includes(t))) return null;
  const m = s.match(/(?:^|[^0-9])([1-9])(?:[^0-9]|$)/);
  if (!m) return null;
  const lvl = Number(m[1]);
  return (lvl >= 1 && lvl <= 9) ? lvl : null;
}
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

    const outlineLvl = _styleOutlineLevel(st);
    byId.set(styleId, { styleId, name, basedOn, outlineLvl });
  }

  const resolved = new Map();
  const visiting = new Set();

  function resolve(styleId) {
    if (resolved.has(styleId)) return resolved.get(styleId);
    if (visiting.has(styleId)) return null;
    visiting.add(styleId);

    const st = byId.get(styleId);
    if (!st) { visiting.delete(styleId); return null; }

    if (st.outlineLvl) { resolved.set(styleId, st.outlineLvl); visiting.delete(styleId); return st.outlineLvl; }

    const inferred = _inferHeadingLevelFromStyleToken(st.styleId, st.name);
    if (inferred) { resolved.set(styleId, inferred); visiting.delete(styleId); return inferred; }

    if (st.basedOn) {
      const parentLvl = resolve(st.basedOn);
      if (parentLvl) { resolved.set(styleId, parentLvl); visiting.delete(styleId); return parentLvl; }
    }

    visiting.delete(styleId);
    return null;
  }

  for (const styleId of byId.keys()) resolve(styleId);

  const out = new Map();
  for (const [styleId, lvl] of resolved.entries()) {
    if (lvl && lvl >= 1 && lvl <= 9) out.set(styleId, lvl);
  }
  return out;
}

function _detectHeadingLevel(p, styleLevelMap) {
  let level = null;

  const pPr = _firstNS(p, 'pPr') || _first(p, 'w:pPr');
  const pStyle = pPr ? (_firstNS(pPr, 'pStyle') || _first(pPr, 'w:pStyle')) : null;
  if (pStyle) {
    const styleId = _getAttr(pStyle, 'w:val');
    if (styleId) {
      level = styleLevelMap?.get(styleId) ?? _headingLevelFromStyleVal(styleId);
    }
  }

  if (!level) level = _headingLevelFromOutline(p);
  return level;
}

// ------------------------------
// Baseline-first plan inspector
// ------------------------------
async function inspectRemovalPlan_JS(bytesU8 /* Uint8Array */) {
  const JSZip = await ensureJSZip();
  const input = (bytesU8 instanceof Uint8Array) ? bytesU8 : new Uint8Array(bytesU8 || []);
  const buf = new Uint8Array(input);

  const zip = await JSZip.loadAsync(buf);

  // Prefer baseline if present
  let docXml = null;
  const baseFile = zip.file(_BASE_PATH);
  if (baseFile) {
    docXml = await baseFile.async('string');
  } else {
    const docFile = zip.file('word/document.xml');
    if (!docFile) return { parts: [{ headings: [] }] };
    docXml = await docFile.async('string');
  }

  // style map from styles.xml (from same zip)
  let styleLevelMap = new Map();
  const stylesFile = zip.file('word/styles.xml');
  if (stylesFile) {
    try {
      const stylesXml = await stylesFile.async('string');
      const stylesDom = _xmlParse(stylesXml);
      styleLevelMap = _buildStyleHeadingLevelMap(stylesDom);
    } catch {
      styleLevelMap = new Map();
    }
  }

  const docDom = _xmlParse(docXml);
  const paras = Array.from(
    docDom.getElementsByTagNameNS
      ? docDom.getElementsByTagNameNS(_DOCX_W_NS, 'p')
      : docDom.getElementsByTagName('w:p')
  );

  const headings = [];
  for (let i = 0; i < paras.length; i++) {
    const p = paras[i];
    const lvl = _detectHeadingLevel(p, styleLevelMap);
    if (!lvl) continue;

    const text = _extractParagraphText(p);
    if (!text) continue;

    const cleanText = _stripLeadingNumber(text);
    const id = `sec_${String(i).padStart(6,'0')}`;
    headings.push({ id, level: lvl, text: cleanText, rawText: text, idx: i });
  }

  // Compute baseline ranges (inclusive indices; endIdx is last paragraph in section)
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

  return { parts: [{ name: (_BASE_PATH ? _BASE_PATH : 'word/document.xml'), headings, paraCount: paras.length }] };
}

// ------------------------------
// Deep cleanup (to avoid “invisible containers” leaving visible artifacts)
// ------------------------------
function _paragraphHasNontrivialContent(p) {
  if (p.getElementsByTagNameNS(_DOCX_W_NS, 'fldSimple').length) return true;
  if (p.getElementsByTagNameNS(_DOCX_W_NS, 'drawing').length) return true;
  if (p.getElementsByTagNameNS(_DOCX_W_NS, 'object').length) return true;
  if (p.getElementsByTagNameNS(_DOCX_W_NS, 'pict').length) return true;

  const ts = p.getElementsByTagNameNS(_DOCX_W_NS, 't');
  for (let i = 0; i < ts.length; i++) {
    if (_normalizeText(ts[i].textContent) !== '') return true;
  }
  return false;
}
function _isEmptyParagraph(p) {
  if (p.namespaceURI !== _DOCX_W_NS || p.localName !== 'p') return false;
  return !_paragraphHasNontrivialContent(p);
}
function _hasPageBreakRun(p) {
  const brs = p.getElementsByTagNameNS(_DOCX_W_NS, 'br');
  for (let i = 0; i < brs.length; i++) {
    const t = brs[i].getAttributeNS(_DOCX_W_NS,'type') || brs[i].getAttribute('w:type');
    if ((t || '').toLowerCase() === 'page') return true;
  }
  return false;
}
function _hasPageBreakBefore(p) {
  const pPr = _firstNS(p, 'pPr');
  if (!pPr) return false;
  return !!pPr.getElementsByTagNameNS(_DOCX_W_NS,'pageBreakBefore')[0];
}
function _pruneEmptySDTs(doc) {
  const sdts = Array.from(doc.getElementsByTagNameNS(_DOCX_W_NS,'sdt'));
  for (const sdt of sdts) {
    if (sdt.getElementsByTagNameNS(_DOCX_W_NS,'tbl').length > 0) continue;
    const ps = sdt.getElementsByTagNameNS(_DOCX_W_NS,'p');
    let hasMeaning = false;
    for (let i = 0; i < ps.length; i++) {
      if (_paragraphHasNontrivialContent(ps[i])) { hasMeaning = true; break; }
    }
    if (!hasMeaning && sdt.parentNode) sdt.parentNode.removeChild(sdt);
  }
}
function _pruneDeadTables(doc) {
  const tbls = Array.from(doc.getElementsByTagNameNS(_DOCX_W_NS,'tbl'));
  for (const t of tbls) {
    const trs = t.getElementsByTagNameNS(_DOCX_W_NS,'tr');
    let hasCell = false;
    for (let i = 0; i < trs.length; i++) {
      if (trs[i].getElementsByTagNameNS(_DOCX_W_NS,'tc').length > 0) { hasCell = true; break; }
    }
    if (!hasCell && t.parentNode) t.parentNode.removeChild(t);
  }
}
function _compactWhitespace(doc) {
  const body = doc.getElementsByTagNameNS(_DOCX_W_NS,'body')[0] || doc.documentElement;

  // Remove empty paragraphs unless they carry a page break
  let n = body.firstChild;
  while (n) {
    const next = n.nextSibling;
    if (n.namespaceURI === _DOCX_W_NS && n.localName === 'p') {
      const keepForPage = _hasPageBreakRun(n) || _hasPageBreakBefore(n);
      if (!keepForPage && _isEmptyParagraph(n)) body.removeChild(n);
    }
    n = next;
  }

  // Collapse duplicate page-break-only paragraphs
  function isPurePB(p) {
    return p.namespaceURI === _DOCX_W_NS && p.localName === 'p' && _hasPageBreakRun(p) && !_paragraphHasNontrivialContent(p);
  }
  n = body.firstChild;
  let prevWasPB = false;
  while (n) {
    const next = n.nextSibling;
    if (isPurePB(n)) {
      if (prevWasPB) body.removeChild(n);
      else prevWasPB = true;
    } else {
      prevWasPB = false;
    }
    n = next;
  }
}

// ------------------------------
// Visibility map -> baseline ranges
// ------------------------------
function _visKeyToIdx(k) {
  if (k == null) return NaN;
  const s = String(k);
  if (s.startsWith('sec_')) {
    const n = Number(s.slice(4));
    return Number.isFinite(n) ? n : NaN;
  }
  const n = Number(s);
  return Number.isFinite(n) ? n : NaN;
}
function _computeHiddenRangesFromPlan(plan, visibilityMap) {
  const headings = plan?.parts?.[0]?.headings || [];
  if (!headings.length) return [];

  const hideIdx = new Set();
  if (visibilityMap && typeof visibilityMap === 'object') {
    for (const [k, v] of Object.entries(visibilityMap)) {
      if (String(v).toUpperCase() !== 'HIDE') continue;
      const idx = _visKeyToIdx(k);
      if (Number.isFinite(idx)) hideIdx.add(idx);
    }
  }
  if (!hideIdx.size) return [];

  const ranges = [];
  for (const h of headings) {
    if (!hideIdx.has(Number(h.idx))) continue;
    const s = Number(h.startIdx);
    const e = Number(h.endIdx);
    if (!Number.isFinite(s) || !Number.isFinite(e)) continue;
    ranges.push([Math.min(s, e), Math.max(s, e)]);
  }
  if (!ranges.length) return [];

  // merge overlaps
  ranges.sort((a,b)=>a[0]-b[0]);
  const merged = [];
  for (const r of ranges) {
    const last = merged[merged.length - 1];
    if (!last || r[0] > last[1] + 1) merged.push(r.slice());
    else last[1] = Math.max(last[1], r[1]);
  }
  return merged;
}

// ------------------------------
// Baseline embedding helper
// ------------------------------
async function _ensureBaselineInZip(zip, originalBytesU8) {
  if (zip.file(_BASE_PATH)) return;

  // Prefer originalBytesU8's document.xml as baseline if provided
  if (originalBytesU8 instanceof Uint8Array && originalBytesU8.length) {
    try {
      const JSZip = await ensureJSZip();
      const z0 = await JSZip.loadAsync(new Uint8Array(originalBytesU8));
      const f0 = z0.file('word/document.xml');
      if (f0) {
        const xml0 = await f0.async('string');
        zip.file(_BASE_PATH, xml0);
        zip.file(_META_PATH, JSON.stringify({ version: 1, baselineCreated: new Date().toISOString(), source: 'originalBytesU8' }, null, 2));
        return;
      }
    } catch {
      // fallback below
    }
  }

  // Fallback: embed current word/document.xml as baseline
  const cur = zip.file('word/document.xml');
  if (cur) {
    const xml = await cur.async('string');
    zip.file(_BASE_PATH, xml);
    zip.file(_META_PATH, JSON.stringify({ version: 1, baselineCreated: new Date().toISOString(), source: 'currentDoc' }, null, 2));
  }
}

// ------------------------------
// Removal: baseline-first (THIS is the important behavioral change)
// ------------------------------
async function applyRemovalWithBackup_JS(bytesU8, visibilityMap, originalBytesU8) {
  const JSZip = await ensureJSZip();
  const input = (bytesU8 instanceof Uint8Array) ? bytesU8 : new Uint8Array(bytesU8 || []);
  const buf = new Uint8Array(input);

  const zip = await JSZip.loadAsync(buf);

  // Ensure baseline is embedded
  await _ensureBaselineInZip(zip, originalBytesU8);

  // If no baseline could be created, do nothing
  const baseFile = zip.file(_BASE_PATH);
  if (!baseFile) return await zip.generateAsync({ type: 'uint8array' });

  // Compute plan from BASELINE (not from current/mutated doc)
  const plan = await inspectRemovalPlan_JS(buf);
  const mergedRanges = _computeHiddenRangesFromPlan(plan, visibilityMap);

  if (!mergedRanges.length) {
    // still return the zip (baseline might have just been embedded)
    return await zip.generateAsync({ type: 'uint8array' });
  }

  // Parse baseline doc and remove there
  const baseXml = await baseFile.async('string');
  const baseDom = _xmlParse(baseXml);

  const paras = Array.from(
    baseDom.getElementsByTagNameNS
      ? baseDom.getElementsByTagNameNS(_DOCX_W_NS, 'p')
      : baseDom.getElementsByTagName('w:p')
  );

  // Remove bottom->top so indices remain valid
  mergedRanges.sort((a,b)=>b[0]-a[0]);
  for (const [startIdx, endIdx] of mergedRanges) {
    for (let i = endIdx; i >= startIdx; i--) {
      const p = paras[i];
      if (p && p.parentNode) p.parentNode.removeChild(p);
    }
  }

  // Deep cleanup to avoid “table skeletons / SDT shells / empty paras”
  _pruneEmptySDTs(baseDom);
  _pruneDeadTables(baseDom);
  _compactWhitespace(baseDom);

  // Write result into the *current export zip* as word/document.xml
  zip.file('word/document.xml', _xmlSerialize(baseDom));

  return await zip.generateAsync({ type: 'uint8array' });
}

async function restoreDocxFromBackup_JS(bytesU8) {
  const JSZip = await ensureJSZip();
  const input = (bytesU8 instanceof Uint8Array) ? bytesU8 : new Uint8Array(bytesU8 || []);
  const zip = await JSZip.loadAsync(new Uint8Array(input));

  const base = zip.file(_BASE_PATH);
  if (!base) return null;

  const originalXml = await base.async('string');
  zip.file('word/document.xml', originalXml);
  return await zip.generateAsync({ type: 'uint8array' });
}

// ------------------------------
// Pyodide helpers (kept intact)
// ------------------------------
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
    pyLoaded = true;
  }
  return py;
}

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

// ---- docVar read/write (JSZip-first, Pyodide fallback) ----
// These functions are the canonical persistence layer for CRONOS_PAYLOAD.
// They MUST work even when no Pyodide helper functions were loaded.
const _CT_PATH = '[Content_Types].xml';
const _SETTINGS_PATH = 'word/settings.xml';
const _CUSTOMPROPS_PATH = 'docProps/custom.xml';
const _CP_NS = 'http://schemas.openxmlformats.org/officeDocument/2006/custom-properties';
const _VT_NS = 'http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes';

function _toU8(x) { return (x instanceof Uint8Array) ? x : new Uint8Array(x || []); }

async function _loadZip(bytesU8) {
  const JSZip = await ensureJSZip();
  return await JSZip.loadAsync(_toU8(bytesU8));
}

async function _zipToU8(zip) {
  const out = await zip.generateAsync({
    type: 'uint8array',
    compression: 'DEFLATE',
    compressionOptions: { level: 6 }
  });
  return out instanceof Uint8Array ? out : new Uint8Array(out || []);
}

function _ensureContentTypesOverride(ctDoc, partName, contentType) {
  if (!ctDoc) return ctDoc;
  const root = ctDoc.documentElement;
  if (!root) return ctDoc;

  const overrides = Array.from(root.getElementsByTagName('Override') || []);
  const part = '/' + String(partName).replace(/^\//, '');
  const found = overrides.find(o => (o.getAttribute('PartName') || '') === part);
  if (found) {
    // Ensure correct ContentType (idempotent)
    if (contentType) found.setAttribute('ContentType', contentType);
    return ctDoc;
  }

  const el = ctDoc.createElement('Override');
  el.setAttribute('PartName', part);
  if (contentType) el.setAttribute('ContentType', contentType);
  root.appendChild(el);
  return ctDoc;
}

async function _ensureContentTypes(zip, partName, contentType) {
  const f = zip.file(_CT_PATH);
  if (!f) return; // extremely rare; most DOCX have it
  const xmlText = await f.async('string');
  const ctDoc = _xmlParse(xmlText);
  _ensureContentTypesOverride(ctDoc, partName, contentType);
  zip.file(_CT_PATH, _xmlSerialize(ctDoc));
}

function _findChildNS(parent, ns, localName) {
  if (!parent) return null;
  for (const n of Array.from(parent.childNodes || [])) {
    if (n.nodeType === 1 && n.namespaceURI === ns && n.localName === localName) return n;
  }
  return null;
}

function _ensureChildNS(doc, parent, ns, qname, localName) {
  let el = _findChildNS(parent, ns, localName);
  if (!el) {
    el = doc.createElementNS(ns, qname);
    parent.appendChild(el);
  }
  return el;
}

function _getAttrAny(el, ns, local, qname) {
  if (!el) return null;
  return el.getAttributeNS(ns, local) ?? el.getAttribute(qname) ?? null;
}

function _setAttrAny(el, ns, local, qname, value) {
  if (!el) return;
  try { el.setAttributeNS(ns, qname, String(value)); }
  catch { el.setAttribute(qname, String(value)); }
}

async function readDocVarSettings(bytesU8, key) {
  // JSZip-first (no Pyodide required)
  try {
    const zip = await _loadZip(bytesU8);
    const f = zip.file(_SETTINGS_PATH);
    if (!f) return null;

    const xmlText = await f.async('string');
    const doc = _xmlParse(xmlText);
    const settings = doc.documentElement;
    if (!settings) return null;

    const docVars = _findChildNS(settings, _DOCX_W_NS, 'docVars');
    if (!docVars) return null;

    const vars = Array.from(docVars.getElementsByTagNameNS(_DOCX_W_NS, 'docVar'));
    for (const v of vars) {
      const name = _getAttrAny(v, _DOCX_W_NS, 'name', 'w:name');
      if (name === key) {
        const val = _getAttrAny(v, _DOCX_W_NS, 'val', 'w:val');
        return (val == null) ? '' : String(val);
      }
    }
    return null;
  } catch (jsErr) {
    // Optional Py fallback (only if function exists)
    try {
      await ensurePy();
      const fn = py?.globals?.get?.('read_docvar_settings');
      if (!fn) return null;
      const buf = _toU8(bytesU8);
      const pyBytes = py.toPy(buf);
      let pyOut;
      try { pyOut = fn(pyBytes, key); }
      finally {
        try { fn.destroy && fn.destroy(); } catch {}
        try { pyBytes.destroy && pyBytes.destroy(); } catch {}
      }
      const out = pyOut?.toJs ? pyOut.toJs() : pyOut;
      try { pyOut?.destroy && pyOut.destroy(); } catch {}
      return out;
    } catch {
      return null;
    }
  }
}

async function writeDocVarSettings(bytesU8, key, jsonStr) {
  try {
    const zip = await _loadZip(bytesU8);

    let xmlText = null;
    const f = zip.file(_SETTINGS_PATH);
    if (f) xmlText = await f.async('string');

    if (!xmlText) {
      xmlText =
        `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
        `<w:settings xmlns:w="${_DOCX_W_NS}"></w:settings>`;
    }

    const doc = _xmlParse(xmlText);
    const settings = doc.documentElement;
    if (!settings) throw new Error('Failed to parse/create word/settings.xml');

    const docVars = _ensureChildNS(doc, settings, _DOCX_W_NS, 'w:docVars', 'docVars');

    let target = null;
    const vars = Array.from(docVars.getElementsByTagNameNS(_DOCX_W_NS, 'docVar'));
    for (const v of vars) {
      const name = _getAttrAny(v, _DOCX_W_NS, 'name', 'w:name');
      if (name === key) { target = v; break; }
    }
    if (!target) {
      target = doc.createElementNS(_DOCX_W_NS, 'w:docVar');
      _setAttrAny(target, _DOCX_W_NS, 'name', 'w:name', key);
      docVars.appendChild(target);
    }
    _setAttrAny(target, _DOCX_W_NS, 'val', 'w:val', jsonStr);

    zip.file(_SETTINGS_PATH, _xmlSerialize(doc));

    // Ensure content type override exists
    await _ensureContentTypes(zip, _SETTINGS_PATH, 'application/vnd.openxmlformats-officedocument.wordprocessingml.settings+xml');

    return await _zipToU8(zip);
  } catch (jsErr) {
    // Optional Py fallback
    try {
      await ensurePy();
      const fn = py?.globals?.get?.('write_docvar_settings');
      if (!fn) return _toU8(bytesU8);
      const buf = _toU8(bytesU8);
      const pyBytes = py.toPy(buf);
      let pyOut;
      try { pyOut = fn(pyBytes, key, jsonStr); }
      finally {
        try { fn.destroy && fn.destroy(); } catch {}
        try { pyBytes.destroy && pyBytes.destroy(); } catch {}
      }
      const out = pyOut?.toJs ? pyOut.toJs() : pyOut;
      try { pyOut?.destroy && pyOut.destroy(); } catch {}
      return out instanceof Uint8Array ? out : new Uint8Array(out || []);
    } catch {
      return _toU8(bytesU8);
    }
  }
}

// Custom properties (docProps/custom.xml) as a secondary channel
async function readDocVarCustom(bytesU8, key) {
  try {
    const zip = await _loadZip(bytesU8);
    const f = zip.file(_CUSTOMPROPS_PATH);
    if (!f) return null;

    const xmlText = await f.async('string');
    const doc = _xmlParse(xmlText);
    const props = doc.documentElement;
    if (!props) return null;

    // property elements in CP namespace; name attr is un-namespaced
    const nodes = Array.from(props.getElementsByTagNameNS(_CP_NS, 'property'));
    for (const p of nodes) {
      const name = p.getAttribute('name') || '';
      if (name !== key) continue;

      // Find first vt:* child
      const vtChild = Array.from(p.childNodes || []).find(n => n.nodeType === 1 && n.namespaceURI === _VT_NS);
      if (!vtChild) return '';
      return String(vtChild.textContent || '');
    }
    return null;
  } catch (jsErr) {
    // Optional Py fallback
    try {
      await ensurePy();
      const fn = py?.globals?.get?.('read_docvar_custom');
      if (!fn) return null;
      const buf = _toU8(bytesU8);
      const pyBytes = py.toPy(buf);
      let pyOut;
      try { pyOut = fn(pyBytes, key); }
      finally {
        try { fn.destroy && fn.destroy(); } catch {}
        try { pyBytes.destroy && pyBytes.destroy(); } catch {}
      }
      const out = pyOut?.toJs ? pyOut.toJs() : pyOut;
      try { pyOut?.destroy && pyOut.destroy(); } catch {}
      return out;
    } catch {
      return null;
    }
  }
}

async function writeDocVarCustom(bytesU8, key, jsonStr) {
  try {
    const zip = await _loadZip(bytesU8);

    let xmlText = null;
    const f = zip.file(_CUSTOMPROPS_PATH);
    if (f) xmlText = await f.async('string');

    if (!xmlText) {
      xmlText =
        `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
        `<cp:Properties xmlns:cp="${_CP_NS}" xmlns:vt="${_VT_NS}"></cp:Properties>`;
    }

    const doc = _xmlParse(xmlText);
    const props = doc.documentElement;
    if (!props) throw new Error('Failed to parse/create docProps/custom.xml');

    // Find property by name or create
    let prop = null;
    const nodes = Array.from(props.getElementsByTagNameNS(_CP_NS, 'property'));
    for (const p of nodes) {
      if ((p.getAttribute('name') || '') === key) { prop = p; break; }
    }

    const nextPid = () => {
      const pids = nodes.map(p => Number(p.getAttribute('pid'))).filter(Number.isFinite);
      const max = pids.length ? Math.max(...pids) : 1;
      return max + 1;
    };

    if (!prop) {
      prop = doc.createElementNS(_CP_NS, 'cp:property');
      prop.setAttribute('fmtid', '{D5CDD505-2E9C-101B-9397-08002B2CF9AE}');
      prop.setAttribute('pid', String(nextPid()));
      prop.setAttribute('name', key);
      props.appendChild(prop);
    } else {
      // Clear existing children
      while (prop.firstChild) prop.removeChild(prop.firstChild);
    }

    // Store as vt:lpwstr (string)
    const v = doc.createElementNS(_VT_NS, 'vt:lpwstr');
    v.textContent = String(jsonStr ?? '');
    prop.appendChild(v);

    zip.file(_CUSTOMPROPS_PATH, _xmlSerialize(doc));

    await _ensureContentTypes(zip, _CUSTOMPROPS_PATH, 'application/vnd.openxmlformats-officedocument.custom-properties+xml');

    return await _zipToU8(zip);
  } catch (jsErr) {
    // Optional Py fallback
    try {
      await ensurePy();
      const fn = py?.globals?.get?.('write_docvar_custom');
      if (!fn) return _toU8(bytesU8);
      const buf = _toU8(bytesU8);
      const pyBytes = py.toPy(buf);
      let pyOut;
      try { pyOut = fn(pyBytes, key, jsonStr); }
      finally {
        try { fn.destroy && fn.destroy(); } catch {}
        try { pyBytes.destroy && pyBytes.destroy(); } catch {}
      }
      const out = pyOut?.toJs ? pyOut.toJs() : pyOut;
      try { pyOut?.destroy && pyOut.destroy(); } catch {}
      return out instanceof Uint8Array ? out : new Uint8Array(out || []);
    } catch {
      return _toU8(bytesU8);
    }
  }
}

// Auto writer: prefer settings.docVars, fall back to custom props
async function writeDocVar(bytesU8, key, jsonStr) {
  try {
    return await writeDocVarSettings(bytesU8, key, jsonStr);
  } catch {
    return await writeDocVarCustom(bytesU8, key, jsonStr);
  }
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

// ------------------------------
// Public API (JSZip-first, Py fallback kept where appropriate)
// ------------------------------
async function inspectRemovalPlan(bytesU8, visibilityMap) {
  try {
    return await inspectRemovalPlan_JS(bytesU8, visibilityMap);
  } catch (e) {
    // Py fallback
    try {
      await ensurePy();
      const fn = py.globals.get('inspect_export_removal_plan');
      if (!fn) return { parts: [{ headings: [] }] };

      const buf = bytesU8 instanceof Uint8Array ? bytesU8 : new Uint8Array(bytesU8 || []);
      const pyBytes = py.toPy(buf);
      const pyMap = py.toPy(serializeVisibilityMapForPython(visibilityMap));
      let pyOut;

      try { pyOut = fn(pyBytes, pyMap); }
      finally {
        try { if (fn && _hasFn(fn.destroy)) fn.destroy(); } catch {}
        try { if (pyBytes && _hasFn(pyBytes.destroy)) pyBytes.destroy(); } catch {}
        try { if (pyMap && _hasFn(pyMap.destroy)) pyMap.destroy(); } catch {}
      }

      const out = pyOut?.toJs ? pyOut.toJs() : pyOut;
      try { if (pyOut && _hasFn(pyOut.destroy)) pyOut.destroy(); } catch {}
      return out || { parts: [{ headings: [] }] };
    } catch {
      return { parts: [{ headings: [] }] };
    }
  }
}

async function applyRemovalWithBackup(bytesU8, visibilityMap, originalBytesU8) {
  // JSZip-first baseline removal
  try {
    return await applyRemovalWithBackup_JS(bytesU8, visibilityMap, originalBytesU8);
  } catch (jsErr) {
    // Py fallback as last resort
    try {
      await ensurePy();
      const fn = py.globals.get('apply_removal_with_backup');
      if (!fn) return new Uint8Array(bytesU8 instanceof Uint8Array ? bytesU8 : new Uint8Array(bytesU8 || []));
      const buf = bytesU8 instanceof Uint8Array ? bytesU8 : new Uint8Array(bytesU8 || []);
      const pyBytes = py.toPy(buf);
      const pyMap = py.toPy(serializeVisibilityMapForPython(visibilityMap));
      const pyOrig = (originalBytesU8 instanceof Uint8Array) ? py.toPy(originalBytesU8) : py.toPy(new Uint8Array(originalBytesU8 || []));
      const out = fn(pyBytes, pyMap, pyOrig);
      try { pyBytes.destroy && pyBytes.destroy(); } catch {}
      try { pyMap.destroy && pyMap.destroy(); } catch {}
      try { pyOrig.destroy && pyOrig.destroy(); } catch {}
      return new Uint8Array(out || []);
    } catch (pyErr) {
      try { console.warn('applyRemovalWithBackup failed; returning input unchanged', { jsErr, pyErr }); } catch {}
      return new Uint8Array(bytesU8 instanceof Uint8Array ? bytesU8 : new Uint8Array(bytesU8 || []));
    }
  }
}

async function restoreDocxFromBackup(bytesU8) {
  // JSZip-first restore from customXml/originalDocument.xml
  try {
    const restored = await restoreDocxFromBackup_JS(bytesU8);
    if (restored instanceof Uint8Array && restored.length) return restored;
  } catch (e) {
    try { console.warn('restoreDocxFromBackup_JS failed; falling back to Pyodide', e); } catch {}
  }

  await ensurePy();
  const fn = py.globals.get('restore_document_from_backup');
  if (!fn) return new Uint8Array(bytesU8 instanceof Uint8Array ? bytesU8 : new Uint8Array(bytesU8 || []));
  const buf = bytesU8 instanceof Uint8Array ? bytesU8 : new Uint8Array(bytesU8 || []);
  const pyBytes = py.toPy(buf);
  const out = fn(pyBytes);
  try { pyBytes.destroy && pyBytes.destroy(); } catch {}
  return new Uint8Array(out || []);
}

// ------------------------------
// Exports
// ------------------------------
if (typeof window !== 'undefined') {
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
}
// ------------------------------

