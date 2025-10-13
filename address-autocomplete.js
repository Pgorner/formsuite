// address-autocomplete.js
// Lightweight address autocomplete using Photon (Komoot) – no API key.
// Polite usage: debounced, min-length, small result set, in-memory cache.

(function (global) {
  const PHOTON_URL = 'https://photon.komoot.io/api/';
  const MIN_LEN = 3;
  const DEBOUNCE_MS = 180;
  const MAX_RESULTS = 8;

  // DE-first defaults
  const DEFAULT_LANG = 'de';
  // Approx Germany bbox [minLon, minLat, maxLon, maxLat]
  const DE_BBOX = [5.9, 47.2, 15.1, 55.2];
  const BIAS_LS_KEY = 'FS_ADDR_BIAS'; // [lon,lat]

  const cache = new Map(); // key -> results
  const negativeCache = new Map(); // normalized query -> ts

  // Preconnect to speed up first request
  (function preconnectOnce(){
    try {
      if (document.getElementById('preconnect-photon')) return;
      const l = document.createElement('link');
      l.id = 'preconnect-photon';
      l.rel = 'preconnect';
      l.href = 'https://photon.komoot.io';
      l.crossOrigin = '';
      document.head.appendChild(l);
    } catch {}
  })();

  function debounce(fn, ms) {
    let t = null;
    return (...args) => {
      clearTimeout(t);
      t = setTimeout(() => fn(...args), ms);
    };
  }

  function h(tag, props = {}, ...children) {
    const el = document.createElement(tag);
    for (const [k, v] of Object.entries(props || {})) {
      if (k === 'class') el.className = v;
      else if (k === 'for') el.htmlFor = v;
      else if (k.startsWith('on') && typeof v === 'function') el.addEventListener(k.slice(2).toLowerCase(), v);
      else if (v != null) el.setAttribute(k, v);
    }
    for (const c of children) {
      if (c == null) continue;
      if (typeof c === 'string') el.appendChild(document.createTextNode(c));
      else el.appendChild(c);
    }
    return el;
  }

  // ---------- DE-first normalization, variants, scoring ----------
  function stripDiacritics(s){
    return String(s||'').normalize('NFKD').replace(/[\u0300-\u036f]/g,'');
  }
  function normalizeUmlauts(s){
    return s
      .replace(/ß/g,'ss')
      .replace(/Ä/g,'Ae').replace(/Ö/g,'Oe').replace(/Ü/g,'Ue')
      .replace(/ä/g,'ae').replace(/ö/g,'oe').replace(/ü/g,'ue');
  }
  function normalizeQuery(q){
    let s = normalizeUmlauts(stripDiacritics(String(q||'').trim()))
      .replace(/[.,;]+/g,' ')
      .replace(/\s+/g,' ')
      .toLowerCase();
    // soften abbreviations
    s = s
      .replace(/\bstr\.?\b/g,' strasse')
      .replace(/\bplz\b/g,'')
      .replace(/\bnr\.?\b/g,'')
      .replace(/\bhausnr\.?\b/g,'')
      .replace(/\bdeutschland\b/g,'');
    return s.trim().replace(/\s+/g,' ');
  }
  const RX_PLZ = /\b\d{5}\b/;
  const RX_HNO = /^(\d+[a-z]?(?:-\d+)?)([a-z]?)$/i;
  function parseTokens(qn){
    const tokens = qn.split(' ').filter(Boolean);
    let postcode=null,houseno=null;
    for(let i=tokens.length-1;i>=0;i--){
      const t=tokens[i];
      if(!postcode && RX_PLZ.test(t)){ postcode=t; continue; }
      if(!houseno && RX_HNO.test(t)){ houseno=t; continue; }
    }
    return { tokens, postcode, houseno };
  }
  function buildVariantsDE(qn){
    const { tokens, houseno, postcode } = parseTokens(qn);
    if(!tokens.length) return [];
    const last=tokens[tokens.length-1];
    const city = /\d/.test(last) ? null : last;
    const street = (city ? tokens.slice(0,-1) : tokens).join(' ');
    const v = new Set();
    const push = s=>{ s=String(s||'').trim().replace(/\s+/g,' '); if(s) v.add(s); };
    push(qn);
    if(city && houseno) push(`${street} ${houseno} ${city}`);
    if(city) push(`${street} ${city}`);
    if(houseno) push(`${street} ${houseno}`);
    if(city && houseno) push(`${city} ${street} ${houseno}`);
    if(city) push(`${city} ${street}`);
    if(postcode) push(qn.replace(postcode,'').replace(/\s+/g,' '));
    return Array.from(v).slice(0,4);
  }
  function scoreFeature(feat, qn){
    const p=feat.properties||{};
    const hay = normalizeQuery([p.name,[p.housenumber,p.street].filter(Boolean).join(' '),p.postcode,p.city||p.town||p.village,p.country].filter(Boolean).join(' '));
    const toks = qn.split(' ').filter(Boolean);
    let s=0;
    toks.forEach(t=>{
      if(hay.startsWith(t+' ')||hay.includes(' '+t+' ')) s+=3;
      else if(hay.includes(t)) s+=1;
    });
    const hno = toks.find(t=>RX_HNO.test(t));
    if(hno && String(p.housenumber||'').toLowerCase()===hno.toLowerCase()) s+=5;
    const plz = toks.find(t=>RX_PLZ.test(t));
    if(plz && String(p.postcode||'')===plz) s+=3;
    return s;
  }
  function highlightMatch(text, qn){
    const toks = qn.split(' ').filter(t=>t.length>=3);
    if(!toks.length) return text;
    let html = text;
    toks.forEach(t=>{
      const rx = new RegExp('('+t.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')+')','ig');
      html = html.replace(rx,'<mark>$1</mark>');
    });
    return html;
  }

  function toDisplayLine(feat) {
    const p = feat.properties || {};
    const parts = [
      p.name,
      [p.housenumber, p.street].filter(Boolean).join(' '),
      p.postcode,
      p.city || p.town || p.village,
      p.country
    ].filter(Boolean);
    return parts.join(', ')
      .replace(/\s+,/g, ',')
      .replace(/,+/g, ',')
      .replace(/,\s*,/g, ', ')
      .replace(/\s{2,}/g, ' ')
      .trim() || p.label || p.name || '—';
  }

  function normalizeValue(feat) {
    const p = feat.properties || {};
    return {
      formatted: toDisplayLine(feat),
      street: p.street || '',
      houseNumber: p.housenumber || '',
      postcode: p.postcode || '',
      city: p.city || p.town || p.village || '',
      country: p.country || '',
      lat: feat.geometry && feat.geometry.coordinates ? feat.geometry.coordinates[1] : null,
      lon: feat.geometry && feat.geometry.coordinates ? feat.geometry.coordinates[0] : null
    };
  }

  async function fetchPhoton(query, { lang = DEFAULT_LANG, bbox = DE_BBOX, biasLonLat = null, signal = undefined } = {}) {
    const qn = normalizeQuery(query);
    if (!qn || qn.length < MIN_LEN) return [];
    const neg = negativeCache.get(qn);
    if (neg && Date.now() - neg < 30_000) return [];

    const variants = buildVariantsDE(qn);
    const featsAll = [];
    const bias = (Array.isArray(biasLonLat) && biasLonLat.length===2) ? biasLonLat : (function(){ try{const v=JSON.parse(localStorage.getItem(BIAS_LS_KEY)||'null'); return (Array.isArray(v)&&v.length===2)?v:null;}catch{return null;} })();

    async function runOne(q){
      const key = JSON.stringify({ q, lang, bbox, bias });
      if (cache.has(key)) return cache.get(key);
      const params = new URLSearchParams();
      params.set('q', q);
      params.set('limit', String(MAX_RESULTS));
      if (lang) params.set('lang', lang);
      if (bbox && Array.isArray(bbox) && bbox.length===4) params.set('bbox', bbox.join(','));
      if (bias) { params.set('lon', bias[0]); params.set('lat', bias[1]); }
      const url = `${PHOTON_URL}?${params.toString()}`;
      const res = await fetch(url, { method: 'GET', signal });
      if (!res.ok) return [];
      const json = await res.json();
      const feats = Array.isArray(json.features) ? json.features : [];
      cache.set(key, feats);
      return feats;
    }

    // Run up to 3 variants in parallel for speed
    const toRun = variants.slice(0, 3);
    const results = await Promise.allSettled(toRun.map(v => runOne(v)));
    for (const r of results) {
      if (r.status === 'fulfilled' && Array.isArray(r.value)) featsAll.push(...r.value);
    }
    if (!featsAll.length) negativeCache.set(qn, Date.now());

    const uniq = new Map();
    featsAll.forEach(f => {
      const id = JSON.stringify([f?.properties?.osm_id, f?.properties?.osm_type, f?.geometry?.coordinates||[]]);
      if (!uniq.has(id)) uniq.set(id, f);
    });
    return Array.from(uniq.values())
      .map(f => ({ f, s: scoreFeature(f, qn) }))
      .sort((a,b) => b.s - a.s)
      .map(x => x.f)
      .slice(0, MAX_RESULTS);
  }

  function mount(container, opts) {
    const id = opts?.id || ('addr_' + Math.random().toString(36).slice(2, 8));
    const labelText = opts?.label || 'Address';
    const required = !!opts?.required;
    let currentValue = opts?.value || null;
    let highlightIndex = -1;
    let featsCache = [];

    // host + structure
    container.classList.add('addr-host');
    container.innerHTML = '';
    const field = h('div', { class: 'addr-field' }); // positioning context

    const label = h('label', { class: 'small fs-label', for: id }, labelText, required ? ' *' : '');
    const input = h('input', {
      id, type: 'text', placeholder: 'Start typing an address…',
      autocomplete: 'off', spellcheck: 'false', 'aria-haspopup': 'listbox', 'aria-expanded': 'false'
    });

    // overlay dropdown (absolute)
    const list = h('div', { class: 'addr-list', role: 'listbox', id: id + '_list' });

    // screen-reader only live region (no visual preview / no extra space)
    const sr = h('div', { class: 'sr-only', 'aria-live': 'polite' });
    // input + spinner wrapper (for vertical centering)
    const inputWrap = h('div', { class: 'addr-input-wrap' });
    // right-side loading indicator (spins via ::before)
    const indicator = h('div', { class: 'addr-indicator', 'aria-hidden': 'true' });

    if (currentValue?.formatted) input.value = currentValue.formatted;

    field.appendChild(label);
    inputWrap.appendChild(input);
    inputWrap.appendChild(indicator);
    field.appendChild(inputWrap);
    field.appendChild(list);
    field.appendChild(sr);
    container.appendChild(field);

    function renderList(items) {
      list.innerHTML = '';
      highlightIndex = -1;
      featsCache = items;
      items.forEach((f, idx) => {
        const line = toDisplayLine(f);
        const item = h('div', { class: 'addr-item', role: 'option', 'data-idx': String(idx) });
        item.innerHTML = highlightMatch(line, normalizeQuery(input.value));
        item.addEventListener('pointerdown', (e) => {
          e.preventDefault(); // retain input focus
          pick(idx);
        });
        list.appendChild(item);
      });
      const open = items.length > 0;
      list.style.display = open ? 'block' : 'none';
      input.setAttribute('aria-expanded', open ? 'true' : 'false');
      sr.textContent = open ? `${items.length} Treffer` : 'Keine Treffer – z. B. "Straße 12, Stadt" oder PLZ hinzufügen';
    }

    function ensureVisible(activeEl) {
      if (!activeEl) return;
      const { top, bottom } = activeEl.getBoundingClientRect();
      const lp = list.getBoundingClientRect();
      if (top < lp.top) activeEl.scrollIntoView({ block: 'nearest' });
      if (bottom > lp.bottom) activeEl.scrollIntoView({ block: 'nearest' });
    }

    function highlight(delta) {
      if (!list.children.length) return;
      highlightIndex = (highlightIndex + delta + list.children.length) % list.children.length;
      Array.from(list.children).forEach((el, i) => {
        el.classList.toggle('active', i === highlightIndex);
        if (i === highlightIndex) ensureVisible(el);
      });
    }

    function pick(idx) {
      const f = featsCache[idx];
      if (!f) return;
      const val = normalizeValue(f);
      currentValue = val;
      input.value = val.formatted || '';
      list.style.display = 'none';
      input.setAttribute('aria-expanded', 'false');
      // NO visible preview; just fire change
      try { opts?.onChange && opts.onChange(val); } catch {}
      // Persist bias for follow-up queries
      try {
        const lon = f?.geometry?.coordinates?.[0];
        const lat = f?.geometry?.coordinates?.[1];
        if (Number.isFinite(lon) && Number.isFinite(lat)) {
          localStorage.setItem(BIAS_LS_KEY, JSON.stringify([lon, lat]));
        }
      } catch {}
      field.classList.remove('loading');
    }

    let inFlightAbort = null;
    const doQuery = debounce(async () => {
      const q = input.value.trim();
      if (q.length < MIN_LEN) { field.classList.remove('loading'); renderList([]); return; }
      sr.textContent = 'Suche läuft…';
      if (inFlightAbort) { try { inFlightAbort.abort(); } catch {} }
      const controller = new AbortController();
      inFlightAbort = controller;
      field.classList.add('loading');
      try {
        const feats = await fetchPhoton(q, { lang: DEFAULT_LANG, bbox: DE_BBOX, signal: controller.signal });
        if (inFlightAbort !== controller) return; // outdated
        renderList(feats.slice(0, MAX_RESULTS));
        if (inFlightAbort === controller) field.classList.remove('loading');
      } catch {
        if (inFlightAbort !== controller) return;
        renderList([]);
        sr.textContent = 'Abfrage fehlgeschlagen.';
        field.classList.remove('loading');
      }
    }, DEBOUNCE_MS);

    input.addEventListener('input', () => {
      currentValue = null;
      try { opts?.onChange && opts.onChange(null); } catch {}
      doQuery();
    });

    input.addEventListener('keydown', (e) => {
      const open = list.style.display !== 'none';
      if (e.key === 'ArrowDown' && open) { e.preventDefault(); highlight(+1); return; }
      if (e.key === 'ArrowUp'   && open) { e.preventDefault(); highlight(-1); return; }
      if (e.key === 'Enter') {
        if (open) {
          e.preventDefault();
          const idx = (highlightIndex >= 0 ? highlightIndex : 0);
          pick(idx);
        } else {
          const s = input.value.trim();
          if (s) {
            currentValue = { formatted: s };
            try { opts?.onChange && opts.onChange(currentValue); } catch {}
          }
        }
      }
      if (e.key === 'Escape' && open) { list.style.display = 'none'; input.setAttribute('aria-expanded','false'); }
    });

    document.addEventListener('click', (e) => {
      if (!container.contains(e.target)) {
        list.style.display = 'none';
        input.setAttribute('aria-expanded','false');
      }
    });

    input.addEventListener('blur', () => {
      const s = input.value.trim();
      if (!s) return;
      if (!currentValue || currentValue.formatted !== s) {
        currentValue = { formatted: s };
        try { opts?.onChange && opts.onChange(currentValue); } catch {}
      }
    });

    // public API
    return {
      get value() { return currentValue; },
      set value(v) {
        currentValue = v;
        input.value = v?.formatted || '';
        // no preview update
      },
      focus: () => input.focus()
    };
  }

  // Inject minimal styles once
  (function injectStyles() {
    if (document.getElementById('addr-autocomplete-styles')) return;
    const css = `
.addr-host { position: relative; }
.addr-field { position: relative; display: grid; gap: 6px; }
.addr-input-wrap { position: relative; }
.addr-field > input[type="text"] {
  width: 100%;
  padding: 10px 12px;
  padding-right: 34px; /* room for spinner */
  border: 1.5px solid var(--border-strong, #d1d5db);
  border-radius: var(--radius-sm, 8px);
  font-size: 14px;
  background: var(--card, #fff);
  color: var(--ink, #0f172a);
  outline: none;
  box-shadow: inset 0 1px 0 rgba(0,0,0,.02);
  transition: border-color .15s ease, box-shadow .15s ease;
}
.addr-field > input[type="text"]:focus {
  border-color: var(--focus-border, #86a7ff);
  box-shadow: 0 0 0 3px var(--focus-ring, rgba(134,167,255,.28));
}

/* Loading indicator */
.addr-indicator {
  position: absolute;
  right: 10px;
  top: 50%;
  transform: translateY(-50%);
  width: 16px; height: 16px;
  display: none;
}
.addr-indicator::before {
  content: '';
  display: block;
  width: 100%; height: 100%;
  border: 2px solid rgba(0,0,0,0.18);
  border-top-color: var(--accent,#2563eb);
  border-radius: 50%;
  animation: addrSpin .8s linear infinite;
}
.addr-field.loading .addr-indicator { display: block; }
@keyframes addrSpin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }

/* Overlayed dropdown */
.addr-list {
  position: absolute;
  top: calc(100% + 6px);
  left: 0; right: 0;
  z-index: 40;
  display: none;
  border: 1px solid var(--border, #e5e7eb);
  border-radius: var(--radius, 10px);
  background: var(--card, #fff);
  box-shadow: var(--shadow-lg, 0 12px 24px rgba(0,0,0,.12));
  max-height: 260px;
  overflow: auto;
  padding: 4px 0;
}
.addr-item {
  padding: 8px 12px;
  cursor: pointer;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.addr-item:hover,
.addr-item.active {
  background: rgba(48,136,255,.08);
}

/* screen-reader-only helper (no layout space) */
.sr-only {
  position: absolute !important;
  width: 1px; height: 1px; padding: 0; margin: 0;
  overflow: hidden; clip: rect(0 0 0 0); clip-path: inset(50%);
  white-space: nowrap; border: 0;
}
`;
    const el = document.createElement('style');
    el.id = 'addr-autocomplete-styles';
    el.textContent = css;
    document.head.appendChild(el);
  })();

  global.AddressAuto = { mount };

})(window);
