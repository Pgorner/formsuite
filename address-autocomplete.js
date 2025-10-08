// address-autocomplete.js
// Lightweight address autocomplete using Photon (Komoot) – no API key.
// Polite usage: debounced, min-length, small result set, in-memory cache.

(function (global) {
  const PHOTON_URL = 'https://photon.komoot.io/api/';
  const MIN_LEN = 3;
  const DEBOUNCE_MS = 250;
  const MAX_RESULTS = 8;

  const cache = new Map(); // key -> results

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

  async function fetchPhoton(query, { lang = 'de', bbox = null, biasLonLat = null } = {}) {
    const key = JSON.stringify({ query, lang, bbox, biasLonLat });
    if (cache.has(key)) return cache.get(key);

    const params = new URLSearchParams();
    params.set('q', query);
    params.set('limit', String(MAX_RESULTS));
    if (lang) params.set('lang', lang);
    if (bbox && Array.isArray(bbox) && bbox.length === 4) params.set('bbox', bbox.join(','));
    if (biasLonLat && Array.isArray(biasLonLat) && biasLonLat.length === 2) {
      params.set('lon', biasLonLat[0]);
      params.set('lat', biasLonLat[1]);
    }

    const url = `${PHOTON_URL}?${params.toString()}`;
    const res = await fetch(url, { method: 'GET' });
    if (!res.ok) throw new Error('Photon request failed: ' + res.status);
    const json = await res.json();
    const feats = Array.isArray(json.features) ? json.features : [];
    cache.set(key, feats);
    return feats;
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

    const label = h('label', { class: 'small', for: id }, labelText, required ? ' *' : '');
    const input = h('input', {
      id, type: 'text', placeholder: 'Start typing an address…',
      autocomplete: 'off', spellcheck: 'false', 'aria-haspopup': 'listbox', 'aria-expanded': 'false'
    });

    // overlay dropdown (absolute)
    const list = h('div', { class: 'addr-list', role: 'listbox', id: id + '_list' });

    // screen-reader only live region (no visual preview / no extra space)
    const sr = h('div', { class: 'sr-only', 'aria-live': 'polite' });

    if (currentValue?.formatted) input.value = currentValue.formatted;

    field.appendChild(label);
    field.appendChild(input);
    field.appendChild(list);
    field.appendChild(sr);
    container.appendChild(field);

    function renderList(items) {
      list.innerHTML = '';
      highlightIndex = -1;
      featsCache = items;
      items.forEach((f, idx) => {
        const line = toDisplayLine(f);
        const item = h('div', { class: 'addr-item', role: 'option', 'data-idx': String(idx) }, line);
        item.addEventListener('pointerdown', (e) => {
          e.preventDefault(); // retain input focus
          pick(idx);
        });
        list.appendChild(item);
      });
      const open = items.length > 0;
      list.style.display = open ? 'block' : 'none';
      input.setAttribute('aria-expanded', open ? 'true' : 'false');
      sr.textContent = open ? `${items.length} Treffer` : '';
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
    }

    const doQuery = debounce(async () => {
      const q = input.value.trim();
      if (q.length < MIN_LEN) { renderList([]); return; }
      sr.textContent = 'Suche läuft…';
      try {
        const feats = await fetchPhoton(q, { lang: navigator.language?.slice(0,2) || 'en' });
        renderList(feats.slice(0, MAX_RESULTS));
      } catch {
        renderList([]);
        sr.textContent = 'Abfrage fehlgeschlagen.';
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
      if (e.key === 'Enter' && open) {
        e.preventDefault();
        const idx = (highlightIndex >= 0 ? highlightIndex : 0);
        pick(idx);
      }
      if (e.key === 'Escape' && open) { list.style.display = 'none'; input.setAttribute('aria-expanded','false'); }
    });

    document.addEventListener('click', (e) => {
      if (!container.contains(e.target)) {
        list.style.display = 'none';
        input.setAttribute('aria-expanded','false');
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
.addr-field > input[type="text"] {
  width: 100%;
  padding: 10px 12px;
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
