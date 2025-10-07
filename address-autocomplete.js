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
    // Build a nice human-readable string
    const p = feat.properties || {};
    const parts = [
      p.name,
      [p.housenumber, p.street].filter(Boolean).join(' '), // prefer street + number together
      p.postcode,
      p.city || p.town || p.village,
      p.country
    ].filter(Boolean);
    // De-duplicate small artifacts
    const line = parts.join(', ').replace(/\s+,/g, ',').replace(/,+/g, ',').replace(/,\s*,/g, ', ').replace(/\s{2,}/g, ' ').trim();
    return line || p.label || p.name || '—';
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
    if (biasLonLat && Array.isArray(biasLonLat) && biasLonLat.length === 2) params.set('lon', biasLonLat[0]), params.set('lat', biasLonLat[1]);

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

    container.classList.add('addr-wrap');

    const label = h('label', { class: 'small', for: id }, labelText, required ? ' *' : '');
    const input = h('input', { id, type: 'text', placeholder: 'Start typing an address…', autocomplete: 'off', spellcheck: 'false' });
    const list = h('div', { class: 'addr-list', role: 'listbox', id: id + '_list' });

    // When we already have a value (edit mode), show it
    if (currentValue?.formatted) input.value = currentValue.formatted;

    const status = h('div', { class: 'addr-status muted small' });

    container.appendChild(label);
    container.appendChild(input);
    container.appendChild(list);
    container.appendChild(status);

    function renderList(feats) {
      list.innerHTML = '';
      highlightIndex = -1;
      feats.forEach((f, idx) => {
        const line = toDisplayLine(f);
        const item = h('div', { class: 'addr-item', role: 'option', 'data-idx': String(idx) }, line);
        item.addEventListener('mousedown', (e) => {
          e.preventDefault(); // keep focus
          pick(idx, feats);
        });
        list.appendChild(item);
      });
      list.style.display = feats.length ? 'block' : 'none';
    }

    function highlight(delta, feats) {
      if (!list.children.length) return;
      highlightIndex = (highlightIndex + delta + list.children.length) % list.children.length;
      Array.from(list.children).forEach((el, i) => el.classList.toggle('active', i === highlightIndex));
      if (feats && feats[highlightIndex]) {
        status.textContent = toDisplayLine(feats[highlightIndex]);
      }
    }

    function pick(idx, feats) {
      const f = feats[idx];
      if (!f) return;
      const val = normalizeValue(f);
      currentValue = val;
      input.value = val.formatted || '';
      list.style.display = 'none';
      status.textContent = val.formatted || '';
      try { opts?.onChange && opts.onChange(val); } catch {}
    }

    const doQuery = debounce(async () => {
      const q = input.value.trim();
      if (q.length < MIN_LEN) { list.style.display = 'none'; status.textContent = ''; return; }
      status.textContent = 'Searching…';
      try {
        const feats = await fetchPhoton(q, { lang: navigator.language?.slice(0,2) || 'en' });
        renderList(feats.slice(0, MAX_RESULTS));
        status.textContent = feats.length ? '' : 'No matches.';
      } catch (e) {
        status.textContent = 'Lookup failed. Try again.';
        list.style.display = 'none';
      }
    }, DEBOUNCE_MS);

    input.addEventListener('input', () => {
      currentValue = null;
      opts?.onChange && opts.onChange(null);
      doQuery();
    });

    input.addEventListener('keydown', async (e) => {
      const visible = list.style.display !== 'none';
      if (e.key === 'ArrowDown' && visible) { e.preventDefault(); highlight(+1); return; }
      if (e.key === 'ArrowUp'   && visible) { e.preventDefault(); highlight(-1); return; }
      if (e.key === 'Enter' && visible) {
        e.preventDefault();
        const items = Array.from(list.children);
        const idx = (highlightIndex >= 0 ? highlightIndex : 0);
        if (items[idx]) items[idx].dispatchEvent(new MouseEvent('mousedown'));
      }
      if (e.key === 'Escape' && visible) { list.style.display = 'none'; }
    });

    document.addEventListener('click', (e) => {
      if (!container.contains(e.target)) list.style.display = 'none';
    });

    // API exposed to caller (optional)
    return {
      get value() { return currentValue; },
      set value(v) {
        currentValue = v;
        input.value = v?.formatted || '';
        status.textContent = v?.formatted || '';
      },
      focus: () => input.focus()
    };
  }

  // Inject minimal styles once
  (function injectStyles() {
    if (document.getElementById('addr-autocomplete-styles')) return;
    const css = `
.addr-wrap { position: relative; }
.addr-wrap .addr-list {
  position: absolute; z-index: 20; left: 0; right: 0; top: calc(100% + 4px);
  background: var(--card, #fff); border: 1px solid var(--border, #e5e7eb);
  border-radius: var(--radius, 10px); box-shadow: 0 6px 16px rgba(0,0,0,.08);
  max-height: 260px; overflow: auto; display:none;
}
.addr-wrap .addr-item { padding: 8px 10px; cursor: pointer; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.addr-wrap .addr-item:hover, .addr-wrap .addr-item.active { background: var(--ghost-bg, #eef2f7); }
.addr-wrap .addr-status { margin-top: 6px; min-height: 1em; }
`;
    const el = document.createElement('style');
    el.id = 'addr-autocomplete-styles';
    el.textContent = css;
    document.head.appendChild(el);
  })();

  global.AddressAuto = { mount };

})(window);
