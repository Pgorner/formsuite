'use strict';

// ---------- basic helpers ----------

function __slug(s) {
  return String(s ?? '')
    .normalize('NFKD').replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/gi, '_')
    .replace(/^_+|_+$/g, '')
    .toLowerCase();
}

function hasOwn(obj, key) {
  return !!obj && Object.prototype.hasOwnProperty.call(obj, key);
}

function getValidFieldIdSet(schema) {
  return new Set((schema?.fields || []).map(f => String(f.id)));
}

// ---------- rule collection & aggregation ----------

function normalizeRuleCollection(raw) {
  const acc = [];

  function inner(val) {
    if (val == null) return;

    if (Array.isArray(val)) {
      for (const v of val) inner(v);
      return;
    }
    if (val instanceof Set) {
      for (const v of val) inner(v);
      return;
    }

    if (typeof val === 'string') {
      const t = val.trim();
      if (!t) return;
      try {
        inner(JSON.parse(t));
      } catch {
        // ignore non-JSON strings
      }
      return;
    }

    if (typeof val === 'object') {
      const maybeRule = val;
      if (
        maybeRule &&
        (maybeRule.action ||
         maybeRule.fieldId ||
         maybeRule.whenField ||
         maybeRule.targets)
      ) {
        acc.push(maybeRule);
        return;
      }
      for (const v of Object.values(maybeRule)) inner(v);
      return;
    }
  }

  inner(raw);
  return acc;
}

function dedupeRules(arr) {
  const seen = new Set();
  const out = [];
  for (const r of (arr || [])) {
    if (!r) continue;
    const shallow = (r && typeof r === 'object') ? { ...r } : r;
    if (shallow && typeof shallow === 'object') {
      delete shallow.version;
      delete shallow.ts;
    }
    const key = JSON.stringify(shallow ?? null);
    if (!seen.has(key)) {
      seen.add(key);
      out.push(r);
    }
  }
  return out;
}

function extractRulesFromState(state) {
  const out = {
    rules: [],
    fieldRules: [],
    meta: { rulesSources: [], fieldRuleSources: [] }
  };
  if (!state || typeof state !== 'object') return out;

  const ruleSources = [];
  const fieldRuleSources = [];

  function addSource(name, obj) {
    if (!obj || typeof obj !== 'object') return;
    if (obj.rules) {
      ruleSources.push([name, normalizeRuleCollection(obj.rules)]);
    }
    if (obj.fieldRules) {
      fieldRuleSources.push([name, normalizeRuleCollection(obj.fieldRules)]);
    }
  }

  const payload = (state.payload && typeof state.payload === 'object')
    ? state.payload
    : null;

  const cronosPayload =
      (payload && payload.CRONOS_PAYLOAD)
    || state.CRONOS_PAYLOAD
    || state.cronos_payload
    || null;

  addSource('payload.CRONOS_PAYLOAD', cronosPayload);
  if (payload) addSource('payload', payload);
  addSource('state', state);

  out.rules      = dedupeRules(ruleSources.flatMap(([, arr]) => arr));
  out.fieldRules = dedupeRules(fieldRuleSources.flatMap(([, arr]) => arr));

  out.meta.rulesSources      = ruleSources
    .filter(([, arr]) => arr.length > 0)
    .map(([name]) => name);

  out.meta.fieldRuleSources  = fieldRuleSources
    .filter(([, arr]) => arr.length > 0)
    .map(([name]) => name);

  return out;
}

function resolveRulesForState(state, payloadOverride) {
  const view = state && typeof state === 'object' ? { ...state } : {};

  if (payloadOverride) {
    view.payload = {
      ...(state?.payload || {}),
      CRONOS_PAYLOAD: payloadOverride
    };
    view.CRONOS_PAYLOAD = payloadOverride;
    view.cronos_payload = payloadOverride;
  }

  const aggregated = extractRulesFromState(view);

  const wsRulesRaw =
    hasOwn(view, 'rules') ? normalizeRuleCollection(view.rules) : [];
  const wsFieldRulesRaw =
    hasOwn(view, 'fieldRules') ? normalizeRuleCollection(view.fieldRules) : [];

  const rules =
    hasOwn(view, 'rules') ? dedupeRules(wsRulesRaw) : aggregated.rules;

  const fieldRules =
    hasOwn(view, 'fieldRules') ? dedupeRules(wsFieldRulesRaw) : aggregated.fieldRules;

  const source = {
    rules: hasOwn(view, 'rules')
      ? 'workspace.rules'
      : (aggregated.meta?.rulesSources?.[0] || 'none'),
    fieldRules: hasOwn(view, 'fieldRules')
      ? 'workspace.fieldRules'
      : (aggregated.meta?.fieldRuleSources?.[0] || 'none'),
    contributingRules: aggregated.meta?.rulesSources || [],
    contributingFieldRules: aggregated.meta?.fieldRuleSources || []
  };

  return { rules, fieldRules, source };
}

// ---------- schema indexes ----------

function __buildSchemaIndex(schema) {
  const idx = { byId: new Map(), byLabel: new Map(), optionToField: new Map() };

  for (const f of (schema?.fields || [])) {
    const id = String(f.id);
    idx.byId.set(id, f);
    if (f.label) {
      idx.byLabel.set(String(f.label).trim().toLowerCase(), f);
    }

    const opts = [];
    if (Array.isArray(f.options)) {
      for (const o of f.options) {
        const label = (o?.label ?? o?.text ?? o?.value ?? o);
        if (label != null) opts.push(String(label));
      }
    }
    if (f.mc?.groups) {
      for (const g of f.mc.groups) {
        for (const it of (g.items || [])) {
          if (it?.value != null) opts.push(String(it.value));
        }
      }
    }
    for (const label of opts) {
      idx.optionToField.set(label.trim().toLowerCase(), f);
    }
  }

  return idx;
}

function __resolveFieldRef(schema, raw) {
  if (!raw) return null;
  const s = String(raw).trim();
  const idx = __buildSchemaIndex(schema);

  if (s.includes('__opt__')) {
    const baseId = s.split('__opt__')[0];
    if (idx.byId.has(baseId)) return idx.byId.get(baseId);

    const slug = s.split('__opt__').pop();
    for (const [labelLower, field] of idx.optionToField.entries()) {
      const labelSlug = __slug(labelLower);
      if (labelSlug === String(slug).toLowerCase()) return field;
    }
  }

  if (idx.byId.has(s)) return idx.byId.get(s);

  const byLabel = idx.byLabel.get(s.toLowerCase());
  if (byLabel) return byLabel;

  const parts = s.split(':').map(x => x.trim());
  if (parts.length >= 1) {
    const tryLabel = idx.byLabel.get(parts[0].toLowerCase());
    if (tryLabel) return tryLabel;
  }

  const own = idx.optionToField.get(s.toLowerCase());
  if (own) return own;

  console.warn('[resolveFieldRef] could not resolve field for:', s);
  return null;
}

function __buildOptionIndex(schema) {
  const byField = new Map();
  for (const f of (schema?.fields || [])) {
    const rec = {
      id: String(f.id),
      label: String(f.label || f.id),
      type: f.type,
      options: []
    };

    if (Array.isArray(f.options)) {
      for (const opt of f.options) {
        const value = String(opt?.value ?? opt?.id ?? opt);
        const label = String(opt?.label ?? opt?.text ?? value);
        rec.options.push({ value, label, slug: __slug(label) });
      }
    }

    if (f?.mc?.groups) {
      for (const g of f.mc.groups) {
        for (const it of (g.items || [])) {
          const value = String(it?.value ?? it?.id ?? it);
          const label = String(it?.label ?? it?.text ?? value);
          rec.options.push({ value, label, slug: __slug(label) });
        }
      }
    }

    byField.set(rec.id, rec);
  }
  return byField;
}

function __coerceRuleForMultichoiceOption(schema, rule, whenField) {
  if (!whenField) return rule;
  const type = whenField.type;
  if (!['multichoice', 'select'].includes(type)) return rule;

  const r = { ...rule };
  if (!Array.isArray(r.values)) r.values = [r.values];
  return r;
}

// ---------- heading baseline ----------

function buildHeadingTargetIndex(baseline) {
  const flat = Array.isArray(baseline?.flat)
    ? baseline.flat
    : Array.isArray(baseline) ? baseline
    : Array.isArray(baseline?.headings) ? baseline.headings
    : [];

  const byId = new Map();
  const byNumber = new Map();
  const bySlug = new Map();

  flat.forEach((h, idx) => {
    const id = String(
      h.id != null ? h.id :
      h.uid != null ? h.uid :
      h.key != null ? h.key : idx
    );
    const label = String(h.label || h.text || h.title || id);
    const num = (h.number || h.num || h.key || '').toString().trim();
    const entry = { ...h, id, label, idx };

    byId.set(id, entry);
    if (num) byNumber.set(num, entry);

    const slug = __slug(label);
    if (slug) bySlug.set(slug, entry);
  });

  function normalizeTarget(t) {
    if (!t && t !== 0) return null;

    if (typeof t === 'string' || typeof t === 'number') {
      const s = String(t).trim();
      if (byId.has(s)) {
        const e = byId.get(s);
        return { id: e.id, idx: e.idx, label: e.label };
      }
      if (byNumber.has(s)) {
        const e = byNumber.get(s);
        return { id: e.id, idx: e.idx, label: e.label };
      }
      const slug = __slug(s);
      if (bySlug.has(slug)) {
        const e = bySlug.get(slug);
        return { id: e.id, idx: e.idx, label: e.label };
      }
      const n = Number(s);
      if (Number.isFinite(n) && flat[n - 1]) {
        const e = flat[n - 1];
        const id = String(e.id ?? e.uid ?? e.key ?? (n - 1));
        return { id, idx: n - 1, label: e.label || e.text || e.title || id };
      }
      return null;
    }

    if (typeof t === 'object') {
      if (t.id && byId.has(String(t.id))) {
        const e = byId.get(String(t.id));
        return { id: e.id, idx: e.idx, label: e.label };
      }
      if (t.number && byNumber.has(String(t.number))) {
        const e = byNumber.get(String(t.number));
        return { id: e.id, idx: e.idx, label: e.label };
      }
      if (t.label) {
        const slug = __slug(t.label);
        if (bySlug.has(slug)) {
          const e = bySlug.get(slug);
          return { id: e.id, idx: e.idx, label: e.label };
        }
      }
    }

    return null;
  }

  function resolveIdx(t) {
    const n = parseTargetIdx(t, { normalizeTarget });
    if (Number.isFinite(n)) return n;
    return NaN;
  }

  function buildLabel(entry, fallback) {
    if (entry && entry.label) return entry.label;
    if (entry && (entry.text || entry.title)) return entry.text || entry.title;
    return fallback != null ? String(fallback) : '';
  }

  function resolve(target) {
    const norm = normalizeTarget(target);
    if (norm && Number.isFinite(norm.idx)) return norm;
    return null;
  }

  return {
    flat,
    byId,
    byNumber,
    bySlug,
    normalizeTarget,
    resolveIdx,
    buildLabel,
    resolve
  };
}

// ---------- parse target indices ----------

function parseTargetIdx(t, headingResolver) {
  if (headingResolver) {
    if (typeof headingResolver.normalizeTarget === 'function') {
      const normalized = headingResolver.normalizeTarget(t);
      if (normalized && Number.isFinite(Number(normalized.idx))) {
        return Number(normalized.idx);
      }
    }
    if (typeof headingResolver.resolveIdx === 'function') {
      const resolved = headingResolver.resolveIdx(t);
      if (Number.isFinite(resolved)) return resolved;
    }
  }
  if (t == null) return NaN;
  const s = String(t);
  const head = s.split('|', 1)[0];
  const n = Number(head);
  return Number.isFinite(n) ? n : NaN;
}

// ---------- value sanitizing ----------

function sanitizeValues(schema, vals) {
  const out = {};
  const fields = Array.isArray(schema?.fields) ? schema.fields : [];

  for (const f of fields) {
    const id = String(f.id);
    let v = vals?.[id];

    const t = String(f.type || '').toLowerCase();

    if (t === 'datediff') {
      if (v && typeof v === 'object') {
        const o = {
          days: Number(v.days ?? 0),
          months: Number(v.months ?? 0),
          years: Number(v.years ?? 0),
          formatted: String(v.formatted ?? '')
        };
        if (o.formatted) out[id] = o;
      }
      continue;
    }

    if (t === 'number') {
      if (v === '' || v == null) continue;
      const n = Number(v);
      if (!Number.isNaN(n)) out[id] = n;
      continue;
    }

    if (t === 'switch' || t === 'checkbox' || t === 'boolean') {
      const b = (v === true || v === 'true' || v === 1 || v === '1');
      out[id] = b;
      continue;
    }

    if (t === 'date') {
      if (!v) continue;
      if (v instanceof Date) {
        const iso = v.toISOString().slice(0, 10);
        out[id] = iso;
      } else {
        out[id] = String(v);
      }
      continue;
    }

    if (t === 'multichoice') {
      let arr = [];
      if (Array.isArray(v)) arr = v;
      else if (v != null && v !== '') arr = [v];
      out[id] = [...new Set(arr.map(String))];
      continue;
    }

    if (t === 'select') {
      if (v == null) continue;
      out[id] = String(v);
      continue;
    }

    if (t === 'address') {
      if (v && typeof v === 'object') {
        const o = { ...v };
        if (!o.formatted && typeof v.formatted === 'string') {
          o.formatted = v.formatted;
        }
        out[id] = o;
      } else if (v != null && v !== '') {
        out[id] = { formatted: String(v) };
      }
      continue;
    }

    if (v != null && v !== '') out[id] = v;
  }

  // keep unknown keys roughly as-is
  for (const k of Object.keys(vals || {})) {
    if (hasOwn(out, k)) continue;
    out[k] = vals[k];
  }

  return out;
}

// ---------- rule value comparison ----------

function ruleMatchesValue(op, expected, actual, fieldType) {
  const t = String(fieldType || '').toLowerCase();
  op = String(op || 'equals');

  if (op === 'isEmpty') {
    if (Array.isArray(actual)) return actual.length === 0;
    return actual == null || actual === '';
  }
  if (op === 'isNotEmpty') {
    if (Array.isArray(actual)) return actual.length > 0;
    return !(actual == null || actual === '');
  }

  const a = actual;

  if (op === 'equals')    return String(a) === String(expected);
  if (op === 'notEquals') return String(a) !== String(expected);

  if (op === 'anyOf') {
    const arr = Array.isArray(expected) ? expected.map(String) : [String(expected)];
    if (Array.isArray(a)) return a.map(String).some(v => arr.includes(v));
    return arr.includes(String(a));
  }

  if (op === 'allOf') {
    const arr = Array.isArray(expected) ? expected.map(String) : [String(expected)];
    if (!Array.isArray(a)) return false;
    const got = new Set(a.map(String));
    return arr.every(v => got.has(String(v)));
  }

  if (op === 'contains' && t === 'text') {
    const s = String(a ?? '').toLowerCase();
    const e = String(expected ?? '').toLowerCase();
    return s.includes(e);
  }

  // numeric/date-ish ops
  if (op === 'gt' || op === 'lt' || op === 'gte' || op === 'lte') {
    const na = Number(a);
    const ne = Number(
      Array.isArray(expected) ? expected[0] : expected
    );
    if (Number.isNaN(na) || Number.isNaN(ne)) return false;
    if (op === 'gt')  return na >  ne;
    if (op === 'lt')  return na <  ne;
    if (op === 'gte') return na >= ne;
    if (op === 'lte') return na <= ne;
  }

  return false;
}

// ---------- normalization against schema ----------

function normalizeHeadingsRulesForSchema(schema, rulesIn, headingBaseline) {
  const rules = Array.isArray(rulesIn) ? rulesIn : [];
  const out = [];
  const headingIndex = buildHeadingTargetIndex(headingBaseline || { flat: [], tree: [] });

  for (const raw of rules) {
    if (!raw) continue;

    const resolvedField = __resolveFieldRef(schema, raw.fieldId ?? raw.field ?? raw.whenField);
    if (!resolvedField) continue;

    const r0 = __coerceRuleForMultichoiceOption(schema, { ...raw }, resolvedField);

    const refAfterCoerce = r0?.fieldId ?? r0?.field ?? r0?.whenField;
    if (!__resolveFieldRef(schema, refAfterCoerce)) {
      const fallbackId = String(resolvedField.id);
      r0.fieldId = fallbackId;
      if (r0.field != null) r0.field = fallbackId;
      if (r0.whenField != null) r0.whenField = fallbackId;
    }

    if (Array.isArray(r0.conditions)) {
      r0.conditions = r0.conditions.filter(c => {
        const ref = c?.fieldId ?? c?.leftFieldId ?? c?.rightFieldId;
        if (!ref) return false;
        return !!__resolveFieldRef(schema, ref);
      });
    }

    if (Array.isArray(r0.targets)) {
      r0.targets = r0.targets
        .map(t => headingIndex.normalizeTarget(t))
        .filter(Boolean);
    }

    out.push(r0);
  }

  return out;
}

function normalizeFieldRulesForSchema(schema, rulesIn) {
  const rules = Array.isArray(rulesIn) ? rulesIn : [];
  const out = [];
  const optionIndex = __buildOptionIndex(schema);
  const fieldIndex  = new Map((schema?.fields || []).map(f => [String(f.id), f]));

  for (const raw of rules) {
    if (!raw) continue;

    const resolvedField = __resolveFieldRef(schema, raw.fieldId ?? raw.field ?? raw.whenField);
    if (!resolvedField) continue;

    const r0 = __coerceRuleForMultichoiceOption(schema, { ...raw }, resolvedField);

    const refAfterCoerce = r0?.fieldId ?? r0?.field ?? r0?.whenField;
    if (!__resolveFieldRef(schema, refAfterCoerce)) {
      const fallbackId = String(resolvedField.id);
      r0.fieldId = fallbackId;
      if (r0.field != null) r0.field = fallbackId;
      if (r0.whenField != null) r0.whenField = fallbackId;
    }

    if (Array.isArray(r0.conditions)) {
      r0.conditions = r0.conditions.filter(c => {
        const ref = c?.fieldId ?? c?.leftFieldId ?? c?.rightFieldId;
        return !!__resolveFieldRef(schema, ref);
      });
    }

    const normTargets = [];
    const tIn = Array.isArray(r0.targets) ? r0.targets : [];
    const pf  = optionIndex.get(String(resolvedField.id));

    for (const t of tIn) {
      if (t && typeof t === 'object' && (t.optionValue != null || t.optionLabel != null)) {
        const fieldId = String(t.id ?? resolvedField.id);
        const cat = optionIndex.get(fieldId) || pf;
        const optVal = String(t.optionValue ?? '');
        const optLab = String(t.optionLabel ?? optVal);
        const parentLabel = fieldIndex.get(fieldId)?.label || fieldId;
        const slug = __slug(optLab || optVal);
        normTargets.push({
          id: `${fieldId}__opt__${slug}`,
          fieldId,
          optionValue: optVal,
          optionLabel: optLab,
          label: t.label || `${parentLabel}: ${optLab || optVal}`
        });
        continue;
      }

      if (typeof t === 'string') {
        const s = t.trim();

        if (s.includes('__opt__')) {
          const [maybeId, slug] = s.split('__opt__');
          const id = String(maybeId);
          const cat = optionIndex.get(id) || pf;
          if (cat && cat.options?.length) {
            const hit = cat.options.find(o => o.slug === __slug(slug));
            if (hit) {
              normTargets.push({
                id: `${cat.id}__opt__${hit.slug}`,
                fieldId: cat.id,
                optionValue: hit.value,
                optionLabel: hit.label,
                label: `${cat.label}: ${hit.label}`
              });
              continue;
            }
          }
        }

        if (s.includes(':')) {
          const [lhs, rhs] = s.split(':');
          const fieldLike = lhs.trim();
          const optLike   = rhs.trim();

          let cat = optionIndex.get(fieldLike);
          if (!cat) {
            for (const [, rec] of optionIndex.entries()) {
              if (__slug(rec.label) === __slug(fieldLike)) { cat = rec; break; }
            }
          }
          if (!cat) cat = pf;

          if (cat && cat.options?.length) {
            const hit = cat.options.find(
              o => __slug(o.label) === __slug(optLike) ||
                   __slug(o.value) === __slug(optLike)
            );
            if (hit) {
              normTargets.push({
                id: `${cat.id}__opt__${hit.slug}`,
                fieldId: cat.id,
                optionValue: hit.value,
                optionLabel: hit.label,
                label: `${cat.label}: ${hit.label}`
              });
              continue;
            }
          }
        }

        if (fieldIndex.has(s)) {
          const f = fieldIndex.get(s);
          normTargets.push({ id: String(f.id), fieldId: String(f.id), label: f.label || f.id });
          continue;
        }

        for (const f of fieldIndex.values()) {
          if (__slug(f.label || f.id) === __slug(s)) {
            normTargets.push({ id: String(f.id), fieldId: String(f.id), label: f.label || f.id });
            break;
          }
        }
        continue;
      }

      if (t && typeof t === 'object') {
        const id = String(t.id ?? t.fieldId ?? '');
        if (fieldIndex.has(id)) {
          normTargets.push({
            id,
            fieldId: id,
            label: t.label || fieldIndex.get(id).label || id
          });
        }
      }
    }

    if (!normTargets.length) {
      normTargets.push({
        id: String(resolvedField.id),
        fieldId: String(resolvedField.id),
        label: resolvedField.label || resolvedField.id
      });
    }

    r0.targets = normTargets;
    out.push(r0);
  }

  return out;
}

function deriveNormalizedRulesForDoc(state, schema, headingBaseline) {
  const { rules: mergedRulesRaw, fieldRules: mergedFieldRaw } =
    resolveRulesForState(state || {});
  return {
    rules:      normalizeHeadingsRulesForSchema(schema, mergedRulesRaw, headingBaseline),
    fieldRules: normalizeFieldRulesForSchema(schema, mergedFieldRaw)
  };
}

// ---------- evaluation: headings ----------

function evaluateRulesToVisibility(schema, values, rules, headingResolver) {
  const out = Object.create(null);
  if (!Array.isArray(rules) || !rules.length) return out;

  const cleanVals = sanitizeValues(schema, values || {});
  const getVal = (fid) => cleanVals[fid];

  for (const r of rules) {
    if (!r) continue;

    const action = (String(r.action || '').toUpperCase() === 'SHOW') ? 'SHOW'
                : (String(r.action || '').toUpperCase() === 'HIDE') ? 'HIDE'
                : null;
    if (!action) continue;

    const fieldId = r.fieldId || r.field || r.whenField;
    const op      = r.op || r.operator || 'equals';
    const exp     = r.values ?? r.value ?? r.expected;
    const targets = Array.isArray(r.targets) ? r.targets : [];

    if (!fieldId) continue;

    const fld = (schema?.fields || []).find(f => String(f.id) === String(fieldId));
    const t   = String(fld?.type || '').toLowerCase();
    const actual = getVal(String(fieldId));

    let match = false;

    if (t === 'date') {
      const expStr = Array.isArray(exp) ? String(exp[0] ?? '') : String(exp ?? '');
      const toDay = (s) => {
        if (!s) return NaN;
        const d = (s instanceof Date) ? s : new Date(String(s));
        if (Number.isNaN(d.getTime())) return NaN;
        return Date.UTC(d.getFullYear(), d.getMonth(), d.getDate());
      };
      const a = toDay(actual);
      const b = toDay(expStr);
      if (!Number.isNaN(a) && !Number.isNaN(b)) {
        if (op === 'equals') match = (a === b);
        else if (op === 'gt') match = (a > b);
        else if (op === 'lt') match = (a < b);
        else if (op === 'gte') match = (a >= b);
        else if (op === 'lte') match = (a <= b);
      }
    } else {
      match = ruleMatchesValue(op, exp, actual, t);
    }

    if (match && Array.isArray(r.conditions) && r.conditions.length) {
      for (const c of r.conditions) {
        const cid = c?.fieldId ?? c?.leftFieldId ?? c?.rightFieldId;
        if (!cid) { match = false; break; }
        const cfld = (schema?.fields || []).find(f => String(f.id) === String(cid));
        const ct   = String(cfld?.type || '').toLowerCase();
        const cav  = getVal(String(cid));
        const cop  = c.op || c.operator || 'equals';
        const cexp = c.values ?? c.value ?? c.expected;
        if (!ruleMatchesValue(cop, cexp, cav, ct)) {
          match = false;
          break;
        }
      }
    }

    if (!match) continue;

    for (const tTarget of targets) {
      const idx = parseTargetIdx(tTarget, headingResolver);
      if (!Number.isFinite(idx)) continue;
      const prev = out[idx];
      if (action === 'SHOW') out[idx] = 'SHOW';
      else if (action === 'HIDE' && prev !== 'SHOW') out[idx] = 'HIDE';
    }
  }

  return out;
}

// ---------- evaluation: fields/options ----------

function evaluateFieldRulesToVisibility(schema, values, rules) {
  const out = Object.create(null);
  if (!Array.isArray(rules) || !rules.length) return out;

  const cleanVals = sanitizeValues(schema, values || {});
  const getVal = (fid) => cleanVals[fid];

  for (const r of rules) {
    if (!r) continue;

    const action = (String(r.action || '').toUpperCase() === 'SHOW') ? 'SHOW'
                : (String(r.action || '').toUpperCase() === 'HIDE') ? 'HIDE'
                : null;
    if (!action) continue;

    const fieldId = r.fieldId || r.field || r.whenField;
    const op      = r.op || r.operator || 'equals';
    const exp     = r.values ?? r.value ?? r.expected;
    const targets = Array.isArray(r.targets) ? r.targets : [];
    const effect  = (String(r.hideMode || 'hide').toLowerCase() === 'disable') ? 'DISABLE' : 'HIDE';

    if (!fieldId) continue;

    const fld = (schema?.fields || []).find(f => String(f.id) === String(fieldId));
    const t   = String(fld?.type || '').toLowerCase();
    const actual = getVal(String(fieldId));

    let match = false;

    if (t === 'date') {
      const expStr = Array.isArray(exp) ? String(exp[0] ?? '') : String(exp ?? '');
      const toDay = (s) => {
        if (!s) return NaN;
        const d = (s instanceof Date) ? s : new Date(String(s));
        if (Number.isNaN(d.getTime())) return NaN;
        return Date.UTC(d.getFullYear(), d.getMonth(), d.getDate());
      };
      const a = toDay(actual);
      const b = toDay(expStr);
      if (!Number.isNaN(a) && !Number.isNaN(b)) {
        if (op === 'equals') match = (a === b);
        else if (op === 'gt') match = (a > b);
        else if (op === 'lt') match = (a < b);
        else if (op === 'gte') match = (a >= b);
        else if (op === 'lte') match = (a <= b);
      }
    } else {
      match = ruleMatchesValue(op, exp, actual, t);
    }

    if (match && Array.isArray(r.conditions) && r.conditions.length) {
      for (const c of r.conditions) {
        const cid = c?.fieldId ?? c?.leftFieldId ?? c?.rightFieldId;
        if (!cid) { match = false; break; }
        const cfld = (schema?.fields || []).find(f => String(f.id) === String(cid));
        const ct   = String(cfld?.type || '').toLowerCase();
        const cav  = getVal(String(cid));
        const cop  = c.op || c.operator || 'equals';
        const cexp = c.values ?? c.value ?? c.expected;
        if (!ruleMatchesValue(cop, cexp, cav, ct)) {
          match = false;
          break;
        }
      }
    }

    if (!match) continue;

    for (const tTarget of targets) {
      // field rules target either a field id or MC option id ("field__opt__slug")
      const id = String(tTarget?.id ?? tTarget?.fieldId ?? tTarget?.key ?? '');
      if (!id) continue;
      const prev = out[id];
      if (action === 'SHOW') out[id] = 'SHOW';
      else if (action === 'HIDE' && prev !== 'SHOW') out[id] = effect;
    }
  }

  return out;
}

// ---------- export for debugging ----------

if (typeof window !== 'undefined') {
  window.FS_RULES = Object.assign(window.FS_RULES || {}, {
    __slug,
    hasOwn,
    getValidFieldIdSet,
    normalizeRuleCollection,
    dedupeRules,
    extractRulesFromState,
    resolveRulesForState,
    __buildSchemaIndex,
    __resolveFieldRef,
    __buildOptionIndex,
    __coerceRuleForMultichoiceOption,
    buildHeadingTargetIndex,
    parseTargetIdx,
    sanitizeValues,
    ruleMatchesValue,
    normalizeHeadingsRulesForSchema,
    normalizeFieldRulesForSchema,
    deriveNormalizedRulesForDoc,
    evaluateRulesToVisibility,
    evaluateFieldRulesToVisibility
  });
}
