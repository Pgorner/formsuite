// fs-export-core.js
// Shared DOCX export pipeline for index.html + extractor.html

(function (global) {
  'use strict';

  const DEFAULT_PAYLOAD_KEY = 'CRONOS_PAYLOAD';

  function _makeTrace(label, extra) {
    try {
      return (typeof global.TRACE === 'function')
        ? global.TRACE(label, extra || {})
        : { log(){}, error(){}, end(){} };
    } catch {
      return { log(){}, error(){}, end(){} };
    }
  }

  async function buildDoc(options) {
    const {
      // required-ish
      originalBytes,
      schema,
      values,
      tagMap,
      rules,
      fieldRules,

      // optional
      headingBaseline,
      payloadKey = DEFAULT_PAYLOAD_KEY,
      writerKind = 'auto',  // 'auto' | 'settings' | 'custom'
      buildHeadingResolver, // async (updatedBytes, headingBaseline, ctx) => headingResolver
      debugLabel
    } = options || {};

    const tr = _makeTrace('fsExportCore.buildDoc', { debugLabel });

    try {
      if (!originalBytes) {
        throw new Error('fsExportCore.buildDoc: originalBytes is required');
      }

      const origU8 = (originalBytes instanceof Uint8Array)
        ? originalBytes
        : new Uint8Array(originalBytes);

      const safeSchema = schema || { title: 'Form', fields: [] };
      const safeValues = values || {};
      const safeTagMap = tagMap || {};
      const safeRules = Array.isArray(rules) ? rules : [];
      const safeFieldRules = Array.isArray(fieldRules) ? fieldRules : [];

      const payloadObj = {
        title: safeSchema.title || 'Form',
        fields: safeSchema.fields || [],
        values: safeValues,
        tagMap: safeTagMap,
        rules: safeRules,
        fieldRules: safeFieldRules,
        updatedAt: new Date().toISOString()
      };

      const json = JSON.stringify(payloadObj);

      // ---- Write payload into DOCX (settings/custom/docVars) ----
      let updated = origU8;

      if (writerKind === 'settings' && typeof global.writeDocVarSettings === 'function') {
        updated = await global.writeDocVarSettings(updated, payloadKey, json);
      } else if (writerKind === 'custom' && typeof global.writeDocVarCustom === 'function') {
        updated = await global.writeDocVarCustom(updated, payloadKey, json);
      } else {
        if (typeof global.writeDocVar === 'function') {
          updated = await global.writeDocVar(updated, payloadKey, json);
        } else if (typeof global.writeDocVarSettings === 'function') {
          updated = await global.writeDocVarSettings(updated, payloadKey, json);
        } else {
          throw new Error('No DOCX payload writer available (writeDocVar / writeDocVarSettings).');
        }
      }

      // ---- SDT replacement based on tagMap + values ----
      const sdtMap = {};
      for (const [tag, fieldId] of Object.entries(safeTagMap)) {
        let v = safeValues[fieldId];
        if (v == null) v = '';
        if (typeof v === 'object') {
          v = v.formatted ?? (function () {
            try { return JSON.stringify(v); }
            catch { return String(v); }
          })();
        }
        sdtMap[tag] = String(v);
      }

      let updated2 = updated;
      if (typeof global.writeSDTs === 'function') {
        updated2 = await global.writeSDTs(updated, sdtMap);
      }

      // ---- Heading resolver + visibility map ----
      let headingResolver = null;

      if (typeof buildHeadingResolver === 'function') {
        headingResolver = await buildHeadingResolver(
          updated2,
          headingBaseline || {},
          { schema: safeSchema, values: safeValues, rules: safeRules }
        );
      } else if (typeof global.buildHeadingTargetIndex === 'function') {
        headingResolver = global.buildHeadingTargetIndex(headingBaseline || {});
      }

      let visibilityMap = {};
      if (typeof global.evaluateRulesToVisibility === 'function'
          && safeSchema && headingResolver) {
        visibilityMap = global.evaluateRulesToVisibility(
          safeSchema,
          safeValues,
          safeRules,
          headingResolver
        );
      }

      // ---- Removal plan + applyRemovalWithBackup (your workingrmv semantics) ----
      try {
        if (typeof global.inspectRemovalPlan === 'function') {
          await global.inspectRemovalPlan(updated2, visibilityMap);
        }
      } catch (e) {
        // non-fatal; just log in TRACE
        try { tr.log && tr.log('inspectRemovalPlan failed (non-fatal)', e); } catch {}
      }

      let updated3 = updated2;
      let removalApplied = false;
      if (typeof global.applyRemovalWithBackup === 'function') {
        updated3 = await global.applyRemovalWithBackup(updated2, visibilityMap, origU8);
        removalApplied = true;
      }

      const finalU8 = (updated3 instanceof Uint8Array)
        ? updated3
        : new Uint8Array(updated3);

      return {
        updatedBytes: finalU8,
        preRemovalBytes: updated2 instanceof Uint8Array ? updated2 : new Uint8Array(updated2 || []),
        removalApplied,
        payload: payloadObj,
        visibilityMap,
        headingResolver,
        sdtMap
      };
    } finally {
      try { tr.end && tr.end(); } catch {}
    }
  }

  global.fsExportCore = {
    buildDoc
  };
})(window);
