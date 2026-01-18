// Google Sheets helper (generic) using OAuth2 refresh token via gauth@latest.
// Consolidated API: parseLink, getValues, setValues, appendValues, clearRange
// plus createSpreadsheet and appendRow for compatibility. Returns { ok, data, error }.

(function () {
  const httpx = require('http@latest');
  const gauth = require('gauth@latest');
  const auth = require('auth@latest');
  const qs = require('qs@latest');
  const log = require('log@latest').create('gsheet');
  const b64 = require('b64@latest');

  const SHEETS_BASE = 'https://sheets.googleapis.com/v4';
  const DRIVE_BASE = 'https://www.googleapis.com/drive/v3';
  const DEFAULT_SCOPE = ['sheets'];

  function configure(opts) {
    if (opts && typeof opts === 'object') { try { gauth.configure(opts); } catch {} }
  }

  function resolveScope(opts) {
    return (opts && (opts.scope || opts.scopes || opts.services)) || DEFAULT_SCOPE;
  }

  async function getToken(scope) {
    try { return await gauth.auth({ scope: resolveScope({ scope }) }); } catch { return null; }
  }

  function parseLink(url) {
    url = '' + (url || '');
    const m = /\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/.exec(url);
    const gidm = /[?#&]gid=(\d+)/.exec(url);
    return { spreadsheetId: m ? m[1] : '', gid: gidm ? gidm[1] : null };
  }

  function buildRange({ sheetName, rangeA1 }) {
    if (!rangeA1 && sheetName) return sheetName;
    if (sheetName && rangeA1) return sheetName + '!' + rangeA1;
    return rangeA1 || 'A1';
  }

  async function apiRequest({ path, method = 'GET', bodyObj, timeoutMs, retry=true, scope }) {
    const token = await getToken(scope);
    if (!token) return { ok: false, error: 'no access token (configure gauth)' };
    const url = SHEETS_BASE + path;
    const headers = Object.assign({ 'Content-Type': 'application/json' }, auth.bearer(token));
    try {
      const r = await httpx.json({ url, method, headers, bodyObj, timeoutMs, retry });
      const data = (r && (r.json || r.raw)) || null;
      const status = r && r.status;
      if (status && status >= 400) {
        const msg = (data && data.error && data.error.message) || (data && data.error && data.error_description) || ('HTTP ' + status);
        return { ok: false, error: msg, status, body: data };
      }
      return { ok: true, data, status };
    } catch (e) {
      log.error('apiRequest:error', (e && (e.message || e)) || 'unknown');
      return { ok: false, error: (e && (e.message || String(e))) || 'unknown' };
    }
  }

  async function createSpreadsheet({ title, sheets }) {
    const t = (title ? String(title) : 'Untitled');
    const bodyObj = { properties: { title: t } };
    if (sheets) {
      const list = Array.isArray(sheets) ? sheets : String(sheets).split(/[,\s]+/).filter(Boolean);
      if (list.length) bodyObj.sheets = list.map((name) => ({ properties: { title: String(name) } }));
    }
    const res = await apiRequest({ path: '/spreadsheets', method: 'POST', bodyObj });
    if (!res.ok) return res;
    const j = res.data || {};
    return { ok: true, data: { spreadsheetId: j.spreadsheetId, title: j.properties && j.properties.title } };
  }

  async function metadata({ link, spreadsheetId, scope }) {
    let id = spreadsheetId || (parseLink(link).spreadsheetId);
    if (!id) return { ok: false, error: 'missing spreadsheetId or link' };
    const path = '/spreadsheets/' + encodeURIComponent(id);
    return apiRequest({ path, method: 'GET', scope });
  }

  async function getValues({ link, spreadsheetId, rangeA1, sheetName, scope }) {
    let id = spreadsheetId || (parseLink(link).spreadsheetId);
    if (!id) return { ok: false, error: 'missing spreadsheetId or link' };
    const range = buildRange({ sheetName, rangeA1 });
    return apiRequest({ path: '/spreadsheets/' + encodeURIComponent(id) + '/values/' + encodeURIComponent(range), method: 'GET', scope });
  }

  async function setValues({ link, spreadsheetId, rangeA1, sheetName, values, valueInputOption, scope }) {
    if (!Array.isArray(values)) return { ok: false, error: 'values must be an array (2D for multiple rows)' };
    let id = spreadsheetId || (parseLink(link).spreadsheetId);
    if (!id) return { ok: false, error: 'missing spreadsheetId or link' };
    const range = buildRange({ sheetName, rangeA1 });
    const vio = valueInputOption || 'USER_ENTERED';
    const path = '/spreadsheets/' + encodeURIComponent(id) + '/values/' + encodeURIComponent(range) + '?' + qs.encode({ valueInputOption: vio });
    const bodyObj = { values: Array.isArray(values[0]) ? values : [ values ] };
    return apiRequest({ path, method: 'PUT', bodyObj, scope });
  }

  async function appendValues({ link, spreadsheetId, sheetName, rangeA1, values, valueInputOption, scope }) {
    if (!Array.isArray(values)) return { ok: false, error: 'values must be an array (2D for multiple rows)' };
    let id = spreadsheetId || (parseLink(link).spreadsheetId);
    if (!id) return { ok: false, error: 'missing spreadsheetId or link' };
    const range = buildRange({ sheetName, rangeA1 });
    const vio = valueInputOption || 'USER_ENTERED';
    const path = '/spreadsheets/' + encodeURIComponent(id) + '/values/' + encodeURIComponent(range) + ':append?' + qs.encode({ valueInputOption: vio });
    const bodyObj = { values: Array.isArray(values[0]) ? values : [ values ] };
    return apiRequest({ path, method: 'POST', bodyObj, scope });
  }

  async function clearRange({ link, spreadsheetId, rangeA1, sheetName, scope }) {
    let id = spreadsheetId || (parseLink(link).spreadsheetId);
    if (!id) return { ok: false, error: 'missing spreadsheetId or link' };
    const range = buildRange({ sheetName, rangeA1 });
    const path = '/spreadsheets/' + encodeURIComponent(id) + '/values/' + encodeURIComponent(range) + ':clear';
    return apiRequest({ path, method: 'POST', bodyObj: {}, scope });
  }

  function parseValuesString(input) {
    const s = String(input || '');
    if (!s) return [];
    return s.split(',').map((row) => row.split('|'));
  }

  async function update({ spreadsheetId, rangeA1, sheetName, values, valuesJson, valueInputOption, scope } = {}) {
    if (!spreadsheetId) return { ok: false, error: 'missing spreadsheetId' };
    let parsed = values;
    if (!parsed && valuesJson) {
      try { parsed = JSON.parse(String(valuesJson)); } catch { parsed = null; }
    }
    if (!parsed && typeof values === 'string') {
      parsed = parseValuesString(values);
    }
    if (!parsed) return { ok: false, error: 'missing values' };
    return setValues({ spreadsheetId, rangeA1, sheetName, values: parsed, valueInputOption, scope });
  }

  async function append({ spreadsheetId, rangeA1, sheetName, values, valuesJson, valueInputOption, scope } = {}) {
    if (!spreadsheetId) return { ok: false, error: 'missing spreadsheetId' };
    let parsed = values;
    if (!parsed && valuesJson) {
      try { parsed = JSON.parse(String(valuesJson)); } catch { parsed = null; }
    }
    if (!parsed && typeof values === 'string') {
      parsed = parseValuesString(values);
    }
    if (!parsed) return { ok: false, error: 'missing values' };
    return appendValues({ spreadsheetId, rangeA1, sheetName, values: parsed, valueInputOption, scope });
  }

  function exportMime(format) {
    const fmt = String(format || '').toLowerCase();
    if (fmt === 'pdf') return 'application/pdf';
    if (fmt === 'xlsx') return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
    if (fmt === 'csv') return 'text/csv';
    if (fmt === 'tsv') return 'text/tab-separated-values';
    return '';
  }

  async function exportSpreadsheet({ spreadsheetId, format, outPath, scope } = {}) {
    if (!spreadsheetId) return { ok: false, error: 'missing spreadsheetId' };
    if (!outPath) return { ok: false, error: 'missing outPath' };
    const mime = exportMime(format);
    if (!mime) return { ok: false, error: 'unsupported format' };
    const token = await getToken(scope || 'drive.readonly');
    if (!token) return { ok: false, error: 'no access token (configure gauth)' };
    const url = DRIVE_BASE + '/files/' + encodeURIComponent(spreadsheetId) + '/export?mimeType=' + encodeURIComponent(mime);
    const res = await sys.http.fetch({ url, method: 'GET', headers: auth.bearer(token) });
    const text = res && res.text || '';
    const status = res && res.status;
    const ok = status ? status >= 200 && status < 300 : true;
    if (!ok) return { ok, status, data: text };
    const storage = sys.storage.get('gsheet');
    await storage.save({ path: outPath, dataBase64: b64.encodeAscii(text) });
    return { ok: true, status, outPath };
  }

  // Compatibility helpers
  async function appendRow(opts) {
    if (!opts || typeof opts !== 'object') return { ok: false, error: 'appendRow: options required' };
    const id = opts.spreadsheetId;
    const values = opts.values;
    const sheet = (opts.sheet && typeof opts.sheet === 'string') ? opts.sheet : undefined;
    const vio = (opts.valueInputOption && typeof opts.valueInputOption === 'string') ? opts.valueInputOption : undefined;
    if (!id || typeof id !== 'string') return { ok: false, error: 'appendRow: missing spreadsheetId' };
    if (!Array.isArray(values)) return { ok: false, error: 'appendRow: values must be an array' };
    return appendValues({ spreadsheetId: id, sheetName: sheet, values, valueInputOption: vio });
  }

  module.exports = {
    configure,
    parseLink,
    metadata,
    getValues,
    setValues,
    appendValues,
    clearRange,
    createSpreadsheet,
    appendRow,
    get: getValues,
    update,
    append,
    clear: clearRange,
    create: createSpreadsheet,
    exportSpreadsheet
  };
})();
