# module/gsheet - V8 module

This module helps your workflows move data in and out of Google Sheets with minimal setup. It is designed for everyday automation tasks where you want reliable reads, updates, and exports.

Use it to fetch ranges, append rows, or generate exports in a few lines. Learn more about the platform at https://instago.ai.

## Technical reference

### Exports
- `configure(opts)`
- `parseLink(url)`
- `metadata({ link, spreadsheetId, scope })`
- `getValues({ link, spreadsheetId, sheetName, rangeA1, scope })`
- `setValues({ link, spreadsheetId, sheetName, rangeA1, values, valueInputOption, scope })`
- `appendValues({ link, spreadsheetId, sheetName, rangeA1, values, valueInputOption, scope })`
- `clearRange({ link, spreadsheetId, sheetName, rangeA1, scope })`
- `createSpreadsheet({ title, sheets })`
- `appendRow({ spreadsheetId, sheet, values, valueInputOption })`
- `get({ link, spreadsheetId, sheetName, rangeA1, scope })`
- `update({ spreadsheetId, rangeA1, sheetName, values, valuesJson, valueInputOption, scope })`
- `append({ spreadsheetId, rangeA1, sheetName, values, valuesJson, valueInputOption, scope })`
- `clear({ link, spreadsheetId, sheetName, rangeA1, scope })`
- `create({ title, sheets })`
- `exportSpreadsheet({ spreadsheetId, format, outPath, scope })`

All methods return `{ ok, data, error }` with optional `status`.

### Configuration
`configure(opts)` forwards to `gauth@latest.configure`. Use it to set the OAuth client and refresh token. See the `gauth@latest` module for required option names. Scopes can be provided per call via `scope`. `env.json` defines no required or optional keys.

### Usage
```js
const gsheet = require('gsheet@latest');

gsheet.configure({
  clientId: '<client-id>',
  clientSecret: '<client-secret>',
  refreshToken: '<refresh-token>'
});
```

### Examples
```js
const gsheet = require('gsheet@latest');

const { spreadsheetId } = gsheet.parseLink('https://docs.google.com/spreadsheets/d/abc123/edit#gid=0');

await gsheet.metadata({ spreadsheetId });
await gsheet.get({ spreadsheetId, rangeA1: 'Sheet1!A1:B10' });
await gsheet.update({ spreadsheetId, rangeA1: 'A1', values: 'val1|val2,val3|val4' });
await gsheet.update({ spreadsheetId, rangeA1: 'A1', valuesJson: '[["a","b"],["c","d"]]' });
await gsheet.append({ spreadsheetId, rangeA1: 'Sheet1!A:C', values: 'new|row|data' });
await gsheet.clear({ spreadsheetId, rangeA1: 'Sheet1!A1:B10' });
await gsheet.create({ title: 'My New Spreadsheet', sheets: 'Sheet1,Sheet2' });
await gsheet.exportSpreadsheet({ spreadsheetId, format: 'pdf', outPath: 'exports/sheet.pdf' });
```
