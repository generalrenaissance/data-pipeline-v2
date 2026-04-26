import test from 'node:test';
import assert from 'node:assert/strict';

import {
  GoogleSheetsSource,
  LocalDumpSource,
  parseSheetRegistry,
  REQUIRED_TAB_KEYS,
  selectSheetSource,
  SHEET_TAB_TITLE_MAP,
  type SheetSource,
} from '../src/infra/sheet-registry';

class StubSource implements SheetSource {
  constructor(
    private rowsByTab: Record<string, unknown[][]>,
    private tabTitles?: string[],
  ) {}

  async load(tab: string): Promise<unknown[][]> {
    return this.rowsByTab[tab] ?? [];
  }

  async listTabs(): Promise<string[]> {
    return this.tabTitles ?? Object.keys(SHEET_TAB_TITLE_MAP).map(k => SHEET_TAB_TITLE_MAP[k]);
  }
}

const FUNDING_HEADERS = [
  'Tag', 'OFFER', 'Branding', 'Workspace', 'Status', 'Email Provider', 'Campaign Manager',
  'Type', 'Accounts', 'Cold Emails', 'Warmup Emails', 'Pair', 'Group', 'Inbox Manager',
];

const OTHER_HEADERS = [
  'Tag', 'OFFER', 'Branding', 'Workspace', 'Status', 'Email Provider', 'Campaign Manager',
  'Type',
];

const CANCELLED_HEADERS = [
  'Tag', 'OFFER', 'Branding', 'Workspace', 'Status', 'Campaign Manager', 'Inbox Manager', 'Type',
];

function buildFunding(): unknown[][] {
  return [
    FUNDING_HEADERS,
    ['RG3001', 'Funding', 'Hey Lending', 'Renaissance 3', 'Active', 'Google', 'Sam', 'OTD', 5, 3, 8, 'Pair 1', 'A', 'IM-1'],
    ['RG3002', 'Funding', 'Hey Lending', 'Renaissance 6', 'Active', 'Outlook', 'Sam', 'Mailin', 5, 3, 8, 'Pair 2', 'A', 'IM-2'],
  ];
}

function buildOther(): unknown[][] {
  return [
    OTHER_HEADERS,
    ['RGO001', 'ERC', 'Bintley Finance', 'ERC 1', 'Active', 'Google', 'Sam', 'OTD'],
  ];
}

function buildArchivedErc(): unknown[][] {
  return [FUNDING_HEADERS];
}

function buildArchivedOther(): unknown[][] {
  return [FUNDING_HEADERS];
}

function buildBrandsAllocation(): unknown[][] {
  // Per parser: row 1 has offer markers every 5 columns; rows 4+ have brand entries.
  return [
    [],
    ['Funding', null, null, null, null, 'ERC', null, null, null, null],
    [],
    [],
    ['Hey Lending', 'heylending.com', 'A', 45000, 'live', 'Bintley Finance', 'bintley.com', 'B', 45100, 'live'],
  ];
}

function buildCancelled(): unknown[][] {
  return [
    CANCELLED_HEADERS,
    ['RGC001', 'Funding', 'Hey Lending', 'Renaissance 3', 'Cancelled', 'Sam', 'IM-1', 'OTD'],
  ];
}

function fullStubRows(): Record<string, unknown[][]> {
  return {
    Funding: buildFunding(),
    Other: buildOther(),
    Archived_ERC_: buildArchivedErc(),
    Archived_Other_: buildArchivedOther(),
    Brands_allocation: buildBrandsAllocation(),
    Cancelled: buildCancelled(),
  };
}

test('parseSheetRegistry parses sheet/brand/cancelled rows from a stub source', async () => {
  const source = new StubSource(fullStubRows());
  const parsed = await parseSheetRegistry(source, new Date('2026-04-26T00:00:00Z'));
  assert.equal(parsed.sheetRows.length, 3); // 2 Funding + 1 Other
  assert.equal(parsed.brandRows.length, 2); // Hey Lending + Bintley Finance
  assert.equal(parsed.cancelledRows.length, 1);
  assert.equal(parsed.sheetRows[0]?.tag, 'RG3001');
  assert.equal(parsed.sheetRows[0]?.source_tab, 'Funding');
  assert.equal(parsed.brandRows.find(b => b.brand_name === 'Hey Lending')?.brand_domain, 'heylending.com');
});

test('parseSheetRegistry throws when listTabs is missing required titles', async () => {
  const source = new StubSource(fullStubRows(), ['Funding', 'Other', 'Cancelled']); // missing 3
  await assert.rejects(
    () => parseSheetRegistry(source, new Date('2026-04-26T00:00:00Z')),
    /Missing expected sheet tabs.*Actual tabs/s,
  );
});

test('parseSheetRegistry throws when Funding row 0 col G is not Campaign Manager', async () => {
  const rows = fullStubRows();
  const badHeaders = [...FUNDING_HEADERS];
  badHeaders[6] = 'Wrong Header';
  rows.Funding = [badHeaders, ...buildFunding().slice(1)];
  const source = new StubSource(rows);
  await assert.rejects(
    () => parseSheetRegistry(source, new Date('2026-04-26T00:00:00Z')),
    /Funding row 0 column G expected Campaign Manager/,
  );
});

test('REQUIRED_TAB_KEYS covers exactly the 6 documented tabs', () => {
  assert.deepEqual([...REQUIRED_TAB_KEYS].sort(), [
    'Archived_ERC_',
    'Archived_Other_',
    'Brands_allocation',
    'Cancelled',
    'Funding',
    'Other',
  ]);
});

test('SHEET_TAB_TITLE_MAP maps underscored keys to live tab titles', () => {
  assert.equal(SHEET_TAB_TITLE_MAP.Funding, 'Funding');
  assert.equal(SHEET_TAB_TITLE_MAP.Archived_ERC_, 'Archived (ERC)');
  assert.equal(SHEET_TAB_TITLE_MAP.Archived_Other_, 'Archived (Other)');
  assert.equal(SHEET_TAB_TITLE_MAP.Brands_allocation, 'Brands allocation');
});

test('selectSheetSource: SHEET_SOURCE=api routes to API and requires creds', () => {
  assert.throws(
    () => selectSheetSource({ SHEET_SOURCE: 'api' } as NodeJS.ProcessEnv),
    /RENAISSANCE_SHEET_ID is required/,
  );
  assert.throws(
    () => selectSheetSource({ SHEET_SOURCE: 'api', RENAISSANCE_SHEET_ID: 'sid' } as NodeJS.ProcessEnv),
    /GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON is required/,
  );
  const sel = selectSheetSource({
    SHEET_SOURCE: 'api',
    RENAISSANCE_SHEET_ID: 'spreadsheet-id-123',
    GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON: JSON.stringify({
      client_email: 'sa@example.iam.gserviceaccount.com',
      private_key: 'TEST-FIXTURE-NOT-A-REAL-KEY',
    }),
  } as NodeJS.ProcessEnv);
  assert.equal(sel.kind, 'api');
  assert.ok(sel.source instanceof GoogleSheetsSource);
});

test('selectSheetSource: RENAISSANCE_SHEET_ID alone defaults to API', () => {
  const sel = selectSheetSource({
    RENAISSANCE_SHEET_ID: 'spreadsheet-id-123',
    GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON: JSON.stringify({
      client_email: 'sa@example.iam.gserviceaccount.com',
      private_key: 'TEST-FIXTURE-NOT-A-REAL-KEY',
    }),
  } as NodeJS.ProcessEnv);
  assert.equal(sel.kind, 'api');
});

test('selectSheetSource: SHEET_SOURCE=local-dump forces dump even with RENAISSANCE_SHEET_ID set', () => {
  const sel = selectSheetSource({
    SHEET_SOURCE: 'local-dump',
    RENAISSANCE_SHEET_ID: 'should-be-ignored',
    SHEET_DUMP_DIR: '/var/data/dump',
  } as NodeJS.ProcessEnv);
  assert.equal(sel.kind, 'local-dump');
  assert.ok(sel.source instanceof LocalDumpSource);
  assert.match(sel.label, /\/var\/data\/dump/);
});

test('selectSheetSource: no env falls back to LocalDumpSource(/tmp/renaissance_sheet)', () => {
  const sel = selectSheetSource({} as NodeJS.ProcessEnv);
  assert.equal(sel.kind, 'local-dump');
  assert.match(sel.label, /\/tmp\/renaissance_sheet/);
});

test('selectSheetSource: rejects malformed service account JSON', () => {
  assert.throws(
    () => selectSheetSource({
      SHEET_SOURCE: 'api',
      RENAISSANCE_SHEET_ID: 'sid',
      GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON: 'not-json',
    } as NodeJS.ProcessEnv),
    /not valid JSON/,
  );
});
