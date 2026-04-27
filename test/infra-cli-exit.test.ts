import test from 'node:test';
import assert from 'node:assert/strict';

import { computeExitCode, envAllowsPartial } from '../src/infra/cli-exit';

test('computeExitCode: fatal errors fail regardless of allowPartial', () => {
  assert.equal(computeExitCode({ errored: true, errorCount: 0, allowPartial: false }), 1);
  assert.equal(computeExitCode({ errored: true, errorCount: 1, allowPartial: true }), 1);
});

test('computeExitCode: partial runs fail unless explicitly allowed', () => {
  assert.equal(computeExitCode({ errored: false, errorCount: 1, allowPartial: false }), 1);
  assert.equal(computeExitCode({ errored: false, errorCount: 1, allowPartial: true }), 0);
});

test('computeExitCode: clean runs succeed', () => {
  assert.equal(computeExitCode({ errored: false, errorCount: 0, allowPartial: false }), 0);
});

test('envAllowsPartial: accepts explicit truthy values only', () => {
  assert.equal(envAllowsPartial(undefined), false);
  assert.equal(envAllowsPartial(''), false);
  assert.equal(envAllowsPartial('false'), false);
  assert.equal(envAllowsPartial('1'), true);
  assert.equal(envAllowsPartial('TRUE'), true);
  assert.equal(envAllowsPartial(' yes '), true);
});
