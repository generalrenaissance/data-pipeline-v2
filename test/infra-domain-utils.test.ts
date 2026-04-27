import test from 'node:test';
import assert from 'node:assert/strict';

import {
  accountProviderCodeToGroup,
  slugToProviderGroup,
  EXCLUDED_SLUGS,
} from '../src/infra/provider-routing';
import { isFreeMailDomain } from '../src/infra/free-mail';
import { emailToDomain, normalizeDomain } from '../src/infra/domain-utils';

test('slugToProviderGroup: known Outlook slugs return outlook', () => {
  assert.equal(slugToProviderGroup('outlook-1'), 'outlook');
  assert.equal(slugToProviderGroup('outlook-2'), 'outlook');
  assert.equal(slugToProviderGroup('outlook-3'), 'outlook');
});

test('accountProviderCodeToGroup: code 1 Custom IMAP/SMTP returns google_otd', () => {
  assert.equal(accountProviderCodeToGroup(1), 'google_otd');
});

test('accountProviderCodeToGroup: code 2 Google returns google_otd', () => {
  assert.equal(accountProviderCodeToGroup(2), 'google_otd');
});

test('accountProviderCodeToGroup: code 3 Microsoft returns outlook', () => {
  assert.equal(accountProviderCodeToGroup(3), 'outlook');
});

test('accountProviderCodeToGroup: AWS and AirMail return unknown', () => {
  assert.equal(accountProviderCodeToGroup(4), 'unknown');
  assert.equal(accountProviderCodeToGroup(8), 'unknown');
});

test('accountProviderCodeToGroup: nullish values return unknown', () => {
  assert.equal(accountProviderCodeToGroup(null), 'unknown');
  assert.equal(accountProviderCodeToGroup(undefined), 'unknown');
});

test('accountProviderCodeToGroup: unexpected numeric values return unknown', () => {
  assert.equal(accountProviderCodeToGroup(0), 'unknown');
  assert.equal(accountProviderCodeToGroup(5), 'unknown');
  assert.equal(accountProviderCodeToGroup(99), 'unknown');
  assert.equal(accountProviderCodeToGroup(Number.NaN), 'unknown');
});

test('slugToProviderGroup: known Renaissance slugs return google_otd', () => {
  assert.equal(slugToProviderGroup('renaissance-1'), 'google_otd');
  assert.equal(slugToProviderGroup('renaissance-3'), 'google_otd');
  assert.equal(slugToProviderGroup('renaissance-8'), 'google_otd');
});

test('slugToProviderGroup: unknown slug returns unknown', () => {
  assert.equal(slugToProviderGroup('erc-1'), 'unknown');
  assert.equal(slugToProviderGroup('section-125-1'), 'unknown');
  assert.equal(slugToProviderGroup('the-dyad'), 'unknown');
  assert.equal(slugToProviderGroup('warm-leads'), 'unknown');
  assert.equal(slugToProviderGroup('automated-applications'), 'unknown');
});

test('slugToProviderGroup: excluded slugs return unknown', () => {
  for (const slug of EXCLUDED_SLUGS) {
    assert.equal(slugToProviderGroup(slug), 'unknown');
  }
});

test('slugToProviderGroup: empty string returns unknown', () => {
  assert.equal(slugToProviderGroup(''), 'unknown');
});

test('slugToProviderGroup: case-sensitive (uppercase variant is unknown)', () => {
  assert.equal(slugToProviderGroup('Outlook-1'), 'unknown');
  assert.equal(slugToProviderGroup('OUTLOOK-1'), 'unknown');
  assert.equal(slugToProviderGroup('Renaissance-3'), 'unknown');
});

test('EXCLUDED_SLUGS contains exactly personal and sam-test', () => {
  assert.deepEqual([...EXCLUDED_SLUGS].sort(), ['personal', 'sam-test']);
});

test('isFreeMailDomain: gmail.com is true', () => {
  assert.equal(isFreeMailDomain('gmail.com'), true);
});

test('isFreeMailDomain: tryunsecuredhq.co is false', () => {
  assert.equal(isFreeMailDomain('tryunsecuredhq.co'), false);
});

test('isFreeMailDomain: uppercase GMAIL.COM is true (case-insensitive)', () => {
  assert.equal(isFreeMailDomain('GMAIL.COM'), true);
});

test('emailToDomain: happy path returns lowercased domain', () => {
  assert.equal(emailToDomain('a@b.com'), 'b.com');
});

test('emailToDomain: no @ returns null', () => {
  assert.equal(emailToDomain('plainstring'), null);
});

test('emailToDomain: trailing @ returns null', () => {
  assert.equal(emailToDomain('user@'), null);
});

test('emailToDomain: uppercase email returns lowercased domain', () => {
  assert.equal(emailToDomain('User@Example.COM'), 'example.com');
});

test('normalizeDomain: trims and lowercases', () => {
  assert.equal(normalizeDomain('  Example.COM  '), 'example.com');
  assert.equal(normalizeDomain('TryUnsecuredHQ.co'), 'tryunsecuredhq.co');
});
