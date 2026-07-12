/// <reference types="node" />

import assert from 'node:assert/strict';
import test from 'node:test';
import { primaryKeySqlLiteral, sqlIdentifier, sqlStringLiteral } from '../lib/sql.ts';

test('sqlStringLiteral doubles every embedded quote', () => {
  assert.equal(sqlStringLiteral("O'Brien"), "'O''Brien'");
  assert.equal(sqlStringLiteral("a''b"), "'a''''b'");
});

test('sqlIdentifier quotes delimiters and embedded double quotes', () => {
  assert.equal(sqlIdentifier('accounts'), '"accounts"');
  assert.equal(
    sqlIdentifier('tenant; DROP TABLE users'),
    '"tenant; DROP TABLE users"',
  );
  assert.equal(sqlIdentifier('odd"name'), '"odd""name"');
});

test('primaryKeySqlLiteral keeps quote attacks inside one string literal', () => {
  const attack = "x' OR 1=1 --";
  const sql = `DELETE FROM accounts WHERE id = ${primaryKeySqlLiteral(attack)}`;

  assert.equal(sql, "DELETE FROM accounts WHERE id = 'x'' OR 1=1 --'");
  assert.equal(
    primaryKeySqlLiteral({ String: attack }),
    "'x'' OR 1=1 --'",
  );
});

test('primaryKeySqlLiteral preserves numeric, boolean, and null values', () => {
  assert.equal(primaryKeySqlLiteral(42), '42');
  assert.equal(primaryKeySqlLiteral({ Integer: 42 }), '42');
  assert.equal(primaryKeySqlLiteral({ Float: '1.25e2' }), '1.25e2');
  assert.equal(primaryKeySqlLiteral({ Boolean: false }), 'FALSE');
  assert.equal(primaryKeySqlLiteral({ Null: null }), 'NULL');
});
