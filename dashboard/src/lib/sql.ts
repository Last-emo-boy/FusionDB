function taggedNumericLiteral(value: unknown): string | null {
  if (typeof value === 'number' && Number.isFinite(value)) return String(value);
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  return /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/.test(trimmed) ? trimmed : null;
}

function fallbackValue(value: unknown): string {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'object') return JSON.stringify(value) ?? String(value);
  return String(value);
}

export function sqlStringLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

export function sqlIdentifier(value: string): string {
  return `"${value.replace(/"/g, '""')}"`;
}

export function primaryKeySqlLiteral(value: unknown): string {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'number' && Number.isFinite(value)) return String(value);
  if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE';

  if (typeof value === 'object') {
    if ('Null' in value) return 'NULL';
    if ('Boolean' in value && typeof value.Boolean === 'boolean') {
      return value.Boolean ? 'TRUE' : 'FALSE';
    }
    if ('Integer' in value) {
      const literal = taggedNumericLiteral(value.Integer);
      if (literal !== null) return literal;
    }
    if ('Float' in value) {
      const literal = taggedNumericLiteral(value.Float);
      if (literal !== null) return literal;
    }
    if ('String' in value) return sqlStringLiteral(String(value.String));
  }

  return sqlStringLiteral(fallbackValue(value));
}
