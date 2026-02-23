const API_BASE = '/api';

export interface QueryResult {
  type: 'select' | 'success';
  columns?: string[];
  rows?: any[][];
  message?: string;
}

export interface QueryResponse {
  result: QueryResult[] | null;
  error: string | null;
}

export interface ColumnInfo {
  name: string;
  data_type: string;
  is_primary: boolean;
  is_indexed: boolean;
  is_nullable: boolean;
  default_value: string | null;
  index_type: string;
}

export interface TableInfo {
  name: string;
  columns: ColumnInfo[];
}

export interface Metrics {
  sql_parse_count: number;
  sql_plan_count: number;
  row_read_count: number;
  row_cache_hit_count: number;
  row_write_count: number;
  fts_search_count: number;
  fts_doc_hits: number;
  wal_write_count: number;
  wal_write_bytes: number;
  query_count: number;
  slow_query_count: number;
  query_total_us: number;
}

export interface SlowQuery {
  sql: string;
  duration_us: number;
  timestamp: string;
}

export async function executeQuery(sql: string): Promise<QueryResponse> {
  const res = await fetch(`${API_BASE}/query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql }),
  });
  return res.json();
}

export async function fetchTables(): Promise<TableInfo[]> {
  const res = await fetch(`${API_BASE}/tables`);
  if (!res.ok) return [];
  return res.json();
}

export async function fetchMetrics(): Promise<Metrics | null> {
  try {
    const res = await fetch(`${API_BASE}/metrics`);
    if (!res.ok) return null;
    return res.json();
  } catch {
    return null;
  }
}

export async function fetchSlowQueries(): Promise<SlowQuery[]> {
  try {
    const res = await fetch(`${API_BASE}/slow_queries`);
    if (!res.ok) return [];
    return res.json();
  } catch {
    return [];
  }
}

export async function createCheckpoint(): Promise<{ status: string; message?: string; error?: string }> {
  const res = await fetch(`${API_BASE}/checkpoint`, { method: 'POST' });
  return res.json();
}
