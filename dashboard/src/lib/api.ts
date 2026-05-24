const API_BASE = '/api';
const CLIENT_SETTINGS_KEY = 'fusiondb.client-settings';
const CLIENT_SETTINGS_EVENT = 'fusiondb:client-settings-changed';

export interface SelectQueryResult {
  type: 'select';
  columns: string[];
  rows: any[][];
}

export interface SuccessQueryResult {
  type: 'success';
  message: string;
}

export type QueryResult = SelectQueryResult | SuccessQueryResult;

export interface ApiEnvelope<T> {
  status: 'ok' | 'error';
  data: T | null;
  error: string | null;
}

export type QueryResponse = ApiEnvelope<QueryResult[]>;

export interface ColumnInfo {
  name: string;
  data_type: string;
  is_primary: boolean;
  is_indexed: boolean;
  is_nullable: boolean;
  is_unique: boolean;
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
  duration_ms: number;
  timestamp: string;
}

export interface OperationInfo {
  operation: string;
  message: string | null;
  supported: boolean;
}

export type OperationResponse = ApiEnvelope<OperationInfo>;

export interface CapabilityInfo {
  backend: string;
  snapshot_supported: boolean;
  compact_supported: boolean;
  prepared_statement_ownership: boolean;
  distributed_mode: string;
}

export interface AuthContextInfo {
  username: string | null;
  authenticated: boolean;
  mode: string;
}

export interface PreparedStatementInfo {
  statement_id: string;
  sql: string;
  statement_count: number;
  owner: string | null;
  created_at_epoch_ms: number;
}

export interface ClientSettings {
  apiBaseUrl: string;
  username: string;
}

const DEFAULT_CLIENT_SETTINGS: ClientSettings = {
  apiBaseUrl: '',
  username: '',
};

function canUseStorage(): boolean {
  return typeof window !== 'undefined' && typeof window.localStorage !== 'undefined';
}

export function getClientSettings(): ClientSettings {
  if (!canUseStorage()) return DEFAULT_CLIENT_SETTINGS;

  try {
    const raw = window.localStorage.getItem(CLIENT_SETTINGS_KEY);
    if (!raw) return DEFAULT_CLIENT_SETTINGS;
    const parsed = JSON.parse(raw) as Partial<ClientSettings>;
    return {
      apiBaseUrl: typeof parsed.apiBaseUrl === 'string' ? parsed.apiBaseUrl : '',
      username: typeof parsed.username === 'string' ? parsed.username : '',
    };
  } catch {
    return DEFAULT_CLIENT_SETTINGS;
  }
}

export function saveClientSettings(next: Partial<ClientSettings>): ClientSettings {
  const merged = {
    ...getClientSettings(),
    ...next,
  };

  if (canUseStorage()) {
    window.localStorage.setItem(CLIENT_SETTINGS_KEY, JSON.stringify(merged));
    window.dispatchEvent(new CustomEvent(CLIENT_SETTINGS_EVENT, { detail: merged }));
  }

  return merged;
}

export function subscribeClientSettings(listener: () => void): () => void {
  if (typeof window === 'undefined') return () => undefined;

  const onStorage = (event: StorageEvent) => {
    if (event.key === CLIENT_SETTINGS_KEY) listener();
  };
  const onCustom = () => listener();

  window.addEventListener('storage', onStorage);
  window.addEventListener(CLIENT_SETTINGS_EVENT, onCustom);

  return () => {
    window.removeEventListener('storage', onStorage);
    window.removeEventListener(CLIENT_SETTINGS_EVENT, onCustom);
  };
}

function buildApiUrl(path: string): string {
  const baseOverride = getClientSettings().apiBaseUrl.trim();
  if (!baseOverride) {
    return `${API_BASE}${path}`;
  }
  return `${baseOverride.replace(/\/$/, '')}${path}`;
}

async function parseEnvelope<T>(response: Response): Promise<ApiEnvelope<T>> {
  const text = await response.text();
  if (!text) {
    return {
      status: response.ok ? 'ok' : 'error',
      data: null,
      error: response.ok ? null : `HTTP ${response.status}`,
    };
  }

  try {
    const parsed = JSON.parse(text) as ApiEnvelope<T>;
    if (
      parsed
      && (parsed.status === 'ok' || parsed.status === 'error')
      && 'data' in parsed
      && 'error' in parsed
    ) {
      return parsed;
    }
  } catch {
    // Fall through to generic error handling.
  }

  return {
    status: response.ok ? 'ok' : 'error',
    data: null,
    error: response.ok ? null : text,
  };
}

async function request<T>(path: string, init?: RequestInit): Promise<ApiEnvelope<T>> {
  const settings = getClientSettings();
  const headers = new Headers(init?.headers ?? {});

  if (!headers.has('Content-Type') && init?.body) {
    headers.set('Content-Type', 'application/json');
  }
  if (settings.username.trim()) {
    headers.set('x-fusiondb-user', settings.username.trim());
  }

  try {
    const response = await fetch(buildApiUrl(path), {
      ...init,
      headers,
    });
    return await parseEnvelope<T>(response);
  } catch (error: any) {
    return {
      status: 'error',
      data: null,
      error: error?.message ?? 'Network request failed',
    };
  }
}

export async function executeQuery(sql: string): Promise<QueryResponse> {
  return request<QueryResult[]>('/query', {
    method: 'POST',
    body: JSON.stringify({ sql }),
  });
}

export async function fetchTables(): Promise<TableInfo[]> {
  const response = await request<TableInfo[]>('/tables');
  return response.status === 'ok' && response.data ? response.data : [];
}

export async function fetchMetrics(): Promise<Metrics | null> {
  const response = await request<Metrics>('/metrics');
  return response.status === 'ok' ? response.data : null;
}

export async function fetchSlowQueries(): Promise<SlowQuery[]> {
  const response = await request<SlowQuery[]>('/slow_queries');
  return response.status === 'ok' && response.data ? response.data : [];
}

export async function createCheckpoint(): Promise<OperationResponse> {
  return request<OperationInfo>('/checkpoint', { method: 'POST' });
}

export async function createCompaction(): Promise<OperationResponse> {
  return request<OperationInfo>('/compact', { method: 'POST' });
}

export async function fetchCapabilities(): Promise<CapabilityInfo | null> {
  const response = await request<CapabilityInfo>('/capabilities');
  return response.status === 'ok' ? response.data : null;
}

export async function fetchAuthContext(): Promise<AuthContextInfo | null> {
  const response = await request<AuthContextInfo>('/auth/context');
  return response.status === 'ok' ? response.data : null;
}

export async function prepareStatement(sql: string): Promise<ApiEnvelope<PreparedStatementInfo>> {
  return request<PreparedStatementInfo>('/prepare', {
    method: 'POST',
    body: JSON.stringify({ sql }),
  });
}

export async function listPreparedStatements(): Promise<PreparedStatementInfo[]> {
  const response = await request<PreparedStatementInfo[]>('/prepare');
  return response.status === 'ok' && response.data ? response.data : [];
}

export async function executePreparedStatement(
  statementId: string,
  params: any[] = [],
): Promise<QueryResponse> {
  return request<QueryResult[]>('/execute', {
    method: 'POST',
    body: JSON.stringify({ statement_id: statementId, params }),
  });
}

export async function deallocatePreparedStatement(
  statementId: string,
): Promise<ApiEnvelope<PreparedStatementInfo>> {
  return request<PreparedStatementInfo>(`/prepare/${statementId}`, {
    method: 'DELETE',
  });
}
