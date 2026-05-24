import { useEffect, useMemo, useState } from 'react';
import {
  Activity,
  Database,
  Clock,
  Zap,
  HardDrive,
  Search,
  AlertTriangle,
  RefreshCw,
  Shield,
  Layers3,
} from 'lucide-react';
import {
  createCheckpoint,
  createCompaction,
  fetchAuthContext,
  fetchCapabilities,
  fetchMetrics,
  fetchSlowQueries,
  fetchTables,
  listPreparedStatements,
} from '../lib/api';
import type {
  AuthContextInfo,
  CapabilityInfo,
  Metrics,
  PreparedStatementInfo,
  SlowQuery,
  TableInfo,
} from '../lib/api';

function StatCard({
  icon: Icon,
  label,
  value,
  sub,
  accent,
}: {
  icon: any;
  label: string;
  value: string | number;
  sub?: string;
  accent?: boolean;
}) {
  return (
    <div className="bg-bg-card border border-border rounded-lg p-4">
      <div className="flex items-center gap-2 mb-2">
        <Icon size={14} className={accent ? 'text-accent' : 'text-text-secondary'} />
        <span className="text-[11px] text-text-secondary uppercase tracking-wider">{label}</span>
      </div>
      <div className={`text-2xl font-semibold ${accent ? 'text-accent' : 'text-text-primary'}`}>
        {value}
      </div>
      {sub && <div className="text-[11px] text-text-muted mt-1">{sub}</div>}
    </div>
  );
}

function statusTone(success: boolean): string {
  return success
    ? 'bg-accent/10 border-accent/30 text-accent'
    : 'bg-danger/10 border-danger/30 text-danger';
}

export default function DashboardPage() {
  const [metrics, setMetrics] = useState<Metrics | null>(null);
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [slowQueries, setSlowQueries] = useState<SlowQuery[]>([]);
  const [capabilities, setCapabilities] = useState<CapabilityInfo | null>(null);
  const [authContext, setAuthContext] = useState<AuthContextInfo | null>(null);
  const [preparedStatements, setPreparedStatements] = useState<PreparedStatementInfo[]>([]);
  const [connected, setConnected] = useState<boolean | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [operationMessage, setOperationMessage] = useState<string | null>(null);
  const [operationError, setOperationError] = useState<string | null>(null);
  const [runningOperation, setRunningOperation] = useState<'checkpoint' | 'compact' | null>(null);

  const load = async () => {
    setRefreshing(true);
    const [m, t, sq, caps, auth, prepared] = await Promise.all([
      fetchMetrics(),
      fetchTables(),
      fetchSlowQueries(),
      fetchCapabilities(),
      fetchAuthContext(),
      listPreparedStatements(),
    ]);
    setMetrics(m);
    setTables(t);
    setSlowQueries(sq);
    setCapabilities(caps);
    setAuthContext(auth);
    setPreparedStatements(prepared);
    setConnected(m !== null && caps !== null);
    setRefreshing(false);
  };

  useEffect(() => {
    load();
    const interval = setInterval(load, 5000);
    return () => clearInterval(interval);
  }, []);

  const avgQueryUs = metrics && metrics.query_count > 0
    ? Math.round(metrics.query_total_us / metrics.query_count)
    : 0;

  const authSummary = useMemo(() => {
    if (!authContext) return 'Authentication context unavailable';
    if (!authContext.authenticated) return 'Legacy anonymous mode';
    return `Scoped as ${authContext.username}`;
  }, [authContext]);

  const runOperation = async (kind: 'checkpoint' | 'compact') => {
    setRunningOperation(kind);
    setOperationMessage(null);
    setOperationError(null);

    const response = kind === 'checkpoint'
      ? await createCheckpoint()
      : await createCompaction();

    if (response.status === 'ok' && response.data) {
      const suffix = response.data.supported ? '' : ' (not supported by current backend)';
      setOperationMessage(`${response.data.message ?? `${kind} completed`}${suffix}`);
      await load();
    } else {
      setOperationError(response.error ?? `${kind} failed`);
    }

    setRunningOperation(null);
  };

  return (
    <div className="p-6 max-w-[1200px]">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-xl font-semibold text-text-primary">Dashboard</h1>
          <p className="text-sm text-text-secondary mt-0.5">
            Monitor your FusionDB instance in real-time
          </p>
        </div>
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <div
              className={`w-2 h-2 rounded-full ${
                connected === null ? 'bg-text-muted' : connected ? 'bg-accent' : 'bg-danger'
              }`}
            />
            <span className="text-xs text-text-secondary">
              {connected === null ? 'Checking...' : connected ? 'Connected' : 'Disconnected'}
            </span>
          </div>
          <button
            onClick={load}
            disabled={refreshing}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-bg-card border border-border rounded-md text-text-secondary hover:text-text-primary hover:bg-bg-hover transition-colors disabled:opacity-50"
          >
            <RefreshCw size={12} className={refreshing ? 'animate-spin' : ''} />
            Refresh
          </button>
          <button
            onClick={() => runOperation('checkpoint')}
            disabled={runningOperation !== null || capabilities?.snapshot_supported === false}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-accent/10 border border-accent/30 rounded-md text-accent hover:bg-accent/20 transition-colors disabled:opacity-50"
          >
            <HardDrive size={12} />
            {runningOperation === 'checkpoint' ? 'Checkpointing...' : 'Checkpoint'}
          </button>
          <button
            onClick={() => runOperation('compact')}
            disabled={runningOperation !== null || !capabilities?.compact_supported}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-bg-card border border-border rounded-md text-text-secondary hover:text-text-primary hover:bg-bg-hover transition-colors disabled:opacity-50"
          >
            <Layers3 size={12} />
            {runningOperation === 'compact' ? 'Compacting...' : 'Compact'}
          </button>
        </div>
      </div>

      {(operationMessage || operationError) && (
        <div className={`mb-4 rounded-lg border px-4 py-3 text-sm ${statusTone(!operationError)}`}>
          {operationError ?? operationMessage}
        </div>
      )}

      <div className="grid grid-cols-4 gap-4 mb-6">
        <StatCard
          icon={Zap}
          label="Total Queries"
          value={metrics?.query_count ?? '—'}
          sub={`Avg ${avgQueryUs}µs per query`}
          accent
        />
        <StatCard
          icon={Database}
          label="Tables"
          value={tables.length}
          sub={`${tables.reduce((s, t) => s + t.columns.length, 0)} total columns`}
        />
        <StatCard
          icon={Activity}
          label="Rows Read"
          value={metrics?.row_read_count?.toLocaleString() ?? '—'}
          sub={`${metrics?.row_cache_hit_count?.toLocaleString() ?? 0} cache hits`}
        />
        <StatCard
          icon={HardDrive}
          label="Rows Written"
          value={metrics?.row_write_count?.toLocaleString() ?? '—'}
          sub={`WAL: ${((metrics?.wal_write_bytes ?? 0) / 1024).toFixed(1)} KB`}
        />
      </div>

      <div className="grid grid-cols-3 gap-4 mb-4">
        <div className="bg-bg-card border border-border rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <Shield size={14} className="text-text-secondary" />
            <span className="text-[11px] text-text-secondary uppercase tracking-wider">Auth Context</span>
          </div>
          <div className="text-sm text-text-primary font-medium">{authSummary}</div>
          <div className="text-[11px] text-text-muted mt-1">
            Mode: {authContext?.mode ?? 'unknown'}
          </div>
        </div>
        <div className="bg-bg-card border border-border rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <Database size={14} className="text-text-secondary" />
            <span className="text-[11px] text-text-secondary uppercase tracking-wider">Backend</span>
          </div>
          <div className="text-sm text-text-primary font-medium">
            {capabilities?.backend ?? 'Unknown'}
          </div>
          <div className="text-[11px] text-text-muted mt-1">
            Distributed: {capabilities?.distributed_mode ?? 'unknown'}
          </div>
        </div>
        <div className="bg-bg-card border border-border rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <Layers3 size={14} className="text-text-secondary" />
            <span className="text-[11px] text-text-secondary uppercase tracking-wider">Capabilities</span>
          </div>
          <div className="text-sm text-text-primary font-medium">
            {capabilities?.compact_supported ? 'Compaction available' : 'Compaction unavailable'}
          </div>
          <div className="text-[11px] text-text-muted mt-1">
            Prepared ownership: {capabilities?.prepared_statement_ownership ? 'enabled' : 'disabled'}
          </div>
          <div className="text-[11px] text-text-muted mt-1">
            Visible prepared handles: {preparedStatements.length}
          </div>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div className="bg-bg-card border border-border rounded-lg">
          <div className="px-4 py-3 border-b border-border flex items-center gap-2">
            <Database size={14} className="text-text-secondary" />
            <span className="text-sm font-medium">Tables</span>
            <span className="ml-auto text-xs text-text-muted">{tables.length} tables</span>
          </div>
          <div className="max-h-64 overflow-auto">
            {tables.length === 0 ? (
              <div className="p-4 text-sm text-text-muted text-center">No tables found</div>
            ) : (
              tables.map((t) => (
                <div
                  key={t.name}
                  className="flex items-center justify-between px-4 py-2.5 border-b border-border last:border-0 hover:bg-bg-hover transition-colors"
                >
                  <div className="flex items-center gap-2">
                    <Database size={12} className="text-accent" />
                    <span className="text-sm text-text-primary">{t.name}</span>
                  </div>
                  <span className="text-xs text-text-muted">{t.columns.length} cols</span>
                </div>
              ))
            )}
          </div>
        </div>

        <div className="bg-bg-card border border-border rounded-lg">
          <div className="px-4 py-3 border-b border-border flex items-center gap-2">
            <AlertTriangle size={14} className="text-warning" />
            <span className="text-sm font-medium">Slow Queries</span>
            <span className="ml-auto text-xs text-text-muted">{slowQueries.length} entries</span>
          </div>
          <div className="max-h-64 overflow-auto">
            {slowQueries.length === 0 ? (
              <div className="p-4 text-sm text-text-muted text-center">No slow queries recorded</div>
            ) : (
              slowQueries.map((sq, i) => (
                <div
                  key={i}
                  className="px-4 py-2.5 border-b border-border last:border-0 hover:bg-bg-hover transition-colors"
                >
                  <div className="flex items-center justify-between">
                    <code className="text-xs text-text-primary truncate max-w-[280px]">
                      {sq.sql}
                    </code>
                    <span className="text-xs text-warning font-mono shrink-0 ml-2">
                      {sq.duration_ms.toFixed(1)}ms
                    </span>
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      </div>

      <div className="mt-4 grid grid-cols-2 gap-4">
        <div className="bg-bg-card border border-border rounded-lg">
          <div className="px-4 py-3 border-b border-border flex items-center gap-2">
            <Layers3 size={14} className="text-text-secondary" />
            <span className="text-sm font-medium">Prepared Statements</span>
            <span className="ml-auto text-xs text-text-muted">{preparedStatements.length} visible</span>
          </div>
          <div className="max-h-56 overflow-auto">
            {preparedStatements.length === 0 ? (
              <div className="p-4 text-sm text-text-muted text-center">
                No prepared statements for the current user context
              </div>
            ) : (
              preparedStatements.map((statement) => (
                <div
                  key={statement.statement_id}
                  className="px-4 py-3 border-b border-border last:border-0 hover:bg-bg-hover transition-colors"
                >
                  <div className="flex items-center justify-between gap-3">
                    <code className="text-xs text-text-primary truncate max-w-[320px]">
                      {statement.sql}
                    </code>
                    <span className="text-[11px] text-text-muted shrink-0">
                      {statement.statement_count} stmt
                    </span>
                  </div>
                  <div className="mt-1 text-[11px] text-text-muted">
                    owner: {statement.owner ?? 'anonymous'}
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
        <div className="grid grid-cols-1 gap-4">
          <StatCard
            icon={Search}
            label="FTS Searches"
            value={metrics?.fts_search_count ?? '—'}
            sub={`${metrics?.fts_doc_hits ?? 0} doc hits`}
          />
          <StatCard
            icon={Clock}
            label="Slow Queries"
            value={metrics?.slow_query_count ?? '—'}
            sub="Above threshold"
          />
          <StatCard
            icon={Activity}
            label="WAL Syncs"
            value={metrics?.wal_write_count ?? '—'}
            sub={`${((metrics?.wal_write_bytes ?? 0) / 1024).toFixed(1)} KB total`}
          />
        </div>
      </div>
    </div>
  );
}
