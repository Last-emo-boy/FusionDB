import { useCallback, useEffect, useState } from 'react';
import { Server, Database, Globe, Shield } from 'lucide-react';
import {
  fetchAuthContext,
  fetchCapabilities,
  getClientSettings,
  saveClientSettings,
  subscribeClientSettings,
} from '../lib/api';
import type { AuthContextInfo, CapabilityInfo } from '../lib/api';

const CAPABILITY_LABELS = [
  'SQL Queries',
  'JOINs',
  'Subqueries',
  'CTEs',
  'Window Functions',
  'Views',
  'ILIKE',
  'CAST',
  'EXISTS',
  'UNION/INTERSECT/EXCEPT',
  'Vector Search (HNSW)',
  'Full-Text Search (BM25)',
  'Hybrid Search (RRF)',
  'AI Embedding',
  'ACID Transactions',
  'WAL',
  'SSTables',
  'Bloom Filters',
];

export default function SettingsPage() {
  const [apiUrl, setApiUrl] = useState('');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [savedMessage, setSavedMessage] = useState<string | null>(null);
  const [capabilities, setCapabilities] = useState<CapabilityInfo | null>(null);
  const [authContext, setAuthContext] = useState<AuthContextInfo | null>(null);

  const loadRemoteState = useCallback(async () => {
    const [caps, auth] = await Promise.all([fetchCapabilities(), fetchAuthContext()]);
    setCapabilities(caps);
    setAuthContext(auth);
  }, []);

  useEffect(() => {
    const sync = () => {
      const settings = getClientSettings();
      setApiUrl(settings.apiBaseUrl);
      setUsername(settings.username);
      setPassword(settings.password);
    };

    sync();
    const initialLoad = window.setTimeout(() => void loadRemoteState(), 0);
    const unsubscribe = subscribeClientSettings(sync);
    return () => {
      window.clearTimeout(initialLoad);
      unsubscribe();
    };
  }, [loadRemoteState]);

  const saveSettings = async () => {
    saveClientSettings({
      apiBaseUrl: apiUrl.trim(),
      username: username.trim(),
      password,
    });
    setSavedMessage('Settings saved locally for this browser.');
    await loadRemoteState();
    window.setTimeout(() => setSavedMessage(null), 2500);
  };

  return (
    <div className="p-6 max-w-[800px]">
      <h1 className="text-xl font-semibold text-text-primary mb-1">Settings</h1>
      <p className="text-sm text-text-secondary mb-6">Configure your FusionDB Studio connection</p>

      <div className="bg-bg-card border border-border rounded-lg mb-4">
        <div className="flex items-center gap-2 px-4 py-3 border-b border-border">
          <Server size={14} className="text-text-secondary" />
          <span className="text-sm font-medium">Connection</span>
        </div>
        <div className="p-4 space-y-3">
          <div>
            <label className="block text-xs text-text-secondary mb-1">API URL Override</label>
            <input
              type="text"
              value={apiUrl}
              onChange={(e) => setApiUrl(e.target.value)}
              placeholder="Leave empty to use /api dev proxy"
              className="w-full px-3 py-2 text-sm bg-bg-primary border border-border rounded-md text-text-primary focus:outline-none focus:border-accent"
            />
            <p className="text-[11px] text-text-muted mt-1">
              Leave empty for the default Vite proxy. Set an absolute URL to target another HTTP endpoint.
            </p>
          </div>
          <div>
            <label className="block text-xs text-text-secondary mb-1">HTTP User</label>
            <input
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              placeholder="postgres"
              className="w-full px-3 py-2 text-sm bg-bg-primary border border-border rounded-md text-text-primary focus:outline-none focus:border-accent"
            />
            <p className="text-[11px] text-text-muted mt-1">
              Used with HTTP Basic authentication and RBAC authorization.
            </p>
          </div>
          <div>
            <label className="block text-xs text-text-secondary mb-1">HTTP Password</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              autoComplete="current-password"
              className="w-full px-3 py-2 text-sm bg-bg-primary border border-border rounded-md text-text-primary focus:outline-none focus:border-accent"
            />
            <p className="text-[11px] text-text-muted mt-1">
              Kept in session storage and cleared when the browser session ends.
            </p>
          </div>
          <div>
            <label className="block text-xs text-text-secondary mb-1">PostgreSQL Wire Protocol</label>
            <div className="px-3 py-2 text-sm bg-bg-primary border border-border rounded-md text-text-muted">
              localhost:8092
            </div>
            <p className="text-[11px] text-text-muted mt-1">
              Connect with any PostgreSQL client: psql, DBeaver, pgAdmin, etc.
            </p>
          </div>
          <div className="flex items-center gap-3">
            <button
              onClick={saveSettings}
              className="px-3 py-1.5 text-xs bg-accent text-black rounded-md hover:bg-accent-hover transition-colors"
            >
              Save Settings
            </button>
            {savedMessage && <span className="text-xs text-accent">{savedMessage}</span>}
          </div>
        </div>
      </div>

      <div className="bg-bg-card border border-border rounded-lg mb-4">
        <div className="flex items-center gap-2 px-4 py-3 border-b border-border">
          <Shield size={14} className="text-text-secondary" />
          <span className="text-sm font-medium">Request Context</span>
        </div>
        <div className="p-4 grid grid-cols-2 gap-3 text-sm">
          <div>
            <span className="text-text-muted text-xs">Current User</span>
            <div className="text-text-primary">{authContext?.username ?? 'anonymous'}</div>
          </div>
          <div>
            <span className="text-text-muted text-xs">Mode</span>
            <div className="text-text-primary">{authContext?.mode ?? 'unknown'}</div>
          </div>
          <div>
            <span className="text-text-muted text-xs">Prepared Ownership</span>
            <div className="text-text-primary">
              {capabilities?.prepared_statement_ownership ? 'enabled' : 'unknown'}
            </div>
          </div>
          <div>
            <span className="text-text-muted text-xs">Authentication</span>
            <div className="text-text-primary">
              {authContext?.authenticated ? 'explicit user header' : 'legacy anonymous'}
            </div>
          </div>
        </div>
      </div>

      <div className="bg-bg-card border border-border rounded-lg mb-4">
        <div className="flex items-center gap-2 px-4 py-3 border-b border-border">
          <Database size={14} className="text-text-secondary" />
          <span className="text-sm font-medium">Database Info</span>
        </div>
        <div className="p-4">
          <div className="grid grid-cols-2 gap-3 text-sm">
            <div>
              <span className="text-text-muted text-xs">Engine</span>
              <div className="text-text-primary">FusionDB v0.1.0</div>
            </div>
            <div>
              <span className="text-text-muted text-xs">Storage</span>
              <div className="text-text-primary">{capabilities?.backend ?? 'FusionStorage (LSM-tree)'}</div>
            </div>
            <div>
              <span className="text-text-muted text-xs">SQL Parser</span>
              <div className="text-text-primary">sqlparser-rs v0.60</div>
            </div>
            <div>
              <span className="text-text-muted text-xs">Wire Protocol</span>
              <div className="text-text-primary">PostgreSQL v3</div>
            </div>
          </div>
        </div>
      </div>

      <div className="bg-bg-card border border-border rounded-lg">
        <div className="flex items-center gap-2 px-4 py-3 border-b border-border">
          <Globe size={14} className="text-text-secondary" />
          <span className="text-sm font-medium">Capabilities</span>
        </div>
        <div className="p-4 space-y-4">
          <div className="grid grid-cols-2 gap-3 text-sm">
            <div>
              <span className="text-text-muted text-xs">Snapshot</span>
              <div className="text-text-primary">{capabilities?.snapshot_supported ? 'supported' : 'unknown'}</div>
            </div>
            <div>
              <span className="text-text-muted text-xs">Compaction</span>
              <div className="text-text-primary">{capabilities?.compact_supported ? 'supported' : 'not supported'}</div>
            </div>
            <div>
              <span className="text-text-muted text-xs">Distributed Mode</span>
              <div className="text-text-primary">{capabilities?.distributed_mode ?? 'isolated'}</div>
            </div>
            <div>
              <span className="text-text-muted text-xs">Prepared Statements</span>
              <div className="text-text-primary">
                {capabilities?.prepared_statement_ownership ? 'owner-scoped' : 'unknown'}
              </div>
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            {CAPABILITY_LABELS.map((feat) => (
              <span
                key={feat}
                className="px-2 py-1 text-[11px] bg-accent/10 text-accent border border-accent/20 rounded"
              >
                {feat}
              </span>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
