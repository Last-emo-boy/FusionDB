import { useState } from 'react';
import { Server, Database, Globe } from 'lucide-react';

export default function SettingsPage() {
  const [apiUrl, setApiUrl] = useState('http://127.0.0.1:3000');

  return (
    <div className="p-6 max-w-[800px]">
      <h1 className="text-xl font-semibold text-text-primary mb-1">Settings</h1>
      <p className="text-sm text-text-secondary mb-6">Configure your FusionDB Studio connection</p>

      {/* Connection */}
      <div className="bg-bg-card border border-border rounded-lg mb-4">
        <div className="flex items-center gap-2 px-4 py-3 border-b border-border">
          <Server size={14} className="text-text-secondary" />
          <span className="text-sm font-medium">Connection</span>
        </div>
        <div className="p-4 space-y-3">
          <div>
            <label className="block text-xs text-text-secondary mb-1">API URL</label>
            <input
              type="text"
              value={apiUrl}
              onChange={(e) => setApiUrl(e.target.value)}
              className="w-full px-3 py-2 text-sm bg-bg-primary border border-border rounded-md text-text-primary focus:outline-none focus:border-accent"
            />
            <p className="text-[11px] text-text-muted mt-1">
              The HTTP API endpoint of your FusionDB instance. The dashboard proxies requests through Vite dev server.
            </p>
          </div>
          <div>
            <label className="block text-xs text-text-secondary mb-1">PostgreSQL Wire Protocol</label>
            <div className="px-3 py-2 text-sm bg-bg-primary border border-border rounded-md text-text-muted">
              localhost:5433
            </div>
            <p className="text-[11px] text-text-muted mt-1">
              Connect with any PostgreSQL client: psql, DBeaver, pgAdmin, etc.
            </p>
          </div>
        </div>
      </div>

      {/* Database Info */}
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
              <div className="text-text-primary">FusionStorage (LSM-tree)</div>
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

      {/* Features */}
      <div className="bg-bg-card border border-border rounded-lg">
        <div className="flex items-center gap-2 px-4 py-3 border-b border-border">
          <Globe size={14} className="text-text-secondary" />
          <span className="text-sm font-medium">Capabilities</span>
        </div>
        <div className="p-4">
          <div className="flex flex-wrap gap-2">
            {[
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
            ].map((feat) => (
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
