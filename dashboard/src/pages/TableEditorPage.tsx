import { useCallback, useEffect, useRef, useState } from 'react';
import {
  Table2,
  Key,
  Search,
  Plus,
  Trash2,
  RefreshCw,
  Columns3,
  Hash,
  Type,
  ToggleLeft,
  Shield,
} from 'lucide-react';
import { fetchAuthContext, fetchTables, executeQuery } from '../lib/api';
import type { AuthContextInfo, TableInfo } from '../lib/api';

function formatValue(val: unknown): string {
  if (val === null || val === undefined) return 'NULL';
  if (typeof val === 'object') {
    if ('Integer' in val) return String(val.Integer);
    if ('Float' in val) return String(val.Float);
    if ('String' in val) return String(val.String);
    if ('Boolean' in val) return String(val.Boolean);
    if ('Null' in val) return 'NULL';
    if ('Vector' in val && Array.isArray(val.Vector)) {
      return `[${val.Vector.slice(0, 3).join(', ')}${val.Vector.length > 3 ? ', ...' : ''}]`;
    }
    return JSON.stringify(val) ?? String(val);
  }
  return String(val);
}

function hasNumericTag(value: unknown): boolean {
  return typeof value === 'object'
    && value !== null
    && ('Integer' in value || 'Float' in value);
}

function DataTypeIcon({ type }: { type: string }) {
  const t = type.toUpperCase();
  if (t.includes('INT')) return <Hash size={12} className="text-blue-400" />;
  if (t.includes('FLOAT') || t.includes('REAL') || t.includes('DOUBLE'))
    return <Hash size={12} className="text-purple-400" />;
  if (t.includes('BOOL')) return <ToggleLeft size={12} className="text-yellow-400" />;
  if (t.includes('TEXT') || t.includes('VARCHAR') || t.includes('CHAR'))
    return <Type size={12} className="text-green-400" />;
  return <Columns3 size={12} className="text-text-secondary" />;
}

export default function TableEditorPage() {
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [rows, setRows] = useState<unknown[][] | null>(null);
  const [columns, setColumns] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [rowCount, setRowCount] = useState<number | null>(null);
  const [filter, setFilter] = useState('');
  const [tableFilter, setTableFilter] = useState('');
  const [insertMode, setInsertMode] = useState(false);
  const [insertValues, setInsertValues] = useState<Record<string, string>>({});
  const [insertError, setInsertError] = useState<string | null>(null);
  const [deleteConfirm, setDeleteConfirm] = useState<number | null>(null);
  const [authContext, setAuthContext] = useState<AuthContextInfo | null>(null);
  const filterRef = useRef(filter);

  useEffect(() => {
    filterRef.current = filter;
  }, [filter]);

  const loadTables = useCallback(async () => {
    const [t, auth] = await Promise.all([fetchTables(), fetchAuthContext()]);
    setTables(t);
    setAuthContext(auth);
    if (t.length > 0) {
      setSelectedTable((current) => current ?? t[0].name);
    }
  }, []);

  useEffect(() => {
    const initialLoad = window.setTimeout(() => void loadTables(), 0);
    return () => window.clearTimeout(initialLoad);
  }, [loadTables]);

  const loadTableData = useCallback(async (tableName: string) => {
    setLoading(true);
    setInsertMode(false);
    setInsertError(null);
    setDeleteConfirm(null);
    const activeFilter = filterRef.current.trim();
    const limitSql = activeFilter
      ? `SELECT * FROM ${tableName} WHERE ${activeFilter} LIMIT 200`
      : `SELECT * FROM ${tableName} LIMIT 200`;
    const res = await executeQuery(limitSql);
    if (res.status === 'ok' && res.data && res.data[0]?.type === 'select') {
      const r = res.data[0];
      setColumns(r.columns);
      setRows(r.rows);
    } else {
      setColumns([]);
      setRows([]);
    }
    // Get count
    const countRes = await executeQuery(`SELECT COUNT(*) FROM ${tableName}`);
    if (countRes.status === 'ok' && countRes.data && countRes.data[0]?.type === 'select' && countRes.data[0].rows[0]) {
      const v = countRes.data[0].rows[0][0];
      setRowCount(
        typeof v === 'object'
          && v !== null
          && 'Integer' in v
          && typeof v.Integer === 'number'
          ? v.Integer
          : null,
      );
    }
    setLoading(false);
  }, []);

  useEffect(() => {
    if (!selectedTable) return;

    const dataLoad = window.setTimeout(() => void loadTableData(selectedTable), 0);
    return () => window.clearTimeout(dataLoad);
  }, [loadTableData, selectedTable]);

  const selectedSchema = tables.find((t) => t.name === selectedTable);
  const filteredTables = tables.filter((t) =>
    t.name.toLowerCase().includes(tableFilter.toLowerCase()),
  );

  const handleInsert = async () => {
    if (!selectedTable || !selectedSchema) return;
    setInsertError(null);

    const cols = selectedSchema.columns.map((c) => c.name);
    const vals = cols.map((c) => {
      const v = insertValues[c];
      if (!v || v.trim() === '' || v.trim().toUpperCase() === 'NULL') return 'NULL';
      // Check if it looks numeric
      if (/^-?\d+$/.test(v.trim())) return v.trim();
      if (/^-?\d+\.\d+$/.test(v.trim())) return v.trim();
      if (v.trim().toLowerCase() === 'true' || v.trim().toLowerCase() === 'false') return v.trim();
      return `'${v.replace(/'/g, "''")}'`;
    });

    const sql = `INSERT INTO ${selectedTable} (${cols.join(', ')}) VALUES (${vals.join(', ')})`;
    const res = await executeQuery(sql);
    if (res.status === 'error' || res.error) {
      setInsertError(res.error ?? 'Insert failed');
    } else {
      setInsertMode(false);
      setInsertValues({});
      loadTableData(selectedTable);
    }
  };

  const handleDelete = async (rowIdx: number) => {
    if (!selectedTable || !selectedSchema || !rows) return;
    const row = rows[rowIdx];
    const pkCol = selectedSchema.columns.find((c) => c.is_primary);
    if (!pkCol) return;
    const pkIdx = columns.indexOf(pkCol.name);
    if (pkIdx === -1) return;
    const pkVal = row[pkIdx];
    const formatted = formatValue(pkVal);
    const isNum = hasNumericTag(pkVal);
    const whereVal = isNum ? formatted : `'${formatted}'`;

    const sql = `DELETE FROM ${selectedTable} WHERE ${pkCol.name} = ${whereVal}`;
    await executeQuery(sql);
    setDeleteConfirm(null);
    loadTableData(selectedTable);
  };

  return (
    <div className="flex h-screen">
      {/* Table List Sidebar */}
      <div className="w-56 shrink-0 border-r border-border bg-bg-secondary flex flex-col">
        <div className="p-3 border-b border-border">
          <div className="relative">
            <Search
              size={13}
              className="absolute left-2.5 top-1/2 -translate-y-1/2 text-text-muted"
            />
            <input
              type="text"
              placeholder="Filter tables..."
              value={tableFilter}
              onChange={(e) => setTableFilter(e.target.value)}
              className="w-full pl-8 pr-3 py-1.5 text-xs bg-bg-card border border-border rounded-md text-text-primary placeholder-text-muted focus:outline-none focus:border-accent"
            />
          </div>
        </div>
        <div className="flex-1 overflow-auto py-1">
          {filteredTables.map((t) => (
            <button
              key={t.name}
              onClick={() => setSelectedTable(t.name)}
              className={`w-full flex items-center gap-2 px-3 py-2 text-left text-[13px] transition-colors ${
                selectedTable === t.name
                  ? 'bg-accent-dim text-accent'
                  : 'text-text-secondary hover:text-text-primary hover:bg-bg-hover'
              }`}
            >
              <Table2 size={13} />
              <span className="truncate">{t.name}</span>
              <span className="ml-auto text-[10px] text-text-muted">{t.columns.length}</span>
            </button>
          ))}
          {filteredTables.length === 0 && (
            <div className="p-3 text-xs text-text-muted text-center">No tables found</div>
          )}
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {selectedTable && selectedSchema ? (
          <>
            {/* Table Header */}
            <div className="flex items-center justify-between px-4 py-3 border-b border-border bg-bg-secondary">
              <div className="flex items-center gap-2">
                <Table2 size={16} className="text-accent" />
                <span className="font-medium text-sm">{selectedTable}</span>
                {rowCount !== null && (
                  <span className="text-xs text-text-muted">
                    {rowCount.toLocaleString()} row{rowCount !== 1 ? 's' : ''}
                  </span>
                )}
              </div>
              <div className="flex items-center gap-2">
                <div className="hidden md:flex items-center gap-1.5 px-2 py-1 text-[11px] text-text-muted bg-bg-card border border-border rounded-md">
                  <Shield size={11} />
                  {authContext?.authenticated ? `HTTP user: ${authContext.username}` : 'legacy anonymous'}
                </div>
                <button
                  onClick={() => {
                    setInsertMode(!insertMode);
                    setInsertValues({});
                    setInsertError(null);
                  }}
                  className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-accent/10 border border-accent/30 rounded-md text-accent hover:bg-accent/20 transition-colors"
                >
                  <Plus size={12} />
                  Insert Row
                </button>
                <button
                  onClick={() => loadTableData(selectedTable)}
                  className="p-1.5 text-text-secondary hover:text-text-primary rounded transition-colors"
                >
                  <RefreshCw size={13} />
                </button>
              </div>
            </div>

            {/* Schema Info */}
            <div className="flex items-center gap-3 px-4 py-2 border-b border-border bg-bg-card text-[11px] overflow-x-auto">
              {selectedSchema.columns.map((col) => (
                <div key={col.name} className="flex items-center gap-1.5 shrink-0">
                  <DataTypeIcon type={col.data_type} />
                  <span className="text-text-secondary">{col.name}</span>
                  <span className="text-text-muted">{col.data_type}</span>
                  {col.is_primary && <Key size={9} className="text-yellow-500" />}
                </div>
              ))}
            </div>

            {/* Filter bar */}
            <div className="flex items-center gap-2 px-4 py-2 border-b border-border bg-bg-secondary">
              <Search size={12} className="text-text-muted" />
              <input
                type="text"
                placeholder="WHERE clause filter (e.g., id > 5 AND name LIKE 'A%')"
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && selectedTable) loadTableData(selectedTable);
                }}
                className="flex-1 text-xs bg-transparent text-text-primary placeholder-text-muted focus:outline-none"
              />
              <button
                onClick={() => selectedTable && loadTableData(selectedTable)}
                className="text-xs text-accent hover:text-accent-hover"
              >
                Apply
              </button>
            </div>

            {/* Insert Row Form */}
            {insertMode && (
              <div className="px-4 py-3 border-b border-border bg-accent/5">
                <div className="flex flex-wrap gap-2 mb-2">
                  {selectedSchema.columns.map((col) => (
                    <div key={col.name} className="flex flex-col gap-0.5">
                      <label className="text-[10px] text-text-muted">{col.name}</label>
                      <input
                        type="text"
                        placeholder={col.data_type}
                        value={insertValues[col.name] ?? ''}
                        onChange={(e) =>
                          setInsertValues((v) => ({ ...v, [col.name]: e.target.value }))
                        }
                        className="w-28 px-2 py-1 text-xs bg-bg-card border border-border rounded text-text-primary placeholder-text-muted focus:outline-none focus:border-accent"
                      />
                    </div>
                  ))}
                </div>
                {insertError && (
                  <div className="text-xs text-danger mb-2">{insertError}</div>
                )}
                <div className="flex gap-2">
                  <button
                    onClick={handleInsert}
                    className="px-3 py-1 text-xs bg-accent text-black rounded hover:bg-accent-hover"
                  >
                    Save
                  </button>
                  <button
                    onClick={() => {
                      setInsertMode(false);
                      setInsertValues({});
                      setInsertError(null);
                    }}
                    className="px-3 py-1 text-xs text-text-secondary hover:text-text-primary"
                  >
                    Cancel
                  </button>
                </div>
              </div>
            )}

            {/* Data Table */}
            <div className="flex-1 overflow-auto">
              {loading ? (
                <div className="flex items-center justify-center h-full text-sm text-text-muted">
                  Loading...
                </div>
              ) : rows && rows.length > 0 ? (
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border bg-bg-secondary sticky top-0 z-10">
                      <th className="w-10 px-3 py-2 text-left text-[11px] text-text-muted font-medium">
                        #
                      </th>
                      {columns.map((col, ci) => (
                        <th
                          key={ci}
                          className="px-3 py-2 text-left text-[11px] text-text-secondary font-medium whitespace-nowrap"
                        >
                          {col}
                        </th>
                      ))}
                      <th className="w-10 px-2 py-2"></th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((row, ri) => (
                      <tr
                        key={ri}
                        className="border-b border-border hover:bg-bg-hover transition-colors group"
                      >
                        <td className="px-3 py-1.5 text-[11px] text-text-muted">{ri + 1}</td>
                        {row.map((val, vi) => (
                          <td
                            key={vi}
                            className={`px-3 py-1.5 text-[13px] font-mono whitespace-nowrap ${
                              formatValue(val) === 'NULL'
                                ? 'text-text-muted italic'
                                : 'text-text-primary'
                            }`}
                          >
                            {formatValue(val)}
                          </td>
                        ))}
                        <td className="px-2 py-1.5">
                          {deleteConfirm === ri ? (
                            <div className="flex items-center gap-1">
                              <button
                                onClick={() => handleDelete(ri)}
                                className="text-[10px] text-danger hover:underline"
                              >
                                Confirm
                              </button>
                              <button
                                onClick={() => setDeleteConfirm(null)}
                                className="text-[10px] text-text-muted hover:underline"
                              >
                                Cancel
                              </button>
                            </div>
                          ) : (
                            <button
                              onClick={() => setDeleteConfirm(ri)}
                              className="opacity-0 group-hover:opacity-100 p-1 text-text-muted hover:text-danger transition-all"
                            >
                              <Trash2 size={12} />
                            </button>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : (
                <div className="flex items-center justify-center h-full text-sm text-text-muted">
                  No rows found
                </div>
              )}
            </div>
          </>
        ) : (
          <div className="flex items-center justify-center h-full text-sm text-text-muted">
            Select a table from the sidebar
          </div>
        )}
      </div>
    </div>
  );
}
