import { useState, useCallback, useRef, useEffect } from 'react';
import { Play, Clock, Download, Copy, Trash2 } from 'lucide-react';
import { EditorView, keymap } from '@codemirror/view';
import { EditorState } from '@codemirror/state';
import { sql } from '@codemirror/lang-sql';
import { oneDark } from '@codemirror/theme-one-dark';
import { basicSetup } from 'codemirror';
import {
  deallocatePreparedStatement,
  executePreparedStatement,
  executeQuery,
  listPreparedStatements,
  prepareStatement,
} from '../lib/api';
import type { PreparedStatementInfo, QueryResult } from '../lib/api';

const DEFAULT_SQL = `-- Welcome to FusionDB SQL Editor
-- Write your SQL queries here and press Ctrl+Enter or click Run

SELECT 1 + 1 AS result;
`;

interface HistoryEntry {
  sql: string;
  time: number;
  error?: string;
  rowCount?: number;
}

export default function SqlEditorPage() {
  const editorRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const [results, setResults] = useState<QueryResult[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [execTime, setExecTime] = useState<number | null>(null);
  const [preparedStatements, setPreparedStatements] = useState<PreparedStatementInfo[]>([]);
  const [_history, setHistory] = useState<HistoryEntry[]>([]);
  const [activeTab, setActiveTab] = useState(0);

  const loadPreparedStatements = useCallback(async () => {
    setPreparedStatements(await listPreparedStatements());
  }, []);

  const runQuery = useCallback(async () => {
    const view = viewRef.current;
    if (!view) return;

    // Get selected text or full content
    const state = view.state;
    const selection = state.sliceDoc(state.selection.main.from, state.selection.main.to);
    const queryText = selection.trim() || state.doc.toString().trim();
    if (!queryText) return;

    setLoading(true);
    setError(null);
    setResults(null);
    const start = performance.now();

    try {
      const response = await executeQuery(queryText);
      const elapsed = Math.round(performance.now() - start);
      setExecTime(elapsed);

      if (response.status === 'error' || response.error) {
        const message = response.error ?? 'Query execution failed';
        setError(message);
        setHistory((h) => [{ sql: queryText, time: elapsed, error: message }, ...h].slice(0, 50));
      } else if (response.data) {
        setResults(response.data);
        setActiveTab(0);
        const totalRows = response.data.reduce(
          (s, r) => s + (r.type === 'select' ? r.rows.length : 0),
          0,
        );
        setHistory((h) => [{ sql: queryText, time: elapsed, rowCount: totalRows }, ...h].slice(0, 50));
        await loadPreparedStatements();
      }
    } catch (e: any) {
      const elapsed = Math.round(performance.now() - start);
      setExecTime(elapsed);
      setError(`Connection error: ${e.message}`);
      setHistory((h) => [{ sql: queryText, time: elapsed, error: e.message }, ...h].slice(0, 50));
    } finally {
      setLoading(false);
    }
  }, [loadPreparedStatements]);

  useEffect(() => {
    loadPreparedStatements();
  }, [loadPreparedStatements]);

  useEffect(() => {
    if (!editorRef.current) return;

    const runKeymap = keymap.of([
      {
        key: 'Ctrl-Enter',
        run: () => {
          runQuery();
          return true;
        },
      },
      {
        key: 'Mod-Enter',
        run: () => {
          runQuery();
          return true;
        },
      },
    ]);

    const state = EditorState.create({
      doc: DEFAULT_SQL,
      extensions: [basicSetup, sql(), oneDark, runKeymap, EditorView.lineWrapping],
    });

    const view = new EditorView({
      state,
      parent: editorRef.current,
    });

    viewRef.current = view;
    return () => view.destroy();
  }, [runQuery]);

  const copyResults = () => {
    if (!results || !results[activeTab]) return;
    const r = results[activeTab];
    if (r.type !== 'select') return;
    const header = r.columns.join('\t');
    const rows = r.rows.map((row) => row.map((v) => formatValue(v)).join('\t')).join('\n');
    navigator.clipboard.writeText(`${header}\n${rows}`);
  };

  const downloadCsv = () => {
    if (!results || !results[activeTab]) return;
    const r = results[activeTab];
    if (r.type !== 'select') return;
    const header = r.columns.join(',');
    const rows = r.rows.map((row) => row.map((v) => `"${formatValue(v)}"`).join(',')).join('\n');
    const blob = new Blob([`${header}\n${rows}`], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'query_results.csv';
    a.click();
    URL.revokeObjectURL(url);
  };

  const prepareCurrentSql = async () => {
    const view = viewRef.current;
    if (!view) return;

    const state = view.state;
    const selection = state.sliceDoc(state.selection.main.from, state.selection.main.to);
    const queryText = selection.trim() || state.doc.toString().trim();
    if (!queryText) return;

    setLoading(true);
    setError(null);
    try {
      const response = await prepareStatement(queryText);
      if (response.status === 'error' || response.error || !response.data) {
        setError(response.error ?? 'Prepare failed');
      } else {
        await loadPreparedStatements();
      }
    } finally {
      setLoading(false);
    }
  };

  const runPreparedStatement = async (statementId: string) => {
    setLoading(true);
    setError(null);
    setResults(null);
    const start = performance.now();
    try {
      const response = await executePreparedStatement(statementId);
      const elapsed = Math.round(performance.now() - start);
      setExecTime(elapsed);
      if (response.status === 'error' || response.error || !response.data) {
        setError(response.error ?? 'Prepared statement execution failed');
      } else {
        setResults(response.data);
        setActiveTab(0);
      }
    } finally {
      setLoading(false);
    }
  };

  const removePreparedStatement = async (statementId: string) => {
    setError(null);
    const response = await deallocatePreparedStatement(statementId);
    if (response.status === 'error' || response.error) {
      setError(response.error ?? 'Deallocate failed');
      return;
    }
    await loadPreparedStatements();
  };

  return (
    <div className="flex flex-col h-screen">
      {/* Toolbar */}
      <div className="flex items-center justify-between px-4 py-2 border-b border-border bg-bg-secondary">
        <div className="flex items-center gap-2">
          <button
            onClick={runQuery}
            disabled={loading}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium bg-accent text-black rounded-md hover:bg-accent-hover transition-colors disabled:opacity-50"
          >
            <Play size={12} />
            {loading ? 'Running...' : 'Run'}
          </button>
          <button
            onClick={prepareCurrentSql}
            disabled={loading}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium bg-bg-card border border-border rounded-md text-text-secondary hover:text-text-primary hover:bg-bg-hover transition-colors disabled:opacity-50"
          >
            Prepare
          </button>
          <span className="text-[11px] text-text-muted">Ctrl+Enter to execute</span>
        </div>
        <div className="flex items-center gap-2">
          {execTime !== null && (
            <div className="flex items-center gap-1 text-[11px] text-text-muted">
              <Clock size={10} />
              {execTime}ms
            </div>
          )}
          {results && (
            <>
              <button
                onClick={copyResults}
                className="p-1.5 text-text-secondary hover:text-text-primary rounded transition-colors"
                title="Copy results"
              >
                <Copy size={13} />
              </button>
              <button
                onClick={downloadCsv}
                className="p-1.5 text-text-secondary hover:text-text-primary rounded transition-colors"
                title="Download CSV"
              >
                <Download size={13} />
              </button>
            </>
          )}
        </div>
      </div>

      {/* Editor */}
      <div className="h-[280px] border-b border-border shrink-0 overflow-hidden bg-[#282c34]">
        <div ref={editorRef} className="h-full" />
      </div>

      <div className="h-[220px] border-b border-border shrink-0 overflow-hidden bg-bg-secondary">
        <div className="px-4 py-2 border-b border-border flex items-center justify-between text-[11px] text-text-muted">
          <span>Prepared statements visible to current HTTP user</span>
          <span>{preparedStatements.length} handle(s)</span>
        </div>
        <div className="h-[176px] overflow-auto">
          {preparedStatements.length === 0 ? (
            <div className="flex items-center justify-center h-full text-text-muted text-sm">
              No prepared statements yet
            </div>
          ) : (
            preparedStatements.map((statement) => (
              <div
                key={statement.statement_id}
                className="px-4 py-3 border-b border-border last:border-0 flex items-start justify-between gap-3 hover:bg-bg-hover transition-colors"
              >
                <div className="min-w-0">
                  <code className="block text-xs text-text-primary truncate">{statement.sql}</code>
                  <div className="mt-1 text-[11px] text-text-muted">
                    owner: {statement.owner ?? 'anonymous'} · {statement.statement_count} stmt
                  </div>
                </div>
                <div className="flex items-center gap-2 shrink-0">
                  <button
                    onClick={() => runPreparedStatement(statement.statement_id)}
                    className="px-2 py-1 text-[11px] bg-accent/10 border border-accent/30 rounded text-accent hover:bg-accent/20"
                  >
                    Run
                  </button>
                  <button
                    onClick={() => removePreparedStatement(statement.statement_id)}
                    className="p-1.5 text-text-muted hover:text-danger rounded transition-colors"
                    title="Deallocate"
                  >
                    <Trash2 size={12} />
                  </button>
                </div>
              </div>
            ))
          )}
        </div>
      </div>

      {/* Results */}
      <div className="flex-1 overflow-auto">
        {error && (
          <div className="m-4 p-3 bg-danger/10 border border-danger/30 rounded-md text-sm text-danger">
            {error}
          </div>
        )}

        {results && results.length > 0 && (
          <div className="flex flex-col h-full">
            {/* Result Tabs */}
            {results.length > 1 && (
              <div className="flex items-center gap-0 border-b border-border bg-bg-secondary px-2">
                {results.map((r, i) => (
                  <button
                    key={i}
                    onClick={() => setActiveTab(i)}
                    className={`px-3 py-2 text-xs border-b-2 transition-colors ${
                      activeTab === i
                        ? 'border-accent text-accent'
                        : 'border-transparent text-text-secondary hover:text-text-primary'
                    }`}
                  >
                    Result {i + 1}
                    {r.type === 'select' && <span className="ml-1 text-text-muted">({r.rows.length})</span>}
                  </button>
                ))}
              </div>
            )}

            {/* Active Result */}
            {(() => {
              const r = results[activeTab];
              if (!r) return null;

              if (r.type === 'success') {
                return (
                  <div className="m-4 p-3 bg-accent/10 border border-accent/30 rounded-md text-sm text-accent">
                    {r.message || 'Query executed successfully'}
                  </div>
                );
              }

              if (r.type === 'select') {
                return (
                  <div className="overflow-auto flex-1">
                    <div className="px-4 py-2 text-[11px] text-text-muted border-b border-border bg-bg-secondary">
                      {r.rows.length} row{r.rows.length !== 1 ? 's' : ''} returned
                    </div>
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b border-border bg-bg-secondary sticky top-0">
                          <th className="w-10 px-3 py-2 text-left text-[11px] text-text-muted font-medium">
                            #
                          </th>
                          {r.columns.map((col, ci) => (
                            <th
                              key={ci}
                              className="px-3 py-2 text-left text-[11px] text-text-secondary font-medium whitespace-nowrap"
                            >
                              {col}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {r.rows.map((row, ri) => (
                          <tr
                            key={ri}
                            className="border-b border-border hover:bg-bg-hover transition-colors"
                          >
                            <td className="px-3 py-1.5 text-[11px] text-text-muted">{ri + 1}</td>
                            {row.map((val, vi) => (
                              <td
                                key={vi}
                                className="px-3 py-1.5 text-[13px] text-text-primary font-mono whitespace-nowrap"
                              >
                                {formatValue(val)}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                );
              }
              return null;
            })()}
          </div>
        )}

        {!error && !results && !loading && (
          <div className="flex items-center justify-center h-full text-text-muted text-sm">
            Run a query to see results here
          </div>
        )}

        {loading && (
          <div className="flex items-center justify-center h-full text-text-secondary text-sm">
            <div className="animate-pulse">Executing query...</div>
          </div>
        )}
      </div>
    </div>
  );
}

function formatValue(val: any): string {
  if (val === null || val === undefined) return 'NULL';
  if (typeof val === 'object') {
    if ('Integer' in val) return String(val.Integer);
    if ('Float' in val) return String(val.Float);
    if ('String' in val) return val.String;
    if ('Boolean' in val) return String(val.Boolean);
    if ('Null' in val) return 'NULL';
    if ('Vector' in val) return `[${val.Vector.slice(0, 3).join(', ')}${val.Vector.length > 3 ? ', ...' : ''}]`;
    return JSON.stringify(val);
  }
  return String(val);
}
