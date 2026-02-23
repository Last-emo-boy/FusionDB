import { useState, useCallback, useRef, useEffect } from 'react';
import { Play, Clock, Download, Copy } from 'lucide-react';
import { EditorView, keymap } from '@codemirror/view';
import { EditorState } from '@codemirror/state';
import { sql } from '@codemirror/lang-sql';
import { oneDark } from '@codemirror/theme-one-dark';
import { basicSetup } from 'codemirror';
import { executeQuery } from '../lib/api';
import type { QueryResult } from '../lib/api';

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
  const [_history, setHistory] = useState<HistoryEntry[]>([]);
  const [activeTab, setActiveTab] = useState(0);

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

      if (response.error) {
        setError(response.error);
        setHistory((h) => [{ sql: queryText, time: elapsed, error: response.error! }, ...h].slice(0, 50));
      } else if (response.result) {
        setResults(response.result);
        setActiveTab(0);
        const totalRows = response.result.reduce((s, r) => s + (r.rows?.length ?? 0), 0);
        setHistory((h) => [{ sql: queryText, time: elapsed, rowCount: totalRows }, ...h].slice(0, 50));
      }
    } catch (e: any) {
      const elapsed = Math.round(performance.now() - start);
      setExecTime(elapsed);
      setError(`Connection error: ${e.message}`);
      setHistory((h) => [{ sql: queryText, time: elapsed, error: e.message }, ...h].slice(0, 50));
    } finally {
      setLoading(false);
    }
  }, []);

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
    if (!r.columns || !r.rows) return;
    const header = r.columns.join('\t');
    const rows = r.rows.map((row) => row.map((v) => formatValue(v)).join('\t')).join('\n');
    navigator.clipboard.writeText(`${header}\n${rows}`);
  };

  const downloadCsv = () => {
    if (!results || !results[activeTab]) return;
    const r = results[activeTab];
    if (!r.columns || !r.rows) return;
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
                    {r.rows && <span className="ml-1 text-text-muted">({r.rows.length})</span>}
                  </button>
                ))}
              </div>
            )}

            {/* Active Result */}
            {(() => {
              const r = results[activeTab];
              if (!r) return null;

              if (r.type === 'success' || (!r.columns && r.message)) {
                return (
                  <div className="m-4 p-3 bg-accent/10 border border-accent/30 rounded-md text-sm text-accent">
                    {r.message || 'Query executed successfully'}
                  </div>
                );
              }

              if (r.columns && r.rows) {
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
