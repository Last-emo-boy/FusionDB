import React, { useState, useEffect } from 'react';
import CodeMirror from '@uiw/react-codemirror';
import { sql } from '@codemirror/lang-sql';
import { Play, Eraser, Database, Clock, Settings, AlertCircle, CheckCircle2, Table as TableIcon, ChevronRight, ChevronDown, RefreshCw, Key } from 'lucide-react';
import axios from 'axios';
import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';
import { Panel, PanelGroup, PanelResizeHandle } from 'react-resizable-panels';

function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

// Types for API Response
interface QueryResult {
  result?: {
    columns: string[];
    rows: any[][];
  }[];
  error?: string;
}

interface HistoryItem {
  id: string;
  sql: string;
  timestamp: Date;
  status: 'success' | 'error';
}

interface TableInfo {
  name: string;
  columns: ColumnInfo[];
}

interface ColumnInfo {
  name: string;
  data_type: string;
  is_primary: bool;
  is_indexed: bool;
}

function App() {
  const [query, setQuery] = useState<string>("SELECT * FROM products");
  const [url, setUrl] = useState<string>("http://localhost:8091/query");
  const [results, setResults] = useState<QueryResult | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [history, setHistory] = useState<HistoryItem[]>([]);
  const [activeTab, setActiveTab] = useState<'tables' | 'history'>('tables');
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());
  const [isRefreshingTables, setIsRefreshingTables] = useState(false);

  useEffect(() => {
      fetchTables();
  }, [url]);

  const fetchTables = async () => {
      setIsRefreshingTables(true);
      try {
          const baseUrl = url.replace("/query", "");
          const response = await axios.get(`${baseUrl}/tables`);
          setTables(response.data);
      } catch (e) {
          console.error("Failed to fetch tables", e);
      } finally {
          setIsRefreshingTables(false);
      }
  };

  const toggleTable = (tableName: string) => {
      const newExpanded = new Set(expandedTables);
      if (newExpanded.has(tableName)) {
          newExpanded.delete(tableName);
      } else {
          newExpanded.add(tableName);
      }
      setExpandedTables(newExpanded);
  };

  const executeQuery = async () => {
    if (!query.trim()) return;
    
    setIsLoading(true);
    
    try {
      const response = await axios.post(url, { sql: query });
      const data = response.data;
      
      setResults(data);
      
      addToHistory(query, data.error ? 'error' : 'success');
      
      // Refresh tables if it was a DDL query (rough check)
      if (query.toUpperCase().includes("CREATE") || query.toUpperCase().includes("DROP")) {
          fetchTables();
      }
    } catch (err: any) {
      setResults({
        error: err.message || "Network Error"
      });
      addToHistory(query, 'error');
    } finally {
      setIsLoading(false);
    }
  };

  const addToHistory = (sql: string, status: 'success' | 'error') => {
    setHistory(prev => [
      {
        id: Math.random().toString(36).substring(7),
        sql,
        timestamp: new Date(),
        status
      },
      ...prev.slice(0, 49) // Keep last 50
    ]);
  };

  const loadHistory = (sql: string) => {
    setQuery(sql);
  };

  // Keyboard shortcut handler
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault();
        executeQuery();
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [query, url]); // Re-bind when query changes so we execute latest

  return (
    <div className="flex h-screen bg-slate-900 text-slate-200 font-sans overflow-hidden">
      {/* Sidebar */}
      <div className="w-64 bg-slate-950 border-r border-slate-800 flex flex-col shrink-0">
        <div className="p-4 border-b border-slate-800 flex items-center gap-2 text-emerald-400">
          <Database className="w-6 h-6" />
          <h1 className="font-bold text-xl tracking-tight">w33dDB Studio</h1>
        </div>

        {/* Sidebar Tabs */}
        <div className="flex border-b border-slate-800">
             <button 
                onClick={() => setActiveTab('tables')}
                className={cn(
                    "flex-1 py-2 text-xs font-medium text-center transition-colors border-b-2",
                    activeTab === 'tables' 
                        ? "text-emerald-400 border-emerald-500 bg-slate-900/50" 
                        : "text-slate-500 border-transparent hover:text-slate-300"
                )}
             >
                 Tables
             </button>
             <button 
                onClick={() => setActiveTab('history')}
                className={cn(
                    "flex-1 py-2 text-xs font-medium text-center transition-colors border-b-2",
                    activeTab === 'history' 
                        ? "text-emerald-400 border-emerald-500 bg-slate-900/50" 
                        : "text-slate-500 border-transparent hover:text-slate-300"
                )}
             >
                 History
             </button>
        </div>

        <div className="flex-1 overflow-y-auto p-2">
          {activeTab === 'tables' ? (
              <div className="space-y-1">
                  <div className="flex items-center justify-between px-2 mb-2">
                      <span className="text-xs font-semibold text-slate-500 uppercase tracking-wider">
                          {tables.length} Tables
                      </span>
                      <button 
                        onClick={fetchTables} 
                        className={cn("p-1 rounded hover:bg-slate-800 text-slate-500 hover:text-emerald-400 transition-all", isRefreshingTables && "animate-spin")}
                        title="Refresh Tables"
                      >
                          <RefreshCw className="w-3 h-3" />
                      </button>
                  </div>
                  
                  {tables.length === 0 && !isRefreshingTables && (
                       <div className="text-slate-600 text-sm px-2 italic">No tables found.</div>
                  )}

                  {tables.map(table => (
                      <div key={table.name} className="select-none">
                          <button 
                              onClick={() => toggleTable(table.name)}
                              className="w-full text-left px-2 py-1.5 rounded hover:bg-slate-900 flex items-center gap-2 group"
                          >
                              {expandedTables.has(table.name) ? (
                                  <ChevronDown className="w-3 h-3 text-slate-500" />
                              ) : (
                                  <ChevronRight className="w-3 h-3 text-slate-500" />
                              )}
                              <span className="text-sm font-medium text-slate-300 group-hover:text-emerald-300 transition-colors">
                                  {table.name}
                              </span>
                          </button>
                          
                          {expandedTables.has(table.name) && (
                              <div className="ml-6 pl-2 border-l border-slate-800 my-1 space-y-1">
                                  {table.columns.map(col => (
                                      <div key={col.name} className="text-xs flex items-center justify-between group/col py-0.5 pr-2">
                                          <div className="flex items-center gap-1.5 text-slate-400">
                                              {col.is_primary && <Key className="w-3 h-3 text-amber-500" />}
                                              <span className={cn(col.is_primary && "text-amber-200")}>{col.name}</span>
                                          </div>
                                          <span className="text-[10px] text-slate-600 font-mono group-hover/col:text-slate-500">
                                              {col.data_type}
                                          </span>
                                      </div>
                                  ))}
                              </div>
                          )}
                      </div>
                  ))}
              </div>
          ) : (
            <div className="space-y-1">
                <div className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2 px-2 mt-2">
                    Run History
                </div>
                {history.map(item => (
                <button
                    key={item.id}
                    onClick={() => loadHistory(item.sql)}
                    className="w-full text-left p-2 rounded hover:bg-slate-900 group transition-colors text-sm border border-transparent hover:border-slate-800"
                >
                    <div className="flex items-center justify-between mb-1">
                    <span className={cn(
                        "text-xs",
                        item.status === 'success' ? "text-emerald-500" : "text-rose-500"
                    )}>
                        {item.timestamp.toLocaleTimeString()}
                    </span>
                    <Clock className="w-3 h-3 text-slate-600 opacity-0 group-hover:opacity-100" />
                    </div>
                    <div className="truncate text-slate-400 font-mono text-xs">
                    {item.sql}
                    </div>
                </button>
                ))}
                {history.length === 0 && (
                <div className="text-slate-600 text-sm px-2 italic">
                    No history yet...
                </div>
                )}
            </div>
          )}
        </div>

        <div className="p-4 border-t border-slate-800 bg-slate-950">
          <label className="text-xs text-slate-500 mb-1 block">Connection URL</label>
          <div className="flex items-center bg-slate-900 rounded border border-slate-800 px-2 py-1">
            <Settings className="w-3 h-3 text-slate-500 mr-2" />
            <input 
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              className="bg-transparent border-none text-xs text-slate-300 w-full focus:outline-none"
            />
          </div>
        </div>
      </div>

      {/* Main Content with Resizable Panels */}
      <div className="flex-1 flex flex-col min-w-0 bg-slate-900">
          <PanelGroup direction="vertical">
              {/* Editor Panel */}
              <Panel defaultSize={40} minSize={20} className="flex flex-col">
                <div className="flex flex-col h-full">
                    <div className="flex items-center justify-between px-4 py-2 bg-slate-900 border-b border-slate-800 shrink-0">
                        <div className="flex items-center gap-2">
                        <span className="text-xs font-medium text-slate-400 bg-slate-800 px-2 py-0.5 rounded">SQL</span>
                        <span className="text-xs text-slate-500">Editor (Ctrl+Enter to run)</span>
                        </div>
                        <div className="flex items-center gap-2">
                        <button 
                            onClick={() => setQuery('')}
                            className="p-1.5 text-slate-400 hover:text-slate-200 hover:bg-slate-800 rounded transition-colors"
                            title="Clear Editor"
                        >
                            <Eraser className="w-4 h-4" />
                        </button>
                        <button 
                            onClick={executeQuery}
                            disabled={isLoading}
                            className={cn(
                            "flex items-center gap-2 px-4 py-1.5 rounded text-sm font-medium transition-all",
                            isLoading 
                                ? "bg-slate-700 text-slate-400 cursor-wait" 
                                : "bg-emerald-600 hover:bg-emerald-500 text-white shadow-lg shadow-emerald-900/20"
                            )}
                        >
                            <Play className="w-4 h-4 fill-current" />
                            {isLoading ? 'Running...' : 'Run Query'}
                        </button>
                        </div>
                    </div>
                    <div className="flex-1 overflow-hidden relative bg-[#1e1e1e]"> 
                        <CodeMirror
                        value={query}
                        height="100%"
                        theme="dark"
                        extensions={[sql()]}
                        onChange={(value) => setQuery(value)}
                        className="text-base h-full"
                        />
                    </div>
                </div>
              </Panel>
              
              <PanelResizeHandle className="h-1 bg-slate-800 hover:bg-emerald-500/50 transition-colors cursor-row-resize" />
              
              {/* Results Panel */}
              <Panel minSize={20} className="flex flex-col bg-slate-900">
                <div className="flex items-center gap-4 px-4 border-b border-slate-800 shrink-0 h-10">
                    <div className="flex items-center gap-2 text-emerald-400 border-b-2 border-emerald-500 h-full px-2">
                        <TableIcon className="w-4 h-4" />
                        <span className="text-sm font-medium">Results</span>
                    </div>
                </div>

                <div className="flex-1 overflow-auto p-4">
                    {results ? (
                    results.error ? (
                        <div className="bg-rose-950/30 border border-rose-900/50 rounded-lg p-4 text-rose-200 flex items-start gap-3">
                        <AlertCircle className="w-5 h-5 text-rose-500 shrink-0 mt-0.5" />
                        <div className="font-mono text-sm whitespace-pre-wrap">{results.error}</div>
                        </div>
                    ) : (
                        results.result && results.result.length > 0 ? (
                        <div className="space-y-8">
                            {results.result.map((res, idx) => (
                            <div key={idx} className="space-y-2">
                                {results.result && results.result.length > 1 && (
                                <div className="text-xs font-semibold text-slate-500 uppercase tracking-wider">
                                    Result Set #{idx + 1}
                                </div>
                                )}
                                <div className="border border-slate-700 rounded-lg overflow-hidden flex flex-col max-h-[500px]">
                                <div className="overflow-auto">
                                    <table className="w-full text-left text-sm whitespace-nowrap relative">
                                    <thead className="bg-slate-800 text-slate-300 sticky top-0 z-10 shadow-sm">
                                        <tr>
                                        {res.columns.map((col, i) => (
                                            <th key={i} className="px-4 py-3 font-medium border-b border-slate-700 bg-slate-800">
                                            {col}
                                            </th>
                                        ))}
                                        </tr>
                                    </thead>
                                    <tbody className="divide-y divide-slate-800 bg-slate-900/50">
                                        {res.rows.map((row, rIdx) => (
                                        <tr key={rIdx} className="hover:bg-slate-800/50 transition-colors group">
                                            {row.map((cell, cIdx) => (
                                            <td key={cIdx} className="px-4 py-2 text-slate-300 border-r border-slate-800/50 last:border-r-0 font-mono text-xs group-hover:border-slate-700/50">
                                                {formatCell(cell)}
                                            </td>
                                            ))}
                                        </tr>
                                        ))}
                                        {res.rows.length === 0 && (
                                        <tr>
                                            <td colSpan={res.columns.length} className="px-4 py-8 text-center text-slate-500 italic">
                                            No rows returned
                                            </td>
                                        </tr>
                                        )}
                                    </tbody>
                                    </table>
                                </div>
                                <div className="bg-slate-800/50 px-4 py-2 border-t border-slate-800 text-xs text-slate-500 flex items-center gap-2 shrink-0">
                                    <CheckCircle2 className="w-3 h-3 text-emerald-500" />
                                    {res.rows.length} rows fetched
                                </div>
                                </div>
                            </div>
                            ))}
                        </div>
                        ) : (
                        <div className="flex flex-col items-center justify-center h-full text-slate-600">
                            <CheckCircle2 className="w-12 h-12 mb-4 opacity-20" />
                            <p>Query executed successfully (No results)</p>
                        </div>
                        )
                    )
                    ) : (
                    <div className="flex flex-col items-center justify-center h-full text-slate-600">
                        <Database className="w-16 h-16 mb-4 opacity-20" />
                        <p>Run a query to see results</p>
                    </div>
                    )}
                </div>
              </Panel>
          </PanelGroup>
      </div>
    </div>
  );
}

function formatCell(cell: any): React.ReactNode {
  if (cell === null) return <span className="text-slate-600 italic">NULL</span>;
  if (typeof cell === 'object') {
      // Handle the specific value format from backend { "Integer": 1 }
      const keys = Object.keys(cell);
      if (keys.length === 1) {
          const type = keys[0];
          const val = cell[type];
          if (type === 'String') return <span className="text-emerald-300">"{val}"</span>;
          if (type === 'Integer' || type === 'Float') return <span className="text-amber-300">{val}</span>;
          if (type === 'Boolean') return <span className="text-purple-300">{val ? 'TRUE' : 'FALSE'}</span>;
          if (val === null) return <span className="text-slate-600 italic">NULL</span>;
          return JSON.stringify(val);
      }
      return JSON.stringify(cell);
  }
  return String(cell);
}

export default App;
