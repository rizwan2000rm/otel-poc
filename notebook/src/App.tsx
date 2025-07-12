import React, { useState } from "react";
import Sidebar from "./Sidebar";

function parseTimestamp(ts: string): number {
  // Example: "2025-07-12 12:30:32.892143 +0000 UTC"
  const match = ts.match(
    /^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})\.(\d{3})(\d{3}) \+0000 UTC$/
  );
  if (match) {
    const [_, date, time, ms, us] = match;
    // Parse as UTC
    const base = Date.parse(`${date}T${time}.${ms}Z`);
    if (!isNaN(base)) {
      // Add microseconds as a fraction of a millisecond
      return base + Number(us) / 1000;
    }
  }
  // Fallback: try Date.parse
  let ms = Date.parse(ts);
  if (!isNaN(ms)) return ms;
  // Fallback: try cleaning
  const cleaned = ts.replace(" +0000 UTC", "").replace(" ", "T") + "Z";
  ms = Date.parse(cleaned);
  if (!isNaN(ms)) return ms;
  return 0;
}

function normalizeRow(row: Record<string, unknown>) {
  return {
    id: String(row["span_id"]),
    parentId: row["parent_span_id"] ? String(row["parent_span_id"]) : null,
    name: String(row["name"]),
    startTime: parseTimestamp(String(row["start_time_unix_nano"])),
    endTime: parseTimestamp(String(row["end_time_unix_nano"])),
    traceId: row["trace_id"] ? String(row["trace_id"]) : undefined,
    kind: typeof row["kind"] === "number" ? (row["kind"] as number) : undefined,
    status_code:
      typeof row["status_code"] === "number"
        ? (row["status_code"] as number)
        : undefined,
    status_message: row["status_message"]
      ? String(row["status_message"])
      : undefined,
    attributes: row["attributes"] as object,
    resource: row["resource"] as object,
  };
}

function findRootAndTree(
  rows: Record<string, unknown>[],
  clickedRow: Record<string, unknown>
) {
  // Normalize all rows for tree logic
  const normRows = rows.map(normalizeRow);
  const id = String(clickedRow["span_id"]);
  let current = normRows.find((r) => r.id === id);
  if (!current) return [];
  const idToRow: Record<string, typeof current> = {};
  normRows.forEach((r) => {
    idToRow[r.id] = r;
  });
  while (current.parentId && idToRow[current.parentId]) {
    current = idToRow[current.parentId];
  }
  const collect = (
    parentId: string | null,
    visited = new Set<string>()
  ): typeof normRows => {
    if (!parentId || visited.has(parentId)) return [];
    visited.add(parentId);
    return normRows
      .filter((r) => r.parentId === parentId)
      .flatMap((r) => [r, ...collect(r.id, visited)]);
  };
  return [current, ...collect(current.id)];
}

function App() {
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState<{
    columns: string[];
    originalRows: Record<string, unknown>[];
  } | null>(null);
  const [selectedTrace, setSelectedTrace] = useState<
    ReturnType<typeof normalizeRow>[] | null
  >(null);

  const runQuery = async () => {
    setLoading(true);
    setError("");
    setResult(null);
    try {
      const res = await fetch("http://localhost:8080/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query }),
      });
      const data = await res.json();
      if (!res.ok || data.error) {
        setError(data.error || "Query failed");
      } else {
        setResult({ columns: data.columns, originalRows: data.rows });
      }
    } catch (e) {
      console.error(e);
      setError("Network or server error");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex flex-col min-h-screen bg-gray-50 m-4">
      {/* Side Panel for Waterfall Chart */}
      {selectedTrace && (
        <Sidebar
          traceRows={selectedTrace}
          onClose={() => setSelectedTrace(null)}
        />
      )}
      {/* Query Database */}
      <div className="flex flex-col justify-center">
        <label htmlFor="query" className="text-base text-gray-700 mb-2">
          Query
        </label>
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="SELECT * FROM TRACES;"
          className="px-4 py-2 w-full border border-gray-300 rounded shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-400 mb-4"
        />
        <button
          onClick={runQuery}
          className="px-1 cursor-pointer py-0.5 w-[90px] bg-blue-500 text-white rounded shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
          disabled={loading}
        >
          {loading ? "Running..." : "Run"}
        </button>
        {error && <div className="text-red-600 mt-2">{error}</div>}
      </div>
      {/* Traces Table */}
      <div className="mt-4">
        {result && result.columns.length > 0 && (
          <div className="overflow-x-auto">
            <table className="min-w-full border border-gray-300 bg-white">
              <thead>
                <tr>
                  {result.columns.map((col) => (
                    <th
                      key={col}
                      className="px-2 py-1 border-b border-gray-200 text-left bg-gray-100"
                    >
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {result.originalRows.map((row, i) => (
                  <tr
                    key={i}
                    className="hover:bg-blue-50 cursor-pointer"
                    onClick={() => {
                      const traceRows = findRootAndTree(
                        result.originalRows,
                        row
                      );
                      if (traceRows.length > 0) {
                        setSelectedTrace(traceRows);
                      }
                    }}
                  >
                    {result.columns.map((col) => (
                      <td
                        key={col}
                        className="px-2 py-1 border-b border-gray-100 text-xs"
                      >
                        {typeof row[col] === "object" && row[col] !== null ? (
                          <pre className="whitespace-pre-wrap">
                            {JSON.stringify(row[col], null, 2)}
                          </pre>
                        ) : (
                          String(row[col] ?? "")
                        )}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {result && result.columns.length === 0 && <div>No data found.</div>}
      </div>
    </div>
  );
}

export default App;
