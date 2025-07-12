import { useState } from "react";
import Sidebar from "./Sidebar";
import stub from "./stub.json";
import { findRootAndTree, type normalizeRow } from "./utils";

function App() {
  const [query, setQuery] = useState("select * from traces;");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState<{
    columns: string[];
    originalRows: Record<string, unknown>[];
  } | null>(null);
  const [selectedTrace, setSelectedTrace] = useState<
    ReturnType<typeof normalizeRow>[] | null
  >(null);

  console.log(result);

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
        setResult({ columns: stub.columns, originalRows: stub.originalRows });
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
