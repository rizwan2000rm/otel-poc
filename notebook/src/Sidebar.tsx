import React from "react";

type TraceRow = {
  id: string;
  parentId: string | null;
  name: string;
  startTime: number;
  endTime: number;
  traceId?: string;
  kind?: number;
  status_code?: number;
  status_message?: string;
  attributes?: object;
  resource?: object;
};

type SidebarProps = {
  traceRows: TraceRow[];
  onClose: () => void;
};

function msBetween(start: number, end: number) {
  return Math.max(0, end - start);
}

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

function getTraceTree(
  rows: TraceRow[],
  parentId: string | null
): (TraceRow & { children: TraceRow[] })[] {
  return rows
    .filter((row) => row.parentId === parentId)
    .map((row) => ({
      ...row,
      children: getTraceTree(rows, row.id),
    }));
}

function flattenTraceTree(
  tree: (TraceRow & { children: TraceRow[] })[],
  depth = 0,
  parentStart = 0
): (TraceRow & { depth: number; parentStart: number })[] {
  let result: (TraceRow & { depth: number; parentStart: number })[] = [];
  for (const node of tree) {
    result.push({ ...node, depth, parentStart });
    if (node.children && node.children.length > 0) {
      result = result.concat(
        flattenTraceTree(node.children, depth + 1, node.startTime)
      );
    }
  }
  return result;
}

function WaterfallChart({ traceRows }: { traceRows: TraceRow[] }) {
  console.log(traceRows);
  if (!traceRows || traceRows.length === 0) return null;
  // Find root(s)
  const tree = getTraceTree(traceRows, null);
  const flat = flattenTraceTree(tree);
  const startTimes = flat.map((s) => Number(s.startTime));
  const endTimes = flat.map((s) => Number(s.endTime));
  const minStart = Math.min(...startTimes);
  const maxEnd = Math.max(...endTimes);
  const totalMs = maxEnd - minStart;
  if (!isFinite(minStart) || !isFinite(maxEnd) || totalMs <= 0) {
    return <div className="p-4 text-red-600">Invalid trace timing data.</div>;
  }
  return (
    <div className="p-4">
      <div className="mb-2 font-bold">Waterfall Chart</div>
      <div
        className="relative border-l border-b border-gray-300"
        style={{ height: flat.length * 32 + 20 }}
      >
        {/* Axis */}
        <div
          className="absolute left-0 top-0 w-full flex justify-between text-xs text-gray-500"
          style={{ pointerEvents: "none" }}
        >
          <span>0ms</span>
          <span>{totalMs}ms</span>
        </div>
        {/* Bars */}
        {flat.map((span, i) => {
          const left = ((Number(span.startTime) - minStart) / totalMs) * 100;
          const width =
            (msBetween(Number(span.startTime), Number(span.endTime)) /
              totalMs) *
            100;
          return (
            <div
              key={span.id}
              className="flex items-center h-8"
              style={{
                position: "absolute",
                top: i * 32 + 20,
                left: 0,
                width: "100%",
              }}
            >
              <div
                style={{ marginLeft: span.depth * 16, width: `${left}%` }}
              ></div>
              <div
                className="bg-blue-400 h-5 rounded shadow text-white text-xs flex items-center px-2"
                style={{ width: `${width}%`, minWidth: 30 }}
                title={`[${span.name}] ${span.startTime} - ${span.endTime}`}
              >
                {span.name}
              </div>
              <span className="ml-2 text-xs text-gray-600">
                {msBetween(Number(span.startTime), Number(span.endTime))}ms
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

const Sidebar: React.FC<SidebarProps> = ({ traceRows, onClose }) => {
  return (
    <div className="fixed top-0 right-0 w-[500px] h-full bg-white shadow-lg z-50 border-l border-gray-200 flex flex-col">
      <button
        className="self-end m-4 px-3 py-1 bg-gray-200 rounded hover:bg-gray-300"
        onClick={onClose}
      >
        Close
      </button>
      <div className="flex-1 overflow-y-auto">
        <WaterfallChart traceRows={traceRows} />
      </div>
    </div>
  );
};

export default Sidebar;
