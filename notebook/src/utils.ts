import type { TraceRow, TraceTreeNode } from "./types";
import { PALETTE } from "./consts";

/**
 * Builds a tree from flat trace rows.
 */
export const buildTraceTree = (
  rows: TraceRow[],
  parentId: string | null
): TraceTreeNode[] => {
  return rows
    .filter((r) => (r.parentId ?? null) === parentId)
    .map((r) => {
      const children = buildTraceTree(rows, r.id);
      return {
        id: r.id,
        name: r.name,
        value: r.endTime - r.startTime,
        startTime: r.startTime,
        endTime: r.endTime,
        children: children.length > 0 ? children : undefined,
      };
    });
};

/**
 * Recursively transforms a trace tree to flame graph data for ECharts.
 */
export const treeToFlameData = (
  node: TraceTreeNode,
  rootValue: number,
  colorIndex = { idx: 0 },
  level = 0,
  start = 0
): any[] => {
  const label = node.name;
  const color = PALETTE[colorIndex.idx % PALETTE.length];
  const data = [
    {
      name: node.id,
      value: [
        level,
        start,
        start + node.value,
        label,
        (node.value / rootValue) * 100,
      ],
      itemStyle: { color },
    },
  ];
  colorIndex.idx++;
  let childStart = start;
  for (const child of node.children || []) {
    data.push(
      ...treeToFlameData(child, rootValue, colorIndex, level + 1, childStart)
    );
    childStart += child.value;
  }
  return data;
};

/**
 * Gets the maximum depth of a trace tree.
 */
export const getTreeMaxDepth = (node: TraceTreeNode, level = 0): number => {
  if (!node.children || node.children.length === 0) return level;
  return Math.max(
    ...node.children.map((child) => getTreeMaxDepth(child, level + 1))
  );
};

/**
 * Parses a timestamp string into a number.
 */
const parseTimestamp = (ts: string): number => {
  // Example: "2025-07-12 12:30:32.892143 +0000 UTC"
  const match = ts.match(
    /^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})\.(\d{3})(\d{3}) \+0000 UTC$/
  );
  if (match) {
    const [, date, time, ms, us] = match;
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
};

export const normalizeRow = (row: Record<string, unknown>) => {
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
};

/**
 * Finds the root span and builds a tree from a clicked row.
 */
export const findRootAndTree = (
  rows: Record<string, unknown>[],
  clickedRow: Record<string, unknown>
) => {
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
};
