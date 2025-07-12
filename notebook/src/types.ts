/**
 * Represents a single trace row/span.
 */
export type TraceRow = {
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

/**
 * Represents a node in the trace tree.
 */
export type TraceTreeNode = {
  id: string;
  name: string;
  value: number;
  startTime: number;
  endTime: number;
  children?: TraceTreeNode[];
};
