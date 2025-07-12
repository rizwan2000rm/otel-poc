import React from "react";
import type { TraceRow } from "./types";
import WaterfallChart from "./WaterfallChart";

const Sidebar: React.FC<{ traceRows: TraceRow[]; onClose: () => void }> = ({
  traceRows,
  onClose,
}) => {
  // Find the root span (first span with no parent)
  const rootSpan = traceRows.find(
    (row) => !row.parentId || row.parentId === "" || row.parentId === null
  );
  const rootDuration = rootSpan
    ? Math.round(rootSpan.endTime - rootSpan.startTime)
    : null;

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/30 z-40 duration-200"
        onClick={onClose}
        aria-label="Close backdrop"
      />
      {/* Modal */}
      <div
        className="fixed top-0 right-0 h-full w-[85vw] max-w-6xl bg-white shadow-lg z-50 border-l border-gray-200 flex flex-col"
        style={{ transition: "width 0.2s" }}
      >
        <div className="flex items-center justify-between">
          <span className="ml-6 text-lg font-semibold text-gray-800 truncate max-w-[60%]">
            {rootSpan ? `${rootSpan.name} (${rootDuration} ms)` : ""}
          </span>
          <button
            className="m-4 px-3 py-1 bg-gray-200 rounded hover:bg-gray-300"
            onClick={onClose}
          >
            Close
          </button>
        </div>
        <div className="flex-1 overflow-y-auto">
          <WaterfallChart traceRows={traceRows} />
        </div>
      </div>
    </>
  );
};

export default Sidebar;
