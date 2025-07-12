import ReactECharts from "echarts-for-react";
import type { TraceRow } from "./types";
import { buildTraceTree, treeToFlameData, getTreeMaxDepth } from "./utils";

const WaterfallChart = ({ traceRows }: { traceRows: TraceRow[] }) => {
  if (!traceRows || traceRows.length === 0) return null;

  // Build tree and get root
  const treeRoots = buildTraceTree(traceRows, null);
  if (treeRoots.length === 0) return null;
  const treeData = treeRoots[0];
  const rootValue = treeData.value;

  // Prepare flame graph data
  const flameData = treeToFlameData(treeData, rootValue);
  const maxLevel = getTreeMaxDepth(treeData);

  // Custom renderItem for flame graph
  const renderItem = (_: any, api: any) => {
    const level = api.value(0);
    const start = api.coord([api.value(1), level]);
    const end = api.coord([api.value(2), level]);
    const height = ((api.size && api.size([0, 1])) || [0, 20])[1];
    const width = end[0] - start[0];
    return {
      type: "rect",
      transition: ["shape"],
      shape: {
        x: start[0],
        y: start[1] - height / 2,
        width,
        height: height - 2,
        r: 2,
      },
      style: {
        fill: api.visual("color"),
      },
      emphasis: {
        style: {
          stroke: "#000",
        },
      },
      textConfig: {
        position: "insideLeft",
      },
      textContent: {
        style: {
          text: api.value(3),
          fontFamily: "Verdana",
          fill: "#fff",
          fontWeight: "bold",
          width: width - 4,
          overflow: "truncate",
          ellipsis: "..",
          truncateMinChar: 1,
        },
        emphasis: {
          style: {
            stroke: "#fff",
            lineWidth: 0.5,
            fontWeight: "bold",
          },
        },
      },
    };
  };

  // ECharts option
  const option = {
    tooltip: {
      formatter: (params: any) => {
        const samples = params.value[2] - params.value[1];
        return `${params.marker} ${
          params.value[3]
        }: (${samples} ms, ${+params.value[4].toFixed(2)}%)`;
      },
    },
    toolbox: {
      feature: {
        restore: {},
      },
      right: 20,
      top: 10,
    },
    xAxis: {
      show: false,
    },
    yAxis: {
      show: false,
      max: maxLevel,
      inverse: true, // root at top
    },
    series: [
      {
        type: "custom",
        renderItem,
        encode: {
          x: [0, 1, 2],
          y: 0,
        },
        data: flameData,
      },
    ],
  };

  return (
    <div className="flex flex-col h-full">
      <ReactECharts
        option={option}
        style={{ flex: 1, height: "100%", width: "100%" }}
      />
    </div>
  );
};

export default WaterfallChart;
