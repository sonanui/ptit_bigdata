import * as echarts from "echarts";
import ReactECharts from "echarts-for-react";
import type { ComponentProps } from "react";

// Một theme cho mọi biểu đồ: cùng bảng màu, font và lưới với phần CSS (styles.css).
echarts.registerTheme("ptit", {
  color: ["#1F3A68", "#2A9D8F", "#E0A030", "#7A4E9E", "#B4232A", "#5B8DB8"],
  backgroundColor: "transparent",
  textStyle: { fontFamily: "'Be Vietnam Pro', system-ui, sans-serif", color: "#3B4660" },
  title: { textStyle: { color: "#16213B" } },
  legend: { textStyle: { color: "#3B4660" }, icon: "roundRect", itemWidth: 12, itemHeight: 8 },
  tooltip: {
    backgroundColor: "#16213B",
    borderWidth: 0,
    textStyle: { color: "#FFFFFF", fontSize: 12 },
    extraCssText: "border-radius:6px;box-shadow:0 6px 18px rgba(22,33,59,.25);",
  },
  categoryAxis: {
    axisLine: { lineStyle: { color: "#C9D1DE" } },
    axisTick: { show: false },
    axisLabel: { color: "#5A6478" },
    splitLine: { show: false },
  },
  valueAxis: {
    axisLine: { show: false },
    axisLabel: { color: "#5A6478" },
    splitLine: { lineStyle: { color: "#E4E9F0" } },
    nameTextStyle: { color: "#5A6478" },
  },
  bar: { itemStyle: { borderRadius: [0, 3, 3, 0] } },
  line: { lineStyle: { width: 2 }, symbolSize: 5 },
  dataZoom: { borderColor: "#C9D1DE", fillerColor: "rgba(31,58,104,.12)", handleStyle: { color: "#1F3A68" } },
});

/** ReactECharts với theme của ứng dụng. */
export default function Chart(props: ComponentProps<typeof ReactECharts>) {
  return <ReactECharts theme="ptit" notMerge {...props} />;
}
