// Thiết lập chung cho Vitest (jsdom). Dữ liệu giả CHỈ tồn tại trong test; bản chạy thật lấy mọi số liệu từ API.
import { afterEach, vi } from "vitest";
import { cleanup } from "@testing-library/react";

// jsdom không có canvas: thay biểu đồ ECharts bằng phần tử rỗng, test chỉ kiểm tra dữ liệu và luồng gọi API.
vi.mock("echarts-for-react", () => ({ default: () => null }));

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});
