import "@fontsource/be-vietnam-pro/400.css";
import "@fontsource/be-vietnam-pro/500.css";
import "@fontsource/be-vietnam-pro/600.css";
import "@fontsource/be-vietnam-pro/700.css";
import "@fontsource/jetbrains-mono/400.css";
import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter, NavLink, Route, Routes } from "react-router-dom";
import "./styles.css";
import Overview from "./pages/Overview";
import GroupBy from "./pages/GroupBy";
import Benchmarks from "./pages/Benchmarks";
import KMeansPage from "./pages/KMeansPage";
import KnnPage from "./pages/KnnPage";
import { RunProvider, useRun } from "./run";
import { datasetName } from "./labels";

// Thứ tự trạm = thứ tự dữ liệu đi qua pipeline: tổng quan luồng → Group By → so sánh engine → hai mô hình ML.
const STATIONS = [
  { to: "/", title: "Tổng quan", hint: "Dữ liệu đi qua những bước nào" },
  { to: "/groupby", title: "Group By", hint: "Doanh thu, hành vi mua theo danh mục" },
  { to: "/benchmarks", title: "MapReduce và Spark", hint: "Có khớp không, chạy nhanh chậm ra sao" },
  { to: "/ml/kmeans", title: "K-Means", hint: "Gom sản phẩm giống nhau" },
  { to: "/ml/knn", title: "KNN", hint: "Đoán sản phẩm có được mua không" },
];

function RunPicker() {
  const { runId, runs, setRunId } = useRun();
  if (!runs.length) return null;
  const current = runs.find((r) => r.runId === runId);
  return (
    <div className="rail-run">
      <label>
        Bộ kết quả đang xem
        <select value={runId ?? ""} onChange={(e) => setRunId(e.target.value)}>
          {runs.map((r) => (
            <option key={r.runId} value={r.runId}>
              {r.runId}
              {r.default ? " (mặc định)" : ""}
            </option>
          ))}
        </select>
      </label>
      {current && <p>{datasetName(current.dataset?.tag)}, xuất lúc {current.createdAt?.slice(0, 16).replace("T", " ")} (giờ UTC)</p>}
    </div>
  );
}

function App() {
  return (
    <RunProvider>
      <div className="shell">
        <aside className="rail">
          <div className="brand">
            <span className="brand-mark" aria-hidden="true">P</span>
            <div>
              <strong>PTIT Big Data</strong>
              <span className="brand-sub">Bài 21: Group By Aggregation</span>
            </div>
          </div>
          <nav aria-label="Các bước của pipeline">
            <ol className="stations">
              {STATIONS.map((s, i) => (
                <li key={s.to}>
                  <NavLink to={s.to} end={s.to === "/"}>
                    <span className="station-no">{i + 1}</span>
                    <span className="station-text">
                      <span className="station-title">{s.title}</span>
                      <span className="station-hint">{s.hint}</span>
                    </span>
                  </NavLink>
                </li>
              ))}
            </ol>
          </nav>
          <RunPicker />
        </aside>
        <main>
          <Routes>
            <Route path="/" element={<Overview />} />
            <Route path="/groupby" element={<GroupBy />} />
            <Route path="/benchmarks" element={<Benchmarks />} />
            <Route path="/ml/kmeans" element={<KMeansPage />} />
            <Route path="/ml/knn" element={<KnnPage />} />
          </Routes>
        </main>
      </div>
    </RunProvider>
  );
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </React.StrictMode>,
);
