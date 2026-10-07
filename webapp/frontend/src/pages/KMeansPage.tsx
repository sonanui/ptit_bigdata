import ReactECharts from "../chart";
import { useEffect, useState } from "react";
import { ModelEntry, num, post, Row, when } from "../api";
import { useGet } from "../hooks";
import { datasetName, featureList, PROFILE_COLUMNS } from "../labels";
import { Kpi } from "./Overview";
import ProductPicker from "./ProductPicker";

const RAW_FIELDS = [
  ["views", "Lượt xem"],
  ["carts", "Lượt thêm vào giỏ"],
  ["purchases", "Lượt mua"],
  ["medianPrice", "Giá bán (giá trung vị)"],
  ["distinctUsers", "Số người xem khác nhau"],
] as const;

/** Câu mô tả một cụm, ghép từ các số trong bảng hồ sơ cụm (không đặt tên tùy ý). */
function describe(r: Row) {
  const pct = Number(r.share_with_purchase) * 100;
  return `${num(r.products, 0)} sản phẩm; trung bình mỗi sản phẩm có ${num(r.mean_views, 0)} lượt xem và ${num(r.mean_purchases, 1)} lượt mua; `
    + `${num(pct, 1)}% sản phẩm có ít nhất một lượt mua; giá trung bình ${num(r.mean_median_price, 0)}.`;
}

export default function KMeansPage() {
  const models = useGet<ModelEntry[]>("/api/ml/kmeans/models");
  const [runId, setRunId] = useState<string | null>(null);
  useEffect(() => {
    if (!runId && models.data?.length) setRunId(models.data[0].runId);
  }, [models.data, runId]);
  const detail = useGet<any>(runId ? `/api/ml/kmeans/${runId}` : null);
  const clusters = useGet<any>(runId ? `/api/ml/kmeans/${runId}/clusters` : null);
  const [raw, setRaw] = useState<Record<string, string>>({});
  const [result, setResult] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  const predict = (body: object) => {
    setError(null);
    post<any>("/api/ml/kmeans/predict", { runId, ...body }).then(setResult).catch((e) => { setResult(null); setError(String(e.message)); });
  };
  const meta = detail.data?.metadata;
  const metrics = detail.data?.metrics;
  const profile: Row[] = clusters.data?.profile ?? [];
  return (
    <>
      <header className="page-head">
        <h1>K-Means: gom các sản phẩm có hành vi giống nhau</h1>
        <p className="lead">
          K-Means tự chia sản phẩm thành K nhóm (gọi là cụm) sao cho các sản phẩm trong cùng một cụm có lượt xem, lượt mua, giá…
          giống nhau nhất. Không ai gán sẵn nhóm cho sản phẩm; thuật toán tự tìm ra. Mô hình được huấn luyện trước bằng Spark MLlib,
          trang này chỉ hiển thị kết quả và cho thử phân cụm.
        </p>
      </header>
      {models.error && <p className="error">{models.error}</p>}
      {models.data?.length === 0 && <p className="note">Chưa có mô hình K-Means nào được xuất ra cho web.</p>}
      {models.data && models.data.length > 0 && (
        <section>
          <label>
            Mô hình (mã lần huấn luyện)
            <select value={runId ?? ""} onChange={(e) => { setRunId(e.target.value); setResult(null); }}>
              {models.data.map((m) => <option key={m.runId}>{m.runId}</option>)}
            </select>
          </label>
          {meta && (
            <div className="grid">
              <Kpi label="Dữ liệu huấn luyện" value={datasetName(meta.dataset.tag)} />
              <Kpi label="Số sản phẩm được phân cụm" value={num(meta.dataset.products, 0)} />
              <Kpi label="Số cụm (K)" value={String(meta.hyperparameters.k)} />
              <Kpi label="Độ tách cụm (silhouette)" value={num(meta.metrics.silhouette, 4)} />
              <Kpi label="Độ gọn của cụm (inertia)" value={num(meta.metrics.inertia, 0)} />
              <Kpi label="Huấn luyện lúc" value={when(meta.training_time)} />
            </div>
          )}
          {meta && (
            <>
              <p className="note">
                Cách đọc: <b>silhouette</b> từ −1 đến 1, càng gần 1 thì các cụm càng tách bạch; <b>inertia</b> là tổng khoảng cách
                từ mỗi sản phẩm tới tâm cụm của nó, càng nhỏ thì cụm càng gọn (chỉ so được giữa các K trên cùng dữ liệu).
              </p>
              <p className="note">
                Mỗi sản phẩm được mô tả bằng: {featureList(meta.features)}. Các số đếm được lấy logarit để bớt chênh lệch giữa sản phẩm
                rất nổi tiếng và sản phẩm ít người xem, rồi đưa về cùng thang đo trước khi phân cụm. Chỉ dùng sản phẩm có từ {meta.dataset.minViews ?? 20} lượt xem trở lên.
              </p>
            </>
          )}
        </section>
      )}
      {metrics?.sweep && (
        <section>
          <h2>Vì sao chọn K = {metrics.k}?</h2>
          <p className="note">
            Nhóm thử K từ {metrics.sweep[0]?.k} đến {metrics.sweep[metrics.sweep.length - 1]?.k}, mỗi K chạy 3 lần với điểm khởi đầu khác nhau,
            rồi chọn K có độ tách cụm (silhouette) trung bình cao nhất. Quy tắc này được cố định trước khi chạy.
          </p>
          <ReactECharts
            style={{ height: 300 }}
            option={{
              tooltip: { trigger: "axis" },
              legend: {},
              xAxis: { type: "category", name: "K", data: metrics.sweep.map((r: any) => r.k) },
              yAxis: [{ type: "value", name: "silhouette" }, { type: "value", name: "inertia", axisLabel: { formatter: (v: number) => num(v, 0) } }],
              series: [
                { name: "Độ tách cụm (silhouette, cao là tốt)", type: "line", data: metrics.sweep.map((r: any) => r.silhouette_mean) },
                { name: "Độ gọn (inertia, thấp là tốt)", type: "line", yAxisIndex: 1, data: metrics.sweep.map((r: any) => r.inertia_mean) },
              ],
            }}
          />
        </section>
      )}
      {clusters.data && (
        <section>
          <h2>Các cụm khác nhau thế nào?</h2>
          <ul className="cluster-list">
            {profile.map((r) => (
              <li key={r.cluster}><b>Cụm {r.cluster}:</b> {describe(r)}</li>
            ))}
          </ul>
          <ReactECharts
            style={{ height: 220 }}
            option={{
              tooltip: {},
              xAxis: { type: "category", data: Object.keys(clusters.data.sizes).map((c) => `Cụm ${c}`) },
              yAxis: { type: "value", name: "số sản phẩm", axisLabel: { formatter: (v: number) => num(v, 0) } },
              series: [{ type: "bar", barWidth: 56, itemStyle: { borderRadius: [3, 3, 0, 0] }, data: Object.values(clusters.data.sizes) }],
            }}
          />
          <h3>Số liệu chi tiết từng cụm (TB = trung bình)</h3>
          <div className="table-wrap">
            <table>
              <thead><tr>{Object.keys(profile[0] ?? {}).map((c) => <th key={c} className={c === "top_category_roots" ? "" : "num"}>{PROFILE_COLUMNS[c] ?? c}</th>)}</tr></thead>
              <tbody>
                {profile.map((r, i) => (
                  <tr key={i}>{Object.entries(r).map(([c, v]) => <td key={c} className={c === "top_category_roots" ? "wrap" : "num"}>{c === "top_category_roots" ? v.replace(/__UNKNOWN__/g, "(chưa phân loại)") : num(v, 4)}</td>)}</tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="note">
            Ngành hàng không được dùng khi phân cụm, chỉ dùng để mô tả sau. Cụm cho biết những sản phẩm nào giống nhau, không giải thích
            vì sao (tương quan, không phải nguyên nhân).
          </p>
        </section>
      )}
      {runId && (
        <section>
          <h2>Thử phân cụm một sản phẩm</h2>
          <p className="note">Máy chủ web dùng mô hình đã huấn luyện để tính sản phẩm gần tâm cụm nào nhất; không huấn luyện lại.</p>
          <h3>Cách 1: chọn một sản phẩm có trong dữ liệu</h3>
          <ProductPicker type="kmeans" runId={runId} onPick={(p) => predict({ productId: p.product_id })} />
          <h3>Cách 2: tự nhập số liệu của một sản phẩm (trong cả tháng)</h3>
          <form onSubmit={(e) => {
            e.preventDefault();
            predict({ raw: Object.fromEntries(RAW_FIELDS.map(([k]) => [k, Number(raw[k] ?? 0)])) });
          }}>
            {RAW_FIELDS.map(([k, label]) => (
              <label key={k}>{label}<input type="number" min="0" step="any" required value={raw[k] ?? ""} onChange={(e) => setRaw({ ...raw, [k]: e.target.value })} /></label>
            ))}
            <button type="submit">Xem thuộc cụm nào</button>
          </form>
          {error && <p className="error">{error}</p>}
          {result && (
            <div className="result">
              <p className="result-head">
                Sản phẩm thuộc <span className="badge ok">cụm {result.cluster}</span>
                {result.sparkCluster !== undefined && (
                  <> {result.matchesSpark
                    ? <span className="badge ok">trùng với kết quả Spark đã tính (cụm {result.sparkCluster})</span>
                    : <span className="badge bad">khác kết quả Spark (cụm {result.sparkCluster})</span>}</>
                )}
              </p>
              {profile.find((r) => Number(r.cluster) === result.cluster) && (
                <p className="note">Cụm {result.cluster}: {describe(profile.find((r) => Number(r.cluster) === result.cluster)!)}</p>
              )}
              {result.outOfDomain && <p className="warn">Kết quả chỉ để tham khảo: {result.warnings.join("; ")}.</p>}
              <ReactECharts
                style={{ height: 200 }}
                option={{
                  tooltip: {},
                  xAxis: { type: "category", data: result.squaredDistances.map((_: number, i: number) => `Cụm ${i}`) },
                  yAxis: { type: "value", name: "khoảng cách tới tâm cụm" },
                  series: [{ type: "bar", barWidth: 56, data: result.squaredDistances }],
                }}
              />
              <p className="note">Sản phẩm được xếp vào cụm có cột thấp nhất (gần tâm cụm nhất).</p>
            </div>
          )}
        </section>
      )}
    </>
  );
}
