import ReactECharts from "../chart";
import { useEffect, useState } from "react";
import { ModelEntry, num, post, when } from "../api";
import { useGet } from "../hooks";
import { datasetName, featureList, modelRowName } from "../labels";
import { Kpi } from "./Overview";
import ProductPicker from "./ProductPicker";

const RAW_FIELDS = [
  ["views", "Lượt xem trong 14 ngày"],
  ["carts", "Lượt thêm vào giỏ"],
  ["purchases", "Lượt mua"],
  ["medianPrice", "Giá bán (giá trung vị)"],
  ["distinctUsers", "Số người xem khác nhau"],
  ["recentViews", "Lượt xem trong 7 ngày gần nhất"],
] as const;

const METRIC_COLUMNS: [string, string][] = [["precision", "Precision"], ["recall", "Recall"], ["f1", "F1"], ["pr_auc", "PR-AUC"],
  ["balanced_accuracy", "Balanced acc."], ["accuracy", "Accuracy"]];
const CI_LABELS: Record<string, string> = {
  knn_f1: "F1 của KNN",
  knn_pr_auc: "PR-AUC của KNN",
  rule_f1: "F1 của mốc so sánh 2 (đã từng có lượt mua)",
  knn_minus_rule_f1: "F1 của KNN cao hơn mốc so sánh 2",
};
const yesNo = (v: number) => (v ? "Có" : "Không");
const viDate = (iso?: string) => (iso ? iso.split("-").reverse().join("/") : "—");

export default function KnnPage() {
  const models = useGet<ModelEntry[]>("/api/ml/knn/models");
  const [runId, setRunId] = useState<string | null>(null);
  useEffect(() => {
    if (!runId && models.data?.length) setRunId(models.data[0].runId);
  }, [models.data, runId]);
  const detail = useGet<any>(runId ? `/api/ml/knn/${runId}` : null);
  const [raw, setRaw] = useState<Record<string, string>>({});
  const [result, setResult] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);
  const predict = (body: object) => {
    setError(null);
    post<any>("/api/ml/knn/predict", { runId, ...body }).then(setResult).catch((e) => { setResult(null); setError(String(e.message)); });
  };
  const meta = detail.data?.metadata;
  const metrics = detail.data?.metrics;
  const knnRow = metrics && Object.entries(metrics.test as Record<string, any>).find(([name]) => name.startsWith("KNN"));
  const ds = meta?.dataset ?? {};
  const bought = result?.neighbours?.filter((n: any) => n.label === 1).length ?? 0;
  return (
    <>
      <header className="page-head">
        <h1>KNN: sản phẩm có được mua trong {ds.labelDays ?? 7} ngày tới?</h1>
        <p className="lead">
          Nhìn vào hành vi của một sản phẩm trong {ds.featureDays ?? 14} ngày gần đây (lượt xem, thêm giỏ, mua, giá…), mô hình đoán xem
          sản phẩm đó có ít nhất một lượt mua trong {ds.labelDays ?? 7} ngày tiếp theo hay không. Cách đoán của KNN: tìm K sản phẩm
          trong quá khứ giống nó nhất, xem bao nhiêu trong số đó đã được mua, nếu đủ nhiều thì đoán “có”.
        </p>
      </header>
      {models.error && <p className="error">{models.error}</p>}
      {models.data?.length === 0 && <p className="note">Chưa có mô hình KNN nào được xuất ra cho web.</p>}
      {models.data && models.data.length > 0 && (
        <section>
          <label>
            Mô hình (mã lần huấn luyện)
            <select value={runId ?? ""} onChange={(e) => { setRunId(e.target.value); setResult(null); }}>
              {models.data.map((m) => <option key={m.runId}>{m.runId}</option>)}
            </select>
          </label>
          {meta && (
            <>
              <div className="grid">
                <Kpi label="Dữ liệu" value={datasetName(ds.tag)} />
                <Kpi label="Số sản phẩm giống nhất được xét (K)" value={String(meta.hyperparameters.k)} />
                <Kpi label="Đoán “có” khi tỷ lệ sản phẩm giống đã được mua ≥" value={num(meta.hyperparameters.threshold * 100, 1) + "%"} />
                <Kpi label="F1 trên dữ liệu kiểm tra" value={num(meta.metrics.test_f1, 4)} />
                <Kpi label="PR-AUC trên dữ liệu kiểm tra" value={num(meta.metrics.test_pr_auc, 4)} />
                <Kpi label="Huấn luyện lúc" value={when(meta.training_time)} />
              </div>
              <h3>Đáp án (nhãn) được tạo thế nào?</h3>
              <p className="note">
                Bộ dữ liệu gốc không có sẵn câu trả lời “sản phẩm có được mua không”, nên nhóm tự tạo từ dữ liệu thật theo một mốc ngày:
                hành vi <b>trước</b> mốc là dữ liệu đầu vào, việc có lượt mua trong {ds.labelDays ?? 7} ngày <b>sau</b> mốc là đáp án.
                Mô hình học ở mốc {viDate(ds.trainT0)} và được kiểm tra ở mốc {viDate(ds.testT0)}, nên lúc học nó không thấy dữ liệu dùng để
                chấm điểm.
              </p>
              <p className="note">
                Thông tin dùng để so sánh hai sản phẩm: {featureList(meta.features)} (đưa về cùng thang đo trước khi tính khoảng cách).
                Spark MLlib không có sẵn KNN để phân loại: Spark chuẩn bị dữ liệu, còn việc tìm sản phẩm giống nhất do code riêng thực hiện
                (Python khi đánh giá, Java khi dự đoán trên web).
              </p>
            </>
          )}
        </section>
      )}
      {metrics && (
        <section>
          <h2>Mô hình đoán đúng đến đâu?</h2>
          <p className="note">
            Chấm trên dữ liệu kiểm tra, chỉ chấm một lần sau khi đã chọn xong K và ngưỡng. Để biết mô hình có ích không, so với hai cách đoán
            đơn giản (mốc so sánh) và với Logistic Regression.
          </p>
          <ul className="glossary">
            <li><b>Precision</b>: trong các sản phẩm mô hình đoán “có mua”, bao nhiêu phần đúng.</li>
            <li><b>Recall</b>: trong các sản phẩm thật sự được mua, mô hình tìm ra được bao nhiêu phần.</li>
            <li><b>F1</b>: điểm chung của Precision và Recall (0 đến 1, càng cao càng tốt). Đây là chỉ số chính.</li>
            <li><b>PR-AUC</b>: chất lượng xếp hạng “khả năng được mua” ở mọi ngưỡng (0 đến 1, càng cao càng tốt).</li>
            <li><b>Accuracy</b> (tỷ lệ đoán đúng) dễ gây hiểu lầm ở đây: phần lớn sản phẩm không được mua, nên luôn đoán “không” vẫn được accuracy cao.</li>
          </ul>
          <div className="table-wrap">
            <table>
              <thead><tr><th>Cách đoán</th>{METRIC_COLUMNS.map(([c, label]) => <th key={c} className="num">{label}</th>)}</tr></thead>
              <tbody>
                {Object.entries(metrics.test as Record<string, any>).map(([name, m]) => (
                  <tr key={name} className={name.startsWith("KNN") ? "highlight" : ""}><td>{modelRowName(name)}</td>{METRIC_COLUMNS.map(([c]) => <td key={c} className="num">{num(m[c], 4)}</td>)}</tr>
                ))}
              </tbody>
            </table>
          </div>
          {knnRow && (
            <>
              <h3>KNN đoán đúng và sai bao nhiêu sản phẩm</h3>
              <table className="confusion">
                <thead><tr><th></th><th className="num">Đoán “không mua”</th><th className="num">Đoán “có mua”</th></tr></thead>
                <tbody>
                  <tr><th>Thực tế không được mua</th><td className="num">{num(knnRow[1].tn, 0)} (đúng)</td><td className="num">{num(knnRow[1].fp, 0)} (sai)</td></tr>
                  <tr><th>Thực tế có được mua</th><td className="num">{num(knnRow[1].fn, 0)} (sai)</td><td className="num">{num(knnRow[1].tp, 0)} (đúng)</td></tr>
                </tbody>
              </table>
            </>
          )}
          <h3>Kết quả có ổn định không? (khoảng tin cậy 95%)</h3>
          <p className="note">Lấy mẫu lại dữ liệu kiểm tra 1 000 lần, tính lại chỉ số mỗi lần; 95% số lần chỉ số nằm trong khoảng dưới đây.</p>
          <div className="table-wrap" style={{ maxWidth: 640 }}>
            <table>
              <thead><tr><th>Chỉ số</th><th className="num">Từ</th><th className="num">Đến</th></tr></thead>
              <tbody>
                {Object.entries(metrics.testBootstrapCI95 as Record<string, number[]>).map(([k, [lo, hi]]) => (
                  <tr key={k}><td>{CI_LABELS[k] ?? k}</td><td className="num">{num(lo, 4)}</td><td className="num">{num(hi, 4)}</td></tr>
                ))}
              </tbody>
            </table>
          </div>
          <h3>Chọn K như thế nào?</h3>
          <p className="note">
            Thử K = {metrics.validationSweep.map((r: any) => r.k).join(", ")} trên một phần dữ liệu huấn luyện được để riêng ra (validation),
            chọn K có F1 cao nhất.
          </p>
          <ReactECharts
            style={{ height: 260 }}
            option={{
              tooltip: { trigger: "axis" },
              legend: {},
              xAxis: { type: "category", name: "K", data: metrics.validationSweep.map((r: any) => r.k) },
              yAxis: { type: "value", min: 0, max: 1 },
              series: [["f1", "F1"], ["precision", "Precision"], ["recall", "Recall"], ["pr_auc", "PR-AUC"]]
                .map(([m, label]) => ({ name: label, type: "line", data: metrics.validationSweep.map((r: any) => r[m]) })),
            }}
          />
        </section>
      )}
      {runId && (
        <section>
          <h2>Thử dự đoán một sản phẩm</h2>
          <p className="note">Máy chủ web tìm các sản phẩm giống nhất trong dữ liệu huấn luyện đã lưu và đếm xem bao nhiêu sản phẩm đã được mua.</p>
          <h3>Cách 1: chọn một sản phẩm trong dữ liệu kiểm tra (biết được đáp án thật)</h3>
          <ProductPicker type="knn" runId={runId} onPick={(p) => predict({ productId: p.product_id })} />
          <h3>Cách 2: tự nhập hành vi của một sản phẩm</h3>
          <form onSubmit={(e) => {
            e.preventDefault();
            predict({ raw: Object.fromEntries(RAW_FIELDS.map(([k]) => [k, Number(raw[k] ?? 0)])) });
          }}>
            {RAW_FIELDS.map(([k, label]) => (
              <label key={k}>{label}<input type="number" min="0" step="any" required value={raw[k] ?? ""} onChange={(e) => setRaw({ ...raw, [k]: e.target.value })} /></label>
            ))}
            <button type="submit">Dự đoán</button>
          </form>
          {error && <p className="error">{error}</p>}
          {result && (
            <div className="result">
              <p className="result-head">
                Dự đoán: <span className={`badge ${result.label ? "ok" : "bad"}`}>{`${result.label ? "CÓ" : "KHÔNG"} được mua trong ${ds.labelDays ?? 7} ngày tới`}</span>
              </p>
              <p>
                {bought}/{result.k} sản phẩm giống nhất đã được mua ({num(result.voteShare * 100, 1)}%); mô hình đoán “có” khi tỷ lệ này
                từ {num(result.threshold * 100, 1)}% trở lên. Tỷ lệ này là số phiếu, không phải xác suất chính xác.
              </p>
              {result.actualLabel !== undefined && (
                <p>
                  Thực tế: sản phẩm này <b>{result.actualLabel ? "có" : "không"}</b> được mua trong {ds.labelDays ?? 7} ngày sau mốc kiểm tra{" "}
                  {result.actualLabel === result.label ? <span className="badge ok">mô hình đoán đúng</span> : <span className="badge bad">mô hình đoán sai</span>}
                  {Math.abs(result.notebookVoteShare - result.voteShare) < 1e-9 && <> · trùng kết quả lúc đánh giá trên notebook</>}
                </p>
              )}
              {result.outOfDomain && <p className="warn">Kết quả chỉ để tham khảo: {result.warnings.join("; ")}.</p>}
              <div className="table-wrap">
                <table>
                  <thead><tr><th>#</th><th>Sản phẩm giống (mã)</th><th>Đã được mua trong {ds.labelDays ?? 7} ngày?</th><th className="num">Độ khác biệt (nhỏ = giống hơn)</th></tr></thead>
                  <tbody>
                    {result.neighbours.map((n: any, i: number) => (
                      <tr key={i}><td>{i + 1}</td><td>{n.productId}</td><td>{yesNo(n.label)}</td><td className="num">{num(n.squaredDistance, 4)}</td></tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </section>
      )}
    </>
  );
}
