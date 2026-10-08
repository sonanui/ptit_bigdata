import { num } from "../api";
import { useGet } from "../hooks";
import { useRun } from "../run";
import { datasetName, REJECT_REASONS } from "../labels";

interface Manifest {
  runId: string;
  createdAt: string;
  gitSha: string;
  dataset: { tag: string; input: string };
  sources: Record<string, string>;
  files: { path: string; sha256: string; bytes: number; rows?: number }[];
}

/** Các bước dữ liệu đi qua trước khi tới web. */
const FLOW: { title: string; detail: string }[] = [
  { title: "Dữ liệu gốc", detail: "File CSV hành vi mua sắm từ Kaggle, giữ nguyên, có mã kiểm tra" },
  { title: "Lưu trên HDFS", detail: "Hệ thống file phân tán của Hadoop chia file thành các khối 128 MB" },
  { title: "Hadoop MapReduce", detail: "Tính doanh thu theo danh mục bằng 5 cách viết, để đối chiếu và đo tốc độ" },
  { title: "Spark", detail: "Làm sạch dữ liệu, tính các bảng thống kê, chuẩn bị dữ liệu cho mô hình" },
  { title: "Học máy", detail: "Huấn luyện K-Means và KNN (Spark MLlib)" },
  { title: "Xuất kết quả cho web", detail: "Đóng gói thành file nhỏ kèm mã kiểm tra; web chỉ đọc các file này" },
];

export default function Overview() {
  const { runId, error } = useRun();
  const manifest = useGet<Manifest>(runId ? `/api/runs/${runId}` : null);
  const parity = useGet<any>(runId ? `/api/analytics/${runId}/parity` : null);
  const quality = useGet<any>(runId ? `/api/analytics/${runId}/quality` : null);
  const m = manifest.data;
  const p = parity.data;
  const q = quality.data;
  return (
    <>
      <header className="page-head">
        <h1>Tổng quan: từ dữ liệu gốc tới kết quả</h1>
        <p className="lead">
          Trang web chỉ hiển thị kết quả đã được tính sẵn từ dữ liệu thật bằng Hadoop và Spark. Web không tự tính lại và không đọc file dữ
          liệu gốc, nên mỗi con số đều truy ngược được về lần chạy đã tạo ra nó.
        </p>
      </header>
      {error && <p className="error">{error}</p>}

      {p && (
        <section className={`hero ${p.matched ? "hero-ok" : "hero-bad"}`}>
          <div className="hero-figure">
            <span className="hero-count">{num(p.sparkGroups, 0)}/{num(p.mrGroups, 0)}</span>
            <span className="hero-unit">danh mục có kết quả trùng khớp</span>
          </div>
          <div className="hero-text">
            <h2>{p.matched ? "Hadoop MapReduce và Spark tính ra cùng một kết quả" : `MapReduce và Spark cho kết quả khác nhau ở ${p.mismatchCount} danh mục`}</h2>
            <p>
              Hai công cụ cùng đọc một file trên HDFS và cùng tính doanh thu theo danh mục từ {num(p.mrPurchaseCount, 0)} lượt mua.
              Tổng tiền, số lượt mua và giá trị trung bình của từng danh mục được so khớp từng đồng.
            </p>
            {!p.matched && <pre className="error">{JSON.stringify(p.mismatches, null, 2)}</pre>}
            <p className="source">
              Kết quả MapReduce: <code>{p.mrOutput}</code>
              <br />
              Kết quả Spark: <code>{p.sparkRevenueRunId}</code>
            </p>
          </div>
        </section>
      )}

      {m && (
        <section>
          <h2>Dữ liệu đi qua những bước nào</h2>
          <ol className="flow">
            {FLOW.map((f) => (
              <li key={f.title}>
                <strong>{f.title}</strong>
                <span>{f.detail}</span>
              </li>
            ))}
          </ol>
          <dl className="facts">
            <div><dt>Bộ dữ liệu</dt><dd>{datasetName(m.dataset.tag)}</dd><dd className="sub">{m.dataset.input}</dd></div>
            {q && <div><dt>Số dòng đọc vào</dt><dd>{num(q.rowsIn, 0)}</dd></div>}
            {q && <div><dt>Số dòng hợp lệ (mỗi dòng là một sự kiện)</dt><dd>{num(q.rowsValid, 0)}</dd></div>}
            {q && (
              <div>
                <dt>Số dòng bị loại</dt>
                <dd>{Object.entries(q.rejected ?? {}).map(([k, v]) => `${num(v as number, 0)} ${REJECT_REASONS[k] ?? k}`).join(", ") || "0"}</dd>
              </div>
            )}
            <div><dt>Thời điểm xuất kết quả (giờ UTC)</dt><dd>{m.createdAt.slice(0, 16).replace("T", " ")}</dd><dd className="sub">phiên bản code {m.gitSha}</dd></div>
          </dl>
        </section>
      )}

      {q?.quality && (
        <section>
          <h2>Dữ liệu sau khi làm sạch</h2>
          <dl className="facts">
            <div><dt>Lượt xem sản phẩm</dt><dd>{num(q.quality.eventTypes?.view, 0)}</dd></div>
            <div><dt>Lượt thêm vào giỏ</dt><dd>{num(q.quality.eventTypes?.cart, 0)}</dd></div>
            <div><dt>Lượt mua</dt><dd>{num(q.quality.eventTypes?.purchase, 0)}</dd></div>
            <div><dt>Số sản phẩm</dt><dd>{num(q.quality.distinct_products, 0)}</dd></div>
            <div><dt>Số người dùng</dt><dd>{num(q.quality.distinct_users, 0)}</dd></div>
            <div><dt>Sự kiện không ghi tên danh mục</dt><dd>{num(q.quality.null_category_code, 0)}</dd></div>
            <div><dt>Sự kiện không ghi thương hiệu</dt><dd>{num(q.quality.null_brand, 0)}</dd></div>
            <div><dt>Sự kiện có giá bằng 0</dt><dd>{num(q.quality.zero_price, 0)}</dd></div>
          </dl>
        </section>
      )}

      {m && (
        <section>
          <h2>Các file kết quả mà web đang dùng</h2>
          <p className="note">Mỗi file có một mã kiểm tra (sha256). Trước khi dùng, máy chủ web tính lại mã này; file bị sửa sẽ bị từ chối thay vì hiện số sai.</p>
          <div className="table-wrap">
            <table>
              <thead>
                <tr><th>File</th><th className="num">Số dòng</th><th className="num">Dung lượng (byte)</th><th>Mã kiểm tra</th></tr>
              </thead>
              <tbody>
                {m.files.map((f) => (
                  <tr key={f.path}>
                    <td>{f.path}</td>
                    <td className="num">{f.rows !== undefined ? num(f.rows, 0) : ""}</td>
                    <td className="num">{num(f.bytes, 0)}</td>
                    <td><code>{f.sha256.slice(0, 12)}</code></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}
    </>
  );
}

export function Kpi({ label, value }: { label: string; value: string }) {
  return (
    <div className="kpi">
      <div className="label">{label}</div>
      <div className="value" style={{ fontSize: value.length > 24 ? "0.95rem" : undefined }}>{value}</div>
    </div>
  );
}
