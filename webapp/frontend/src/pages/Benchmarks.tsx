import ReactECharts from "../chart";
import { num } from "../api";
import { useGet } from "../hooks";
import { useRun } from "../run";

/** Định dạng benchmarks/<name>.json do bước tổng hợp giai đoạn 3 tạo (docs/evidence/bench). */
interface Benchmark {
  experiment: string;
  title: string;
  dataset: string;
  scope: string;
  metric: string;
  unit: string;
  rows: { label: string; median: number; min: number; max: number; runs: number; details?: Record<string, unknown> }[];
  notes?: string[];
  source: string;
}

/** Câu hỏi mà mỗi thí nghiệm trả lời (mã E2..E7 do bước tổng hợp benchmark ghi). */
const QUESTIONS: Record<string, string> = {
  E2: "5 cách viết MapReduce chạy nhanh chậm thế nào?",
  E3: "Cho MapReduce chạy nhiều phần việc song song có nhanh hơn không?",
  E4: "Với Spark, cách viết, định dạng file, số lõi CPU và số phần chia ảnh hưởng ra sao?",
  E5: "MapReduce và Spark làm cùng một bài toán thì tốn bao lâu?",
  E6: "Dữ liệu lớn gấp 10 lần, 100 lần thì thời gian tăng ra sao?",
  E7: "K-Means lặp nhiều vòng: giữ dữ liệu trong bộ nhớ (cache) có nhanh hơn không?",
};

function BenchmarkCard({ name }: { name: string }) {
  const { runId } = useRun();
  const b = useGet<Benchmark>(runId ? `/api/analytics/${runId}/benchmarks/${name}` : null);
  if (b.error) return <section><p className="error">{b.error}</p></section>;
  if (!b.data) return null;
  const d = b.data;
  return (
    <section>
      <div className="section-head"><h2>{d.title}</h2><span className="tag">{d.experiment}</span></div>
      {QUESTIONS[d.experiment] && <p className="question">{QUESTIONS[d.experiment]}</p>}
      <p className="note">Dữ liệu: {d.dataset}. Đại lượng đo: {d.metric.toLowerCase()}, tính bằng {d.unit === "ms" ? "mili giây (1 000 ms = 1 giây)" : d.unit}.</p>
      <ReactECharts
        style={{ height: Math.max(220, d.rows.length * 34 + 60) }}
        option={{
          tooltip: {
            trigger: "axis",
            formatter: (p: any[]) => {
              const r = d.rows[p[0].dataIndex];
              return `${r.label}<br/>trung vị ${num(r.median, 0)} ${d.unit} (nhanh nhất ${num(r.min, 0)}, chậm nhất ${num(r.max, 0)}), ${r.runs} lần đo`;
            },
          },
          grid: { left: 210, right: 40, top: 12, bottom: 28 },
          xAxis: { type: "value", axisLabel: { formatter: (v: number) => num(v, 0) } },
          yAxis: { type: "category", inverse: true, data: d.rows.map((r) => r.label) },
          series: [
            { type: "bar", data: d.rows.map((r) => r.median), name: "trung vị" },
            {
              // thanh min–max quanh median
              type: "custom",
              renderItem: (_: unknown, api: any) => {
                const i = api.value(0);
                const lo = api.coord([api.value(1), i]);
                const hi = api.coord([api.value(2), i]);
                return { type: "line", shape: { x1: lo[0], y1: lo[1], x2: hi[0], y2: hi[1] }, style: { stroke: "#1d2330", lineWidth: 2 } };
              },
              encode: { x: [1, 2], y: 0 },
              data: d.rows.map((r, i) => [i, r.min, r.max]),
              z: 10,
            },
          ],
        }}
      />
      <div className="table-wrap"><table>
        <thead><tr><th>Cấu hình</th><th className="num">Trung vị</th><th className="num">Nhanh nhất</th><th className="num">Chậm nhất</th><th className="num">Số lần đo</th></tr></thead>
        <tbody>
          {d.rows.map((r) => (
            <tr key={r.label}>
              <td>{r.label}</td>
              <td className="num">{num(r.median, 0)}</td>
              <td className="num">{num(r.min, 0)}</td>
              <td className="num">{num(r.max, 0)}</td>
              <td className="num">{r.runs}</td>
            </tr>
          ))}
        </tbody>
      </table></div>
      <details className="tech">
        <summary>Ghi chú kỹ thuật và nguồn số liệu</summary>
        {d.notes?.length ? <ul className="notes">{d.notes.map((n) => <li key={n}>{n}</li>)}</ul> : null}
        <p className="source">Phạm vi đo: {d.scope}. Nguồn: {d.source}</p>
      </details>
    </section>
  );
}

export default function Benchmarks() {
  const { runId } = useRun();
  const list = useGet<string[]>(runId ? `/api/analytics/${runId}/benchmarks` : null);
  return (
    <>
      <header className="page-head"><h1>Hadoop MapReduce và Spark: kết quả có khớp, chạy nhanh chậm ra sao</h1><p className="lead">Cả hai công cụ cùng tính doanh thu theo danh mục trên cùng dữ liệu HDFS và cho kết quả trùng khớp (xem trang Tổng quan). Trang này so sánh thời gian chạy.</p></header>
      <section>
        <p className="warn">
          Lưu ý khi đọc số: mọi phép đo chạy trên <b>một laptop</b> (Windows 11, 4 nhân, RAM 7,9 GB), không phải cụm nhiều máy.
          MapReduce chạy trong Docker ở chế độ một máy, Spark chạy trực tiếp trên máy và đọc dữ liệu qua Docker, nên điều kiện đọc dữ liệu
          của hai bên không hoàn toàn giống nhau. Các con số chỉ đúng cho máy này, không chứng minh công cụ nào nhanh hơn nói chung.
        </p>
        <p className="note">
          Cách đọc biểu đồ: mỗi thanh là thời gian trung vị của 3 lần đo (đã bỏ lần chạy khởi động đầu tiên); đoạn kẻ đen là khoảng từ lần
          nhanh nhất đến lần chậm nhất. <b>Thanh càng ngắn càng nhanh.</b>
        </p>
        <ul className="glossary">
          <li><b>D1, D2, D3</b>: mẫu 1% (424 nghìn dòng), mẫu 10% (4,2 triệu dòng) và cả tháng 10/2019 (42,4 triệu dòng).</li>
          <li><b>MR V1–V5</b>: 5 cách viết MapReduce cho cùng bài toán. V1 cơ bản; V2 gộp sớm trước khi gửi đi (combiner); V3 gộp ngay trong
            bước map; V4 dùng bảng mã danh mục để gộp nhanh hơn; V5 gom kết quả thành lô, gửi ít bản ghi nhất.</li>
          <li><b>Spark RDD / DataFrame</b>: hai kiểu viết chương trình Spark. <b>CSV</b> là đọc file gốc; <b>Parquet</b> là file đã làm sạch,
            lưu theo cột nên chỉ cần đọc các cột cần dùng.</li>
          <li><b>local[n]</b>: Spark dùng n lõi CPU của máy.</li>
        </ul>
      </section>
      {list.error && <p className="error">{list.error}</p>}
      {list.data?.length === 0 && <p className="note">Serving run này chưa có benchmark.</p>}
      {list.data?.map((name) => <BenchmarkCard key={name} name={name} />)}
    </>
  );
}
