import { useState } from "react";
import { Row } from "../api";
import { useGet } from "../hooks";

/** Chọn sản phẩm thật trong artifact của mô hình (tìm theo product_id, brand, category_code). */
export default function ProductPicker({ type, runId, onPick }: { type: "kmeans" | "knn"; runId: string; onPick: (p: Row) => void }) {
  const [q, setQ] = useState("");
  const [search, setSearch] = useState("");
  const found = useGet<Row[]>(`/api/ml/${type}/${runId}/products?limit=10&q=${encodeURIComponent(search)}`);
  return (
    <div>
      <form onSubmit={(e) => { e.preventDefault(); setSearch(q); }}>
        <label>Tìm sản phẩm<input value={q} placeholder="mã sản phẩm, thương hiệu hoặc danh mục" onChange={(e) => setQ(e.target.value)} /></label>
        <button type="submit" className="secondary">Tìm</button>
      </form>
      {found.error && <p className="error">{found.error}</p>}
      <div className="table-wrap"><table>
        <thead><tr><th>product_id</th><th>Thương hiệu</th><th>Danh mục</th><th className="num">Lượt xem</th><th className="num">Lượt mua</th><th></th></tr></thead>
        <tbody>
          {found.data?.map((p) => (
            <tr key={p.product_id}>
              <td>{p.product_id}</td><td>{p.brand}</td><td>{p.category_code}</td>
              <td className="num">{p.views}</td><td className="num">{p.purchases}</td>
              <td><button type="button" onClick={() => onPick(p)}>Chọn</button></td>
            </tr>
          ))}
        </tbody>
      </table></div>
    </div>
  );
}
