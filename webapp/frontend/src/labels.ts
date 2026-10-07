// Tên dễ hiểu cho các khóa kỹ thuật trong serving artifact. Chỉ đổi cách hiển thị, không đổi dữ liệu;
// khóa không có trong bảng thì hiện nguyên tên gốc.

/** Bộ dữ liệu (tag của pipeline). */
export const DATASET_NAMES: Record<string, string> = {
  d0: "Dữ liệu mẫu 7 dòng (kiểm thử)",
  d1: "Mẫu 1% tháng 10/2019",
  d2: "Mẫu 10% tháng 10/2019",
  d3: "Cả tháng 10/2019",
};
export const datasetName = (tag?: string) => (tag ? DATASET_NAMES[tag.toLowerCase()] ?? tag : "—");

/** Đặc trưng của sản phẩm mà mô hình dùng. */
export const FEATURE_NAMES: Record<string, string> = {
  log_views: "lượt xem",
  log_carts: "lượt thêm giỏ",
  log_purchases: "lượt mua",
  cart_rate: "tỷ lệ thêm giỏ trên lượt xem",
  purchase_rate: "tỷ lệ mua trên lượt xem",
  log_median_price: "giá bán",
  log_distinct_users: "số người xem khác nhau",
  recent_view_share: "phần lượt xem rơi vào 7 ngày gần nhất",
};
export const featureList = (features: string[] = []) => features.map((f) => FEATURE_NAMES[f] ?? f).join(", ");

/** Cột của bảng hồ sơ cụm K-Means. */
export const PROFILE_COLUMNS: Record<string, string> = {
  cluster: "Cụm",
  products: "Số sản phẩm",
  mean_views: "TB lượt xem",
  mean_carts: "TB thêm giỏ",
  mean_purchases: "TB lượt mua",
  mean_distinct_users: "TB số người xem",
  mean_median_price: "TB giá",
  mean_cart_rate: "TB tỷ lệ thêm giỏ",
  mean_purchase_rate: "TB tỷ lệ mua",
  share_with_purchase: "Tỷ lệ SP có lượt mua",
  top_category_roots: "3 ngành hàng nhiều nhất",
};

/** Tên dòng trong bảng đánh giá KNN (khóa do notebook ghi); ngưỡng "0.400" hiện thành "40%". */
export function modelRowName(raw: string) {
  const name = raw.replace(/ngưỡng (\d+(?:\.\d+)?)/, (_, v) => `ngưỡng ${(Number(v) * 100).toLocaleString("vi-VN", { maximumFractionDigits: 1 })}%`);
  if (name.startsWith("KNN")) return `KNN (mô hình của nhóm) ${name.slice(3).trim()}`;
  if (name.startsWith("Baseline lớp đa số")) return "Mốc so sánh 1: luôn đoán “không được mua”";
  if (name.startsWith("Baseline luật lịch sử")) return "Mốc so sánh 2: đoán “được mua” nếu 14 ngày trước đã có lượt mua";
  if (name.startsWith("Logistic Regression")) return `Logistic Regression (mô hình tham chiếu của Spark MLlib) ${name.slice("Logistic Regression".length).trim()}`;
  return name;
}

/** Lý do loại dòng khi làm sạch dữ liệu. */
export const REJECT_REASONS: Record<string, string> = { HEADER: "dòng tiêu đề", MALFORMED: "dòng sai định dạng" };
