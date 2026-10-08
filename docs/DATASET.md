# Dataset: eCommerce behavior data from multi category store

## Nguồn và điều kiện sử dụng

| Mục | Giá trị |
|---|---|
| Trang Kaggle | https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store (dataset id 411512, chủ sở hữu `mkechinov`) |
| Nguồn gốc dữ liệu | REES46 Marketing Platform / dự án Open CDP; 7 tháng (2019-10 → 2020-04) của một cửa hàng đa danh mục. Kaggle chỉ chứa Oct và Nov 2019 (giới hạn 20 GB), các tháng khác ở link REES46 |
| Giấy phép (metadata Kaggle API, đọc 2026-10-06) | `copyright-authors` — bản quyền thuộc tác giả gốc |
| Điều kiện ghi trong mô tả | "You can use this dataset for free. Just mention the source of it: link to this page and link to REES46 Marketing Platform (https://rees46.com)." Báo cáo phải trích dẫn **cả hai** link |
| Phân phối trong repo | **Không** commit file CSV (`data/raw/` nằm trong `.gitignore`). Mỗi thành viên tự tải và đặt vào `data/raw/`, kiểm tra bằng SHA-256 dưới đây |

## File gốc đang dùng (đã có trên máy, không tải lại)

Người dùng đã tự tải 2 file từ Kaggle vào `data/raw/` (ngày 2019-09-29 theo mtime của file trên máy). Giữ nguyên, không sửa và không giải nén lại. Số liệu đo ngày 2026-10-06 bằng `sha256sum`, `wc -l`, `stat` (Git Bash trên host):

| File | Bytes | Số dòng vật lý (gồm header) | Số sự kiện (= dòng − 1) | SHA-256 |
|---|---:|---:|---:|---|
| `2019-Oct.csv` | 5 668 612 855 | 42 448 765 | 42 448 764 | `fedd938409b5f836ec89b39c861b13dad99fc7cd9beb1fddd97a2d50488b5b80` |
| `2019-Nov.csv` | 9 006 762 395 | 67 501 980 | 67 501 979 | `addd9a27ed99abdece368019ebfba19568972c7fcbe949db3484aab8a3bcffec` |

"Số sự kiện" giả định mỗi sự kiện nằm trên một dòng vật lý. Giả định này được kiểm tra bằng preflight (dòng malformed sẽ bị báo lỗi).

Kiểm tra lại trên máy khác:

```bash
sha256sum data/raw/2019-Oct.csv data/raw/2019-Nov.csv
```

## Schema thực tế

Header của **cả hai** file (đọc bằng `head -1`), khớp với `CsvEventParser.HEADER`:

```text
event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session
```

Dòng ví dụ (`2019-Oct.csv`, dòng 2–3):

```text
2019-10-01 00:00:00 UTC,view,44600062,2103807459595387724,,shiseido,35.79,541312140,72d76fde-8bb3-4e00-8c23-a032dfed738c
2019-10-01 00:00:00 UTC,view,3900821,2053013552326770905,appliances.environment.water_heater,aqua,33.20,554748717,9333dfbd-b87a-4708-9857-6336556b0fcc
```

| Cột | Mô tả theo Kaggle | Quan sát/ghi chú |
|---|---|---|
| `event_time` | Thời điểm (UTC) | Định dạng `yyyy-MM-dd HH:mm:ss UTC` |
| `event_type` | `view`, `cart`, `remove_from_cart`, `purchase` | Phân bố thật: chờ preflight/EDA (T2.2) |
| `product_id` | ID sản phẩm | |
| `category_id` | ID danh mục | Số 19 chữ số, vượt `INT`; phải đọc dưới dạng chuỗi hoặc `BIGINT` |
| `category_code` | Mã phân cấp danh mục, có thể rỗng | Dòng ví dụ đầu tiên có giá trị rỗng |
| `brand` | Tên brand viết thường, có thể rỗng | |
| `price` | Giá dạng số thực | Số chữ số thập phân thật: chờ counter `INVALID_PRICE` (F6) |
| `user_id` | ID người dùng cố định | Định danh giả: chỉ công bố kết quả tổng hợp (R14) |
| `user_session` | ID phiên tạm thời | |

## Vị trí trên HDFS

| Vùng | Đường dẫn | Nội dung |
|---|---|---|
| raw | `/data/ecommerce/raw/2019-Oct.csv` | Bản sao nguyên vẹn, nạp bằng `scripts/hdfs-ingest.sh` |
| raw/sample | `/data/ecommerce/raw/sample/` | Mẫu tất định do `DatasetTool sample` tạo (T1.4) |

Thống kê chất lượng dữ liệu (null, giá ≤ 0, trùng lặp, phân bố `event_type`) sẽ được bổ sung sau T1.4/T2.2 bằng số liệu thật.
