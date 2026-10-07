# Báo cáo đo MapReduce: từ synthetic tới dữ liệu thật

## Kết quả trên dữ liệu Kaggle thật (2026-10-06/07, máy nhóm)

Số liệu chính thức của dự án. Bảng đầy đủ E2–E7 (median, min–max, 1 warmup + 3 lần đo), cách đo và giới hạn:
[`evidence/bench/SUMMARY.md`](evidence/bench/SUMMARY.md); dữ liệu thô `evidence/bench/{mr,spark}/*/runs.json`.
Môi trường: Windows 11 (i5-9300HF 4 nhân/8 luồng, RAM 7,9 GB), HDFS 1 NameNode + 1 DataNode trong Docker,
MR LocalJobRunner trong container JDK 11 (1 map slot, 2 reducer, poll 100 ms), input và output trên HDFS.

| Dữ liệu | V1 job ms | V2 | V3 | V4 | V5 | Map output V1 → V2 (sau combiner) → V5 |
|---|---:|---:|---:|---:|---:|---|
| D1: mẫu 1% (424 610 dòng) | 4 753 | 4 394 | 5 906 | 7 670 | 3 206 | 7 464 → 308 → 2 |
| D2: mẫu 10% (4 246 194 dòng) | 29 221 | 27 161 | 33 581 | 31 956 | 10 418 | 74 120 → 1 758 → 10 |
| D3: cả tháng 10 (42 448 765 dòng) | 189 970 | 185 022 | — | — | 61 236 | 742 849 → 15 511 → 86 |

Mọi lần chạy có kết quả khớp tuyệt đối với V1. Trên dữ liệu thật V5 nhanh nhất (D3: 3,1 lần so với V1); V3, V4 không nhanh hơn V1
trên D1/D2 (V4 còn phải tải dictionary); D3 chỉ đo V1, V2, V5 vì thời gian. End-to-end của `RevenueTool` lớn hơn nhiều thời gian job
(D3 V1: 525 s so với 190 s) vì preflight và SHA-256 trước/sau job quét lại toàn bộ input ở client (F4 trong plan).
So sánh với Spark (E4–E6) và lưu ý về môi trường I/O khác nhau: `evidence/bench/SUMMARY.md`.

## Đo ban đầu trên dữ liệu synthetic (máy tác giả, trước khi có dataset thật)

### Phép đo

- macOS arm64, JDK11.0.32.1, Hadoop3.4.2 LocalJobRunner.
- Synthetic200.000 purchase events,32categories,seed21; một mapper,2reducers, cache10000, compression=false. Không phải dữ liệu Kaggle thật.
- Cùng input fingerprint/policy/group mode; mỗi variant đúng một job.
- 1warmup+3measured runs/variant, randomized order seed21.20runs valid; mọi output so exact với V1; integration oracle tính tay kiểm tra độc lập.
- Poll completion100ms áp dụng chung V1–V5. Profile scan1085ms đo từ artifact. Job elapsed gồm submit/wait/counter collection; CLI gồm kiểm tra trước/sau, job và validation. Cột CLI+profile cộng scan cost cho V4/V5, chưa gồm original common preflight,JVM startup,profile serialization; không phải toàn bộ thời gian từ tải CSV.

### Kết quả median

| Variant | Job ms (min–max) | CLI ms | CLI+profile ms | Map records | Reduce records | Shuffle bytes | Speedup job |
|---|---:|---:|---:|---:|---:|---:|---:|
| v1 | 1266 (1263–1337) | 2586 | 2586 | 200000 | 200000 | 4137536 | 1.00× |
| v2 | 1158 (1146–1290) | 2427 | 2427 | 200000 | 32 | 674 | 1.09× |
| v3 | 1051 (1046–1053) | 2318 | 2318 | 32 | 32 | 674 | 1.20× |
| v4 | 1146 (1143–1237) | 2485 | 3570 | 32 | 32 | 674 | 1.10× |
| v5 | 851 (838–1612) | 2158 | 3243 | 2 | 2 | 676 | 1.49× |

### Kết luận có bằng chứng

V3 giảm emit trước sort từ200000 xuống32. V5 giảm sort records tiếp xuống2batch; payload vẫn32aggregate entries nên shuffle676bytes gần bằng674bytes của V2–V4, không phải giảm16lần bytes. V5 median job851ms so V1 1266ms (1.49×, giảm32.8%); một lượt V5 lên1612ms, vì vậy không cam kết tốc độ ổn định từ ba lượt đo.

V4 median1146ms chậm hơn V3 1051ms trên workload này: V3 đã giữ đủ32keys, dense không giảm thêm emit và phải load/validate dictionary. V4 tối ưu representation/flush, không giảm bậc độ phức tạp so V3 khi cache đủ lớn. Đây là phương án có điều kiện, chưa có bằng chứng thắng V3 trong phép đo này.

V4/V5 cộng profile scan cho lần dùng đầu lần lượt3570/3243ms, vẫn cao hơn baselineCLI2586ms. Dictionary dùng lại giúp amortize preparation khi chạy nhiều truy vấn; đo riêng để không giấu chi phí. Không ép ranking V1<V2<V3<V4<V5 nếu bằng chứng không hỗ trợ. Cận dưới Ω(N) không thể loại bỏ vì mọi event phải được đọc; tối ưu chủ yếu giảm sort/shuffle/allocation.

Với default polling5000ms, median job khoảng5331–5544ms cho mọi variant. Evidence default-poll5000 giữ phép đo này để giải thích vì sao runtime local có thể trông tương đương dù work giảm mạnh. Đây là client wait overhead chung, không chứng minh dense/batch vô ích.

### Correctness và build

Clean Maven integration verify:18unit+6integration tests pass; Python5tests pass; reviewer độc lập không thấy Critical/Important defect.600purchase/3group oracle:V1/V2 phát600,V3/V4 phát3,V5 phát2batch; cùng sum/count/AVG và một job. JAR clean không có Salted/SelectiveSalt/HotKey classes.

### Evidence

- [Summary](evidence/summary.csv), [raw20runs](evidence/runs.json), [matrix](evidence/matrix.json), [environment/checksums](evidence/environment.json).
- [Profile scan](evidence/profile.json), [input manifest](evidence/input.json), [dictionary](evidence/groups.json).
- [Default5000ms polling summary](evidence/default-poll5000/summary.csv).
- Thuật toán salting hai job và tài liệu cũ đã được xóa khỏi project.

Giới hạn: một máy,một mapper,32groups,3repeats; chưa đo high-cardinality,many-mapper,YARN hoặc Kaggle nhiềuGB. Không suy diễn strict speed ranking hay giải quyết skew từ kết quả này.
