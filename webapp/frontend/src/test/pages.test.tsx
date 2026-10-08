import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { ApiError, num, post } from "../api";
import KMeansPage from "../pages/KMeansPage";
import KnnPage from "../pages/KnnPage";
import { fakeApi } from "./fakeApi";

const KNN_RUN = "run-knn";
const knnMeta = {
  algorithm: "k-nearest neighbours (exact, numpy)",
  hyperparameters: { k: 3, threshold: 0.5 },
  metrics: { test_f1: 0.5, test_pr_auc: 0.6 },
  training_time: "t",
  label: "future_purchases > 0",
  dataset: { trainT0: "2019-10-15", testT0: "2019-10-25" },
  features: ["a"],
  feature_preprocessing: "scaler",
};
const knnMetrics = {
  test: { "KNN (K=3)": { precision: 0.75, recall: 0.5, f1: 0.6214, pr_auc: 0.6708, balanced_accuracy: 0.7, accuracy: 0.8, tp: 7, fp: 0, fn: 0, tn: 9 } },
  validationSweep: [{ k: 3, f1: 1, precision: 1, recall: 1, pr_auc: 1 }],
  testBootstrapCI95: {},
};

describe("api", () => {
  it("định dạng số kiểu Việt Nam, giá trị thiếu là gạch ngang", () => {
    expect(num(1234.5, 1)).toBe("1.234,5");
    expect(num("")).toBe("—");
    expect(num(undefined)).toBe("—");
  });

  it("lỗi backend (ProblemDetail) được chuyển thành ApiError với nội dung detail", async () => {
    fakeApi({ "POST /api/ml/knn/predict": [422, { status: 422, detail: "Cần đúng một trong hai: productId hoặc raw" }] });
    await expect(post("/api/ml/knn/predict", {})).rejects.toEqual(new ApiError(422, "422: Cần đúng một trong hai: productId hoặc raw"));
  });
});

describe("KMeansPage", () => {
  it("chưa có mô hình thì nói rõ, không hiển thị số liệu", async () => {
    fakeApi({ "GET /api/ml/kmeans/models": [200, []] });
    render(<KMeansPage />);
    expect(await screen.findByText("Chưa có mô hình K-Means nào được xuất ra cho web.")).toBeTruthy();
    expect(screen.queryByText("Silhouette")).toBeNull();
  });

  it("lỗi API được hiển thị nguyên văn", async () => {
    fakeApi({ "GET /api/ml/kmeans/models": [503, { status: 503, detail: "Checksum sai cho ml/kmeans/model.json" }] });
    render(<KMeansPage />);
    expect(await screen.findByText("503: Checksum sai cho ml/kmeans/model.json")).toBeTruthy();
  });
});

describe("KnnPage", () => {
  it("chọn sản phẩm -> POST predict với productId -> hiển thị nhãn, vote_share và láng giềng do backend trả", async () => {
    const calls = fakeApi({
      "GET /api/ml/knn/models": [200, [{ runId: KNN_RUN, servingRunId: "s", metadata: knnMeta }]],
      [`GET /api/ml/knn/${KNN_RUN}`]: [200, { runId: KNN_RUN, metadata: knnMeta, metrics: knnMetrics }],
      [`GET /api/ml/knn/${KNN_RUN}/products*`]: [200, [{ product_id: "p1", brand: "b", category_code: "c", views: "30", purchases: "2" }]],
      "POST /api/ml/knn/predict": [200, {
        label: 1, voteShare: 2 / 3, threshold: 0.5, k: 3, actualLabel: 1, notebookVoteShare: 2 / 3,
        outOfDomain: false, warnings: [],
        neighbours: [{ productId: "n1", label: 1, squaredDistance: 0.1 }, { productId: "n2", label: 1, squaredDistance: 0.2 },
                     { productId: "n3", label: 0, squaredDistance: 0.3 }],
      }],
    });
    render(<KnnPage />);
    fireEvent.click(await screen.findByRole("button", { name: "Chọn" }));
    expect(await screen.findByText("CÓ được mua trong 7 ngày tới")).toBeTruthy();
    expect(screen.getByText(/2\/3 sản phẩm giống nhất đã được mua/)).toBeTruthy();
    expect(screen.getByText("n3")).toBeTruthy();
    expect(calls.find((c) => c.method === "POST")?.body).toEqual({ runId: KNN_RUN, productId: "p1" });
    // Confusion matrix lấy đúng từ metrics test của dòng KNN.
    expect(screen.getByText("KNN đoán đúng và sai bao nhiêu sản phẩm")).toBeTruthy();
    // F1 phải giữ 4 chữ số thập phân (lỗi cũ: bị làm tròn thành 1 vì nhầm là cột đếm).
    expect(screen.getByText("0,6214")).toBeTruthy();
  });

  it("form số đếm thô gửi đủ 6 trường; lỗi 422 của backend hiện ra cho người dùng", async () => {
    const calls = fakeApi({
      "GET /api/ml/knn/models": [200, [{ runId: KNN_RUN, servingRunId: "s", metadata: knnMeta }]],
      [`GET /api/ml/knn/${KNN_RUN}`]: [200, { runId: KNN_RUN, metadata: knnMeta, metrics: knnMetrics }],
      [`GET /api/ml/knn/${KNN_RUN}/products*`]: [200, []],
      "POST /api/ml/knn/predict": [422, { status: 422, detail: "views phải > 0 (tỷ lệ chia cho views)" }],
    });
    const { container } = render(<KnnPage />);
    await screen.findByRole("button", { name: "Dự đoán" });
    const inputs = container.querySelectorAll<HTMLInputElement>('form input[type="number"]');
    expect(inputs.length).toBe(6);
    inputs.forEach((input) => fireEvent.change(input, { target: { value: "0" } }));
    fireEvent.click(screen.getByRole("button", { name: "Dự đoán" }));
    await waitFor(() => expect(screen.getByText("422: views phải > 0 (tỷ lệ chia cho views)")).toBeTruthy());
    expect(calls.find((c) => c.method === "POST")?.body).toEqual({
      runId: KNN_RUN,
      raw: { views: 0, carts: 0, purchases: 0, medianPrice: 0, distinctUsers: 0, recentViews: 0 },
    });
  });
});

describe("when", () => {
  it("đổi ISO sang UTC rút gọn; chuỗi không phải ngày thì giữ nguyên, không làm hỏng trang", async () => {
    const { when } = await import("../api");
    expect(when("2026-10-06T17:01:39.415664+00:00")).toBe("2026-10-06 17:01 UTC");
    expect(when("t")).toBe("t");
    expect(when(undefined)).toBe("—");
  });
});

describe("modelRowName", () => {
  it("đổi tên dòng kỹ thuật sang lời thường, ngưỡng thành phần trăm", async () => {
    const { modelRowName } = await import("../labels");
    expect(modelRowName("KNN (K=15, ngưỡng 0.400)")).toBe("KNN (mô hình của nhóm) (K=15, ngưỡng 40%)");
    expect(modelRowName("Baseline lớp đa số")).toBe("Mốc so sánh 1: luôn đoán “không được mua”");
    expect(modelRowName("Tên lạ")).toBe("Tên lạ");
  });
});

describe("clusterNames", () => {
  it("chỉ đặt tên khi K = 2 và cụm lớn hơn ở cả lượt xem lẫn tỷ lệ có lượt mua", async () => {
    const { clusterNames } = await import("../pages/KMeansPage");
    const hot = { cluster: "0", mean_views: "1911.75", share_with_purchase: "0.985" };
    const normal = { cluster: "1", mean_views: "93.96", share_with_purchase: "0.309" };
    expect(clusterNames([hot, normal])).toEqual({ "0": "nhóm bán chạy (hot)", "1": "nhóm bình thường" });
    expect(clusterNames([{ ...hot, share_with_purchase: "0.1" }, normal])).toEqual({});
    expect(clusterNames([hot, normal, { ...normal, cluster: "2" }])).toEqual({});
  });
});
