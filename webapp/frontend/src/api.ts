// Gọi backend Spring Boot. Mọi số liệu hiển thị đều lấy từ API (serving artifacts của pipeline);
// frontend không chứa dữ liệu cứng.

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    ...init,
    headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) },
  });
  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = await res.json();
      detail = body.detail ?? JSON.stringify(body);
    } catch {
      /* giữ statusText */
    }
    throw new ApiError(res.status, `${res.status}: ${detail}`);
  }
  return res.json() as Promise<T>;
}

export const get = <T>(path: string) => request<T>(path);
export const post = <T>(path: string, body: unknown) =>
  request<T>(path, { method: "POST", body: JSON.stringify(body) });

export type Row = Record<string, string>;

export interface Health {
  servingDir: string;
  runs: number;
  defaultRun: string | null;
  status: "UP" | "NO_DATA";
}

export interface TablePage {
  runId: string;
  table: string;
  total: number;
  rows: Row[];
}

export interface ModelEntry {
  runId: string;
  servingRunId: string;
  metadata: Record<string, any>;
}

/** Thời điểm ISO -> "YYYY-MM-DD HH:mm UTC"; chuỗi không phải ngày hợp lệ thì giữ nguyên. */
export const when = (iso?: string) => {
  if (!iso) return "—";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : `${d.toISOString().slice(0, 16).replace("T", " ")} UTC`;
};

export const num = (v: string | number | undefined | null, digits = 2) => {
  const n = typeof v === "number" ? v : Number(v);
  return v === undefined || v === null || v === "" || Number.isNaN(n)
    ? "—"
    : n.toLocaleString("vi-VN", { maximumFractionDigits: digits });
};
