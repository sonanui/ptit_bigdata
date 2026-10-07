import { createContext, ReactNode, useContext, useEffect, useState } from "react";
import { get, Health } from "./api";

interface RunInfo {
  runId: string | null;
  setRunId: (id: string) => void;
  runs: { runId: string; default: boolean; createdAt: string; dataset: Record<string, string> }[];
  error: string | null;
}

const RunContext = createContext<RunInfo>({ runId: null, setRunId: () => {}, runs: [], error: null });

/** Serving run đang xem (mặc định = _LATEST của backend). */
export function RunProvider({ children }: { children: ReactNode }) {
  const [runId, setRunId] = useState<string | null>(null);
  const [runs, setRuns] = useState<RunInfo["runs"]>([]);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    Promise.all([get<Health>("/api/health"), get<RunInfo["runs"]>("/api/runs")])
      .then(([health, list]) => {
        setRuns(list);
        setRunId(health.defaultRun);
        if (health.status === "NO_DATA") setError(`Chưa có bộ kết quả nào để hiển thị (thư mục ${health.servingDir} trống). Xem docs/HUONG_DAN_CHAY.md.`);
      })
      .catch((e) => setError(String(e.message ?? e)));
  }, []);
  return <RunContext.Provider value={{ runId, setRunId, runs, error }}>{children}</RunContext.Provider>;
}

export const useRun = () => useContext(RunContext);

/** Chân biểu đồ: nguồn dữ liệu, theo quy tắc hiển thị của plan §18.7. */
export function Source({ text }: { text: string }) {
  const { runId } = useRun();
  return <p className="source">Nguồn: {text}. Bộ kết quả <code>{runId}</code>.</p>;
}
