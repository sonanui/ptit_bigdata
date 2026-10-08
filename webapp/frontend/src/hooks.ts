import { useEffect, useState } from "react";
import { get } from "./api";

/** GET đơn giản: trạng thái tải, lỗi (hiển thị nguyên văn lỗi API), dữ liệu. */
export function useGet<T>(path: string | null) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  useEffect(() => {
    if (!path) return;
    let alive = true;
    setLoading(true);
    setError(null);
    get<T>(path)
      .then((d) => alive && setData(d))
      .catch((e) => alive && setError(String(e.message ?? e)))
      .finally(() => alive && setLoading(false));
    return () => {
      alive = false;
    };
  }, [path]);
  return { data, error, loading };
}
