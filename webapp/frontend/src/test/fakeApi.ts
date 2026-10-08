import { vi } from "vitest";

/** Ghi lại các request và trả response theo bảng "METHOD path" -> [status, body]. Không khớp -> 404 ProblemDetail. */
export function fakeApi(routes: Record<string, [number, unknown]>) {
  const calls: { method: string; path: string; body?: unknown }[] = [];
  const fetchMock = vi.fn(async (input: string, init?: RequestInit) => {
    const method = init?.method ?? "GET";
    const path = String(input);
    calls.push({ method, path, body: init?.body ? JSON.parse(String(init.body)) : undefined });
    const key = Object.keys(routes).find((k) => {
      const [m, p] = k.split(" ");
      return m === method && (p.endsWith("*") ? path.startsWith(p.slice(0, -1)) : path === p);
    });
    const [status, body] = key ? routes[key] : [404, { status: 404, detail: `không có ${method} ${path}` }];
    return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
  });
  vi.stubGlobal("fetch", fetchMock);
  return calls;
}
