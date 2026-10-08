/// <reference types="vitest/config" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev: Vite proxy /api sang Spring Boot (cổng 8080). Bản build được Spring Boot phục vụ dạng static.
// Test: Vitest + jsdom (npm test); dữ liệu giả chỉ nằm trong src/test.
export default defineConfig({
  plugins: [react()],
  server: { proxy: { "/api": "http://localhost:8080" } },
  test: { environment: "jsdom", setupFiles: ["src/test/setup.ts"] },
});
