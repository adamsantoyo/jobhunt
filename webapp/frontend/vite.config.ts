import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev-only proxy: the SPA is served from Vite in dev and talks to the FastAPI
// backend on 127.0.0.1:8000. In production the backend serves the built dist/
// from the same origin, so no proxy is needed there.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "dist",
    sourcemap: false,
  },
});
