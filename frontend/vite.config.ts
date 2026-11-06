import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const gateway = process.env.VITE_GATEWAY ?? "http://localhost:18080";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": gateway,
      "/ws": { target: gateway, ws: true },
    },
  },
});
