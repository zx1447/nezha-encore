import { cronJobs } from "encore.dev/cron";
import { reconnect } from "./nezha";
import https from "https";

const crons = cronJobs();

// 每 14 分钟检查 nezha 连接，断了就重连
crons.interval("nezha-keepalive", { minutes: 5 }, async () => {
  console.log("[Nezha] keepalive check...");
  try {
    // reconnect 内部会检查是否需要重连
    await reconnect();
  } catch (e) {
    console.log("[Nezha] keepalive error:", (e as Error).message);
  }
});

// 每 5 分钟 ping 自己的 URL 保活（防止 Encore scale down）
crons.interval("self-ping", { minutes: 5 }, async () => {
  const url = process.env.ENCORE_API_URL || "https://staging-nezha-encore-nx5i.encr.app";
  console.log("[KeepAlive] ping", url);
  try {
    await new Promise<void>((resolve) => {
      https.get(url, { timeout: 10000, rejectUnauthorized: false }, (res) => {
        res.resume();
        console.log("[KeepAlive] response:", res.statusCode);
        resolve();
      }).on("error", (e) => {
        console.log("[KeepAlive] error:", e.message);
        resolve();
      }).on("timeout", function(this: any) {
        this.destroy();
        resolve();
      });
    });
  } catch (e) {
    console.log("[KeepAlive] failed:", (e as Error).message);
  }
});

export default crons;
