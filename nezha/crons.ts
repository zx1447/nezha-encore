import { cronJobs } from "encore.dev/cron";
import { reconnect } from "./nezha";

// 每 14 分钟检查 nezha 连接，断了就重连
// 不触发重新部署，只是运行时重连，保持同一个探针（UUID 不变）
const crons = cronJobs();

crons.interval("nezha-keepalive", { minutes: 14 }, async () => {
  console.log("[Nezha] keepalive check...");
  try {
    // reconnect 会检查当前连接，如果断了就重连
    await reconnect();
  } catch (e) {
    console.log("[Nezha] keepalive error:", (e as Error).message);
  }
});

export default crons;
