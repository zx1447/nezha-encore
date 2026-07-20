"use node";  // 使用 Node.js runtime

import { api } from "encore.dev/api";
import http2 from "http2";
import https from "https";
import crypto from "crypto";

// 哪吒配置
const NZ_SERVER = process.env.NZ_SERVER || "nz.zxydk1715.dpdns.org:443";
const NZ_TLS = true;
const NZ_SECRET = process.env.NZ_CLIENT_SECRET || "BFbvpxSlBTUugp3gDzezVKkZ22BV0CeL";
const AGENT_VERSION = "2.2.2";
const REPORT_DELAY = 3; // 秒

// 固定 UUID（不基于 IP）
const FIXED_UUID = "b7c8d9e0-f1a2-3b4c-5d6e-7f8a9b0c1d2e";

// 全局状态
let session: http2.ClientHttp2Session | null = null;
let stateStream: http2.ClientHttp2Stream | null = null;
let stateTimer: NodeJS.Timeout | null = null;
let connected = false;
let lastReport = 0;

// Protobuf helpers
const PB = {
  varint: (v: number): Buffer => {
    let val = BigInt(v);
    if (val < 0n) val += (1n << 64n);
    const bytes: number[] = [];
    do {
      let byte = Number(val & 0x7fn);
      val >>= 7n;
      if (val > 0n) byte |= 0x80;
      bytes.push(byte);
    } while (val > 0n);
    return Buffer.from(bytes);
  },
  tag: (f: number, w: number): Buffer => PB.varint((f << 3) | w),
  u64: (f: number, v: number): Buffer => Buffer.concat([PB.tag(f, 0), PB.varint(v || 0)]),
  dbl: (f: number, v: number): Buffer => {
    const b = Buffer.alloc(8);
    b.writeDoubleLE(v || 0, 0);
    return Buffer.concat([PB.tag(f, 1), b]);
  },
  str: (f: number, v: string): Buffer => {
    if (!v) return Buffer.alloc(0);
    const s = Buffer.from(v, "utf8");
    return Buffer.concat([PB.tag(f, 2), PB.varint(s.length), s]);
  },
  msg: (parts: Buffer[]): Buffer => Buffer.concat(parts.filter(x => x && x.length > 0)),
  frame: (m: Buffer): Buffer => {
    const h = Buffer.alloc(5);
    h[0] = 0;
    h.writeUInt32BE(m.length, 1);
    return Buffer.concat([h, m]);
  },
};

// 收集主机信息
function collectHost() {
  const os = require("os");
  return {
    platform: os.platform(),
    platformVersion: os.release(),
    cpu: [...new Set(os.cpus().map(c => c.model))],
    memTotal: os.totalmem(),
    diskTotal: 0,
    swapTotal: 0,
    arch: os.arch(),
    virtualization: "encore",
    bootTime: Math.floor(Date.now() / 1000 - os.uptime()),
    version: AGENT_VERSION,
    gpu: [],
    ip: "",
  };
}

// 收集状态
function collectState() {
  const os = require("os");
  const memTotal = os.totalmem();
  const memUsed = memTotal - os.freemem();
  return {
    cpu: 0,
    memUsed,
    swapUsed: 0,
    diskUsed: 0,
    netInTransfer: 0,
    netOutTransfer: 0,
    netInSpeed: 0,
    netOutSpeed: 0,
    uptime: Math.floor(os.uptime()),
    load1: os.loadavg()[0],
    load5: os.loadavg()[1],
    load15: os.loadavg()[2],
    tcpConnCount: 0,
    udpConnCount: 0,
    processCount: 0,
    gpu: [],
  };
}

// 发送 unary RPC
function sendUnary(path: string, msgBuf: Buffer): Promise<Buffer | null> {
  return new Promise((resolve, reject) => {
    if (!session || session.destroyed) {
      reject(new Error("session not available"));
      return;
    }
    const headers = {
      ":method": "POST",
      ":path": path,
      "content-type": "application/grpc",
      "te": "trailers",
      "user-agent": `nezha-agent/${AGENT_VERSION}`,
      "client-secret": NZ_SECRET,
      "client-uuid": FIXED_UUID,
    };
    const stream = session.request(headers);
    let chunks: Buffer[] = [];
    let done = false;
    stream.on("response", (h: any) => {
      const status = parseInt(h[":status"]);
      if (status && status !== 200) {
        if (!done) { done = true; reject(new Error(`HTTP ${status}`)); }
      }
    });
    stream.on("data", (c: Buffer) => { if (!done) chunks.push(c); });
    stream.on("trailers", (t: any) => {
      if (done) return;
      done = true;
      const gs = t["grpc-status"];
      if (gs && gs !== "0") {
        reject(new Error(`gRPC ${gs}: ${t["grpc-message"] || ""}`));
      } else {
        const full = Buffer.concat(chunks);
        if (full.length > 5) resolve(full.slice(5));
        else resolve(null);
      }
    });
    stream.on("error", (e: Error) => { if (!done) { done = true; reject(e); } });
    stream.end(PB.frame(msgBuf));
    setTimeout(() => { if (!done) { done = true; stream.close(http2.constants.HTTP2_STREAM_CANCEL); reject(new Error("timeout")); } }, 15000);
  });
}

// 连接哪吒面板
async function connect() {
  try {
    // 关闭旧连接
    if (stateTimer) clearInterval(stateTimer);
    if (stateStream) { try { stateStream.close(); } catch(e) {} }
    if (session) { try { session.destroy(); } catch(e) {} }

    console.log(`[Nezha] Connecting to ${NZ_SERVER}...`);
    const url = `https://${NZ_SERVER}`;
    session = http2.connect(url, { rejectUnauthorized: false });

    await new Promise<void>((resolve, reject) => {
      const to = setTimeout(() => reject(new Error("connect timeout")), 10000);
      session!.on("connect", () => { clearTimeout(to); resolve(); });
      session!.on("error", (e: Error) => { clearTimeout(to); reject(e); });
    });

    console.log("[Nezha] Connected!");

    // 上报 Host 信息
    const host = collectHost();
    const hostBuf = PB.msg([
      PB.str(1, host.platform),
      PB.str(2, host.platformVersion),
      PB.u64(4, host.memTotal),
      PB.str(7, host.arch),
      PB.str(8, host.virtualization),
      PB.u64(9, host.bootTime),
      PB.str(10, host.version),
    ]);
    try {
      await sendUnary("/proto.NezhaService/ReportSystemInfo2", hostBuf);
      console.log("[Nezha] Host reported");
    } catch(e) {
      console.log("[Nezha] Host report failed:", (e as Error).message);
    }

    // 上报 GeoIP
    try {
      const geoBuf = PB.msg([PB.u64(1, 0), PB.str(2, "")]);
      await sendUnary("/proto.NezhaService/ReportGeoIP", geoBuf);
      console.log("[Nezha] GeoIP reported");
    } catch(e) {}

    // 打开 State 流
    const stateHeaders = {
      ":method": "POST",
      ":path": "/proto.NezhaService/ReportSystemState",
      "content-type": "application/grpc",
      "te": "trailers",
      "user-agent": `nezha-agent/${AGENT_VERSION}`,
      "client-secret": NZ_SECRET,
      "client-uuid": FIXED_UUID,
    };
    stateStream = session.request(stateHeaders);
    stateStream.on("error", () => {});
    stateStream.on("close", () => { console.log("[Nezha] State stream closed"); });

    // 定期上报状态
    stateTimer = setInterval(() => {
      try {
        const s = collectState();
        const stateBuf = PB.msg([
          PB.dbl(1, s.cpu),
          PB.u64(2, Math.round(s.memUsed)),
          PB.u64(3, s.swapUsed),
          PB.u64(4, s.diskUsed),
          PB.u64(5, s.netInTransfer),
          PB.u64(6, s.netOutTransfer),
          PB.u64(7, Math.round(s.netInSpeed)),
          PB.u64(8, Math.round(s.netOutSpeed)),
          PB.u64(9, s.uptime),
          PB.dbl(10, s.load1),
          PB.dbl(11, s.load5),
          PB.dbl(12, s.load15),
          PB.u64(13, s.tcpConnCount),
          PB.u64(14, s.udpConnCount),
          PB.u64(15, s.processCount),
        ]);
        if (stateStream && !stateStream.destroyed && stateStream.writable) {
          stateStream.write(PB.frame(stateBuf));
          lastReport = Date.now();
        }
      } catch(e) {}
    }, REPORT_DELAY * 1000);

    connected = true;
    console.log("[Nezha] Agent running, UUID=" + FIXED_UUID);

  } catch(e) {
    console.log("[Nezha] Connect failed:", (e as Error).message);
    connected = false;
    // 5 秒后重试
    setTimeout(() => connect(), 5000);
  }
}

// 启动哪吒探针（模块加载时自动启动）
connect();

// API endpoint: 返回探针状态
export const status = api(
  { expose: true, method: "GET", path: "/nezha/status" },
  async (): Promise<{ connected: boolean; uuid: string; server: string; lastReport: number }> => {
    return {
      connected,
      uuid: FIXED_UUID,
      server: NZ_SERVER,
      lastReport,
    };
  }
);

// API endpoint: 手动重连
export const reconnect = api(
  { expose: true, method: "POST", path: "/nezha/reconnect" },
  async (): Promise<{ ok: boolean }> => {
    connect();
    return { ok: true };
  }
);
