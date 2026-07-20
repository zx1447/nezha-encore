"use node";

import { api } from "encore.dev/api";
import http2 from "http2";
import https from "https";
import os from "os";
import fs from "fs";

const NZ_SERVER = process.env.NZ_SERVER || "nz.zxydk1715.dpdns.org:443";
const NZ_SECRET = process.env.NZ_CLIENT_SECRET || "BFbvpxSlBTUugp3gDzezVKkZ22BV0CeL";
const AGENT_VERSION = "2.2.2";
const FIXED_UUID = "b7c8d9e0-f1a2-3b4c-5d6e-7f8a9b0c1d2e";

let session: http2.ClientHttp2Session | null = null;
let stateStream: http2.ClientHttp2Stream | null = null;
let stateTimer: NodeJS.Timeout | null = null;
let connected = false;
let lastError = "not started";
let lastReport = 0;
let publicIP = "";

// CPU 计算状态
let prevCpuTotal = 0;
let prevCpuBusy = 0;

// 网络统计状态
let prevNetIn = 0;
let prevNetOut = 0;
let prevNetTime = 0;

const PB = {
  varint: (v: number): Buffer => {
    let val = BigInt(v); if (val < 0n) val += (1n << 64n);
    const bytes: number[] = [];
    do { let byte = Number(val & 0x7fn); val >>= 7n; if (val > 0n) byte |= 0x80; bytes.push(byte); } while (val > 0n);
    return Buffer.from(bytes);
  },
  tag: (f: number, w: number): Buffer => PB.varint((f << 3) | w),
  u64: (f: number, v: number): Buffer => Buffer.concat([PB.tag(f, 0), PB.varint(v || 0)]),
  dbl: (f: number, v: number): Buffer => { const b = Buffer.alloc(8); b.writeDoubleLE(v || 0, 0); return Buffer.concat([PB.tag(f, 1), b]); },
  str: (f: number, v: string): Buffer => { if (!v) return Buffer.alloc(0); const s = Buffer.from(v, "utf8"); return Buffer.concat([PB.tag(f, 2), PB.varint(s.length), s]); },
  bytes: (f: number, v: Buffer): Buffer => { if (!v || !v.length) return Buffer.alloc(0); return Buffer.concat([PB.tag(f, 2), PB.varint(v.length), v]); },
  msg: (parts: Buffer[]): Buffer => Buffer.concat(parts.filter(x => x && x.length > 0)),
  frame: (m: Buffer): Buffer => { const h = Buffer.alloc(5); h[0] = 0; h.writeUInt32BE(m.length, 1); return Buffer.concat([h, m]); },
};

function readFile(path: string): string {
  try { return fs.readFileSync(path, "utf8"); } catch { return ""; }
}

// 获取公网 IP
function getPublicIP(): Promise<string> {
  return new Promise((resolve) => {
    const urls = ["https://api.ipify.org", "https://ifconfig.me/ip", "https://ipinfo.io/ip"];
    let resolved = false;
    urls.forEach(url => {
      https.get(url, { headers: { "User-Agent": "curl/7.88.1" }, timeout: 5000, rejectUnauthorized: false } as any, (res) => {
        let data = "";
        res.on("data", (c: Buffer) => data += c);
        res.on("end", () => { if (!resolved) { const ip = data.trim(); if (ip && /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(ip)) { resolved = true; resolve(ip); } } });
      }).on("error", () => {}).on("timeout", function(this: any) { this.destroy(); });
    });
    setTimeout(() => { if (!resolved) { resolved = true; resolve(""); } }, 10000);
  });
}

// 收集 Host 信息
function collectHost() {
  const cpuModels = [...new Set(os.cpus().map(c => c.model))];
  
  // 磁盘容量
  let diskTotal = 0;
  try {
    if (fs.statfsSync) {
      const stat = fs.statfsSync("/");
      if (stat && stat.blocks && stat.bsize && stat.bfree <= stat.blocks) {
        diskTotal = stat.blocks * stat.bsize;
      }
    }
  } catch {}
  
  // Swap
  let swapTotal = 0;
  try {
    const meminfo = readFile("/proc/meminfo");
    const m = meminfo.match(/SwapTotal:\s+(\d+)/);
    if (m) swapTotal = parseInt(m[1]) * 1024;
  } catch {}

  return {
    platform: os.platform(),
    platformVersion: os.release(),
    cpu: cpuModels,
    memTotal: os.totalmem(),
    diskTotal,
    swapTotal,
    arch: os.arch(),
    virtualization: "encore",
    bootTime: Math.floor(Date.now() / 1000 - os.uptime()),
    version: AGENT_VERSION,
    gpu: [],
  };
}

// 收集 State 信息
function collectState() {
  // CPU 使用率（对比两次 /proc/stat）
  let cpuPercent = 0;
  try {
    const procStat = readFile("/proc/stat");
    const fields = procStat.split("\n")[0].match(/cpu\s+(.*)/);
    if (fields) {
      const v = fields[1].trim().split(/\s+/).map(Number);
      const user = v[0]||0, nice = v[1]||0, system = v[2]||0;
      const idle = v[3]||0, iowait = v[4]||0, irq = v[5]||0;
      const softirq = v[6]||0, steal = v[7]||0;
      const total = user + nice + system + idle + iowait + irq + softirq + steal;
      const busy = total - idle - iowait;
      if (prevCpuTotal > 0) {
        const totalDiff = total - prevCpuTotal;
        const busyDiff = busy - prevCpuBusy;
        if (totalDiff > 0) cpuPercent = Math.max(0, Math.min(100, (busyDiff / totalDiff) * 100));
      }
      prevCpuTotal = total;
      prevCpuBusy = busy;
    }
  } catch {}

  // 内存
  const memTotal = os.totalmem();
  const memUsed = memTotal - os.freemem();

  // Swap
  let swapUsed = 0;
  try {
    const meminfo = readFile("/proc/meminfo");
    const swTotal = parseInt(meminfo.match(/SwapTotal:\s+(\d+)/)?.[1] || "0");
    const swFree = parseInt(meminfo.match(/SwapFree:\s+(\d+)/)?.[1] || "0");
    swapUsed = (swTotal - swFree) * 1024;
  } catch {}

  // 磁盘使用
  let diskUsed = 0;
  try {
    if (fs.statfsSync) {
      const stat = fs.statfsSync("/");
      if (stat && stat.blocks && stat.bsize && stat.bfree <= stat.blocks) {
        diskUsed = (stat.blocks - stat.bfree) * stat.bsize;
      }
    }
  } catch {}

  // 网络统计
  let netInTransfer = 0, netOutTransfer = 0, netInSpeed = 0, netOutSpeed = 0;
  try {
    const lines = readFile("/proc/net/dev").split("\n").slice(2);
    for (const line of lines) {
      const m = line.trim().match(/^([^:]+):\s*(\d+)\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+(\d+)/);
      if (m && !["lo","tun","docker","veth","br-","vnet","kube","Meta","tailscale","fw"].some(s => m[1].startsWith(s))) {
        netInTransfer += parseInt(m[2]);
        netOutTransfer += parseInt(m[3]);
      }
    }
    const now = Date.now();
    if (prevNetTime > 0) {
      const elapsed = (now - prevNetTime) / 1000;
      netInSpeed = Math.max(0, (netInTransfer - prevNetIn) / elapsed);
      netOutSpeed = Math.max(0, (netOutTransfer - prevNetOut) / elapsed);
    }
    prevNetIn = netInTransfer;
    prevNetOut = netOutTransfer;
    prevNetTime = now;
  } catch {}

  // TCP/UDP 连接
  let tcpConnCount = 0, udpConnCount = 0;
  try {
    readFile("/proc/net/tcp").split("\n").slice(1).forEach(l => { const p = l.trim().split(/\s+/); if (p.length >= 4 && p[3] === "01") tcpConnCount++; });
    readFile("/proc/net/udp").split("\n").slice(1).forEach(() => udpConnCount++);
  } catch {}

  // 进程数
  let processCount = 0;
  try { fs.readdirSync("/proc").forEach(e => { if (/^\d+$/.test(e)) processCount++; }); } catch {}

  return {
    cpu: cpuPercent, memUsed, swapUsed, diskUsed,
    netInTransfer, netOutTransfer, netInSpeed, netOutSpeed,
    uptime: Math.floor(os.uptime()),
    load1: os.loadavg()[0], load5: os.loadavg()[1], load15: os.loadavg()[2],
    tcpConnCount, udpConnCount, processCount, gpu: [],
  };
}

function sendUnary(path: string, msgBuf: Buffer): Promise<Buffer | null> {
  return new Promise((resolve, reject) => {
    if (!session || session.destroyed) { reject(new Error("no session")); return; }
    const headers: any = { ":method": "POST", ":path": path, "content-type": "application/grpc", "te": "trailers", "user-agent": `nezha-agent/${AGENT_VERSION}`, "client-secret": NZ_SECRET, "client-uuid": FIXED_UUID };
    const stream = session.request(headers);
    let chunks: Buffer[] = []; let done = false;
    stream.on("response", (h: any) => { const status = parseInt(h[":status"]); if (status && status !== 200) { if (!done) { done = true; reject(new Error(`HTTP ${status}`)); } } });
    stream.on("data", (c: Buffer) => { if (!done) chunks.push(c); });
    stream.on("trailers", (t: any) => { if (done) return; done = true; const gs = t["grpc-status"]; if (gs && gs !== "0") { reject(new Error(`gRPC ${gs}`)); } else { const full = Buffer.concat(chunks); resolve(full.length > 5 ? full.slice(5) : null); } });
    stream.on("error", (e: Error) => { if (!done) { done = true; reject(e); } });
    stream.end(PB.frame(msgBuf));
    setTimeout(() => { if (!done) { done = true; stream.close(http2.constants.HTTP2_STREAM_CANCEL); reject(new Error("timeout")); } }, 15000);
  });
}

async function connect() {
  try {
    if (stateTimer) clearInterval(stateTimer);
    if (stateStream) { try { stateStream.close(); } catch(e) {} }
    if (session) { try { session.destroy(); } catch(e) {} }
    
    lastError = "connecting...";
    publicIP = await getPublicIP();
    console.log("[Nezha] Public IP:", publicIP);
    
    const url = `https://${NZ_SERVER}`;
    session = http2.connect(url, { rejectUnauthorized: false });
    
    await new Promise<void>((resolve, reject) => {
      const to = setTimeout(() => { lastError = "timeout"; reject(new Error("connect timeout")); }, 10000);
      session!.on("connect", () => { clearTimeout(to); resolve(); });
      session!.on("error", (e: Error) => { clearTimeout(to); lastError = e.message; reject(e); });
    });
    
    // Host 上报
    const host = collectHost();
    const hostBuf = PB.msg([
      PB.str(1, host.platform), PB.str(2, host.platformVersion),
      // field 3 = cpu models (repeated string)
      ...host.cpu.map(c => PB.str(3, c)),
      PB.u64(4, host.memTotal),
      PB.u64(5, host.diskTotal),
      PB.u64(6, host.swapTotal),
      PB.str(7, host.arch),
      PB.str(8, host.virtualization),
      PB.u64(9, host.bootTime),
      PB.str(10, host.version),
    ]);
    try { await sendUnary("/proto.NezhaService/ReportSystemInfo2", hostBuf); console.log("[Nezha] Host reported, memTotal:", host.memTotal, "diskTotal:", host.diskTotal); } catch(e) { console.log("[Nezha] Host report failed:", (e as Error).message); }
    
    // GeoIP
    if (publicIP) {
      const ipInnerMsg = PB.msg([PB.str(1, publicIP)]);
      const geoBuf = PB.msg([PB.u64(1, publicIP.includes(":") ? 1 : 0), PB.bytes(2, ipInnerMsg)]);
      try { await sendUnary("/proto.NezhaService/ReportGeoIP", geoBuf); console.log("[Nezha] GeoIP reported:", publicIP); } catch(e) {}
    }
    
    // State 流
    const stateHeaders: any = { ":method": "POST", ":path": "/proto.NezhaService/ReportSystemState", "content-type": "application/grpc", "te": "trailers", "user-agent": `nezha-agent/${AGENT_VERSION}`, "client-secret": NZ_SECRET, "client-uuid": FIXED_UUID };
    stateStream = session.request(stateHeaders);
    stateStream.on("error", () => {});
    stateStream.on("close", () => { connected = false; lastError = "state stream closed"; });
    
    // 定期上报状态
    stateTimer = setInterval(() => {
      try {
        const s = collectState();
        const stateBuf = PB.msg([
          PB.dbl(1, s.cpu),
          PB.u64(2, Math.round(s.memUsed)),
          PB.u64(3, Math.round(s.swapUsed)),
          PB.u64(4, Math.round(s.diskUsed)),
          PB.u64(5, Math.round(s.netInTransfer)),
          PB.u64(6, Math.round(s.netOutTransfer)),
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
    }, 3000);
    
    connected = true;
    lastError = "connected ok, IP=" + publicIP;
    console.log("[Nezha] Agent running, UUID=" + FIXED_UUID + " IP=" + publicIP);
  } catch(e) {
    connected = false;
    lastError = `failed: ${(e as Error).message}`;
    console.log("[Nezha] Error:", lastError);
    setTimeout(() => connect(), 5000);
  }
}

connect();

export const status = api(
  { expose: true, method: "GET", path: "/nezha/status" },
  async (): Promise<{ connected: boolean; uuid: string; server: string; lastError: string; lastReport: number; ip: string }> => {
    return { connected, uuid: FIXED_UUID, server: NZ_SERVER, lastError, lastReport, ip: publicIP };
  }
);

export const reconnect = api(
  { expose: true, method: "POST", path: "/nezha/reconnect" },
  async (): Promise<{ ok: boolean; error: string }> => {
    const ok = await connect();
    return { ok, error: lastError };
  }
);
