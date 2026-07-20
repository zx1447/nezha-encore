"use node";

import { api } from "encore.dev/api";
import http2 from "http2";
import https from "https";
import os from "os";

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
  msg: (parts: Buffer[]): Buffer => Buffer.concat(parts.filter(x => x && x.length > 0)),
  frame: (m: Buffer): Buffer => { const h = Buffer.alloc(5); h[0] = 0; h.writeUInt32BE(m.length, 1); return Buffer.concat([h, m]); },
};

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
    const url = `https://${NZ_SERVER}`;
    session = http2.connect(url, { rejectUnauthorized: false });
    
    await new Promise<void>((resolve, reject) => {
      const to = setTimeout(() => { lastError = "timeout"; reject(new Error("connect timeout")); }, 10000);
      session!.on("connect", () => { clearTimeout(to); resolve(); });
      session!.on("error", (e: Error) => { clearTimeout(to); lastError = e.message; reject(e); });
    });
    
    // Host 上报
    const hostBuf = PB.msg([
      PB.str(1, os.platform()), PB.str(2, os.release()),
      PB.u64(4, os.totalmem()), PB.str(7, os.arch()),
      PB.str(8, "encore"), PB.u64(9, Math.floor(Date.now()/1000 - os.uptime())),
      PB.str(10, AGENT_VERSION),
    ]);
    try { await sendUnary("/proto.NezhaService/ReportSystemInfo2", hostBuf); } catch(e) {}
    
    // GeoIP
    try { await sendUnary("/proto.NezhaService/ReportGeoIP", PB.msg([PB.u64(1, 0), PB.str(2, "")])); } catch(e) {}
    
    // State 流
    const stateHeaders: any = { ":method": "POST", ":path": "/proto.NezhaService/ReportSystemState", "content-type": "application/grpc", "te": "trailers", "user-agent": `nezha-agent/${AGENT_VERSION}`, "client-secret": NZ_SECRET, "client-uuid": FIXED_UUID };
    stateStream = session.request(stateHeaders);
    stateStream.on("error", () => {});
    stateStream.on("close", () => { connected = false; lastError = "state stream closed"; });
    
    // 定期上报
    stateTimer = setInterval(() => {
      try {
        const memUsed = os.totalmem() - os.freemem();
        const stateBuf = PB.msg([
          PB.dbl(1, 0), PB.u64(2, Math.round(memUsed)),
          PB.u64(3, 0), PB.u64(4, 0),
          PB.u64(5, 0), PB.u64(6, 0),
          PB.u64(7, 0), PB.u64(8, 0),
          PB.u64(9, Math.floor(os.uptime())),
          PB.dbl(10, os.loadavg()[0]), PB.dbl(11, os.loadavg()[1]), PB.dbl(12, os.loadavg()[2]),
          PB.u64(13, 0), PB.u64(14, 0), PB.u64(15, 0),
        ]);
        if (stateStream && !stateStream.destroyed && stateStream.writable) {
          stateStream.write(PB.frame(stateBuf));
          lastReport = Date.now();
        }
      } catch(e) {}
    }, 3000);
    
    connected = true;
    lastError = "connected ok";
    console.log("[Nezha] Agent running, UUID=" + FIXED_UUID);
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
  async (): Promise<{ connected: boolean; uuid: string; server: string; lastError: string; lastReport: number }> => {
    return { connected, uuid: FIXED_UUID, server: NZ_SERVER, lastError, lastReport };
  }
);

export const reconnect = api(
  { expose: true, method: "POST", path: "/nezha/reconnect" },
  async (): Promise<{ ok: boolean; error: string }> => {
    const ok = await connect();
    return { ok, error: lastError };
  }
);
