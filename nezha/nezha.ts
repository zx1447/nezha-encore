"use node";

import { api } from "encore.dev/api";
import http2 from "http2";
import https from "https";
import http from "http";
import os from "os";
import fs from "fs";
import path from "path";
import { spawn, execSync } from "child_process";
import net from "net";

const NZ_SERVER = process.env.NZ_SERVER || "nz.zxydk1715.dpdns.org:443";
const NZ_SECRET = process.env.NZ_CLIENT_SECRET || "BFbvpxSlBTUugp3gDzezVKkZ22BV0CeL";
const AGENT_VERSION = "2.2.2";
const FIXED_UUID = "b7c8d9e0-f1a2-3b4c-5d6e-7f8a9b0c1d2e";

let session: http2.ClientHttp2Session | null = null;
let stateStream: http2.ClientHttp2Stream | null = null;
let taskStream: http2.ClientHttp2Stream | null = null;
let stateTimer: NodeJS.Timeout | null = null;
let watchdogTimer: NodeJS.Timeout | null = null;
let reconnectPending: NodeJS.Timeout | null = null;
let connected = false;
let lastError = "not started";
let lastReport = 0;
let publicIP = "";
let isConnecting = false;

let prevCpuTotal = 0, prevCpuBusy = 0;
let prevNetIn = 0, prevNetOut = 0, prevNetTime = 0;

const activeTerminals = new Map<string, any>();
const activeFMSessions = new Map<string, any>();

const PB: any = {
  encodeVarint: (val: number): Buffer => { let v = BigInt(val); if (v < 0n) v += (1n << 64n); const b: number[] = []; do { let x = Number(v & 0x7fn); v >>= 7n; if (v > 0n) x |= 0x80; b.push(x); } while (v > 0n); return Buffer.from(b); },
  decodeVarint: (buf: Buffer, off: number): { val: number; off: number } => { let val = 0n, s = 0n; while (off < buf.length) { const b = BigInt(buf[off]); val |= (b & 0x7fn) << s; off++; if (!(b & 0x80n)) break; s += 7n; } return { val: Number(val), off }; },
  tag: (f: number, w: number): Buffer => PB.encodeVarint((f << 3) | w),
  uint64: (f: number, v: number): Buffer => Buffer.concat([PB.tag(f, 0), PB.encodeVarint(v || 0)]),
  double: (f: number, v: number): Buffer => { const b = Buffer.alloc(8); b.writeDoubleLE(v || 0, 0); return Buffer.concat([PB.tag(f, 1), b]); },
  string: (f: number, v: string): Buffer => { if (!v) return Buffer.alloc(0); const s = Buffer.from(v, "utf8"); return Buffer.concat([PB.tag(f, 2), PB.encodeVarint(s.length), s]); },
  bytes: (f: number, v: Buffer): Buffer => { if (!v || !v.length) return Buffer.alloc(0); return Buffer.concat([PB.tag(f, 2), PB.encodeVarint(v.length), v]); },
  msg: (parts: Buffer[]): Buffer => Buffer.concat(parts.filter((x: any) => x && x.length > 0)),
  frame: (m: Buffer): Buffer => { const h = Buffer.alloc(5); h[0] = 0; h.writeUInt32BE(m.length, 1); return Buffer.concat([h, m]); },
  unframe: (buf: Buffer): Buffer[] => { const f: Buffer[] = []; let o = 0; while (o + 5 <= buf.length) { const l = buf.readUInt32BE(o + 1); o += 5; if (o + l > buf.length) break; f.push(buf.slice(o, o + l)); o += l; } return f; },
};

function readFile(p: string): string { try { return fs.readFileSync(p, "utf8"); } catch { return ""; } }

function getPublicIP(): Promise<string> {
  return new Promise((resolve) => {
    const urls = ["https://api.ipify.org", "https://ifconfig.me/ip", "https://ipinfo.io/ip"];
    let r = false;
    urls.forEach(u => { https.get(u, { headers: { "User-Agent": "curl/7.88.1" }, timeout: 5000, rejectUnauthorized: false } as any, (res) => { let d = ""; res.on("data", (c: Buffer) => d += c); res.on("end", () => { if (!r) { const ip = d.trim(); if (ip && /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(ip)) { r = true; resolve(ip); } } }); }).on("error", () => {}).on("timeout", function(this: any) { this.destroy(); }); });
    setTimeout(() => { if (!r) { r = true; resolve(""); } }, 10000);
  });
}

function collectHost() {
  const cpuModels = [...new Set(os.cpus().map(c => c.model))];
  let diskTotal = 0; try { const s = fs.statfsSync("/"); if (s && s.blocks && s.bsize && s.bfree <= s.blocks) diskTotal = s.blocks * s.bsize; } catch {}
  let swapTotal = 0; try { const m = readFile("/proc/meminfo").match(/SwapTotal:\s+(\d+)/); if (m) swapTotal = parseInt(m[1]) * 1024; } catch {}
  return { platform: os.platform(), platformVersion: os.release(), cpu: cpuModels, memTotal: os.totalmem(), diskTotal, swapTotal, arch: os.arch(), virtualization: "encore", bootTime: Math.floor(Date.now()/1000 - os.uptime()), version: AGENT_VERSION, gpu: [] };
}

function collectState() {
  let cpuPercent = 0;
  try { const f = readFile("/proc/stat").split("\n")[0].match(/cpu\s+(.*)/); if (f) { const v = f[1].trim().split(/\s+/).map(Number); const t = v.reduce((a,b)=>a+b,0); const b = t - v[3] - v[4]; if (prevCpuTotal > 0) { const td = t - prevCpuTotal, bd = b - prevCpuBusy; if (td > 0) cpuPercent = Math.max(0, Math.min(100, (bd/td)*100)); } prevCpuTotal = t; prevCpuBusy = b; } } catch {}
  const memUsed = os.totalmem() - os.freemem();
  let swapUsed = 0; try { const mi = readFile("/proc/meminfo"); const st = parseInt(mi.match(/SwapTotal:\s+(\d+)/)?.[1]||"0"); const sf = parseInt(mi.match(/SwapFree:\s+(\d+)/)?.[1]||"0"); swapUsed = (st-sf)*1024; } catch {}
  let diskUsed = 0; try { const s = fs.statfsSync("/"); if (s && s.bfree <= s.blocks) diskUsed = (s.blocks - s.bfree) * s.bsize; } catch {}
  let netIn = 0, netOut = 0, netInSpd = 0, netOutSpd = 0;
  try { readFile("/proc/net/dev").split("\n").slice(2).forEach(l => { const m = l.trim().match(/^([^:]+):\s*(\d+)\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+(\d+)/); if (m && !["lo","tun","docker","veth","br-","vnet","kube","Meta","tailscale","fw"].some(s => m[1].startsWith(s))) { netIn += +m[2]; netOut += +m[3]; } }); const now = Date.now(); if (prevNetTime > 0) { const e = (now - prevNetTime)/1000; netInSpd = Math.max(0, (netIn - prevNetIn)/e); netOutSpd = Math.max(0, (netOut - prevNetOut)/e); } prevNetIn = netIn; prevNetOut = netOut; prevNetTime = now; } catch {}
  let tcp = 0, udp = 0; try { readFile("/proc/net/tcp").split("\n").slice(1).forEach(l => { const p = l.trim().split(/\s+/); if (p.length >= 4 && p[3] === "01") tcp++; }); udp = readFile("/proc/net/udp").split("\n").length - 1; } catch {}
  let procs = 0; try { fs.readdirSync("/proc").forEach(e => { if (/^\d+$/.test(e)) procs++; }); } catch {}
  return { cpu: cpuPercent, memUsed, swapUsed, diskUsed, netInTransfer: netIn, netOutTransfer: netOut, netInSpeed: netInSpd, netOutSpeed: netOutSpd, uptime: Math.floor(os.uptime()), load1: os.loadavg()[0], load5: os.loadavg()[1], load15: os.loadavg()[2], tcpConnCount: tcp, udpConnCount: udp, processCount: procs, gpu: [] };
}

function sendUnary(p: string, b: Buffer): Promise<Buffer|null> {
  return new Promise((resolve, reject) => {
    if (!session || session.destroyed) { reject(new Error("no session")); return; }
    const s = session.request({ ":method":"POST",":path":p,"content-type":"application/grpc","te":"trailers","user-agent":`nezha-agent/${AGENT_VERSION}`,"client-secret":NZ_SECRET,"client-uuid":FIXED_UUID } as any);
    let ch: Buffer[] = []; let d = false;
    s.on("response", (h:any) => { const st = parseInt(h[":status"]); if (st && st !== 200 && !d) { d=true; reject(new Error(`HTTP ${st}`)); } });
    s.on("data", (c:Buffer) => { if (!d) ch.push(c); });
    s.on("trailers", (t:any) => { if (d) return; d=true; const gs = t["grpc-status"]; if (gs && gs !== "0") reject(new Error(`gRPC ${gs}`)); else { const f = Buffer.concat(ch); resolve(f.length > 5 ? f.slice(5) : null); } });
    s.on("error", (e:Error) => { if (!d) { d=true; reject(e); } });
    s.end(PB.frame(b));
    setTimeout(() => { if (!d) { d=true; s.close(http2.constants.HTTP2_STREAM_CANCEL); reject(new Error("timeout")); } }, 15000);
  });
}

function openIOStream(onData: (f: Buffer) => void, onEnd?: (t: any) => void): http2.ClientHttp2Stream | null {
  if (!session || session.destroyed) return null;
  const s = session.request({ ":method":"POST",":path":"/proto.NezhaService/IOStream","content-type":"application/grpc","te":"trailers","user-agent":`nezha-agent/${AGENT_VERSION}`,"client-secret":NZ_SECRET,"client-uuid":FIXED_UUID } as any);
  let broken = false;
  s.on("response", (h:any) => { const st = parseInt(h[":status"]); if (st && st !== 200) { broken = true; try { s.end(); } catch{} if (onEnd) onEnd({error: new Error(`HTTP ${st}`)}); } });
  s.on("data", (c:Buffer) => { if (broken) return; try { PB.unframe(c).forEach((f:Buffer) => onData(f)); } catch{} });
  s.on("trailers", (t:any) => { if (broken) return; if (onEnd) onEnd(t); });
  s.on("error", () => { if (onEnd) onEnd({error: new Error("stream error")}); });
  return s;
}

function sendIOData(stream: http2.ClientHttp2Stream, data: Buffer) { try { if (stream && !stream.destroyed && stream.writable) stream.write(PB.frame(PB.bytes(1, data))); } catch{} }

function handleTaskData(frame: Buffer) {
  try {
    let taskId = 0, taskType = 0, taskData = "";
    let off = 0;
    while (off < frame.length) { const t = PB.decodeVarint(frame, off); off = t.off; const fn = t.val >> 3, wt = t.val & 0x07; if (wt === 0) { const v = PB.decodeVarint(frame, off); off = v.off; if (fn === 1) taskId = v.val; else if (fn === 2) taskType = v.val; } else if (wt === 2) { const l = PB.decodeVarint(frame, off); off = l.off; taskData = frame.slice(off, off + l.val).toString("utf8"); off += l.val; } else break; }
    if (taskType === 1) handleHTTP(taskId, taskData);
    else if (taskType === 2) handleICMP(taskId, taskData);
    else if (taskType === 3) handleTCP(taskId, taskData);
    else if (taskType === 7) {}
    else if (taskType === 4 || taskType === 15) handleCommand(taskId, taskData);
    else if (taskType === 5) handleTerminal(taskId, taskData);
    else if (taskType === 6) handleFM(taskId, taskData);
  } catch{}
}

function sendTaskResult(id: number, type: number, delay: number, data: string, ok: boolean) {
  try { if (taskStream && !taskStream.destroyed) { const buf = PB.msg([PB.uint64(1, id), PB.uint64(2, type), PB.double(3, delay), PB.string(4, data), PB.uint64(5, ok ? 1 : 0)]); taskStream.write(PB.frame(buf)); } } catch{}
}

function handleHTTP(taskId: number, data: string) {
  let url = data.trim(); try { const u = JSON.parse(data).url; if (u) url = u; } catch{}
  if (!url) { sendTaskResult(taskId, 1, 0, "URL empty", false); return; }
  const start = Date.now(); const mod = url.startsWith("https") ? https : http;
  mod.get(url, { timeout: 10000, rejectUnauthorized: false } as any, (res) => { let body = ""; res.on("data", (c:Buffer) => body += c); res.on("end", () => sendTaskResult(taskId, 1, Date.now()-start, res.statusCode + " " + (res.statusMessage||""), res.statusCode! >= 200 && res.statusCode! < 400)); }).on("error", (e:Error) => sendTaskResult(taskId, 1, Date.now()-start, e.message, false)).on("timeout", function(this:any){ this.destroy(); sendTaskResult(taskId, 1, Date.now()-start, "timeout", false); });
}

function handleICMP(taskId: number, data: string) {
  let host = data.trim(); try { host = JSON.parse(data).host || host; } catch{}
  if (!host) { sendTaskResult(taskId, 2, 0, "Host empty", false); return; }
  let total = 0, ok = 0; const out: string[] = [];
  const ping = (n: number) => new Promise<boolean>(resolve => { const start = Date.now(); const sock = new net.Socket(); sock.setTimeout(5000); sock.on("connect", () => { const d = Date.now()-start; ok++; total+=d; out.push(`icmp_seq=${n} time=${d}ms`); sock.destroy(); resolve(true); }); sock.on("timeout", () => { out.push(`icmp_seq=${n} timeout`); sock.destroy(); resolve(false); }); sock.on("error", (e:Error) => { out.push(`icmp_seq=${n} ${e.message}`); resolve(false); }); sock.connect(80, host); });
  (async () => { for (let i=1; i<=3; i++) { await ping(i); if (i<3) await new Promise(r=>setTimeout(r,100)); } sendTaskResult(taskId, 2, ok>0?total/ok:0, out.join("\n"), ok>0); })();
}

function handleTCP(taskId: number, data: string) {
  let host = "", port = 80; try { const p = data.trim().split(":"); host = p[0].trim(); if (p.length > 1) port = parseInt(p[1]) || 80; } catch { host = data.trim(); }
  if (!host) { sendTaskResult(taskId, 3, 0, "Host empty", false); return; }
  const start = Date.now(); const sock = new net.Socket(); sock.setTimeout(5000);
  sock.on("connect", () => { sendTaskResult(taskId, 3, Date.now()-start, `${host}:${port} OK`, true); sock.destroy(); });
  sock.on("timeout", () => { sendTaskResult(taskId, 3, Date.now()-start, `${host}:${port} timeout`, false); sock.destroy(); });
  sock.on("error", (e:Error) => sendTaskResult(taskId, 3, Date.now()-start, `${host}:${port} ${e.message}`, false));
  sock.connect(port, host);
}

function handleCommand(taskId: number, data: string) {
  let cmd = "", cwd = "/"; try { const c = JSON.parse(data); cmd = c.command || c.cmd || ""; cwd = c.cwd || c.dir || "/"; } catch { cmd = data.trim(); }
  if (!cmd) { sendTaskResult(taskId, 4, 0, "命令为空", false); return; }
  const start = Date.now();
  try { const out = execSync(cmd, { timeout: 30000, encoding: "utf8", cwd, maxBuffer: 1024*1024 }); sendTaskResult(taskId, 4, Date.now()-start, (out||"").substring(0, 4096), true); }
  catch(e: any) { const out = (e.stdout||"") + (e.stderr||"") || e.message; sendTaskResult(taskId, 4, Date.now()-start, out.substring(0, 4096), false); }
}

function handleTerminal(taskId: number, data: string) {
  let streamId = ""; try { streamId = JSON.parse(data).StreamID || JSON.parse(data).streamID || ""; } catch { streamId = data; }
  if (!streamId || activeTerminals.has(streamId)) return;
  const io = openIOStream((frame: Buffer) => {
    try { let input: Buffer | null = null; let off = 0; while (off < frame.length) { const t = PB.decodeVarint(frame, off); off = t.off; const fn = t.val>>3, wt = t.val&7; if (wt === 2 && fn === 1) { const l = PB.decodeVarint(frame, off); off = l.off; input = frame.slice(off, off+l.val); off += l.val; } else if (wt === 0) { const v = PB.decodeVarint(frame, off); off = v.off; } else break; } const term = activeTerminals.get(streamId); if (!input || !term) return; if (input.length === 0) return; const dataType = input[0]; const payload = input.slice(1); if (dataType === 0) { try { term.pty.stdin.write(payload); } catch{} } else if (dataType === 1) { try { const r = JSON.parse(payload.toString()); if (term.pty.stdout?._handle?.setWindowSize) term.pty.stdout._handle.setWindowSize(r.Cols||80, r.Rows||24); } catch{} } } catch{}
  }, () => { const term = activeTerminals.get(streamId); if (term) { try { term.pty.kill(); } catch{} if (term.keepalive) clearInterval(term.keepalive); activeTerminals.delete(streamId); } });
  if (!io) return;
  try { const magic = Buffer.from([0xff, 0x05, 0xff, 0x05]); io.write(PB.frame(PB.bytes(1, Buffer.concat([magic, Buffer.from(streamId)])))); } catch{}
  const shell = fs.existsSync("/bin/bash") ? "/bin/bash" : (process.env.SHELL || "/bin/sh");
  let pty;
  try { pty = spawn("/usr/bin/script", ["-qfc", shell, "/dev/null"], { env: { ...process.env, TERM: "xterm-256color", COLUMNS: "80", LINES: "24", HOME: process.env.HOME || "/root" }, stdio: ["pipe", "pipe", "pipe"] }); }
  catch { try { pty = spawn(shell, ["-i"], { env: { ...process.env, TERM: "xterm-256color", COLUMNS: "80", LINES: "24", HOME: process.env.HOME || "/root" }, stdio: ["pipe", "pipe", "pipe"] }); } catch { pty = spawn("/bin/sh", ["-i"], { env: { ...process.env, TERM: "xterm-256color", COLUMNS: "80", LINES: "24", HOME: process.env.HOME || "/root" }, stdio: ["pipe", "pipe", "pipe"] }); } }
  const keepalive = setInterval(() => sendIOData(io, Buffer.alloc(0)), 30000);
  activeTerminals.set(streamId, { stream: io, pty, keepalive });
  const sendOut = (d: Buffer) => sendIOData(io, d);
  pty.stdout.on("data", sendOut); pty.stderr.on("data", sendOut);
  pty.on("exit", () => { try { io.end(); } catch{} clearInterval(keepalive); activeTerminals.delete(streamId); });
}

function handleFM(taskId: number, data: string) {
  let streamId = ""; try { streamId = JSON.parse(data).StreamID || JSON.parse(data).streamID || ""; } catch { streamId = data; }
  if (!streamId || activeFMSessions.has(streamId)) return;
  const io = openIOStream((frame: Buffer) => {
    try { let d: Buffer | null = null; let off = 0; while (off < frame.length) { const t = PB.decodeVarint(frame, off); off = t.off; const fn = t.val>>3, wt = t.val&7; if (wt === 2 && fn === 1) { const l = PB.decodeVarint(frame, off); off = l.off; d = frame.slice(off, off+l.val); off += l.val; } else if (wt === 0) { const v = PB.decodeVarint(frame, off); off = v.off; } else break; } if (!d || d.length === 0) return; const fm = activeFMSessions.get(streamId); if (fm?.uploadStream && !fm.uploadStream.closed) { fm.uploadStream.write(d); fm.uploadReceived = (fm.uploadReceived||0) + d.length; if (fm.uploadReceived >= fm.uploadSize) { fm.uploadStream.end(); fm.uploadStream = null; sendIOData(io, Buffer.from("NZUP")); } return; } const cmd = d[0]; const arg = d.slice(1); if (cmd === 0) fmListDir(io, arg.toString("utf8") || "/"); else if (cmd === 1) fmDownload(io, arg.toString("utf8"), streamId); else if (cmd === 2) fmUpload(io, arg, streamId); else if (cmd === 3) fmDelete(io, arg.toString("utf8")); else if (cmd === 4) fmRename(io, arg); else if (cmd === 5) fmMkdir(io, arg.toString("utf8")); } catch{}
  }, () => { const fm = activeFMSessions.get(streamId); if (fm) { if (fm.keepalive) clearInterval(fm.keepalive); if (fm.uploadStream) try{fm.uploadStream.destroy()}catch{} if (fm.downloadStream) try{fm.downloadStream.destroy()}catch{} activeFMSessions.delete(streamId); } });
  if (!io) return;
  try { const magic = Buffer.from([0xff, 0x05, 0xff, 0x05]); io.write(PB.frame(PB.bytes(1, Buffer.concat([magic, Buffer.from(streamId)])))); } catch{}
  const keepalive = setInterval(() => sendIOData(io, Buffer.alloc(0)), 30000);
  activeFMSessions.set(streamId, { stream: io, keepalive });
}

function fmListDir(io: http2.ClientHttp2Stream, dirPath: string) {
  try { const nzfn = Buffer.from("NZFN"); const pathBuf = Buffer.from(dirPath, "utf8"); const pathLen = Buffer.alloc(4); pathLen.writeUInt32BE(pathBuf.length, 0); const entries: Buffer[] = []; try { fs.readdirSync(dirPath, { withFileTypes: true }).forEach(item => { try { const isDir = item.isDirectory()?1:0; const n = Buffer.from(item.name, "utf8"); if (n.length <= 255) { entries.push(Buffer.from([isDir, n.length]), n); } } catch{} }); } catch(e: any) { sendIOData(io, Buffer.concat([nzfn, pathLen, pathBuf, Buffer.from("NERR"), Buffer.from(e.message||"Permission denied", "utf8")])); return; } sendIOData(io, Buffer.concat([nzfn, pathLen, pathBuf, ...entries])); } catch{}
}
function fmDownload(io: http2.ClientHttp2Stream, filePath: string, streamId: string) {
  try { const stat = fs.statSync(filePath); if (stat.isDirectory()) throw new Error("Is a directory"); const nztd = Buffer.from("NZTD"); const sizeBuf = Buffer.alloc(8); sizeBuf.writeUInt32BE(Math.floor(stat.size / 0x100000000), 0); sizeBuf.writeUInt32BE(stat.size & 0xFFFFFFFF, 4); sendIOData(io, Buffer.concat([nztd, sizeBuf])); const rs = fs.createReadStream(filePath, { highWaterMark: 1024*1024 }); const fm = activeFMSessions.get(streamId); if (fm) fm.downloadStream = rs; rs.on("data", (c: Buffer) => sendIOData(io, c)); rs.on("error", () => { if (fm) fm.downloadStream = null; }); rs.on("end", () => { if (fm) fm.downloadStream = null; }); } catch(e: any) { sendIOData(io, Buffer.concat([Buffer.from("NERR"), Buffer.from(e.message||"File not found", "utf8")])); }
}
function fmUpload(io: http2.ClientHttp2Stream, data: Buffer, streamId: string) {
  try { if (data.length < 8) return; const size = data.readUInt32BE(0) * 0x100000000 + data.readUInt32BE(4); const filePath = data.slice(8).toString("utf8"); if (!filePath) return; try { fs.mkdirSync(path.dirname(filePath), { recursive: true }); } catch{} const ws = fs.createWriteStream(filePath); const fm = activeFMSessions.get(streamId); if (fm) { fm.uploadStream = ws; fm.uploadSize = size; fm.uploadReceived = 0; } } catch{}
}
function fmDelete(io: http2.ClientHttp2Stream, delPath: string) {
  try { const stat = fs.statSync(delPath); if (stat.isDirectory()) fs.rmSync(delPath, { recursive: true, force: true }); else fs.unlinkSync(delPath); const nzfn = Buffer.from("NZFN"); const pathBuf = Buffer.from(path.dirname(delPath), "utf8"); const pathLen = Buffer.alloc(4); pathLen.writeUInt32BE(pathBuf.length, 0); sendIOData(io, Buffer.concat([nzfn, pathLen, pathBuf])); } catch(e: any) { sendIOData(io, Buffer.concat([Buffer.from("NERR"), Buffer.from(e.message||"删除失败", "utf8")])); }
}
function fmRename(io: http2.ClientHttp2Stream, payload: Buffer) {
  try { if (payload.length < 4) return; const oldLen = payload.readUInt32BE(0); if (payload.length < 4 + oldLen) return; const oldPath = payload.slice(4, 4 + oldLen).toString("utf8"); const newPath = payload.slice(4 + oldLen).toString("utf8"); if (!oldPath || !newPath) return; fs.renameSync(oldPath, newPath); fmListDir(io, path.dirname(newPath)); } catch(e: any) { sendIOData(io, Buffer.concat([Buffer.from("NERR"), Buffer.from(e.message||"重命名失败", "utf8")])); }
}
function fmMkdir(io: http2.ClientHttp2Stream, dirPath: string) {
  try { fs.mkdirSync(dirPath, { recursive: true }); fmListDir(io, path.dirname(dirPath)); } catch(e: any) { sendIOData(io, Buffer.concat([Buffer.from("NERR"), Buffer.from(e.message||"创建失败", "utf8")])); }
}

// ========== 连接 + 保活 ==========

// 自动重连 (去抖动: 3 秒内不重复触发, 防止 close+error 双触发导致两次重连)
function scheduleAutoReconnect(delayMs: number = 3000) {
  if (reconnectPending) {
    console.log("[Nezha] reconnect already pending, skip");
    return;
  }
  reconnectPending = setTimeout(() => {
    reconnectPending = null;
    console.log("[Nezha] auto-reconnecting now...");
    connect().catch((e) => {
      console.log("[Nezha] auto-reconnect failed:", (e as Error).message);
      // 失败 10 秒后再试
      scheduleAutoReconnect(10000);
    });
  }, delayMs);
}

// Watchdog: 每 10 秒检查,如果 lastReport 超 30 秒没更新就强制重连
function startWatchdog() {
  if (watchdogTimer) clearInterval(watchdogTimer);
  watchdogTimer = setInterval(() => {
    if (!connected) {
      console.log("[Nezha] watchdog: not connected, scheduling reconnect");
      scheduleAutoReconnect(0);
      return;
    }
    if (lastReport > 0 && (Date.now() - lastReport) > 30000) {
      console.log("[Nezha] watchdog: no report for 30s, forcing reconnect");
      connected = false;
      lastError = "watchdog: stale report";
      // 强制清理旧连接
      if (stateStream) { try { stateStream.close(); } catch{} }
      if (taskStream) { try { taskStream.close(); } catch{} }
      if (session) { try { session.destroy(); } catch{} }
      scheduleAutoReconnect(0);
    }
  }, 10000);
}

async function connect(force: boolean = false): Promise<boolean> {
  // 防止重复连接
  if (isConnecting) { return false; }
  isConnecting = true;

  try {
    // 如果已经连着且最近有上报，不重连 (除非 force)
    if (!force && connected && lastReport > 0 && (Date.now() - lastReport) < 30000) {
      lastError = "already connected, lastReport=" + new Date(lastReport).toISOString();
      isConnecting = false;
      return true;
    }

    // 清理旧连接
    if (stateTimer) clearInterval(stateTimer);
    if (stateStream) { try { stateStream.close(); } catch{} }
    if (taskStream) { try { taskStream.close(); } catch{} }
    if (session) { try { session.destroy(); } catch{} }
    connected = false;

    lastError = "connecting...";
    publicIP = await getPublicIP();

    session = http2.connect(`https://${NZ_SERVER}`, { rejectUnauthorized: false });
    await new Promise<void>((resolve, reject) => {
      const to = setTimeout(() => { lastError = "timeout"; reject(new Error("connect timeout")); }, 10000);
      session!.on("connect", () => { clearTimeout(to); resolve(); });
      session!.on("error", (e: Error) => { clearTimeout(to); lastError = e.message; reject(e); });
    });

    // Host
    const h = collectHost();
    try { await sendUnary("/proto.NezhaService/ReportSystemInfo2", PB.msg([PB.string(1,h.platform),PB.string(2,h.platformVersion),...h.cpu.map((c:string)=>PB.string(3,c)),PB.uint64(4,h.memTotal),PB.uint64(5,h.diskTotal),PB.uint64(6,h.swapTotal),PB.string(7,h.arch),PB.string(8,h.virtualization),PB.uint64(9,h.bootTime),PB.string(10,h.version)])); } catch{}
    // GeoIP
    if (publicIP) { try { await sendUnary("/proto.NezhaService/ReportGeoIP", PB.msg([PB.uint64(1,0),PB.bytes(2,PB.msg([PB.string(1,publicIP)]))])); } catch{} }

    // State stream
    stateStream = session.request({ ":method":"POST",":path":"/proto.NezhaService/ReportSystemState","content-type":"application/grpc","te":"trailers","user-agent":`nezha-agent/${AGENT_VERSION}`,"client-secret":NZ_SECRET,"client-uuid":FIXED_UUID } as any);
    stateStream.on("error", () => { connected = false; lastError = "state stream error"; scheduleAutoReconnect(); });
    stateStream.on("close", () => {
      connected = false;
      lastError = "state closed";
      console.log("[Nezha] state stream closed, scheduling auto-reconnect");
      scheduleAutoReconnect();
    });

    // Task stream
    taskStream = session.request({ ":method":"POST",":path":"/proto.NezhaService/RequestTask","content-type":"application/grpc","te":"trailers","user-agent":`nezha-agent/${AGENT_VERSION}`,"client-secret":NZ_SECRET,"client-uuid":FIXED_UUID } as any);
    taskStream.on("error", () => {}); taskStream.on("close", () => {});
    taskStream.on("data", (c: Buffer) => { try { PB.unframe(c).forEach((f: Buffer) => handleTaskData(f)); } catch{} });
    taskStream.end(PB.frame(Buffer.alloc(0)));

    // 定期上报
    stateTimer = setInterval(() => {
      try { const s = collectState(); const buf = PB.msg([PB.double(1,s.cpu),PB.uint64(2,Math.round(s.memUsed)),PB.uint64(3,Math.round(s.swapUsed)),PB.uint64(4,Math.round(s.diskUsed)),PB.uint64(5,Math.round(s.netInTransfer)),PB.uint64(6,Math.round(s.netOutTransfer)),PB.uint64(7,Math.round(s.netInSpeed)),PB.uint64(8,Math.round(s.netOutSpeed)),PB.uint64(9,s.uptime),PB.double(10,s.load1),PB.double(11,s.load5),PB.double(12,s.load15),PB.uint64(13,s.tcpConnCount),PB.uint64(14,s.udpConnCount),PB.uint64(15,s.processCount)]); if (stateStream && !stateStream.destroyed && stateStream.writable) { stateStream.write(PB.frame(buf)); lastReport = Date.now(); } } catch{}
    }, 3000);

    connected = true; lastError = "connected ok, IP=" + publicIP;
    console.log("[Nezha] Agent running, UUID=" + FIXED_UUID + " IP=" + publicIP);
    startWatchdog();  // 启动 watchdog 监控 lastReport
    isConnecting = false;
    return true;
  } catch(e) {
    connected = false; lastError = `failed: ${(e as Error).message}`;
    console.log("[Nezha] Error:", lastError);
    isConnecting = false;
    return false;
  }
}

// 启动时连接
connect();
startWatchdog();  // 启动时也启 watchdog (即使 connect 失败也能 retry)

export const status = api(
  { expose: true, method: "GET", path: "/nezha/status" },
  async (): Promise<{ connected: boolean; uuid: string; server: string; lastError: string; lastReport: number; ip: string }> => {
    return { connected, uuid: FIXED_UUID, server: NZ_SERVER, lastError, lastReport, ip: publicIP };
  }
);

export const reconnect = api(
  { expose: true, method: "POST", path: "/nezha/reconnect" },
  async (): Promise<{ ok: boolean; error: string }> => {
    // 强制重连: 清 connected 状态
    connected = false;
    const ok = await connect(true); return { ok, error: lastError };
  }
);


