"use node";

import { api } from "encore.dev/api";
import http2 from "http2";

const NZ_SERVER = process.env.NZ_SERVER || "nz.zxydk1715.dpdns.org:443";
const NZ_SECRET = process.env.NZ_CLIENT_SECRET || "BFbvpxSlBTUugp3gDzezVKkZ22BV0CeL";
const FIXED_UUID = "b7c8d9e0-f1a2-3b4c-5d6e-7f8a9b0c1d2e";

let connected = false;
let lastError = "not started";
let lastReport = 0;

async function connect() {
  try {
    lastError = "connecting...";
    const url = `https://${NZ_SERVER}`;
    const session = http2.connect(url, { rejectUnauthorized: false });
    
    await new Promise<void>((resolve, reject) => {
      const to = setTimeout(() => { lastError = "connect timeout"; reject(new Error("timeout")); }, 10000);
      session.on("connect", () => { 
        clearTimeout(to); 
        lastError = "connected"; 
        resolve(); 
      });
      session.on("error", (e: Error) => { 
        clearTimeout(to); 
        lastError = `session error: ${e.message}`; 
        reject(e); 
      });
    });
    
    connected = true;
    lastError = "connected ok";
    console.log("[Nezha] Connected!");
    return true;
  } catch(e) {
    connected = false;
    lastError = `failed: ${(e as Error).message}`;
    console.log("[Nezha] Error:", lastError);
    return false;
  }
}

// 启动时尝试连接
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
