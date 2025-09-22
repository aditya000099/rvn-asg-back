// loadtest.js
import fetch from "node-fetch";
import { randomUUID } from "crypto";

const url = "http://localhost:8000/"; // your endpoint

// Some sample values
const deviceIds = [randomUUID(), randomUUID(), randomUUID()];
const appVersions = ["0.1.0", "0.2.0", "1.0.0"];
const platforms = ["windows", "linux", "mac"];
const architectures = ["x86_64", "arm64"];

function randomChoice(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

async function sendRequest() {
  const payload = {
    deviceId: randomChoice(deviceIds),
    appVersion: randomChoice(appVersions),
    platform: randomChoice(platforms),
    architecture: randomChoice(architectures),
    timestamp: new Date().toISOString(),
  };

  try {
    await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
  } catch (err) {
    console.error("Request failed:", err.message);
  }
}

async function runLoadTest() {
  const promises = [];
  for (let i = 0; i < 1000; i++) {
    promises.push(sendRequest());
  }
  await Promise.all(promises);
  console.log("✅ Sent 1000 requests in ~1s");
}

runLoadTest();
