// loadtest.js
import fetch from "node-fetch";

const ENDPOINT = "http://localhost:8000/"; // adjust if different

// Possible values for randomization
const deviceIds = ["dev1", "dev2", "dev3"];
const appVersions = ["0.1.0", "0.2.0"];
const platforms = ["windows", "linux"];
const architectures = ["x86_64", "arm64"];

function randomChoice(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randomEvent() {
  return {
    deviceId: randomChoice(deviceIds),
    appVersion: randomChoice(appVersions),
    platform: randomChoice(platforms),
    architecture: randomChoice(architectures),
    timestamp: new Date().toISOString(),
  };
}

async function sendRequest(data) {
  try {
    await fetch(ENDPOINT, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
    });
  } catch (err) {
    console.error("Request failed:", err.message);
  }
}

async function runLoadTest() {
  let totalSent = 0;

  for (let sec = 0; sec < 10; sec++) {
    console.log(`Second ${sec + 1}...`);
    const promises = [];

    for (let i = 0; i < 100; i++) {
      promises.push(sendRequest(randomEvent()));
    }

    await Promise.all(promises);
    totalSent += 100;
    console.log(`Sent ${totalSent} events so far`);
    await new Promise((r) => setTimeout(r, 1000)); // wait 1s before next batch
  }

  console.log("Load test completed.");
}

runLoadTest();
