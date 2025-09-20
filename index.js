import express from "express";
import bodyParser from "body-parser";
import { analyticsProducer, db } from "./db/db.js";
import cors from "cors";
import { startWorker } from "./klite/src/worker.js";

const app = express();

app.use(bodyParser.json());

app.use(
  cors({
    origin: "http://localhost:1420",
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type"],
  })
);

app.get("/", async (req, res) => {
  res.send("working");
});

app.post("/", async (req, res) => {
  try {
    const { deviceId, appVersion, platform, architecture, timestamp } =
      req.body;

    if (!deviceId || !appVersion || !platform || !architecture) {
      return res.status(400).json({ message: "Missing required fields" });
    }

    const logEntry = {
      deviceId,
      appVersion,
      platform,
      architecture,
      timestamp: timestamp || new Date().toISOString(),
    };

    const { offset } = await analyticsProducer.send("analytics", 0, logEntry);

    res.status(201).json({ message: "Analytics stored", offset });
  } catch (err) {
    console.error("Send error:", err);
    res.status(500).json({ message: "Queue error" });
  }
});

const config = {
  topics: {
    analytics: {
      consumerGroups: {
        default: {
          partitions: [0],
          endpoint: "https://api.example.com/analytics",
          batchSize: 50,
          interval: "5s",
        },
      },
    },
    orders: {
      consumerGroups: {
        "order-processor": {
          partitions: [0, 1],
          endpoint: "https://api.example.com/process-orders",
          batchSize: 50,
          interval: "5s",
        },
      },
    },
  },
};

app.listen(8000, async () => {
  await startWorker({ db, config, signal: AbortSignal.timeout(0) })
    .then(() => console.log("Worker running"))
    .catch((err) => console.error("Worker error:", err));
  console.log("Server running at port 8000");
});
