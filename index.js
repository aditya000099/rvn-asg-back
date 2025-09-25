import express from "express";
import bodyParser from "body-parser";
import { analyticsProducer, db } from "./db/db.js";
import { writeBatchToParquet } from "./db/parquet.js";
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
  console.log(req.body);

  if (Array.isArray(req.body.messages) && req.body.messages.length > 0) {
    const first = req.body.messages[0];
    if (first && first.data) console.log(first.data);
    else console.log(first);
  } else if (req.body.data) {
    // console.log(req.body.data);
  } else {
    const { deviceId, appVersion, platform, architecture, timestamp } =
      req.body;
    if (deviceId || appVersion || platform || architecture || timestamp) {
      console.log({ deviceId, appVersion, platform, architecture, timestamp });
    }
  }
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

app.post("/export", async (req, res) => {
  try {
    const payload = req.body;
    if (!payload || !payload.messages || !Array.isArray(payload.messages)) {
      return res
        .status(400)
        .json({ message: "Invalid payload: messages array required" });
    }

    const { file, count } = await writeBatchToParquet(payload.messages);

    return res
      .status(201)
      .json({ message: "Exported to parquet", file, count });
  } catch (err) {
    console.error("/export error:", err);
    return res
      .status(500)
      .json({ message: "Export failed", error: String(err) });
  }
});

const config = {
  topics: {
    analytics: {
      consumerGroups: {
        default: {
          partitions: [0],
          endpoint: "http://localhost:8000/export",
          batchSize: 50,
          interval: "5s",
        },
      },
    },
  },
};

app.listen(8000, async () => {
  await startWorker({ db, config })
    .then(() => console.log("Worker running"))
    .catch((err) => console.error("Worker error:", err));
  console.log("Server running at port 8000");
});
