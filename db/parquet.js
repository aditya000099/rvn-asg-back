import fs from "fs";
import parquet from "parquetjs-lite";
import { createConsumer } from "../klite/src/index.js";
import { db } from "./db.js";

const PARQUET_DIR = "./parquet";
const BATCH_LIMIT = 1000;

function getNewParquetFileName() {
  const now = new Date();
  const ts = now.toISOString().replace(/[:.]/g, "-");
  return `${PARQUET_DIR}/analytics-${ts}.parquet`;
}

const schema = new parquet.ParquetSchema({
  deviceId: { type: "UTF8" },
  appVersion: { type: "UTF8" },
  platform: { type: "UTF8" },
  architecture: { type: "UTF8" },
  timestamp: { type: "UTF8" },
});

async function exportToParquet() {
  const consumer = createConsumer({ db, group: "analytics-group" });
  if (!fs.existsSync(PARQUET_DIR)) {
    fs.mkdirSync(PARQUET_DIR);
  }

  let totalExported = 0;
  let fileCount = 0;
  let done = false;

  while (!done) {
    let batch = [];
    while (batch.length < BATCH_LIMIT) {
      const messages = await consumer.fetch("analytics", 0, {
        maxMessages: Math.min(100, BATCH_LIMIT - batch.length),
      });

      if (messages.length === 0) {
        done = true;
        break;
      }

      console.log(
        `Fetched ${messages.length} messages (offsets ${messages[0].offset} → ${
          messages[messages.length - 1].offset
        })`
      );

      batch.push(...messages);
    }

    if (batch.length === 0) {
      if (fileCount === 0) {
        console.log("No new messages to export.");
      }
      break;
    }

    fileCount++;
    const parquetFile = getNewParquetFileName();
    const writer = await parquet.ParquetWriter.openFile(schema, parquetFile);

    for (const msg of batch) {
      await writer.appendRow({
        ...msg.data,
        timestamp: new Date(msg.data.timestamp).toISOString(),
      });
    }

    // ✅ Commit once for the whole batch
    const lastOffset = batch[batch.length - 1].offset;
    await consumer.commit("analytics", 0, lastOffset);

    await writer.close();
    totalExported += batch.length;

    console.log(
      `Wrote ${batch.length} records to ${parquetFile} and committed offset ${lastOffset}.`
    );
  }

  if (totalExported > 0) {
    console.log(
      `✅ Exported a total of ${totalExported} records in ${fileCount} file(s).`
    );
  }
}

exportToParquet().catch(console.error);
