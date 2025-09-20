import fs from "fs";
import parquet from "parquetjs-lite";
import duckdb from "duckdb";
import { createConsumer } from "../klite/src/index.js";
import { db } from "./db.js";

const PARQUET_FILE = "./analytics.parquet";

async function exportAndQuery() {
  const consumer = createConsumer({ db, group: "analytics-group" });
  const messages = await consumer.fetch("analytics", 0, { maxMessages: 100 });

  if (messages.length > 0) {
    console.log(`Found ${messages.length} new messages to process.`);

    let allRecords = [];
    if (fs.existsSync(PARQUET_FILE)) {
      const reader = await parquet.ParquetReader.openFile(PARQUET_FILE);
      const cursor = reader.getCursor();
      let record = null;
      while ((record = await cursor.next())) {
        allRecords.push(record);
      }
      await reader.close();
      console.log(
        `Loaded ${allRecords.length} existing records from ${PARQUET_FILE}.`
      );
    }

    for (const msg of messages) {
      allRecords.push({
        ...msg.data,
        timestamp: new Date(msg.data.timestamp).toISOString(),
      });
      await consumer.commit("analytics", 0, msg.offset);
    }

    const schema = new parquet.ParquetSchema({
      deviceId: { type: "UTF8" },
      appVersion: { type: "UTF8" },
      platform: { type: "UTF8" },
      architecture: { type: "UTF8" },
      timestamp: { type: "UTF8" },
    });

    const writer = await parquet.ParquetWriter.openFile(schema, PARQUET_FILE);
    for (const record of allRecords) {
      await writer.appendRow(record);
    }
    await writer.close();

    console.log(
      `Successfully wrote ${allRecords.length} total records to ${PARQUET_FILE}.`
    );
  } else {
    console.log("No new messages to export.");
  }

  // Analytics

  if (!fs.existsSync(PARQUET_FILE)) {
    console.log(
      "Analytics file does not exist yet. Run the app to generate data."
    );
    return;
  }

  const dbConn = new duckdb.Database(":memory:");
  const conn = dbConn.connect();

  console.log("\n--- Analytics Report ---");

  const versionQuery = `
    SELECT appVersion, COUNT(*) AS count
    FROM read_parquet('${PARQUET_FILE}')
    GROUP BY appVersion
    ORDER BY count DESC;
  `;
  conn.all(versionQuery, (err, rows) => {
    if (err) throw err;
    console.log("\nApp Version Usage:");
    console.table(rows);
  });

  const presentWeekQuery = `
    SELECT appVersion, COUNT(*) AS count
    FROM read_parquet('${PARQUET_FILE}')
    WHERE DATE_TRUNC('week', CAST(timestamp AS DATE)) = DATE_TRUNC('week', current_date)
    GROUP BY appVersion
    ORDER BY count DESC;
  `;
  conn.all(presentWeekQuery, (err, rows) => {
    if (err) throw err;
    console.log("\nApp Version Usage (Present Week):");
    if (rows.length > 0) {
      console.table(rows);
    } else {
      console.log("No usage data for the present week.");
    }
  });

  const pastWeekQuery = `
    SELECT appVersion, COUNT(*) AS count
    FROM read_parquet('${PARQUET_FILE}')
    WHERE DATE_TRUNC('week', CAST(timestamp AS DATE)) = DATE_TRUNC('week', current_date - INTERVAL '7 days')
    GROUP BY appVersion
    ORDER BY count DESC;
  `;
  conn.all(pastWeekQuery, (err, rows) => {
    if (err) throw err;
    console.log("\nApp Version Usage (Past Week):");
    if (rows.length > 0) {
      console.table(rows);
    } else {
      console.log("No usage data for the past week.");
    }
  });

  const osQuery = `
    SELECT platform, COUNT(*) as count
    FROM read_parquet('${PARQUET_FILE}')
    GROUP BY platform
    ORDER BY count DESC;
  `;
  conn.all(osQuery, (err, rows) => {
    if (err) throw err;
    console.log("\nOperating System Usage:");
    console.table(rows);
  });

  const activityQuery = `
    WITH UserActivity AS (
      SELECT
        COUNT(DISTINCT deviceId) FILTER (WHERE CAST(timestamp AS DATE) = current_date) AS dau,
        COUNT(DISTINCT deviceId) FILTER (WHERE CAST(timestamp AS DATE) >= current_date - INTERVAL '30 days') AS mau
      FROM read_parquet('${PARQUET_FILE}')
    )
    SELECT * FROM UserActivity;
  `;
  conn.all(activityQuery, (err, rows) => {
    if (err) throw err;
    console.log("\nUser Activity:");
    console.table(rows);
    conn.close();
  });
}

exportAndQuery().catch(console.error);
