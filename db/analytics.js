import fs from "fs";
import duckdb from "duckdb";

const PARQUET_DIR = "./parquet";

async function runAnalytics() {
  if (!fs.existsSync(PARQUET_DIR)) {
    console.log(
      "Parquet folder does not exist yet. Run the export to generate data."
    );
    return;
  }

  const files = fs
    .readdirSync(PARQUET_DIR)
    .filter((f) => f.endsWith(".parquet"));
  if (files.length === 0) {
    console.log("No parquet files found in the parquet folder.");
    return;
  }

  const dbConn = new duckdb.Database(":memory:");
  const conn = dbConn.connect();

  // Build a UNION ALL query to read all parquet files
  const parquetPaths = files.map((f) => `${PARQUET_DIR}/${f}`);
  const unionSource = parquetPaths
    .map((p) => `SELECT * FROM read_parquet('${p}')`)
    .join(" UNION ALL ");

  console.log("\n--- Analytics Report ---");

  const versionQuery = `
    SELECT appVersion, COUNT(*) AS count
    FROM (${unionSource})
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
    FROM (${unionSource})
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
    FROM (${unionSource})
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
    FROM (${unionSource})
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
      FROM (${unionSource})
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

runAnalytics().catch(console.error);
