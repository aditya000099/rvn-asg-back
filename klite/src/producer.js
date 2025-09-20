import { encode } from "@msgpack/msgpack";

export function createProducer({ db, batchDelay = 10 }) {
  const ensuredTables = new Set();
  const pendingBatches = new Map(); // key: "topic:partition", value: { messages, callbacks, timer }

  async function ensureTable(topic, partition) {
    const tableName = `klite_${topic}_${partition}`;
    if (ensuredTables.has(tableName)) return;

    await db.execute(`
      CREATE TABLE IF NOT EXISTS "${tableName}" (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data BLOB NOT NULL,
        created DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `);
    ensuredTables.add(tableName);
  }

  async function flushBatch(topic, partition) {
    const key = `${topic}:${partition}`;
    const pending = pendingBatches.get(key);
    if (!pending || pending.messages.length === 0) return;

    pendingBatches.delete(key);
    clearTimeout(pending.timer);

    const tableName = `klite_${topic}_${partition}`;
    const batch = pending.messages.map((msg) => ({
      sql: `INSERT INTO "${tableName}" (data) VALUES (?)`,
      args: [encode(msg)],
    }));

    try {
      const results = await db.batch(batch);
      const firstOffset = Number(results[0].lastInsertRowid);

      // Resolve all callbacks with their respective offsets
      pending.callbacks.forEach((callback, index) => {
        callback.resolve({ offset: firstOffset + index });
      });
    } catch (error) {
      // Reject all callbacks
      pending.callbacks.forEach((callback) => callback.reject(error));
    }
  }

  async function send(topic, partition, data) {
    await ensureTable(topic, partition);

    const key = `${topic}:${partition}`;
    let pending = pendingBatches.get(key);

    if (!pending) {
      pending = { messages: [], callbacks: [], timer: null };
      pendingBatches.set(key, pending);
    }

    // Add message and create promise for this specific send
    const promise = new Promise((resolve, reject) => {
      pending.messages.push(data);
      pending.callbacks.push({ resolve, reject });
    });

    // Clear existing timer and set new one
    if (pending.timer) {
      clearTimeout(pending.timer);
    }

    pending.timer = setTimeout(() => {
      flushBatch(topic, partition);
    }, batchDelay);

    return promise;
  }

  async function sendBatch(topic, partition, messages) {
    await ensureTable(topic, partition);

    // For explicit batch sends, bypass the auto-batching and send immediately
    const tableName = `klite_${topic}_${partition}`;

    const batch = messages.map((msg) => ({
      sql: `INSERT INTO "${tableName}" (data) VALUES (?)`,
      args: [encode(msg)],
    }));

    const results = await db.batch(batch);
    const firstOffset = Number(results[0].lastInsertRowid);

    return {
      firstOffset,
      count: messages.length,
    };
  }

  // Flush all pending batches on shutdown
  async function flush() {
    const promises = [];
    for (const [key] of pendingBatches) {
      const [topic, partition] = key.split(":");
      promises.push(flushBatch(topic, partition));
    }
    await Promise.all(promises);
  }

  return { send, sendBatch, flush };
}
