import { decode } from "@msgpack/msgpack";

export function createConsumer({ db, group }) {
  let offsetTableEnsured = false;
  const committedOffsets = new Set();

  async function ensureOffsetTable() {
    if (offsetTableEnsured) return;

    await db.execute(`
      CREATE TABLE IF NOT EXISTS klite_consumer_offsets (
        consumer_group VARCHAR NOT NULL,
        topic VARCHAR NOT NULL,
        partition INTEGER NOT NULL,
        commit_offset INTEGER NOT NULL,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (consumer_group, topic, partition)
      )
    `);
    offsetTableEnsured = true;
  }

  async function getLastOffset(topic, partition) {
    await ensureOffsetTable();

    const key = `${group}:${topic}:${partition}`;

    const result = await db.execute({
      sql: `SELECT commit_offset FROM klite_consumer_offsets 
            WHERE consumer_group = ? AND topic = ? AND partition = ?`,
      args: [group, topic, partition],
    });

    if (result.rows.length > 0) {
      committedOffsets.add(key);
      return result.rows[0].commit_offset;
    }
    return -1;
  }

  async function fetch(topic, partition, options = {}) {
    const maxMessages = options.maxMessages || 100;
    const tableName = `klite_${topic}_${partition}`;

    const lastOffset = await getLastOffset(topic, partition);

    try {
      const result = await db.execute({
        sql: `SELECT id as offset, data, created FROM "${tableName}" 
              WHERE id > ? 
              ORDER BY id ASC 
              LIMIT ?`,
        args: [lastOffset, maxMessages],
      });

      return result.rows.map((row) => ({
        offset: Number(row.offset),
        data: decode(row.data),
        created: row.created,
      }));
    } catch (error) {
      // Table doesn't exist yet - return empty array
      if (error.message?.includes("no such table")) {
        return [];
      }
      throw error;
    }
  }

  async function commit(topic, partition, offset) {
    await ensureOffsetTable();

    const key = `${group}:${topic}:${partition}`;

    if (committedOffsets.has(key)) {
      // Row exists, use UPDATE directly
      await db.execute({
        sql: `UPDATE klite_consumer_offsets 
              SET commit_offset = ?, updated_at = CURRENT_TIMESTAMP
              WHERE consumer_group = ? AND topic = ? AND partition = ?`,
        args: [offset, group, topic, partition],
      });
    } else {
      // First commit, try INSERT
      try {
        await db.execute({
          sql: `INSERT INTO klite_consumer_offsets (consumer_group, topic, partition, commit_offset, updated_at) 
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)`,
          args: [group, topic, partition, offset],
        });
        committedOffsets.add(key);
      } catch (err) {
        // Row already exists (race condition), use UPDATE
        await db.execute({
          sql: `UPDATE klite_consumer_offsets 
                SET commit_offset = ?, updated_at = CURRENT_TIMESTAMP
                WHERE consumer_group = ? AND topic = ? AND partition = ?`,
          args: [offset, group, topic, partition],
        });
        committedOffsets.add(key);
      }
    }
  }

  return { fetch, commit };
}
