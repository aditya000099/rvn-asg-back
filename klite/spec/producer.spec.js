import { describe, it, expect, beforeEach } from 'vitest';
import { createClient } from '@libsql/client';
import { createProducer } from '../src/producer.js';
import { decode } from '@msgpack/msgpack';

describe('Producer', () => {
  let db;
  let producer;

  beforeEach(async () => {
    db = createClient({
      url: ':memory:'
    });
    producer = createProducer({ db, batchDelay: 5 });
  });

  describe('send', () => {
    it('should send a single message to a topic partition', async () => {
      const message = { orderId: 123, amount: 99.99 };
      const result = await producer.send('orders', 0, message);
      
      expect(result).toHaveProperty('offset');
      expect(result.offset).toBe(1);
    });

    it('should create table on first send', async () => {
      await producer.send('test-topic', 0, { test: true });
      
      const result = await db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='klite_test-topic_0'"
      );
      
      expect(result.rows.length).toBe(1);
    });

    it('should store messages with correct data', async () => {
      const message = { key: 'value', number: 42 };
      await producer.send('test', 0, message);
      
      const result = await db.execute('SELECT * FROM klite_test_0');
      const row = result.rows[0];
      
      expect(row.id).toBe(1);
      expect(decode(row.data)).toEqual(message);
      expect(row.created).toBeDefined();
    });

    it('should batch multiple sends within delay window', async () => {
      const promises = [
        producer.send('batch-test', 0, { msg: 1 }),
        producer.send('batch-test', 0, { msg: 2 }),
        producer.send('batch-test', 0, { msg: 3 })
      ];
      
      const results = await Promise.all(promises);
      
      expect(results[0].offset).toBe(1);
      expect(results[1].offset).toBe(2);
      expect(results[2].offset).toBe(3);
      
      const dbResult = await db.execute('SELECT COUNT(*) as count FROM "klite_batch-test_0"');
      expect(dbResult.rows[0].count).toBe(3);
    });

    it('should handle different partitions independently', async () => {
      await producer.send('multi', 0, { partition: 0 });
      await producer.send('multi', 1, { partition: 1 });
      
      const result0 = await db.execute('SELECT * FROM klite_multi_0');
      const result1 = await db.execute('SELECT * FROM klite_multi_1');
      
      expect(result0.rows.length).toBe(1);
      expect(result1.rows.length).toBe(1);
      expect(decode(result0.rows[0].data)).toEqual({ partition: 0 });
      expect(decode(result1.rows[0].data)).toEqual({ partition: 1 });
    });
  });

  describe('sendBatch', () => {
    it('should send multiple messages in a single batch', async () => {
      const messages = [
        { id: 1, type: 'A' },
        { id: 2, type: 'B' },
        { id: 3, type: 'C' }
      ];
      
      const result = await producer.sendBatch('batch', 0, messages);
      
      expect(result.firstOffset).toBe(1);
      expect(result.count).toBe(3);
    });

    it('should bypass auto-batching for explicit batch sends', async () => {
      const messages = [{ msg: 1 }, { msg: 2 }];
      
      // Start auto-batch send
      const autoPromise = producer.send('test', 0, { msg: 0 });
      
      // Immediately send explicit batch (should not wait)
      const batchResult = await producer.sendBatch('test', 0, messages);
      
      // Auto-batch should complete after
      const autoResult = await autoPromise;
      
      expect(batchResult.firstOffset).toBe(1);
      expect(batchResult.count).toBe(2);
      expect(autoResult.offset).toBe(3);
    });

    it('should store all messages correctly', async () => {
      const messages = [
        { order: 1 },
        { order: 2 },
        { order: 3 }
      ];
      
      await producer.sendBatch('orders', 0, messages);
      
      const result = await db.execute('SELECT * FROM klite_orders_0 ORDER BY id');
      
      expect(result.rows.length).toBe(3);
      expect(decode(result.rows[0].data)).toEqual({ order: 1 });
      expect(decode(result.rows[1].data)).toEqual({ order: 2 });
      expect(decode(result.rows[2].data)).toEqual({ order: 3 });
    });
  });

  describe('flush', () => {
    it('should flush all pending batches', async () => {
      const promises = [
        producer.send('flush-test', 0, { msg: 1 }),
        producer.send('flush-test', 0, { msg: 2 }),
        producer.send('flush-test', 1, { msg: 3 })
      ];
      
      // Don't wait for auto-batch
      await producer.flush();
      
      const results = await Promise.all(promises);
      
      expect(results.every(r => r.offset > 0)).toBe(true);
      
      const result0 = await db.execute('SELECT COUNT(*) as count FROM "klite_flush-test_0"');
      const result1 = await db.execute('SELECT COUNT(*) as count FROM "klite_flush-test_1"');
      
      expect(result0.rows[0].count).toBe(2);
      expect(result1.rows[0].count).toBe(1);
    });
  });

  describe('error handling', () => {
    it('should handle table creation errors gracefully', async () => {
      // Close database to trigger error
      await db.close();
      
      await expect(producer.send('test', 0, { test: true }))
        .rejects.toThrow();
    });

    it('should only create table once per partition', async () => {
      // Send multiple messages to same partition
      await producer.send('once', 0, { msg: 1 });
      await producer.send('once', 0, { msg: 2 });
      await producer.send('once', 0, { msg: 3 });
      
      // Verify table exists
      const tables = await db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'klite_once_%'"
      );
      
      expect(tables.rows.length).toBe(1);
    });
  });
});