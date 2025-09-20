import { describe, it, expect, beforeEach } from 'vitest';
import { createClient } from '@libsql/client';
import { createProducer } from '../src/producer.js';
import { createConsumer } from '../src/consumer.js';

describe('Consumer', () => {
  let db;
  let producer;
  let consumer;

  beforeEach(async () => {
    db = createClient({
      url: ':memory:'
    });
    producer = createProducer({ db, batchDelay: 0 }); // No delay for tests
    consumer = createConsumer({ db, group: 'test-group' });
  });

  describe('fetch', () => {
    it('should fetch messages from a partition', async () => {
      await producer.send('test', 0, { msg: 1 });
      await producer.send('test', 0, { msg: 2 });
      await producer.send('test', 0, { msg: 3 });

      const messages = await consumer.fetch('test', 0);

      expect(messages.length).toBe(3);
      expect(messages[0].data).toEqual({ msg: 1 });
      expect(messages[1].data).toEqual({ msg: 2 });
      expect(messages[2].data).toEqual({ msg: 3 });
      expect(messages[0].offset).toBe(1);
      expect(messages[1].offset).toBe(2);
      expect(messages[2].offset).toBe(3);
    });

    it('should respect maxMessages option', async () => {
      for (let i = 1; i <= 10; i++) {
        await producer.send('test', 0, { msg: i });
      }

      const messages = await consumer.fetch('test', 0, { maxMessages: 5 });

      expect(messages.length).toBe(5);
      expect(messages[0].data).toEqual({ msg: 1 });
      expect(messages[4].data).toEqual({ msg: 5 });
    });

    it('should return empty array when no messages', async () => {
      const messages = await consumer.fetch('empty', 0);
      expect(messages).toEqual([]);
    });

    it('should only fetch uncommitted messages', async () => {
      await producer.send('test', 0, { msg: 1 });
      await producer.send('test', 0, { msg: 2 });
      await producer.send('test', 0, { msg: 3 });

      // Fetch and commit first two
      let messages = await consumer.fetch('test', 0);
      await consumer.commit('test', 0, 2);

      // Fetch again - should only get message 3
      messages = await consumer.fetch('test', 0);

      expect(messages.length).toBe(1);
      expect(messages[0].data).toEqual({ msg: 3 });
      expect(messages[0].offset).toBe(3);
    });

    it('should handle multiple partitions independently', async () => {
      await producer.send('multi', 0, { partition: 0 });
      await producer.send('multi', 1, { partition: 1 });

      const messages0 = await consumer.fetch('multi', 0);
      const messages1 = await consumer.fetch('multi', 1);

      expect(messages0.length).toBe(1);
      expect(messages1.length).toBe(1);
      expect(messages0[0].data).toEqual({ partition: 0 });
      expect(messages1[0].data).toEqual({ partition: 1 });
    });
  });

  describe('commit', () => {
    it('should commit offset for a partition', async () => {
      await producer.send('test', 0, { msg: 1 });
      await producer.send('test', 0, { msg: 2 });

      await consumer.commit('test', 0, 1);

      const messages = await consumer.fetch('test', 0);
      expect(messages.length).toBe(1);
      expect(messages[0].data).toEqual({ msg: 2 });
    });

    it('should update offset on subsequent commits', async () => {
      await producer.send('test', 0, { msg: 1 });
      await producer.send('test', 0, { msg: 2 });
      await producer.send('test', 0, { msg: 3 });

      await consumer.commit('test', 0, 1);
      let messages = await consumer.fetch('test', 0);
      expect(messages.length).toBe(2);

      await consumer.commit('test', 0, 2);
      messages = await consumer.fetch('test', 0);
      expect(messages.length).toBe(1);

      await consumer.commit('test', 0, 3);
      messages = await consumer.fetch('test', 0);
      expect(messages.length).toBe(0);
    });

    it('should use UPDATE after first commit for performance', async () => {
      await producer.send('test', 0, { msg: 1 });
      await producer.send('test', 0, { msg: 2 });
      await producer.send('test', 0, { msg: 3 });

      // First commit - will INSERT
      await consumer.commit('test', 0, 1);
      
      // Second commit - should UPDATE
      await consumer.commit('test', 0, 2);
      
      // Verify the offset was updated
      const result = await db.execute({
        sql: 'SELECT commit_offset FROM klite_consumer_offsets WHERE consumer_group = ? AND topic = ? AND partition = ?',
        args: ['test-group', 'test', 0]
      });
      
      expect(result.rows[0].commit_offset).toBe(2);
    });

    it('should handle commits for different partitions independently', async () => {
      await producer.send('multi', 0, { msg: 1 });
      await producer.send('multi', 0, { msg: 2 });
      await producer.send('multi', 1, { msg: 3 });
      await producer.send('multi', 1, { msg: 4 });

      await consumer.commit('multi', 0, 1);
      await consumer.commit('multi', 1, 2);

      const messages0 = await consumer.fetch('multi', 0);
      const messages1 = await consumer.fetch('multi', 1);

      expect(messages0.length).toBe(1);
      expect(messages0[0].offset).toBe(2);
      expect(messages1.length).toBe(0); // All committed in partition 1
    });
  });

  describe('consumer groups', () => {
    it('should track offsets independently per consumer group', async () => {
      const consumer1 = createConsumer({ db, group: 'group1' });
      const consumer2 = createConsumer({ db, group: 'group2' });

      await producer.send('test', 0, { msg: 1 });
      await producer.send('test', 0, { msg: 2 });
      await producer.send('test', 0, { msg: 3 });

      // Group 1 commits offset 2
      await consumer1.commit('test', 0, 2);

      // Group 2 commits offset 1
      await consumer2.commit('test', 0, 1);

      // Fetch for each group
      const messages1 = await consumer1.fetch('test', 0);
      const messages2 = await consumer2.fetch('test', 0);

      expect(messages1.length).toBe(1);
      expect(messages1[0].offset).toBe(3);

      expect(messages2.length).toBe(2);
      expect(messages2[0].offset).toBe(2);
      expect(messages2[1].offset).toBe(3);
    });

    it('should resume from last committed offset after restart', async () => {
      await producer.send('test', 0, { msg: 1 });
      await producer.send('test', 0, { msg: 2 });
      await producer.send('test', 0, { msg: 3 });

      // Commit offset 2
      await consumer.commit('test', 0, 2);

      // Create new consumer instance (simulating restart)
      const newConsumer = createConsumer({ db, group: 'test-group' });

      // Should resume from offset 2
      const messages = await newConsumer.fetch('test', 0);

      expect(messages.length).toBe(1);
      expect(messages[0].offset).toBe(3);
      expect(messages[0].data).toEqual({ msg: 3 });
    });
  });

  describe('error handling', () => {
    it('should handle missing table gracefully', async () => {
      const messages = await consumer.fetch('nonexistent', 0);
      expect(messages).toEqual([]);
    });

    it('should create consumer_offsets table on first use', async () => {
      await producer.send('test', 0, { msg: 1 });
      await consumer.commit('test', 0, 1);

      const result = await db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='klite_consumer_offsets'"
      );

      expect(result.rows.length).toBe(1);
    });

    it('should handle race condition in offset creation', async () => {
      // Create two consumers for the same group
      const consumer2 = createConsumer({ db, group: 'test-group' });
      
      await producer.send('test', 0, { msg: 1 });
      
      // Try to commit from both consumers simultaneously
      const promises = [
        consumer.commit('test', 0, 1),
        consumer2.commit('test', 0, 1)
      ];
      
      // Should not throw, one will INSERT, other will UPDATE
      await expect(Promise.all(promises)).resolves.toBeDefined();
      
      // Verify offset was set
      const result = await db.execute({
        sql: 'SELECT commit_offset FROM klite_consumer_offsets WHERE consumer_group = ? AND topic = ? AND partition = ?',
        args: ['test-group', 'test', 0]
      });
      
      expect(result.rows[0].commit_offset).toBe(1);
    });
  });

  describe('message ordering', () => {
    it('should maintain message order within partition', async () => {
      const messages = [];
      for (let i = 1; i <= 100; i++) {
        messages.push({ seq: i });
      }

      await producer.sendBatch('order', 0, messages);

      const fetched = await consumer.fetch('order', 0, { maxMessages: 100 });

      expect(fetched.length).toBe(100);
      for (let i = 0; i < 100; i++) {
        expect(fetched[i].data.seq).toBe(i + 1);
        expect(fetched[i].offset).toBe(i + 1);
      }
    });
  });
});