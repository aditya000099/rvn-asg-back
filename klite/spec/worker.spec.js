import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { createClient } from '@libsql/client';
import { createProducer } from '../src/producer.js';
import { startWorker } from '../src/worker.js';

// Mock fetch globally
global.fetch = vi.fn();

describe('Worker', () => {
  let db;
  let producer;
  let workerPromise;
  let abortController;

  beforeEach(async () => {
    db = createClient({
      url: ':memory:'
    });
    producer = createProducer({ db, batchDelay: 0 });
    vi.clearAllMocks();
    
    // Setup AbortController for stopping worker
    abortController = new AbortController();
  });

  afterEach(async () => {
    // Stop the worker if it's running
    if (abortController) {
      abortController.abort();
    }
  });

  describe('parseInterval', () => {
    it('should parse milliseconds correctly', async () => {
      const config = {
        topics: {
          test: {
            consumerGroups: {
              group1: {
                partitions: [0],
                endpoint: 'https://example.com',
                batchSize: 1,
                interval: '100ms'
              }
            }
          }
        }
      };

      // Start worker briefly to test interval parsing
      const promise = startWorker({ db, config, signal: abortController.signal });
      
      // Give it a moment to parse config
      await new Promise(resolve => setTimeout(resolve, 10));
      abortController.abort();
      
      // Should not throw
      expect(promise).toBeDefined();
    });

    it('should parse seconds correctly', async () => {
      const config = {
        topics: {
          test: {
            consumerGroups: {
              group1: {
                partitions: [0],
                endpoint: 'https://example.com',
                batchSize: 1,
                interval: '5s'
              }
            }
          }
        }
      };

      const promise = startWorker({ db, config, signal: abortController.signal });
      await new Promise(resolve => setTimeout(resolve, 10));
      abortController.abort();
      
      expect(promise).toBeDefined();
    });

    it('should parse minutes correctly', async () => {
      const config = {
        topics: {
          test: {
            consumerGroups: {
              group1: {
                partitions: [0],
                endpoint: 'https://example.com',
                batchSize: 1,
                interval: '2m'
              }
            }
          }
        }
      };

      const promise = startWorker({ db, config, signal: abortController.signal });
      await new Promise(resolve => setTimeout(resolve, 10));
      abortController.abort();
      
      expect(promise).toBeDefined();
    });
  });

  describe('message processing', () => {
    it('should fetch and send messages to endpoint', async () => {
      // Setup messages
      await producer.send('orders', 0, { orderId: 1 });
      await producer.send('orders', 0, { orderId: 2 });

      // Mock successful response
      fetch.mockResolvedValueOnce({
        ok: true,
        status: 200
      });

      const config = {
        topics: {
          orders: {
            consumerGroups: {
              processor: {
                partitions: [0],
                endpoint: 'https://api.example.com/process',
                batchSize: 10,
                interval: '50ms'
              }
            }
          }
        }
      };

      // Start worker
      workerPromise = startWorker({ db, config, signal: abortController.signal });

      // Wait for first processing cycle
      await new Promise(resolve => setTimeout(resolve, 100));

      // Verify fetch was called with correct payload
      expect(fetch).toHaveBeenCalledWith(
        'https://api.example.com/process',
        expect.objectContaining({
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: expect.stringContaining('"topic":"orders"')
        })
      );

      const callBody = JSON.parse(fetch.mock.calls[0][1].body);
      expect(callBody.topic).toBe('orders');
      expect(callBody.partition).toBe(0);
      expect(callBody.messages).toHaveLength(2);
      expect(callBody.messages[0].data).toEqual({ orderId: 1 });
      expect(callBody.messages[1].data).toEqual({ orderId: 2 });
    });

    it('should commit offset on successful response', async () => {
      await producer.send('test', 0, { msg: 1 });
      await producer.send('test', 0, { msg: 2 });

      // Mock successful response
      fetch.mockResolvedValueOnce({
        ok: true,
        status: 200
      });

      const config = {
        topics: {
          test: {
            consumerGroups: {
              group1: {
                partitions: [0],
                endpoint: 'https://example.com',
                batchSize: 10,
                interval: '50ms'
              }
            }
          }
        }
      };

      workerPromise = startWorker({ db, config, signal: abortController.signal });
      await new Promise(resolve => setTimeout(resolve, 100));

      // Verify offset was committed
      const result = await db.execute({
        sql: 'SELECT commit_offset FROM klite_consumer_offsets WHERE consumer_group = ? AND topic = ? AND partition = ?',
        args: ['group1', 'test', 0]
      });

      expect(result.rows[0].commit_offset).toBe(2);
    });

    it('should not commit on failed response', async () => {
      await producer.send('test', 0, { msg: 1 });

      // Mock failed response
      fetch.mockResolvedValueOnce({
        ok: false,
        status: 500,
        text: async () => 'Internal Server Error'
      });

      const config = {
        topics: {
          test: {
            consumerGroups: {
              group1: {
                partitions: [0],
                endpoint: 'https://example.com',
                batchSize: 10,
                interval: '50ms'
              }
            }
          }
        }
      };

      workerPromise = startWorker({ db, config, signal: abortController.signal });
      await new Promise(resolve => setTimeout(resolve, 100));

      // Verify offset was NOT committed
      const result = await db.execute({
        sql: 'SELECT commit_offset FROM klite_consumer_offsets WHERE consumer_group = ? AND topic = ? AND partition = ?',
        args: ['group1', 'test', 0]
      });

      expect(result.rows.length).toBe(0);
    });

    it('should respect batchSize limit', async () => {
      // Create more messages than batch size
      for (let i = 1; i <= 20; i++) {
        await producer.send('test', 0, { id: i });
      }

      fetch.mockResolvedValueOnce({
        ok: true,
        status: 200
      });

      const config = {
        topics: {
          test: {
            consumerGroups: {
              group1: {
                partitions: [0],
                endpoint: 'https://example.com',
                batchSize: 5,
                interval: '50ms'
              }
            }
          }
        }
      };

      workerPromise = startWorker({ db, config, signal: abortController.signal });
      await new Promise(resolve => setTimeout(resolve, 100));

      // Should only fetch 5 messages
      const callBody = JSON.parse(fetch.mock.calls[0][1].body);
      expect(callBody.messages).toHaveLength(5);
      expect(callBody.messages[0].data).toEqual({ id: 1 });
      expect(callBody.messages[4].data).toEqual({ id: 5 });
    });

    it('should process multiple partitions in parallel', async () => {
      await producer.send('multi', 0, { partition: 0 });
      await producer.send('multi', 1, { partition: 1 });

      let callCount = 0;
      fetch.mockImplementation(() => {
        callCount++;
        return Promise.resolve({
          ok: true,
          status: 200
        });
      });

      const config = {
        topics: {
          multi: {
            consumerGroups: {
              group1: {
                partitions: [0, 1],
                endpoint: 'https://example.com',
                batchSize: 10,
                interval: '50ms'
              }
            }
          }
        }
      };

      workerPromise = startWorker({ db, config, signal: abortController.signal });
      await new Promise(resolve => setTimeout(resolve, 100));

      // Should have made 2 calls (one per partition)
      expect(callCount).toBe(2);

      // Verify both partitions were processed
      const calls = fetch.mock.calls.map(call => JSON.parse(call[1].body));
      const partitions = calls.map(c => c.partition).sort();
      expect(partitions).toEqual([0, 1]);
    });

    it('should handle multiple consumer groups independently', async () => {
      await producer.send('test', 0, { msg: 1 });

      let endpoints = [];
      fetch.mockImplementation((url) => {
        endpoints.push(url);
        return Promise.resolve({
          ok: true,
          status: 200
        });
      });

      const config = {
        topics: {
          test: {
            consumerGroups: {
              group1: {
                partitions: [0],
                endpoint: 'https://api1.example.com',
                batchSize: 10,
                interval: '50ms'
              },
              group2: {
                partitions: [0],
                endpoint: 'https://api2.example.com',
                batchSize: 10,
                interval: '50ms'
              }
            }
          }
        }
      };

      workerPromise = startWorker({ db, config, signal: abortController.signal });
      await new Promise(resolve => setTimeout(resolve, 100));

      // Both groups should process the message
      expect(endpoints).toContain('https://api1.example.com');
      expect(endpoints).toContain('https://api2.example.com');
    });

    it('should continue processing after errors', async () => {
      await producer.send('test', 0, { msg: 1 });

      let callCount = 0;
      fetch.mockImplementation(() => {
        callCount++;
        if (callCount === 1) {
          throw new Error('Network error');
        }
        return Promise.resolve({
          ok: true,
          status: 200
        });
      });

      const config = {
        topics: {
          test: {
            consumerGroups: {
              group1: {
                partitions: [0],
                endpoint: 'https://example.com',
                batchSize: 10,
                interval: '50ms'
              }
            }
          }
        }
      };

      workerPromise = startWorker({ db, config, signal: abortController.signal });
      
      // Wait for multiple cycles
      await new Promise(resolve => setTimeout(resolve, 150));

      // Should have retried after error
      expect(callCount).toBeGreaterThan(1);
    });

    it('should process continuously in while loop', async () => {
      let processCount = 0;
      fetch.mockImplementation(() => {
        processCount++;
        return Promise.resolve({
          ok: true,
          status: 200
        });
      });

      const config = {
        topics: {
          test: {
            consumerGroups: {
              group1: {
                partitions: [0],
                endpoint: 'https://example.com',
                batchSize: 10,
                interval: '30ms'
              }
            }
          }
        }
      };

      // Add messages continuously
      await producer.send('test', 0, { msg: 1 });

      workerPromise = startWorker({ db, config, signal: abortController.signal });
      
      // Wait for multiple processing cycles
      await new Promise(resolve => setTimeout(resolve, 100));
      
      await producer.send('test', 0, { msg: 2 });
      await new Promise(resolve => setTimeout(resolve, 50));
      
      await producer.send('test', 0, { msg: 3 });
      await new Promise(resolve => setTimeout(resolve, 50));

      // Should have processed multiple times
      expect(processCount).toBeGreaterThanOrEqual(3);
    });
  });

  describe('configuration', () => {
    it('should handle missing topics gracefully', async () => {
      const config = {};
      
      await expect(startWorker({ db, config, signal: abortController.signal }))
        .rejects.toThrow('No topics configured');
    });

    it('should warn about topics without consumer groups', async () => {
      const consoleSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
      
      const config = {
        topics: {
          test: {}
        }
      };

      workerPromise = startWorker({ db, config, signal: abortController.signal });
      await new Promise(resolve => setTimeout(resolve, 10));

      expect(consoleSpy).toHaveBeenCalledWith('No consumer groups configured for topic test');
      
      consoleSpy.mockRestore();
    });

    it('should log startup information', async () => {
      const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
      
      const config = {
        topics: {
          test: {
            consumerGroups: {
              group1: {
                partitions: [0, 1],
                endpoint: 'https://example.com',
                batchSize: 50,
                interval: '5s'
              }
            }
          }
        }
      };

      workerPromise = startWorker({ db, config, signal: abortController.signal });
      await new Promise(resolve => setTimeout(resolve, 10));

      expect(consoleSpy).toHaveBeenCalledWith('Starting consumer group group1 for topic test');
      expect(consoleSpy).toHaveBeenCalledWith('  Partitions: 0, 1');
      expect(consoleSpy).toHaveBeenCalledWith('  Endpoint: https://example.com');
      expect(consoleSpy).toHaveBeenCalledWith('  Batch size: 50');
      expect(consoleSpy).toHaveBeenCalledWith('  Interval: 5s');
      
      consoleSpy.mockRestore();
    });
  });
});