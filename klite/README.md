# SQLite Message Queue

A minimal Kafka-like message queue implementation using SQLite and JavaScript. Simple, reliable, and perfect for applications that need persistent message queuing without the operational complexity of Kafka.

## Features

- **Topics & Partitions** - Organize messages by topic and partition
- **Consumer Groups** - Multiple consumers can coordinate through consumer groups
- **Offset Management** - Track message consumption progress
- **Crash Recovery** - Consumers resume from last committed offset after restart
- **No Dependencies** - Just SQLite and JavaScript

## Database Schema

### Message Tables

Each topic-partition combination gets its own table:

```sql
CREATE TABLE IF NOT EXISTS ${topic}_${partition} (
  id INTEGER PRIMARY KEY AUTOINCREMENT,  -- message offset
  data BLOB NOT NULL,                    -- message payload
  created DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

Example: Topic "orders" with partition 0 creates table `orders_0`

### Consumer Offset Tracking

Single table tracks consumption progress for all consumer groups:

```sql
CREATE TABLE IF NOT EXISTS consumer_offsets (
  consumer_group VARCHAR NOT NULL,
  topic VARCHAR NOT NULL,
  partition INTEGER NOT NULL,
  commit_offset INTEGER NOT NULL,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (consumer_group, topic, partition)
);
```

## JavaScript API

### Producer

Send messages to topics:

```js
const producer = createProducer({ db: sqliteConnection });

// Send single message
const { offset } = await producer.send('orders', 0, { orderId: 123 });

// Send multiple messages
const { firstOffset, count } = await producer.sendBatch('orders', 0, [
  { orderId: 123 },
  { orderId: 124 },
  { orderId: 125 }
]);
```

### Consumer

Fetch and commit messages:

```js
const consumer = createConsumer({ 
  db: sqliteConnection,
  group: 'order-processors' 
});

// Fetch messages (returns array)
const messages = await consumer.fetch('orders', 0, { 
  maxMessages: 100  // optional, defaults to reasonable limit
});

// Process messages
for (const msg of messages) {
  console.log(msg.offset, msg.data, msg.created);
  await processOrder(msg.data);
  
  // Commit offset after successful processing
  await consumer.commit('orders', 0, msg.offset);
}
```

### Worker

Process messages continuously with HTTP endpoints:

```js
import { startWorker } from './worker.js';

const config = {
  topics: {
    orders: {
      consumerGroups: {
        'order-processor': {
          partitions: [0, 1],
          endpoint: 'https://api.example.com/process-orders',
          batchSize: 50,
          interval: '5s'  // supports ms, s, m units
        },
        'order-analytics': {
          partitions: [0, 1, 2, 3],
          endpoint: 'https://analytics.example.com/ingest',
          batchSize: 100,
          interval: '10s'
        }
      }
    },
    events: {
      consumerGroups: {
        'event-handler': {
          partitions: [0],
          endpoint: 'https://api.example.com/handle-events',
          batchSize: 25,
          interval: '2s'
        }
      }
    }
  }
};

// Start the worker
await startWorker({ db, config });
```

The worker will:
- Continuously fetch messages from assigned partitions
- Batch messages up to the configured size
- POST to the endpoint with format: `{ topic, partition, messages: [...] }`
- Commit offsets only on successful (200) responses
- Retry on failures with the configured interval

## Usage Example

```js
// Producer service
async function publishOrder(order) {
  const partition = order.userId % 4;  // simple partitioning by user
  await producer.send('orders', partition, order);
}

// Consumer service  
async function consumeOrders() {
  const consumer = createConsumer({ db, group: 'order-processors' });
  
  // Assign partitions manually (e.g., via config)
  const assignedPartitions = [0, 1];  // this consumer handles partitions 0 and 1
  
  while (true) {
    for (const partition of assignedPartitions) {
      const messages = await consumer.fetch('orders', partition);
      
      for (const msg of messages) {
        await processOrder(msg.data);
        await consumer.commit('orders', partition, msg.offset);
      }
    }
    
    await sleep(1000);  // poll every second
  }
}
```

## Design Decisions

- **No Auto-commit**: Consumers must explicitly commit offsets for reliability and debugging
- **Manual Partition Assignment**: Consumers are manually assigned partitions via configuration
- **At-Least-Once Delivery**: Messages may be reprocessed on crashes (ensure idempotent processing)
- **Simple Recovery**: Consumers automatically resume from last committed offset on restart

## Limitations

- No automatic rebalancing - partition assignments are static
- No built-in partition coordination - consumers must be configured not to overlap
- No retention policies - messages stay forever unless manually deleted
- No consumer heartbeats - dead consumers must be detected externally

## When to Use This

Perfect for:
- Single-node applications needing persistent queues
- Systems with predictable partition assignments
- Development and testing environments
- Applications where SQLite is already in use

Not suitable for:
- High-throughput scenarios (>10k messages/sec)
- Systems needing automatic rebalancing
- Multi-node distributed setups

