import { createConsumer } from './consumer.js';

function parseInterval(interval) {
  const match = interval.match(/^(\d+)(ms|s|m)$/);
  if (!match) throw new Error(`Invalid interval format: ${interval}`);
  
  const [, value, unit] = match;
  const num = parseInt(value, 10);
  
  switch (unit) {
    case 'ms': return num;
    case 's': return num * 1000;
    case 'm': return num * 60 * 1000;
    default: throw new Error(`Unknown interval unit: ${unit}`);
  }
}

async function processPartition(consumer, topic, partition, endpoint, batchSize) {
  const messages = await consumer.fetch(topic, partition, { maxMessages: batchSize });
  
  if (messages.length === 0) return;
  
  try {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        topic,
        partition,
        messages: messages.map(msg => ({
          offset: msg.offset,
          data: msg.data,
          created: msg.created
        }))
      })
    });
    
    if (response.ok) {
      // Commit the highest offset
      const lastMessage = messages[messages.length - 1];
      await consumer.commit(topic, partition, lastMessage.offset);
      console.log(`[${topic}:${partition}] Processed ${messages.length} messages, committed offset ${lastMessage.offset}`);
    } else {
      console.error(`[${topic}:${partition}] Endpoint returned ${response.status}: ${await response.text()}`);
    }
  } catch (error) {
    console.error(`[${topic}:${partition}] Error processing batch:`, error);
  }
}

async function startConsumerGroup(db, topic, groupName, groupConfig, signal) {
  const consumer = createConsumer({ db, group: groupName });
  const { partitions, endpoint, batchSize, interval } = groupConfig;
  const intervalMs = parseInterval(interval);
  
  console.log(`Starting consumer group ${groupName} for topic ${topic}`);
  console.log(`  Partitions: ${partitions.join(', ')}`);
  console.log(`  Endpoint: ${endpoint}`);
  console.log(`  Batch size: ${batchSize}`);
  console.log(`  Interval: ${interval}`);
  
  // Process until stopped
  while (!signal?.aborted) {
    try {
      // Process all partitions in parallel
      const promises = partitions.map(partition => 
        processPartition(consumer, topic, partition, endpoint, batchSize)
      );
      await Promise.all(promises);
      
      // Wait before next iteration
      await new Promise(resolve => setTimeout(resolve, intervalMs));
    } catch (error) {
      console.error(`[${groupName}] Unexpected error:`, error);
      // Wait before retrying on error
      await new Promise(resolve => setTimeout(resolve, intervalMs));
    }
  }
}

export async function startWorker({ db, config, signal }) {
  const { topics } = config;
  
  if (!topics) {
    throw new Error('No topics configured');
  }
  
  const startPromises = [];
  
  for (const [topicName, topicConfig] of Object.entries(topics)) {
    const { consumerGroups } = topicConfig;
    
    if (!consumerGroups) {
      console.warn(`No consumer groups configured for topic ${topicName}`);
      continue;
    }
    
    for (const [groupName, groupConfig] of Object.entries(consumerGroups)) {
      startPromises.push(
        startConsumerGroup(db, topicName, groupName, groupConfig, signal)
      );
    }
  }
  
  await Promise.all(startPromises);
  console.log('Worker started successfully');
  
  // Keep the process alive
  process.on('SIGINT', () => {
    console.log('Shutting down worker...');
    process.exit(0);
  });
}