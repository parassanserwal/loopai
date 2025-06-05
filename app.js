const express = require('express');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(express.json());


const INGESTIONS = {};
const BATCH_QUEUE = []; 
const PRIORITY_MAP = { HIGH: 3, MEDIUM: 2, LOW: 1 };


function getOverallStatus(batches) {
  const statuses = batches.map(b => b.status);
  if (statuses.every(s => s === 'yet_to_start')) return 'yet_to_start';
  if (statuses.every(s => s === 'completed')) return 'completed';
  return 'triggered';
}


function sortQueue() {
  BATCH_QUEUE.sort((a, b) => {
    if (PRIORITY_MAP[b.priority] !== PRIORITY_MAP[a.priority]) {
      return PRIORITY_MAP[b.priority] - PRIORITY_MAP[a.priority];
    }
    return a.createdAt - b.createdAt;
  });
}


app.post('/ingest', (req, res) => {
  const { ids, priority } = req.body;
  if (!Array.isArray(ids) || !['HIGH', 'MEDIUM', 'LOW'].includes(priority)) {
    return res.status(400).json({ error: 'Invalid payload' });
  }
  const ingestion_id = uuidv4();
  const createdAt = Date.now();
  
  const batches = [];
  for (let i = 0; i < ids.length; i += 3) {
    const batch_ids = ids.slice(i, i + 3);
    const batch_id = uuidv4();
    const batch = {
      batch_id,
      ids: batch_ids,
      status: 'yet_to_start',
      createdAt,
    };
    batches.push(batch);
    BATCH_QUEUE.push({
      ingestion_id,
      batch_id,
      ids: batch_ids,
      priority,
      createdAt,
      status: 'yet_to_start',
    });
  }
  INGESTIONS[ingestion_id] = {
    ingestion_id,
    priority,
    createdAt,
    batches,
  };
  sortQueue();
  res.json({ ingestion_id });
});


app.get('/status/:ingestion_id', (req, res) => {
  const { ingestion_id } = req.params;
  const ingestion = INGESTIONS[ingestion_id];
  if (!ingestion) return res.status(404).json({ error: 'Not found' });

  for (const batch of ingestion.batches) {
    const q = BATCH_QUEUE.find(
      b => b.ingestion_id === ingestion_id && b.batch_id === batch.batch_id
    );
    if (q) batch.status = q.status;
  }
  res.json({
    ingestion_id,
    status: getOverallStatus(ingestion.batches),
    batches: ingestion.batches.map(b => ({
      batch_id: b.batch_id,
      ids: b.ids,
      status: b.status,
    })),
  });
});


async function processBatches() {
  while (true) {
    sortQueue();
    const batch = BATCH_QUEUE.find(b => b.status === 'yet_to_start');
    if (batch) {
      batch.status = 'triggered';
      
      await Promise.all(
        batch.ids.map(
          id =>
            new Promise(resolve =>
              setTimeout(() => {
                resolve();
              }, 1000)
            )
        )
      );
      batch.status = 'completed';
    }
    await new Promise(resolve => setTimeout(resolve, 5000));
  }
}
processBatches();


const PORT = process.env.PORT || 5001;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});