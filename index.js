const { Client } = require('pg');
const { fork } = require('child_process');
const path = require('path');

const dbConfig = {
  connectionString: 'postgresql://root:Acquaint_Viewer7_Eloquent@elegebr.c5qnvtxpcnuu.us-east-1.rds.amazonaws.com:5432/elegebrqa',
  ssl: {
    rejectUnauthorized: false
  }
};

async function fetchDataAndDistribute() {
  const client = new Client(dbConfig);
  await client.connect();

  console.log('Buscando dados do banco de dados...');
  const supporters = await client.query(`
    SELECT 
        id, 
        name, 
        TO_CHAR(birthday::date, 'YYYY-MM-DD') as birthday, 
        mother_name 
    FROM 
        public."supporters" 
    WHERE 
        name IS NOT NULL 
        AND birthday IS NOT NULL 
        AND mother_name IS NOT NULL 
        AND (rpa_filled = false OR rpa_filled IS NULL)
  `);

  const totalSupporters = supporters.rowCount;
  let processedCount = 0;
  let skippedCount = 0;
  let failedCount = 0;
  let notFoundCount = 0;

  console.log(`Encontradas ${totalSupporters} pessoas para processar.`);

  const maxConcurrentProcesses = 5;
  const activeWorkers = [];

  function spawnWorker(person) {
    return new Promise((resolve) => {
      const worker = fork(path.resolve(__dirname, 'worker.js'), [JSON.stringify(person)]);
      activeWorkers.push(worker);

      worker.on('message', (result) => {
        switch (result.status) {
          case 'success':
            processedCount++;
            break;
          case 'skipped':
            skippedCount++;
            break;
          case 'failed':
            failedCount++;
            break;
          case 'not_found':
            notFoundCount++;
            break;
        }

        console.log(`Pessoas processadas: ${processedCount}, Pessoas puladas: ${skippedCount}, Falhas ao processar: ${failedCount}, Pessoas não encontradas: ${notFoundCount}, Restantes: ${totalSupporters - (processedCount + skippedCount + failedCount + notFoundCount)}`);
        resolve();
      });

      worker.on('exit', () => {
        activeWorkers.splice(activeWorkers.indexOf(worker), 1);
      });
    });
  }

  (async () => {
    for (const person of supporters.rows) {
      while (activeWorkers.length >= maxConcurrentProcesses) {
        await new Promise(resolve => setTimeout(resolve, 100)); 
      }
      spawnWorker(person);
    }

    while (activeWorkers.length > 0) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  })();

  await client.end();
}

fetchDataAndDistribute().catch(console.error);
