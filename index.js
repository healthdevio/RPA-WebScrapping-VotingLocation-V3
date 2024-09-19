const { Cluster } = require('puppeteer-cluster');
const os = require('os');
const cluster = require('cluster');
const path = require('path');
require('dotenv').config();
const fs = require('fs');
const { Client } = require('pg');
const redis = require('redis');
const util = require('util');

const redisClient = redis.createClient();
redisClient.on('error', (err) => {
  console.error('Erro ao conectar ao Redis:', err);
});
redisClient.get = util.promisify(redisClient.get);
redisClient.set = util.promisify(redisClient.set);

function formatDate(dateStr) {
  const [day, month, year] = dateStr.split('/');
  return `${day}/${month}/${year}`;
}

function calculateAge(birthDate) {
  const [day, month, year] = birthDate.split('/');
  const birth = new Date(`${year}-${month}-${day}`);
  const ageDifMs = Date.now() - birth.getTime();
  const ageDate = new Date(ageDifMs);
  return Math.abs(ageDate.getUTCFullYear() - 1970);
}

async function getPeopleFromDatabase(offset = 0, limit = 1000) {
  const client = new Client({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
    ssl: {
      rejectUnauthorized: false, 
    },  
  });

  await client.connect();

  console.log(`Executando consulta SQL com offset ${offset} e limite ${limit}`);

  const query = `
    SELECT name, original_birth_date, mother_name 
    FROM "People" 
    WHERE city_id = 4
    LIMIT ${limit} OFFSET ${offset};
  `;

  const res = await client.query(query);
  
  console.log(`Dados retornados do banco de dados: ${JSON.stringify(res.rows, null, 2)}`);

  await client.end();

  const filteredPeople = res.rows.filter((person) => {
    const age = calculateAge(person.original_birth_date);
    console.log(`Calculando idade para ${person.name}: ${age} anos`);
    return age >= 16 && age <= 30;
  });

  console.log(`Pessoas filtradas entre 16 e 30 anos: ${JSON.stringify(filteredPeople, null, 2)}`);

  return filteredPeople;
}

async function fetchVoterDataWithCache(name, birthDate, motherName) {
  const cacheKey = `voter_data_${name}_${birthDate}_${motherName}`;

  try {
    const cachedData = await redisClient.get(cacheKey);
    if (cachedData) {
      console.log(`Dados de cache encontrados para ${name}`);
      return JSON.parse(cachedData);
    }

    return null;
  } catch (error) {
    console.error(`Erro ao buscar ou salvar dados no cache para ${name}:`, error);
    throw error;
  }
}

async function createPuppeteerCluster() {
  const cluster = await Cluster.launch({
    concurrency: Cluster.CONCURRENCY_BROWSER,
    maxConcurrency: 5, 
    puppeteerOptions: {
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-accelerated-2d-canvas',
        '--no-first-run',
        '--no-zygote',
        '--disable-gpu',
      ],
      headless: true,
      userDataDir: './user_data', 
    },
  });

  await cluster.task(async ({ page, data: { name, birthDate, motherName } }) => {
    try {
      const cachedData = await fetchVoterDataWithCache(name, birthDate, motherName);

      if (cachedData) {
        console.log(`Dados de cache usados para ${name}`);
        return cachedData;
      }

      console.log(`Processando ${name}...`);

      await page.goto('https://www.tre-ce.jus.br/servicos-eleitorais/titulo-e-local-de-votacao/consulta-por-nome', {
        waitUntil: 'networkidle2',
      });

      const cienteButton = await page.$('button[title="Ciente"]');
      if (cienteButton) {
        await cienteButton.click();
        console.log('Pop-up de cookies fechado.');
      }

      const formattedBirthDate = formatDate(birthDate);
      console.log(`Data de nascimento formatada: ${formattedBirthDate}`);

      await page.waitForSelector('#LV_NomeTituloCPF', { visible: true, timeout: 5000 });
      await page.type('#LV_NomeTituloCPF', name);
      console.log(`Nome preenchido: ${name}`);

      await page.waitForSelector('#LV_DataNascimento', { visible: true, timeout: 5000 });
      await page.type('#LV_DataNascimento', formattedBirthDate);
      console.log(`Data de nascimento preenchida: ${formattedBirthDate}`);

      await page.waitForSelector('#LV_NomeMae', { visible: true, timeout: 5000 });
      await page.type('#LV_NomeMae', motherName);
      console.log(`Nome da mãe preenchido: ${motherName}`);

      const submitButton = await page.$('#consultar-local-votacao-form-submit');
      if (submitButton) {
        await page.evaluate((button) => button.click(), submitButton);
        console.log(`Submetendo formulário para ${name}`);
      }

      await page.waitForFunction(() => !document.body.innerText.includes('carregando conteúdo'), { timeout: 90000 });

      if ((await page.$('div.alert.alert-warning')) !== null) {
        console.log(`Pessoa não encontrada no sistema do TRE: ${name}`);
        return { error: 'Pessoa não encontrada' };
      }

      const data = await page.evaluate(() => {
        const extractZonaSecao = () => {
          const element = Array.from(document.querySelectorAll('p')).find(
            (el) => el.textContent.includes('Zona:') && el.textContent.includes('Seção:')
          );
          if (element) {
            const text = element.textContent;
            const zonaMatch = text.match(/Zona:\s*(\d+)/);
            const secaoMatch = text.match(/Seção:\s*(\d+)/);
            return {
              zona: zonaMatch ? zonaMatch[1] : null,
              secao: secaoMatch ? secaoMatch[1] : null,
            };
          }
          return { zona: null, secao: null };
        };

        const getText = (label) => {
          const element = Array.from(document.querySelectorAll('p')).find((el) => el.textContent.includes(label));
          return element ? element.textContent.split(': ')[1].trim() : null;
        };

        const extractInscricao = () => {
          const element = Array.from(document.querySelectorAll('p')).find((el) => el.textContent.includes('Inscrição:'));
          return element ? element.textContent.split(': ')[1].trim() : null;
        };

        const { zona, secao } = extractZonaSecao();
        const inscricao = extractInscricao();

        return {
          inscricao,
          zona,
          secao,
          local: getText('Local'),
          endereco: getText('Endereço'),
          municipio: getText('Município'),
          biometria: document.body.innerText.includes('ELEITOR/ELEITORA COM BIOMETRIA COLETADA'),
        };
      });

      await redisClient.set(`voter_data_${name}_${birthDate}_${motherName}`, JSON.stringify(data), 'EX', 3600);

      return data;
    } catch (error) {
      console.error(`Erro ao processar ${name}: ${error.message}`);
      return { error: error.message };
    }
  });

  return cluster;
}

(async function () {
  if (cluster.isMaster) {
    const numCPUs = os.cpus().length;
    const batchSize = 1000; 
    let offset = 0;
    let totalProcessed = 0;
    let noResultsCounter = 0; 

    while (noResultsCounter < 3) {  
      console.log(`Buscando pessoas no banco de dados com offset ${offset} e batchSize ${batchSize}`);
      const people = await getPeopleFromDatabase(offset, batchSize); 

      if (people.length === 0) {
        console.log('Nenhuma pessoa foi encontrada no banco de dados.');
        noResultsCounter++;
        offset += batchSize;
        continue; 
      }

      console.log(`Iniciando processamento do batch com ${people.length} pessoas.`);
      noResultsCounter = 0; 

      for (let i = 0; i < numCPUs; i++) {
        const worker = cluster.fork();
        worker.send(people.slice(i * Math.ceil(people.length / numCPUs), (i + 1) * Math.ceil(people.length / numCPUs)));
      }

      cluster.on('message', (worker, message) => {
        console.log(`Worker ${worker.id} finalizou o processamento:`, message);
      });

      totalProcessed += people.length;
      offset += batchSize; 
    }

    console.log(`Processamento completo. Total de registros processados: ${totalProcessed}`);
  } else {
    process.on('message', async (people) => {
      const puppeteerCluster = await createPuppeteerCluster(); 

      for (const person of people) {
        const { name, original_birth_date: birthDate, mother_name: motherName } = person;
        puppeteerCluster.queue({ name, birthDate, motherName });
      }

      await puppeteerCluster.idle();
      await puppeteerCluster.close();
    });
  }
})();
