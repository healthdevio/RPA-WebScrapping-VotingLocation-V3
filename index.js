const { Cluster } = require('puppeteer-cluster');
const os = require('os');
const cluster = require('cluster');
require('dotenv').config();
const { Client } = require('pg');
const { normalize } = require('./normalize');
const fs = require('fs');
const path = require('path');

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
    SELECT id, name, original_birth_date, mother_name 
    FROM "People" 
    WHERE city_id = 4 AND hydrate
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

async function updatePersonVoterData(personId, voterData) {
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
  console.log(`Atualizando dados de votação para a pessoa ID: ${personId}`);

  const query = `
    UPDATE "People"
    SET titulo_eleitor = $1,
        zona_eleitoral = $2,
        secao_eleitoral = $3,
        local_votacao = $4,
        endereco_votacao = $5,
        municipio_votacao = $6,
        biometria = $7
    WHERE id = $8
  `;

  const values = [
    voterData.inscricao,
    voterData.zona,
    voterData.secao,
    voterData.local,
    voterData.endereco,
    voterData.municipio,
    voterData.biometria,
    personId
  ];

  try {
    await client.query(query, values);
    console.log(`Dados de votação atualizados com sucesso para a pessoa ID: ${personId}`);
  } catch (error) {
    console.error(`Erro ao atualizar os dados de votação para a pessoa ID ${personId}:`, error);
  } finally {
    await client.end();
  }
}

async function createPuppeteerCluster() {
  const cluster = await Cluster.launch({
    concurrency: Cluster.CONCURRENCY_BROWSER,
    maxConcurrency: 1, 
    puppeteerOptions: {
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-accelerated-2d-canvas',
        '--no-first-run',
        '--no-zygote',
        '--disable-gpu',
        '--disable-extensions',
      ],
    },
    monitor: true,
    timeout: 180000,  
  });

  await cluster.task(async ({ page, data: { person, counts } }) => {
    const { id, name, original_birth_date: birthDate, mother_name: motherName } = person;

    try {
      console.log(`==> HELLO WORLD FETCH VOTER DATA`);

      if (!name || !birthDate || !motherName) {
        throw new Error('Nome, data de nascimento e nome da mãe são obrigatórios.');
      }

      page.on('request', (request) => {
        console.log('URL:', request.url());
        console.log('Método:', request.method());
        console.log('Cabeçalhos:', request.headers());
        if (request.postData()) {
          console.log('Payload:', request.postData());
        }
        console.log('-----');
      });

      console.log('EM CIMA DO AWAIT Navegado para o site do TRE-CE.');
      await page.goto(
        'https://www.tre-ce.jus.br/servicos-eleitorais/titulo-e-local-de-votacao/consulta-por-nome',
        {
          waitUntil: 'domcontentloaded',
          timeout: 120000,
        }
      );
      console.log('Navegado para o site do TRE-CE.');

      // Pausa para garantir que o conteúdo seja carregado
      await new Promise((resolve) => setTimeout(resolve, 5000));

      await page.waitForSelector('.cookies .botao button', { visible: true, timeout: 5000 });
      const cienteButton = await page.$('div.botao button.btn');
      if (cienteButton) {
        await cienteButton.click();
        console.log('Pop-up de cookies fechado.');
      } else {
        console.log('Botão "Ciente" não encontrado.');
      }

      const modalButton = await page.$('app-menu-option[title="8. Onde votar"]');
      if (modalButton) {
        await modalButton.click();
        console.log('Modal do Formulário aberto.');
      } else {
        console.log('Botão "Onde votar" não encontrado.');
      }

      const formattedBirthDate = formatDate(birthDate);
      const normalizedName = normalize(name.toUpperCase());
      const normalizedMotherName = normalize(motherName.toUpperCase());

      // Log dos dados antes de submeter
      console.log(`Submetendo dados da pessoa: 
        Nome: ${normalizedName}, 
        Data de Nascimento: ${formattedBirthDate}, 
        Nome da Mãe: ${normalizedMotherName}`);

      // Preenchendo o formulário com os dados da pessoa
      await page.waitForSelector('[formcontrolname=TituloCPFNome]', { visible: true, timeout: 5000 });
      await page.type('[formcontrolname=TituloCPFNome]', normalizedName);
      console.log(`Nome preenchido: ${normalizedName}`);

      await page.waitForSelector('[formcontrolname=dataNascimento]', { visible: true, timeout: 5000 });
      await page.type('[formcontrolname=dataNascimento]', formattedBirthDate);
      console.log(`Data de nascimento preenchida: ${formattedBirthDate}`);

      await page.waitForSelector('[formcontrolname=nomeMae]', { visible: true, timeout: 6000 });
      await page.type('[formcontrolname=nomeMae]', normalizedMotherName);
      console.log(`Nome da mãe preenchido: ${normalizedMotherName}`);

      await page.waitForSelector('.btn-tse', { visible: true, timeout: 6000 });
      const submitButton = await page.$('.btn-tse');
      if (submitButton) {
        await page.evaluate((b) => b.click(), submitButton);
        console.log(`Submetendo formulário para: ${normalizedName}`);
      } else {
        throw new Error('Botão de submissão não encontrado');
      }

      console.log('[FORM]: FORM submitted');
      await new Promise((resolve) => setTimeout(resolve, 5000));

      const data = await page.evaluate(() => {
        const voterComponent = document.querySelector('.componente-onde-votar');
        if (!voterComponent) {
          return { error: true, message: 'Pessoa não encontrada no sistema do TRE' };
        }

        const labels = Array.from(document.querySelectorAll('.lado-ov .data-box .label'))
          .map((el) => el.textContent.trim() ?? null);
        const descs = Array.from(document.querySelectorAll('.lado-ov .data-box .desc'))
          .map((el) => el.textContent.trim() ?? null);

        const result = {};
        const possibleLabels = {
          'Local de votação': 'local',
          Endereço: 'endereco',
          'Município/UF': 'municipio',
          Bairro: 'bairro',
          Seção: 'secao',
          País: 'pais',
          Zona: 'zona',
        };

        labels.forEach((label, i) => {
          const key = possibleLabels[label];
          if (key) {
            result[key] = descs[i] || null;
          }
        });

        result.biometria = document.body.innerText.includes('ELEITOR/ELEITORA COM BIOMETRIA COLETADA');
        return { error: false, data: result };
      });

      if (data.error) {
        const screenshotPath = path.join(__dirname, 'rpa', `pessoa_nao_encontrada_${name.replace(/\s+/g, '_')}.png`);
        
        // Verifica se o diretório "rpa" existe, caso contrário, cria
        if (!fs.existsSync(path.join(__dirname, 'rpa'))) {
          fs.mkdirSync(path.join(__dirname, 'rpa'), { recursive: true });
        }
        
        await page.screenshot({ path: screenshotPath });
        throw new Error(data.message);
      }
      
      console.log(`Dados encontrados com sucesso: ${JSON.stringify(data.data)}`);
      counts.success++;
      await updatePersonVoterData(id, data.data);
      return data.data;

    } catch (error) {
      console.error(`Erro ao processar ${name}: ${error.message}`);

      const errorScreenshotPath = path.join(__dirname, 'rpa', `erro_${name.replace(/\s+/g, '_')}.png`);
      
      // Verifica se o diretório "rpa" existe, caso contrário, cria
      if (!fs.existsSync(path.join(__dirname, 'rpa'))) {
        fs.mkdirSync(path.join(__dirname, 'rpa'), { recursive: true });
      }
      
      await page.screenshot({ path: errorScreenshotPath, fullPage: true });
      console.log(`Screenshot do erro salvo em: ${errorScreenshotPath}`);
      
      counts.failure++;
      return { error: error.message };
    } finally {
      await page.close();
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
    let totalSuccess = 0;
    let totalFailure = 0;
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

      const workers = [];
      for (let i = 0; i < numCPUs; i++) {
        const worker = cluster.fork();
        workers.push(worker);

        worker.send({
          people: people.slice(i * Math.ceil(people.length / numCPUs), (i + 1) * Math.ceil(people.length / numCPUs)),
          counts: { success: 0, failure: 0 }
        });

        worker.on('message', ({ success, failure }) => {
          totalSuccess += success;
          totalFailure += failure;
          totalProcessed += success + failure;
          console.log(`Worker ${worker.id} finalizou o processamento: ${success} com sucesso, ${failure} falharam.`);
        });

        worker.on('exit', (code, signal) => {
          console.log(`Worker ${worker.process.pid} foi encerrado. Código: ${code}, Sinal: ${signal}`);
        });
      }

      for (const worker of workers) {
        await new Promise((resolve) => worker.on('exit', resolve));
        worker.kill();
      }

      offset += batchSize;
    }

    console.log(`Processamento completo. Total de registros processados: ${totalProcessed}`);
    console.log(`Total com sucesso: ${totalSuccess}, total com falha: ${totalFailure}`);
  } else {
    process.on('message', async ({ people, counts }) => {
      const puppeteerCluster = await createPuppeteerCluster();

      try {
        for (const person of people) {
          puppeteerCluster.queue({ person, counts });
        }

        await puppeteerCluster.idle();
      } catch (error) {
        console.error('Erro durante o processamento:', error);
      } finally {
        await puppeteerCluster.close();
        process.send({ success: counts.success, failure: counts.failure });
        process.exit(); 
      }
    });
  }
})();

