const puppeteer = require('puppeteer');
const { Client } = require('pg');
const { normalize } = require('./normalize');
const fs = require('fs');
const path = require('path');

// Conexão direta com o banco de dados usando dados fornecidos
const dbConfig = {
  user: 'zerooitocincoadm',
  host: 'zerooitocincodb.c5qnvtxpcnuu.us-east-1.rds.amazonaws.com',
  database: '085db',
  password: 'GrzYTKeT3ZmSg4JfpnVqUYSd',
  port: 5432,
  ssl: {
    rejectUnauthorized: false, // Ignora erros de certificado
  },
};

// Função para formatar a data
function formatDate(dateStr) {
  const [day, month, year] = dateStr.split('/');
  return `${day}/${month}/${year}`;
}

// Função para calcular a idade
function calculateAge(birthDate) {
  const [day, month, year] = birthDate.split('/');
  const birth = new Date(`${year}-${month}-${day}`);
  const ageDifMs = Date.now() - birth.getTime();
  const ageDate = new Date(ageDifMs);
  return Math.abs(ageDate.getUTCFullYear() - 1970);
}

// Função para buscar dados do banco de dados
async function getPeopleFromDatabase(offset = 0, limit = 1000) {
  const client = new Client(dbConfig);
  await client.connect();
  console.log(`Executando consulta SQL com offset ${offset} e limite ${limit}`);

  const query = `
    SELECT id, name, original_birth_date, mother_name 
    FROM "People" 
    WHERE city_id = 4 AND hydrate
    LIMIT ${limit} OFFSET ${offset};
  `;

  const res = await client.query(query);
  await client.end();

  const filteredPeople = res.rows.filter((person) => {
    const age = calculateAge(person.original_birth_date);
    return age >= 16 && age <= 30;
  });

  return filteredPeople;
}

// Função para atualizar dados do eleitor no banco de dados
async function updatePersonVoterData(personId, voterData) {
  const client = new Client(dbConfig);
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

// Função principal que cria o browser e executa o processo de scraping
async function executeVoterDataProcess(person) {
  const { id, name, original_birth_date: birthDate, mother_name: motherName } = person;

  const browser = await puppeteer.launch({
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-accelerated-2d-canvas',
      '--no-first-run',
      '--no-zygote',
      '--disable-gpu',
      '--incognito',
    ],
    headless: true
  });

  const page = await browser.newPage();

  try {
    await page.goto('https://www.tre-ce.jus.br/servicos-eleitorais/titulo-e-local-de-votacao/consulta-por-nome', {
      waitUntil: 'domcontentloaded',
      timeout: 120000,
    });

    // Fechar pop-up de cookies
    await page.waitForSelector('.cookies .botao button', { visible: true, timeout: 5000 });
    const cienteButton = await page.$('div.botao button.btn');
    if (cienteButton) await cienteButton.click();

    const formattedBirthDate = formatDate(birthDate);
    const normalizedName = normalize(name.toUpperCase());
    const normalizedMotherName = normalize(motherName.toUpperCase());

    // Preencher os campos do formulário
    await page.type('input[placeholder="Número do título eleitoral ou CPF ou nome"]', normalizedName);
    await page.type('input[placeholder="Data de nascimento (dia/mês/ano)"]', formattedBirthDate);
    await page.type('[formcontrolname=nomeMae]', normalizedMotherName);

    // Submeter o formulário
    const submitButton = await page.$('button[type="submit"]');
    if (submitButton) {
      await page.evaluate(button => button.click(), submitButton);
    } else {
      throw new Error('Botão de submissão não encontrado');
    }

    // Espera para o carregamento do conteúdo
    await page.waitForFunction(() => !document.body.innerText.includes('carregando conteúdo'), { timeout: 120000 });

    if ((await page.$('div.alert.alert-warning')) !== null) {
      console.log(`Pessoa não encontrada no sistema do TSE: ${name}`);
      return { error: 'Pessoa não encontrada' };
    }

    // Aguardar tempo adicional para garantir o carregamento
    await page.waitForTimeout(3000);

    // Extrair dados da página
    const data = await page.evaluate(() => {
      const voterComponent = document.querySelector('.componente-onde-votar');
      if (!voterComponent) {
        return {
          error: true,
          message: 'Pessoa não encontrada no sistema do TSE',
        };
      }

      const labels = Array.from(
        document.querySelectorAll('.lado-ov .data-box .label'),
      ).map((el) => el.textContent.trim() ?? null);

      const descs = Array.from(
        document.querySelectorAll('.lado-ov .data-box .desc'),
      ).map((el) => el.textContent.trim() ?? null);

      const result = {};
      const possibleLabels = {
        'Local de votação': 'local',
        'Endereço': 'endereco',
        'Município/UF': 'municipio',
        'Bairro': 'bairro',
        'Seção': 'secao',
        'Zona': 'zona',
      };

      labels.forEach((label, i) => {
        const key = possibleLabels[label];
        if (key) {
          result[key] = descs[i] || null;
        }
      });

      result.biometria = document.body.innerText.includes(
        'ELEITOR/ELEITORA COM BIOMETRIA COLETADA'
      );

      return { error: false, data: result };
    });

    if (data.error) {
      const screenshotPath = path.join(
        __dirname,
        'rpa',
        `pessoa_nao_encontrada_${name.replace(/\s+/g, '_')}.png`,
      );
      await page.screenshot({ path: screenshotPath });
      throw new Error(data.message);
    }

    console.log(`Dados encontrados com sucesso: ${JSON.stringify(data.data)}`);
    await updatePersonVoterData(id, data.data);
    return data.data;

  } catch (error) {
    console.error(`Erro ao processar ${name}: ${error.message}`);
    const screenshotPath = path.join(__dirname, 'rpa', `error_${name}.png`);
    await page.screenshot({ path: screenshotPath });
    throw error;
  } finally {
    await page.close();
    await browser.close();
  }
}

// Executar o processo para buscar pessoas do banco de dados e processar
(async function () {
  const people = await getPeopleFromDatabase();
  for (const person of people) {
    try {
      await executeVoterDataProcess(person);
    } catch (error) {
      console.error(`Erro no processamento do RPA para ${person.name}: ${error.message}`);
    }
  }
})();
