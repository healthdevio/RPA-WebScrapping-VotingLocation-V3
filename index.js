const { Client } = require('pg');
const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');
const { normalize } = require('./normalize');
const { formatDate, calculateAge, getPeopleFromDatabase } = require('./utils');
const dbConfig = {
  user: 'zerooitocincoadm',
  host: 'zerooitocincodb.c5qnvtxpcnuu.us-east-1.rds.amazonaws.com',
  database: '085db',
  password: 'GrzYTKeT3ZmSg4JfpnVqUYSd',
  port: 5432,
  ssl: {
    rejectUnauthorized: false,
  },
};

class FetchVotersDataUseCase {
  constructor() {
    this.browser = null;
  }

  async initBrowser() {
    console.log('Initializing browser');
    this.browser = await puppeteer.launch({
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
      headless: true, // Modo headless ativo para rodar em servidores
      executablePath: '/usr/bin/chromium',
    });
    console.log('Browser successfully initialized');
  }

  async closeBrowser() {
    if (this.browser) {
      await this.browser.close();
    }
  }

  async executeForAllPeople() {
    try {
      await this.initBrowser();

      let offset = 0;
      const limit = 1000;
      let people = [];

      do {
        people = await getPeopleFromDatabase(offset, limit);
        console.log(`Pessoas obtidas da base de dados: ${people.length}`);

        for (const person of people) {
          console.log(`Processando dados de: ${person.name}`);
          const result = await this.executeForPerson(person);
          await this.updatePersonInDatabase(person.id, result);
        }

        offset += limit;
      } while (people.length > 0);
    } catch (error) {
      console.error('Erro durante a execução:', error);
    } finally {
      await this.closeBrowser();
    }
  }

  async executeForPerson({ id, name, original_birth_date, mother_name }) {
    const formattedBirthDate = formatDate(original_birth_date);

    const page = await this.browser.newPage();
    page.on('request', (request) => {
      console.log('URL:', request.url());
      if (request.postData()) {
        console.log('Payload:', request.postData());
      }
    });

    try {
      await page.goto('https://www.tre-ce.jus.br/servicos-eleitorais/titulo-e-local-de-votacao/consulta-por-nome', {
        waitUntil: 'domcontentloaded',
        timeout: 120000,
      });

      await page.waitForSelector('.cookies .botao button', { visible: true, timeout: 5000 });
      const cienteButton = await page.$('div.botao button.btn');
      if (cienteButton) await cienteButton.click();

      await page.waitForSelector('[formcontrolname=TituloCPFNome]', { visible: true, timeout: 5000 });
      await page.type('[formcontrolname=TituloCPFNome]', normalize(name.toUpperCase()));

      await page.waitForSelector('[formcontrolname=dataNascimento]', { visible: true, timeout: 5000 });
      await page.type('[formcontrolname=dataNascimento]', formattedBirthDate);

      await page.waitForSelector('[formcontrolname=nomeMae]', { visible: true, timeout: 6000 });
      await page.type('[formcontrolname=nomeMae]', normalize(mother_name.toUpperCase()));

      await page.waitForSelector('.btn-tse', { visible: true, timeout: 6000 });
      const button = await page.$('.btn-tse');
      if (button) {
        await button.click();
      }

      await new Promise((resolve) => setTimeout(resolve, 5000));

      const data = await page.evaluate(() => {
        const voterComponent = document.querySelector('.componente-onde-votar');
        if (!voterComponent) {
          return { error: true, message: 'Pessoa não encontrada no sistema do TRE' };
        }

        const labels = Array.from(document.querySelectorAll('.lado-ov .data-box .label')).map((el) => el.textContent.trim() ?? null);
        const descs = Array.from(document.querySelectorAll('.lado-ov .data-box .desc')).map((el) => el.textContent.trim() ?? null);

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
          if (key) result[key] = descs[i] || null;
        });

        result.biometria = document.body.innerText.includes('ELEITOR/ELEITORA COM BIOMETRIA COLETADA');

        return { error: false, data: result };
      });

      if (data.error) {
        throw new Error(data.message);
      }
      return data.data;
    } catch (error) {
      console.error(`Erro ao processar ${name}:`, error.message);
      const screenshotPath = path.join(__dirname, 'rpa', `erro_${name.replace(/\s+/g, '_')}.png`);
      if (!fs.existsSync(path.join(__dirname, 'rpa'))) {
        fs.mkdirSync(path.join(__dirname, 'rpa'), { recursive: true });
      }
      await page.screenshot({ path: screenshotPath });
      throw error;
    } finally {
      await page.close();
    }
  }

  async updatePersonInDatabase(personId, data) {
    const client = new Client(dbConfig);
    await client.connect();

    const query = `
      UPDATE "People" 
      SET voting_location = $1, updated_at = NOW() 
      WHERE id = $2;
    `;
    const values = [JSON.stringify(data), personId];

    try {
      await client.query(query, values);
      console.log(`Dados de ${personId} atualizados com sucesso.`);
    } catch (error) {
      console.error('Erro ao atualizar dados no banco de dados:', error);
    } finally {
      await client.end();
    }
  }
}

// Inicializa e executa o processo
const useCase = new FetchVotersDataUseCase();
useCase.executeForAllPeople();
