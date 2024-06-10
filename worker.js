const puppeteer = require('puppeteer');
const { Client } = require('pg');

const dbConfig = {
  connectionString: 'postgresql://zerooitocincoadm:GrzYTKeT3ZmSg4JfpnVqUYSd@zerooitocincodb.c5qnvtxpcnuu.us-east-1.rds.amazonaws.com:5432/085db?schema=public',
  ssl: {
    rejectUnauthorized: false
  }
};

function formatDate(dateStr) {
  const [year, month, day] = dateStr.split('-');
  return `${day}/${month}/${year}`;
}

async function processPerson(person) {
  console.log(`\nProcessando pessoa com nome: ${person.name}`);
  const browser = await puppeteer.launch({ headless: false, args: ['--no-sandbox'] });
  const page = await browser.newPage();

  try {
    await page.goto('https://www.tre-ce.jus.br/servicos-eleitorais/titulo-e-local-de-votacao/consulta-por-nome', { waitUntil: 'networkidle2' });

    if (await page.$('button[title="Ciente"]') !== null) {
      await page.click('button[title="Ciente"]');
      console.log('Pop-up de cookies fechado.');
    }

    const formattedBirthDate = formatDate(person.birth_date);

    if (await page.$('#LV_NomeTituloCPF') !== null) {
      await page.type('#LV_NomeTituloCPF', person.name);
      console.log(`Nome preenchido: ${person.name}`);
    } else {
      return { status: 'skipped', person };
    }

    if (await page.$('#LV_DataNascimento') !== null) {
      await page.type('#LV_DataNascimento', formattedBirthDate);
      console.log(`Data de nascimento preenchida: ${formattedBirthDate}`);
    } else {
      return { status: 'skipped', person };
    }

    if (await page.$('#LV_NomeMae') !== null) {
      await page.type('#LV_NomeMae', person.mother_name);
      console.log(`Nome da mãe preenchido: ${person.mother_name}`);
    } else {
      return { status: 'skipped', person };
    }

    try {
      await page.waitForSelector('#consultar-local-votacao-form-submit', { visible: true, timeout: 90000 });
      const button = await page.$('#consultar-local-votacao-form-submit');
      await page.evaluate(b => b.click(), button);

      console.log(`Submetendo formulário para nome: ${person.name}`);

      await page.waitForFunction(() => !document.body.innerText.includes('carregando conteúdo'), { timeout: 90000 });

      if (await page.$('div.alert.alert-warning') !== null) {
        console.log(`Pessoa não encontrada no sistema do TRE: ${person.name}`);
        await page.screenshot({ path: `pessoa_nao_encontrada_${person.name}.png` });
        return { status: 'not_found', person };
      }

      const data = await page.evaluate(() => {
        const getText = (selector) => {
          const element = Array.from(document.querySelectorAll('p')).find(el => el.textContent.includes(selector));
          return element ? element.textContent.split(': ')[1].trim() : null;
        };

        return {
          zona: getText('Zona:'),
          secao: getText('Seção:'),
          local: getText('Local:'),
          endereco: getText('Endereço:'),
          municipio: getText('Município:'),
          biometria: document.body.innerText.includes("ELEITOR/ELEITORA COM BIOMETRIA COLETADA")
        };
      });

      if (!data.zona || !data.secao || !data.local || !data.endereco || !data.municipio) {
        console.log(`Dados nulos encontrados para ${person.name}. Pulando atualização.`);
        return { status: 'failed', person };
      }

      console.log(`Dados encontrados para nome: ${person.name}: ${JSON.stringify(data)}`);
      return { status: 'success', data, person };
    } catch (formError) {
      console.log(`Erro ao submeter formulário para ${person.name}`);
      return { status: 'failed', person };
    }
  } catch (error) {
    console.log(`Erro ao processar ${person.name}: ${error.message}`);
    return { status: 'failed', person };
  } finally {
    await browser.close();
  }
}

async function main() {
  const client = new Client(dbConfig);
  await client.connect();

  const person = JSON.parse(process.argv[2]);
  const result = await processPerson(person);

  if (result.status === 'success') {
    const { data, person } = result;
    await client.query(
      'UPDATE public."People" SET zona_eleitoral = $1, secao_eleitoral = $2, local_votacao = $3, endereco_votacao = $4, municipio_votacao = $5, biometria = $6, preenchidorpa = true WHERE id = $7',
      [parseInt(data.zona, 10), parseInt(data.secao, 10), data.local, data.endereco, data.municipio, data.biometria, person.id]
    );
  }

  await client.end();
  process.send(result);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
