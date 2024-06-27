import puppeteer from "puppeteer";
import { MongoClient } from 'mongodb';

const FUNDS_RANKING_URL = 'https://www.fundsexplorer.com.br/ranking';
const MONGO_URI = 'mongodb://localhost:27017';
const MONGO_DB = 'fin_data';

const run = async () => {
    const rawFundData = await scrapeFundData();
    if (!rawFundData) return;

    const sanitizedFundData = rawFundData.map(toModel);
    toMongoInBulk(sanitizedFundData);
};

const scrapeFundData = async () => {
    console.log('Starting to scrape data...');

    const browser = await puppeteer.launch();
    const page = await browser.newPage();

    try {
        await page.goto(FUNDS_RANKING_URL);
        await page.waitForSelector(`.default-fiis-table__container__table`);

        const data = await page.evaluate(() => {
            const header = [...document.querySelectorAll('.default-fiis-table__container__table > thead > tr')][0];    
            const columns = [...header.cells].map(cell => cell.textContent);
            
            const rows = [...document.querySelectorAll('.default-fiis-table__container__table > tbody > tr')];

            const data = rows.map(row => {
                const fund = {};
                
                for (let i = 0; i < columns.length; i++) {
                    fund[columns[i]] = row.cells[i].textContent;
                }

                return fund;
            });

            return data;
        });

        console.log('Data scraped successfully!')
        return data;
    } catch (err) {
        console.log(`Error: ${err}`);
    } finally {
        await browser.close();
    }
};

const toModel = rawFundData => {
    return {
        code: rawFundData['Fundos'],
        category: rawFundData['Setor'],
        price: toNum(rawFundData['Preço Atual (R$)']),
        liquidity: toNum(rawFundData['Liquidez Diária (R$)']),
        pvpa: percent(rawFundData['P/VPA']),
        dy: percent(rawFundData['DY (12M) Acumulado'])
    };
};

const toNum = str => {
    if (str.trim() === 'N/A') return 0;

    return parseFloat(str
        .replace(/\./g, '')
        .replace(/,/g, '.')
    );
};

const percent = str => {
    if (str.trim() === 'N/A') return 0;

    return parseFloat(str
        .replace(/\./g, '')
        .replace(/,/g, '.')
        .replace('%', '')
        .replace(' ', '')
    ) / 100;
};

const toMongoInBulk = async fundData => {
    console.log('Starting to persist in bulk...');

    let client;

    try {
        client = new MongoClient(MONGO_URI);
        await client.connect();
    } catch (err) {
        console.log('Could not connect to Mongo!');
        return;
    }

    try {
        const db = client.db(MONGO_DB);
        const collection = db.collection('fii_data');

        const bulkOperations = fundData.map(({ code, category, price, liquidity, pvpa, dy }) => ({
            updateOne: {
                filter: { code },
                update: {
                    $set: { category, price, liquidity, pvpa, dy }
                },
                upsert: true
            }
        }));

        const result = await collection.bulkWrite(bulkOperations);
        console.log(`Updated ${result.upsertedCount} records successfully!`);
    } catch (err) {
        console.log(`Error: ${err}`);
    } finally {
        await client.close();
    }
};

run();