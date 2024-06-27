import puppeteer from "puppeteer";

const FUNDS_RANKING_URL = 'https://www.fundsexplorer.com.br/ranking';

const run = async () => {
    const rawFundData = await scrapeFundData();

    if (!rawFundData) {
        console.log('could not scrape fund data');
        return;
    }

    const sanitizedFundData = rawFundData.map(toModel);
};

const scrapeFundData = async () => {
    const browser = await puppeteer.launch();
    const page = await browser.newPage();

    try {
        await page.goto(FUNDS_RANKING_URL);
        await page.waitForSelector(`.default-fiis-table__container__table`);

        return await page.evaluate(() => {
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

run();