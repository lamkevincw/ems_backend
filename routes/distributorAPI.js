var express = require("express");
var app = express();
const puppeteer = require("puppeteer");
var moment = require("moment");

const site_login = "https://saga.sagatech.ca/ucs/main.php";
const site_url = "https://saga.sagatech.ca/ucs/main.php?func=device&id=";
const site_logout = `https://saga.sagatech.ca/ucs/main.php?func=logout`;
let credentials = require("../sagatech.json");

let CACHED_DATA = false;
let scraped_date = moment();

async function scrapeData(queryID) {
    /* Initiate the Puppeteer browser */
    console.log("Start puppet")
    const browser = await puppeteer.launch();
    const page = await browser.newPage();

    // Login to sagatech website
    await page.goto(site_login, { waitUntil: 'networkidle0' });
    await page.type('#username', credentials.username);
    await page.type('#passwd', credentials.password);
    await Promise.all([
        page.click('#LoginButton'),
        page.waitForNavigation({ waitUntil: 'networkidle0' }),
    ]);
    console.log("Logged in at: " + page.url());
    let devList = await page.evaluate(() => {
        const rows = document.querySelectorAll('.devList tr');
        return Array.from(rows, row => row);
    });
    console.log(devList.length);

    // Scrape data from relevant devices
    var dataset = {};
    for (var i = 1; i < devList.length + 1; i++) {
        await page.goto(site_url + i, { waitUntil: 'networkidle0' });

        // let deviceMetadata = await page.evaluate(() => {
        //     const tds = Array.from(document.getElementsByClassName("devDetail"));
        //     return tds.map(td => td.innerText);
        // });
        let headerRow = await page.evaluate(() => {
            const rows = document.querySelectorAll('.devList tr');
            return Array.from(rows, row => {
                const columns = row.querySelectorAll('th');
                return Array.from(columns, column => column.innerText);
            });
        });
        let dataRows = await page.evaluate(() => {
            const rows = document.querySelectorAll('.devList tr');
            return Array.from(rows, row => {
                const columns = row.querySelectorAll('td');
                return Array.from(columns, column => column.innerText);
            });
        });
        dataRows[0] = headerRow[0];
        
        dataset[i] = dataRows;
    }

    // console.log(deviceMetadata);
    // console.log(data);
    // Logout and close browser when finished scraping
    await page.goto(site_logout, { waitUntil: 'networkidle0' });
    await browser.close();
    console.log("Logged out of sagatech.")
    return dataset;
}

app.get("/", function (req, res, next) {
    if (scraped_date.diff(moment(), "days") == 0 && CACHED_DATA) {
        res.send(CACHED_DATA);
    } else {
        scrapeData(req.query.id).then((data) => {
            scraped_date = moment();
            CACHED_DATA = {...data};
            res.send(data);
        })
        .catch(console.log);
    }
});

module.exports = app;