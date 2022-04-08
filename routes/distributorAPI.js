var express = require("express");
var app = express();
const puppeteer = require("puppeteer");

const site_login = "https://saga.sagatech.ca/ucs/main.php";
const site_url = "https://saga.sagatech.ca/ucs/main.php?func=device&id=1";
const site_logout = `https://saga.sagatech.ca/ucs/main.php?func=logout`;
const MOVIE_ID = ``;
async function scrapeData() {
    /* Initiate the Puppeteer browser */
    console.log("Start puppet")
    const browser = await puppeteer.launch();
    const page = await browser.newPage();
    /* Go to the IMDB Movie page and wait for it to load */
    // await page.goto(site_logout, { waitUntil: 'networkidle0' });
    await page.goto(site_login, { waitUntil: 'networkidle0' });
    /* Run javascript inside of the page */

    await page.type('#username', "ems");
    await page.type('#passwd', "19092441");
    await Promise.all([
        page.click('#LoginButton'),
        page.waitForNavigation({ waitUntil: 'networkidle0' }),
    ]);
    // await page.click('#LoginButton');
    // await page.waitForNavigation();
    console.log("Logged in at: " + page.url());
    await page.goto(site_url, { waitUntil: 'networkidle0' });

    let deviceMetadata = await page.evaluate(() => {
        const tds = Array.from(document.getElementsByClassName("devDetail"));
        return tds.map(td => td.innerText);
    });
    let headers = await page.evaluate(() => {
        const rows = document.querySelectorAll('.devList tr');
        return Array.from(rows, row => {
            const columns = row.querySelectorAll('th');
            return Array.from(columns, column => column.innerText);
        });
    });
    let data = await page.evaluate(() => {
        const rows = document.querySelectorAll('.devList tr');
        return Array.from(rows, row => {
            const columns = row.querySelectorAll('td');
            return Array.from(columns, column => column.innerText);
        });
    });
    data[0] = headers[0];
    // let data = await page.evaluate(() => {
    //     let title = document.querySelector('div[class="title_wrapper"] > h1').innerText;
    //     let rating = document.querySelector('span[itemprop="ratingValue"]').innerText;
    //     let ratingCount = document.querySelector('span[itemprop="ratingCount"]').innerText;
    //     /* Returning an object filled with the scraped data */
    //     return {
    //         title,
    //         rating,
    //         ratingCount
    //     }
    // });
    /* Outputting what we scraped */
    // console.log(deviceMetadata);
    // console.log(data);
    await page.goto(site_logout, { waitUntil: 'networkidle0' });
    await browser.close();
    return data;
}

app.get("/", function (req, res, next) {
    scrapeData().then((data) => {
        res.send(data);
    });

});

module.exports = app;