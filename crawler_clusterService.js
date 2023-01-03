import { createPlaywrightRouter } from 'crawlee';
import * as cheerio from "cheerio";
import _ from "lodash";
import { MongoClient } from "mongodb";
import cluster from 'cluster';
import { cpus } from 'os';
// STORES THE NUMBER OF CPUS WE WILL BE USING, HERE WE ARE USING HALF THE NUMBER I.E 4
const numCPUs = (cpus().length) / 2;
var path=process.env.MONGO_URL;
// CONNECT TO THE MONOGODB
export const client = new MongoClient(path, { useNewUrlParser: true, useUnifiedTopology: true });

// CREATING THE ROUTER
export const router = createPlaywrightRouter();
// FUNCTION TO ADD THE LINKS TO OUR DATABASE DURING THE MASTER PROCESS
async function addLinks(link, id, collectionName) {
    try {
        await client.connect();
        console.log("Database connected");
        const findLi = await client.db("crawled_links").collection(collectionName).findOne({ link: link });
        if (!findLi) {
            const result = await client.db("crawled_links").collection(collectionName).insertOne({ link: link, "url_id": id, "processed": false });
            console.log(`link added ${result.insertedId}`);

        }
        else {
            console.log("Link already present");
        }

    } catch (err) {
        console.log('error', err);
    } finally {
        // await client.close();
    }
}

// FUNCTION TO CHECK IF THE CURRENT CRAWLING NEEDS TO BE DONE OR NOT
async function toCrawl(link, id, collectionName, obj) {

    try {
        await client.connect();
        console.log("Database connected");
        const url = await client.db("crawled_links").collection(collectionName).findOne({ link: link });
        if (url) {
            if (url["url_id"] == id && url["processed"] == false) {

                obj.check = true;
            }
            else {
                obj.check = false;
            }
        }
        else {
            const result = await client.db("crawled_links").collection(collectionName).insertOne({ link: link, "url_id": id, processed: false });
            console.log(`link added ${result.insertedId}`);
            obj.check = true;
        }
    } catch (err) {
        console.log('error', err);
    } finally {
        // await client.close();
    }


}

// FUNCTION TO UPDATE THAT THE COMPLETE CRAWLING FOR THE GIVEN URL INCLUDING ITS ALL PAGES IS DONE
async function updateLink(link, id, collectionName) {
    try {
        await client.connect();
        console.log("Database connected");
        const url = await client.db("crawled_links").collection(collectionName).findOne({ link: link });
        if (url) {
            if (url["url_id"] == id && url["processed"] == false) {
                const updated = await client.db("crawled_links").collection(collectionName).updateOne({ link: link }, { $set: { "processed": true } });
                console.log(`link processed ${updated.modifiedCount}`)
            }
        }
    } catch (err) {
        console.log('error', err);
    } finally {
        // await client.connect();
    }
}

// FUNCTION TO ADD PRODUCT LINKS TO OUR DATABASE

async function addProduct(product, collectionName) {
    try {
        // Connect to the MongoDB cluster
        await client.connect();
        console.log("Database connected");
        const findProd = await client.db("crawledData").collection(collectionName).findOne({ productLink: product["productLink"] });

        if (findProd && findProd["categorizedBySellerIn"].length > 0) {
            // updating existing product
            const newCategory = product["categorizedBySellerIn"][0];

            if (!findProd["categorizedBySellerIn"].find(r => ((_.isEqual(r["path"], newCategory["path"]) == true) || r["url"] == newCategory["url"]))) {

                var updated = [...findProd["categorizedBySellerIn"], newCategory];
                const result = await client.db("crawledData").collection(collectionName).updateOne({ _id: findProd["_id"] }, { $set: { categorizedBySellerIn: updated } });
                console.log(`Data updated: ${result.modifiedCount}`);
            }
        }
        else {
            // adding new product
            const result = await client.db("crawledData").collection(collectionName).insertOne(product);
            console.log(`Data added with id: ${result.insertedId}`);
        }


    } catch (e) {
        console.error(e);
    } finally {
        // await client.close();
    }
}

// REMOVE OFFSET/PAGINATION QUERIES FROM THE URLS

async function removeOffsets(link, website) {
    var url = link;
    if (website == "hnm") {
        if (link.includes("?")) {
            url = link.slice(0, link.indexOf("?"));
        }
    }
    if (website == "myntra") {
        if (link.includes("?f=")) {
            url = link.slice(0, link.indexOf("&p="))
        }
        else if (link.includes("?p=")) {
            url = link.slice(0, link.indexOf("?p="));
        }
    }
    return url;
}

// MYNTRA CATEGORY|PAGE
router.addHandler('MYNTRA CATEGORY|PAGE', async ({ request, page, enqueueLinks, log }) => {
    // VARIABLE TO STORE THE ID OF THE WORKER, BY DEFAULT IDS OF WORKER ARE ONE-INDEXED SO
    // WORKER ID-1 IS DONE
    var id = cluster.worker.id - 1;
    if (id >= numCPUs) {
        id = id % numCPUs;
    }
    // REMOVING OFFSETS/PAGINATION QUERIES FROM URLS SO AS TO KEEP A TRACK OF THE CATEGORY WE ARE 
    // CRAWLING
    var link = await removeOffsets(request.url, "myntra");
    // AN OBJECT WHOSE CHECK VALUE TELLS US WHETHER THE CURRENT CRAWLING NEEDS TO BE DONE OR NOT
    // IT CHECKS THAT IF THE THE LINK IS UNPROCESSED AND THE ID ASSIGNED IS SAME AS THE WORKER ID
    var obj = { check: true }
    await toCrawl(link, id, "links", obj);
    if (obj.check) {
        const start = Date.now();
        log.debug(`Processing CATEGORY|PAGE request: ${request.url}`);

        // extracting the page number
        const requestUrl = request.url;
        var pageNumber = 1;
        const idx = requestUrl.lastIndexOf("p=");
        if (idx != -1)
            pageNumber = Number(requestUrl.substring(idx + 2));
        var limit = await page.locator(".title-count").allTextContents();
        var m = Number(limit[0].split(" ")[2]);
        // extracting category
        const categoryList = page.locator('.breadcrumbs-item span')
        const category = await categoryList.allTextContents();
        category.shift();

        // pushing data 
        const productUrls = page.locator('.product-base a');
        const count2 = await productUrls.count();

        for (var i = 0; i < count2; i++) {
            const element = productUrls.nth(i);
            const url = await element.getAttribute('href');

            const resultProduct = {
                seller: "https://www.myntra.com/",
                productLink: "https://www.myntra.com/" + url,
                categorizedBySellerIn: [{ url: requestUrl, path: category, rank: i + 1 + 50 * (pageNumber - 1), updatedAt: Date.now() }]
            }
            await addProduct(resultProduct, "products");
        }
        // enqueuing next page url
        const nextPageCount = await (page.locator('.pagination-number a')).count();

        if (50 * pageNumber <= m && nextPageCount > 0) {
            await page.waitForSelector('.pagination-number a');
            await enqueueLinks({
                selector: '.pagination-number > a',
                label: 'MYNTRA CATEGORY|PAGE',
            })
        }
        else {
            await updateLink(link, id, "links");
        }

        const end = Date.now();
        log.debug(`{Request Completed: ${request.url}, Start time: ${start} ms, End time: ${end} ms, Execution time: ${end - start} ms}`);

    }


});

//MYNTRA START URL
router.addHandler('MYNTRA', async ({ request, page, enqueueLinks, log }) => {
    console.log("started myntra crawl");
    const start = Date.now();
    const $ = cheerio.load(await page.content());
    console.log($("title").text());
    //await page.waitForSelector('.desktop-categoryName', { state: 'hidden', timeout: 0 });
   // await page.waitForSelector('.desktop-categoryLink', { state: 'hidden', timeout: 0 });
    // VARIABLE TO STORE LINKS THAT NEEDS TO BE CRAWLED
    var links = [];
    $(".desktop-navLinks").children(".desktop-navContent").children(".desktop-navLink").children("a").each((index, element) => {
        // VARIABLE TO STORE THE PARENT CATEGORY
        var t = $(element).text();
        console.log(t);
        // ADD ONLY LINKS THAT BELONG TO MEN,WOMEN,KIDS CATEGORIES I.E SKIP BEAUTY,HOME,STUDIO ETC. CATEGORIES
        if (t == "Men" || t == "Women" || t == "Kids") {
            $(element).siblings(".desktop-backdropStyle").children(".desktop-paneContent").children(".desktop-categoryContainer").children("li").children("ul").children("li").children("a").each((i, e) => {
                links.push("https://www.myntra.com" + $(e).attr("href"));
         
            });

        }
    })
    // IF OUR PROCESS IS A MASTER PROCESS THEN ADD THOSE LINKS TO OUR DATABASE
    if (cluster.isPrimary) {
        log.debug(`Proessing start request: ${request.url}`);
        for (let i = 0; i < links.length; i++) {
            var lid = i % numCPUs;
            console.log(`adding link ${links[i]}`);
            await addLinks(links[i], lid, "links");
        }

    }
    // IF OUR PROCESS IS A WORKER/CHILD PROCESS THEN START THE CRAWLING
    else if (cluster.isWorker) {
        i = cluster.worker.id - 1;
        console.log(`worker ${i} working---`);
        await enqueueLinks({
            selector: '.desktop-categoryName',
            label: 'MYNTRA CATEGORY|PAGE',
            transformRequestFunction(req) {
                if (links.find(r => r == req.url)) {
                    return req;
                }
                else {
                    return false;
                }
            }
        })


        await enqueueLinks({
            selector: '.desktop-categoryLink',
            label: 'MYNTRA CATEGORY|PAGE',
            transformRequestFunction(req) {
                if (links.find(r => r == req.url)) {
                    return req;
                }
                else {
                    return false;
                }
            }
        })

    }


    const end = Date.now();
    log.debug(`{Request Completed: ${request.url}, Start time: ${start} ms, End time: ${end} ms, Execution time: ${end - start} ms}`);
});

// H&M CATEGORY EXTRACTION URL
router.addHandler('HNM CATEGORY|PAGE', async ({ request, page, enqueueLinks, log }) => {
    var id = cluster.worker.id - 1;
    if (id >= numCPUs) {
        id = id % numCPUs;
    }
    var link = await removeOffsets(request.url, "hnm");
    var obj = { check: true }
    await toCrawl(link, id, "links", obj);
    if (obj.check) {
        console.log(`worker ${cluster.worker.id - 1} has to work for ${request.url}`);
        var start = Date.now();
        const $ = cheerio.load(await page.content());
        var end = Date.now();
        log.debug(`Proessing start request: ${request.url}`);
        console.log(`Time elapsed: ${end - start} milliseconds`);
        var l = $(".load-more-heading").attr("data-items-shown");
        var h = $(".load-more-heading").attr("data-total");

        // title -> stores the category
        var t = "";
        // extracting the category
        $(".Breadcrumbs-module--container__r8UxR").children('.Breadcrumbs-module--list__EvLWC').children(".Breadcrumbs-module--listItem__ROEM2").each((index, element) => {
            t += $(element).text();
        })
        var title = t.split("/").slice(1);
        // an array pdt used to avoid addition of products present at different ranks in the same page
        // to get added to our database
        var pdt = [];
        $(".item-details ul a").each((index, element) => {
            var prUrl = "https://www2.hm.com" + $(element).attr("href");

            if (!pdt.find(r => r == prUrl)) {
                pdt.push(prUrl);
                var prod = {};
                prod["seller"] = { "url": "https://www2.hm.com/en_in/" }
                prod["productLink"] = prUrl;
                prod["categorizedBySellerIn"] = [];
                var category = { "url": request.url, "path": title, "rank": index + 1, "updatedAt": Date.now() };
                prod["categorizedBySellerIn"].push(category);
                addProduct(prod, "products");
                // once we get a product detail, connect to the database and add this data to the database


            }

        })
        // for enqueuing next page urls if more items needs to be loaded i.e when l<h
        if (l != h) {
            await page.waitForSelector(".pagination-links-list");
            await enqueueLinks({
                selector: ".pagination-links-list",
                label: "HNM CATEGORY|PAGE"
            })
        }
        // when l==h enqueue the child categories urls of the given category
        else if (l == h) {
            await updateLink(link, id, "links");
        }
    }
    // VARIABLE TO COUNT THE CHILD CATEGORIES
    const urls = page.locator(".current + .menu .item a");
    // VARIABLE TO COUNT THE NUMBER OF CHILD CATEGORIES
    const ln = await urls.count();
    // IF THERE EXISTS A CHILD CATEGORY THEN CRAWL IT
    if (ln) {
        await page.waitForSelector(".current + .menu .item a");
        await enqueueLinks({
            selector: ".current + .menu .item a",
            label: "HNM CATEGORY|PAGE"
        })
    }
})

// H&M START URL
router.addHandler('HNM', async ({ request, page, enqueueLinks, log }) => {
    const start = Date.now();
    // LOAD THE MAIN PAGE
    const $ = cheerio.load(await page.content());
    await page.waitForSelector(".ZBbS .TzhG", { state: 'hidden', timeout: 0 });
    // VARIABLE TO STORE THE LINKS THAT ARE TO BE CRAWLED
    var links = [];
    $(".ZBbS li > .A9Gx").each((i, el) => {
        var t = $(el).text();
        if (t == "Ladies" || t == "Men" || t == "Kids" || t == "Baby") {
            $(el).siblings(".OTqB").children(".hVLp").children("li").children("span").each((n, e) => {
                var u = $(e).text();
                if (u == "Shop by Product") {
                    $(e).siblings("ul").children("li").children("a").each((index, element) => {
                        var url = "https://www2.hm.com" + $(element).attr("href");
                        if (!url.includes("view-all")) {
                            links.push(url);
                        }
                    })
                }
            })
        }
    });
    // IF THE PROCESS IS MASTER ADD LINKS TO OUR DATABASE
    if (cluster.isPrimary) {
        for (let i = 0; i < links.length; i++) {
            var lid = i % numCPUs;
            console.log(`adding link ${links[i]}`);
            await addLinks(links[i], lid, "links");
        }

    }
    // ELSE START CRAWLING
    else if (cluster.isWorker) {
        i = cluster.worker.id - 1;
        console.log(`worker ${i} working---`);
        await enqueueLinks({
            selector: ".ZBbS .TzhG",
            label: "HNM CATEGORY|PAGE",
            transformRequestFunction(req) {
                if (links.find(r => r == req.url)) {
                    return req;
                }
                else {
                    return false;
                }
            }
        });
    }
    const end = Date.now();
    log.debug(`{Request Completed: ${request.url}, Start time: ${start} ms, End time: ${end} ms, Execution time: ${end - start} ms}`);
});



