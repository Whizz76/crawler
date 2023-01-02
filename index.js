import { PlaywrightCrawler, log } from 'crawlee';
import { router } from './crawler_clusterService.js';
import cluster from 'cluster';
import { cpus } from 'os';
import process from 'process';
import { url } from "./url.js";
import { MongoClient } from "mongodb";
import express from "express";
const app=express();
var path = process.env.MONGO_URL || url;
const port=process.env.PORT || 8900;
app.listen(port,()=>{
    console.log("server connected");
})
// VARIABLE TO CHECK IF THERE IS A NEED TO FORK A WORKER
var fork = false;
// CONNECTING TO THE DATABASE THAT CONTAINS ALL THE LINKS THAT NEEDS TO BE DEPLOYED
const cl = new MongoClient(path, { useNewUrlParser: true, useUnifiedTopology: true });
// FUNCTION TO CHECK IF THERE EXISTS ANY LINK THAT'S STILL NOT PROCESSED
async function findOne() {
    try {
        const find = await cl.db("crawled_links").collection("links").findOne({ processed: false });
        // IF THERE EXISTS AN UNPROCESSED LINK THEN SET FORK TO TRUE ELSE FALSE
        if (find) {
            fork = true;
        }
        else {
            fork = false;
        }
    } catch (err) {
        console.log(err);
    } finally {
        // await client.close();
    }
}
// NUMBER OF CORES THAT WE WILL USE
const numCPUs = (cpus().length) / 2;
log.setLevel(log.LEVELS.DEBUG);

log.debug('Setting up crawler.');
const crawler = new PlaywrightCrawler({
    useSessionPool: true,
    persistCookiesPerSession: true,

    browserPoolOptions: {
        useFingerprints: false,
    },

    // Instead of the long requestHandler with
    // if clauses we provide a router instance.
    requestHandler: router,
    // headless:false,
});

log.debug('Adding requests to the queue.');
 if (cluster.isPrimary) {
    // Fork workers.
    for (let i = 0; i < numCPUs; i++) {
        cluster.fork();
    }
    cluster.on('exit', async (worker, code, signal) => {
        console.log(`worker ${worker.process.pid} died`);
        // IF THERE EXISTS A LINK/LINKS THAT'S STILL NOT PROCESSED THEN FORK A WORKER/CLUSTER
        await findOne();
        if (fork) {
            cluster.fork();
        }


    });

}
app.get('/',async (req,res)=>{
   // res.send("Welcome to Render!");
    if(cluster.isPrimary){
        res.send(`Master ${process.pid} is running`);
        await crawler.addRequests([{ url: 'https://www.myntra.com/', label: "MYNTRA" }, { url: 'https://www2.hm.com/en_in/index.html', label: "HNM" }]);
        await crawler.run();
    }
    else{
        res.send(`Worker ${process.pid} started`);
        await crawler.addRequests([{ url: "https://www.myntra.com/rain-jacket", label: "MYNTRA CATEGORY|PAGE" }, { url: 'https://www2.hm.com/en_in/women/seasonal-trending/holiday.html', label: "HNM CATEGORY|PAGE" }]);
        await crawler.run();
        process.exit(0);
    }
});
app.get('/masterH&M',async (req,res)=>{
    res.send(`Master ${process.pid} is running`);
    if (cluster.isPrimary) {
        console.log(`Number of CPUs is ${numCPUs}`);
        console.log(`Master ${process.pid} is running`);
        await crawler.addRequests([{ url: 'https://www2.hm.com/en_in/index.html', label: "HNM" }]);
        await crawler.run();
    }
});
app.get('/masterMyntra',async (req,res)=>{
    res.send(`Master ${process.pid} is running`);
    if (cluster.isPrimary) {
        console.log(`Number of CPUs is ${numCPUs}`);
        console.log(`Master ${process.pid} is running`);
        await crawler.addRequests([{ url: 'https://www.myntra.com/', label: "MYNTRA" }]);
        await crawler.run();
    }
});
app.get('/workerH&M',async (req,res)=>{
    res.send(`Worker ${process.pid} started`);
    if(cluster.isWorker){
        await crawler.addRequests([{ url: 'https://www2.hm.com/en_in/women/seasonal-trending/holiday.html', label: "HNM CATEGORY|PAGE" }]);
        await crawler.run();
        // AFTER CRAWLING IS DONE EXIT THE PROCESS
        process.exit(0);
    }
})
app.get('/workerMyntra',async (req,res)=>{
    res.send(`Worker ${process.pid} started`);
    if(cluster.isWorker){
        await crawler.addRequests([{ url: "https://www.myntra.com/rain-jacket", label: "MYNTRA CATEGORY|PAGE" }]);
        await crawler.run();
        // AFTER CRAWLING IS DONE EXIT THE PROCESS
        process.exit(0);
    }
})
