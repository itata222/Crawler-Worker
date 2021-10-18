const express =require('express');
const {   sendMessageToQueue, pollMessageFromQueue, deleteMessagesFromQueue } = require('../utils/sqs');
const { setWorkAsDoneInRedis, isUrlExistInRedis, saveUrlInRedis, getCurrentLevelData, incrementLevelData, incrementUrlsInCurrentLevelScannedData, getWorkDictData, incrementTotalUrlsData, addUrlsToCurrentLevelToScanData } = require('../utils/redis');
const { scrapeUrl } = require('../utils/scraper');

const router=new express.Router();
const QueueName='Crawler-Queue1';

router.post('/crawl',async(req,res)=>{
    
    while(true){
        //--------------------extracting data--------------------
        console.log(0)
        const {rootUrl,maxDepth,maxTotalPages,totalUrls}= await getWorkDictData();
        const currentLevelData = await getCurrentLevelData();
        const polledResponse = await pollMessageFromQueue({QueueName,rootUrl});      
        
        console.log(1)
        //--------------------polling tests------------------------------
        if(polledResponse==undefined) break;
        else if(polledResponse.Messages==undefined||polledResponse.Messages.length===0) continue;

        console.log(2)
        //--------------------decalre data after verifying polling request succeded ---------------
        const Messages=polledResponse.Messages;
        const QueueUrl=polledResponse.QueueUrl;
        const currentUrl=Messages[0].Body.split('$')[1].split('$')[0];
        const parentUrl=Messages[0].Body.split('$')[1].split('$')[1];
        
        console.log(3)
        //---------------------check if the whole work has need to be done------------------
        if((parseInt(totalUrls)>=maxTotalPages)||(maxDepth===parseInt(currentLevelData.currentLevel)&&
            parseInt(currentLevelData.urlsInCurrenLtevelAlreadyScanned)===parseInt(currentLevelData.urlsInCurrentLevelToScan))){
            await setWorkAsDoneInRedis()
            break;
        }

        console.log(4)
        //----------------------general tests------------------------
        const isExist=await isUrlExistInRedis({currentUrl,rootUrl})
        if(isExist){
            await incrementTotalUrlsData();
            await deleteMessagesFromQueue({Messages,QueueUrl})
            await incrementUrlsInCurrentLevelScannedData();
            continue;
        }

        console.log(5)
        //-----------------------scrape-------------------------
        const childrenUrls = await scrapeUrl(encodeURI(currentUrl))
        if(childrenUrls==undefined)continue;
        
        console.log(6)
        //----------------------update Redis after scrape data----------------------
        const parentAddress=rootUrl===currentUrl?null:parentUrl;
        const position=rootUrl===currentUrl?0:totalUrls;
        await addUrlsToCurrentLevelToScanData(childrenUrls.length)
        await incrementTotalUrlsData();
        await incrementUrlsInCurrentLevelScannedData();
        await deleteMessagesFromQueue({Messages,QueueUrl})
        await saveUrlInRedis({parentAddress,myAddress:currentUrl,depth:currentLevelData.currentLevel,rootUrl,position,childrenUrls})

        console.log(7)
        //----------------------update SQS---------------------------
        await deleteMessagesFromQueue({Messages,QueueUrl}) 

        console.log(8)
        //----------------------check if the level done-------------------
        if(parseInt(currentLevelData.urlsInCurrentLevelAlreadyScanned)+1===parseInt(currentLevelData.urlsInCurrentLevelToScan)){
            await incrementLevelData()
            for(let i=0;i<childrenUrls[i];i++)
                await sendMessageToQueue({url:childrenUrls[i],rootUrl,QueueUrl,parentUrl:currentUrl});
            console.log(9)
        }
        console.log(10)
    }
})


module.exports=router
