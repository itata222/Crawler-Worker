const express =require('express');
const axios=require('axios');
const cheerio=require('cheerio');
const {   sendMessageToQueue, pollMessageFromQueue, deleteMessagesFromQueue } = require('../utils/sqs');
const { setWorkAsDoneInRedis, isUrlExistInRedis, saveUrlInRedis, getCurrentLevelData,incrementChildsData, incrementLevelData } = require('../utils/redis');
const { parseUrl } = require('../utils/scraper');

const router=new express.Router();

router.post('/crawl',async(req,res)=>{
    const {maxiDepth,maxPages,rootUrl,QueueName} = req.body
    let running=true;

    while(running){
        console.log(maxiDepth,maxPages,rootUrl,QueueName)
        let Messages,QueueUrl;
        const polledResponse = await pollMessageFromQueue({QueueName,rootUrl});
        console.log('polledrespo',polledResponse)
        if(polledResponse==undefined){
            running=false;
            break;
        }
        else if(polledResponse.Messages==undefined||polledResponse.Messages.length===0)
            continue;
        
        Messages=polledResponse.Messages;
        QueueUrl=polledResponse.QueueUrl;
        
        console.log(2)
        try {
            const currentUrl=Messages[0].Body.split('$$$')[1];
            const isExist=await isUrlExistInRedis({currentUrl})
            const currentLevelData = await getCurrentLevelData();
            if(parseInt(currentLevelData.childs)>=parseInt(maxPages))
                await setWorkAsDoneInRedis()
            else if(!isExist){   
                await deleteMessagesFromQueue({Messages,QueueUrl})                
                // const workHasDoneResponse=await axios.post('http://localhost:5000/get-url',{url:currentUrl});
                // console.log(6,workHasDoneResponse.data)
                // if(workHasDoneResponse.data.done===true)
                //     break;
                const urlsToSave=[];
                const childrenUrls = await parseUrl(encodeURI(currentUrl))
                if(childrenUrls!=undefined)
                    for (let i = 0; i < childrenUrls.length; i++) {
                        if (i<parseInt(maxPages)-parseInt(currentLevelData.childs)) 
                            urlsToSave.push(childrenUrls[i])
                        else
                            break;
                    }
                else
                    continue;
                
                urlsToSave.map(async(url,i)=>{
                    console.log('url',url)
                    if(running){
                        if(i===0) await incrementLevelData();
                        await sendMessageToQueue({url,rootUrl,QueueUrl});
                        url==='/'?await saveUrlInRedis({rootUrl,parentAddress:currentUrl,myAddress:url,depth:parseInt(currentLevelData.level+1)}):
                                  await saveUrlInRedis({rootUrl,parentAddress:null,myAddress:url,depth:parseInt(0)});
                        await incrementChildsData();
                        console.log(parseInt(currentLevelData.childs))
                        if(parseInt(currentLevelData.childs)>=maxPages){
                            console.log(2124444444444)
                            await setWorkAsDoneInRedis()
                            running=false;
                        }
                    }
                })
            }
            else
                continue;
        } catch (e) {
            console.log('eeeeerrrooorrr',e)
            running=false;
        }
    }
})


module.exports=router