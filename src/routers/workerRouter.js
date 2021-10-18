const express =require('express');
const {   sendMessageToQueue, pollMessageFromQueue, deleteMessagesFromQueue } = require('../utils/sqs');
const { setWorkAsDoneInRedis, isUrlExistInRedis, saveUrlInRedis, getCurrentLevelData,incrementChildsData, incrementLevelData } = require('../utils/redis');
const { parseUrl } = require('../utils/scraper');

const router=new express.Router();

router.post('/crawl',async(req,res)=>{
    const {maxiDepth,maxPages,rootUrl,QueueName} = req.body
    let Messages,QueueUrl,running=true;

    while(running){
        const polledResponse = await pollMessageFromQueue({QueueName,rootUrl});
        if(polledResponse==undefined)
            break;
        else if(polledResponse.Messages==undefined||polledResponse.Messages.length===0)
            continue;
        
        Messages=polledResponse.Messages;
        QueueUrl=polledResponse.QueueUrl;
        
        try {
            const currentUrl=Messages[0].Body.split('$$$')[1];
            const isExist=await isUrlExistInRedis({currentUrl,rootUrl})
            const currentLevelData = await getCurrentLevelData();
            if(isExist&&rootUrl.includes(currentUrl)){
                await saveUrlInRedis({rootUrl,parentAddress:null,myAddress:rootUrl,depth:parseInt(0)});
                await setWorkAsDoneInRedis()
                break;
            }
            if(parseInt(currentLevelData.childs)>=parseInt(maxPages))
                await setWorkAsDoneInRedis()
            else if(!isExist){   
                await deleteMessagesFromQueue({Messages,QueueUrl})                
                const urlsToSave=[];
                const childrenUrls = await parseUrl(encodeURI(currentUrl))
                if(childrenUrls!=undefined)
                    for (let i = 0; i < childrenUrls.length; i++) 
                        if (i<parseInt(maxPages)-parseInt(currentLevelData.childs)) 
                            urlsToSave.push(childrenUrls[i])
                        else
                            break;
                    
                else
                    continue;
                
                urlsToSave.map(async(url,i)=>{
                    if(running){
                        if(i===0) await incrementLevelData();
                        await sendMessageToQueue({url,rootUrl,QueueUrl});
                        url !== '/'?await saveUrlInRedis({rootUrl,parentAddress:currentUrl,myAddress:url,depth:parseInt(currentLevelData.level+1)}):
                                  await saveUrlInRedis({rootUrl,parentAddress:null,myAddress:rootUrl,depth:parseInt(0)});
                        await incrementChildsData();
                        if(parseInt(currentLevelData.childs)>=maxPages){
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
            await setWorkAsDoneInRedis()
            running=false;
        }
    }
})


module.exports=router