const redisClient = require('../db/redis');

const isUrlExistInRedis=(async({currentUrl,rootUrl})=>{
    let isExist,response;
    try {     
        response=await redisClient.keysAsync(`${rootUrl}$$$${currentUrl}`);
        let response2=await redisClient.keysAsync(`*`);
        (response==null||response?.length===0)?isExist=false:isExist=true;
        console.log(response,response2)
        return isExist
    } catch (e) {
        console.log(e)
        return e;
    }
})
const getCurrentLevelData=(async()=>{
    try {        
        const response=await redisClient.hgetallAsync('levelData');
 
        return response;
    } catch (e) {
        console.log(e)
        return e;
    }
})
const incrementUrlsInCurrentLevelScannedData=(async()=>{
    try {        
        const response=await redisClient.hincrbyAsync('levelData','urlsInCurrentLevelAlreadyScanned',1) 
        return response;
    } catch (e) {
        return e;
    }
})
const setUrlsInCurrentLevelToScanData=(async(childsUrl)=>{
    try {        
        const response=await redisClient.hsetAsync('levelData','urlsInCurrentLevelToScan',parseInt(childsUrl)) 
        return response;
    } catch (e) {
        return e;
    }
})
const incrementLevelData=(async()=>{
    try {        
        const response=await redisClient.hincrbyAsync('levelData','currentLevel',1)   
        return response;
    } catch (e) {
        return e;
    }
})

const saveUrlInRedis=async({parentAddress,myAddress,depth,rootUrl})=>{
    let address=myAddress;
    try {     
        if(myAddress!=undefined&&!myAddress.includes('http'))
            address=myAddress.substring(1)
        const urlObj={parentAddress,myAddress:address,depth,rootUrl}
        const urlStr=JSON.stringify(urlObj)

        await redisClient.setexAsync(`${rootUrl}$$$${address}` , 3600 , urlStr);

    } catch (e) {
        return e;
    }
}

const setWorkAsDoneInRedis=(async()=>{
    try {        
        await redisClient.hsetAsync('workDict','finished','true')

        const workDict= await redisClient.hgetallAsync("workDict");

        return workDict;
    } catch (e) {
        return e;
    }
})


module.exports={
    isUrlExistInRedis,
    getCurrentLevelData,
    saveUrlInRedis,
    incrementLevelData,
    setWorkAsDoneInRedis,
    setUrlsInCurrentLevelToScanData,
    incrementUrlsInCurrentLevelScannedData
}