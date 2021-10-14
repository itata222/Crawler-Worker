const redisClient = require('../db/redis');

const isUrlExistInRedis=(async({currentUrl})=>{
    let isExist;
    try {        
        const response=await redisClient.getAsync(currentUrl);
        response==null?isExist=false:isExist=true;
            
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
const incrementChildsData=(async()=>{

    try {        
        const response=await redisClient.hincrbyAsync('levelData','childs',1)
            
        return response;
    } catch (e) {
        console.log(e)
        return e;
    }
})
const incrementLevelData=(async()=>{

    try {        
        const response=await redisClient.hincrbyAsync('levelData','level',1)
            
        return response;
    } catch (e) {
        console.log(e)
        return e;
    }
})

const saveUrlInRedis=async({parentAddress,myAddress,depth,rootUrl})=>{
    let address=myAddress;
    if(!myAddress.includes('http'))
        address=myAddress.substring(1)

    const urlObj={parentAddress,myAddress:address,depth,rootUrl}
    const urlStr=JSON.stringify(urlObj)
    try {        
        await redisClient.setexAsync(`${rootUrl}$$$${address}` , 3600 , urlStr);

    } catch (e) {
        console.log(e)
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
    incrementChildsData
}