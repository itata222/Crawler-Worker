const redisClient = require('../db/redis');

const sendUrlsToRedis=(async({urls})=>{
    
    try {        
        urls.map((url)=>{
            await redisClient.setexAsync(url,300,'1');
        })

    } catch (e) {
        return e;
    }
})


module.exports={
    sendUrlsToRedis
}