const express =require('express');
const axios=require('axios');
const cheerio=require('cheerio');
const { pollMessagesFromQueue } = require('../middlewares/sqs');
const { sendUrlsToRedis } = require('../utils/redis');

const router=new express.Router();

router.post('/crawl',pollMessagesFromQueue,async(req,res)=>{
    const messages=req.messages;
    console.log(messages)
    try {
        if(messages.length>0){
            axios(messages[0]).then((res)=>{
            const html=res.data;
            const $=cheerio.load(html);
            const urls= [];
            $($('a',html)).each((i,link)=>{
                urls.push($(link).attr('href'));
            })
            console.log(urls)
            await sendUrlsToRedis({urls});
            
            }).catch(e=>console.log(e))
        }

    } catch (e) {
        res.status(500).send({
            status:500,
            message:'workers failed to crawl'
        })
    }
})


module.exports=router