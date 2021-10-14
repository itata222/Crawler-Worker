const axios = require('axios');
const cheerio = require('cheerio');

const parseUrl = async (url) => {
    try {
        const urls = []
        const res = await axios.get(url)
        const $ = cheerio.load(res.data);
        const links = $('a'); //jquery get all hyperlinks
        $(links).each(function (i, link) {
            urls.push($(link).attr('href'))
        });
        console.log(urls.length)
        return new Promise((resolve, reject) => { resolve(urls) });
    } catch (e) {
        return new Promise((resolve, reject) => { reject(undefined) });
    }
    
}

module.exports = {
    parseUrl
}