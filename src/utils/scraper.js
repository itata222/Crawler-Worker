const axios = require("axios");
const cheerio = require("cheerio");

const scrapeUrl = async (url) => {
  console.log("url", url);
  try {
    const urlResponse = await axios.get(url);
    const $ = cheerio.load(urlResponse.data);
    const links = [];
    $("a").each((i, el) => {
      const link = getFullUrl($(el).attr("href"));
      if (isLinkValid(link)) links.push(link);
    });
    return links;
  } catch (err) {
    return;
  }
};

const getFullUrl = (url) => {
  try {
    if (url.includes("http")) {
      return url;
    } else {
      return `http://${url}`;
    }
  } catch (e) {
    return;
  }
};
const isLinkValid = (url) => {
  const urlRegex = /^(?:http(s)?:\/\/)?[\w.-]+(?:\.[\w\.-]+)+[\w\-\._~:/?#[\]@!\$&'\(\)\*\+,;=.]+$/;
  return urlRegex.test(url);
};

module.exports = {
  scrapeUrl,
};
