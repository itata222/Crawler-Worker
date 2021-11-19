const axios = require("axios");
const cheerio = require("cheerio");

const scrapeUrl = async (url) => {
  console.log("not existing - url", url);
  try {
    const urlResponse = await axios.get(url);
    const $ = cheerio.load(urlResponse.data);
    const links = new Set();
    $("a").each((i, el) => {
      const link = getFullUrl($(el).attr("href"));
      if (isLinkValid(link)) links.add(link);
    });
    return Array.from(links);
  } catch (err) {
    return;
  }
};

const getFullUrl = (url) => {
  try {
    const urlObj = new URL(url);
    if (url.includes("http")) {
      if (urlObj.protocol === "https:") urlObj.protocol = "http:";
      return urlObj.href;
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
