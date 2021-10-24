const redisClient = require("../db/redis");
const express = require("express");
const { sendMessageToQueue, pollMessageFromQueue, deleteMessagesFromQueue } = require("../utils/sqs");
const {
  isUrlExistInRedis,
  saveUrlInRedis,
  getCurrentLevelData,
  incrementLevelData,
  incrementUrlsInCurrentLevelScannedData,
  incrementTotalUrlsData,
  addUrlsToNextLevelToScanData,
  incrementDeathEndsData,
} = require("../utils/redis");
const { scrapeUrl } = require("../utils/scraper");

const QueueName = process.env.QUEUE_NAME;
const router = new express.Router();

router.post("/crawl", async (req, res) => {
  const { workID, rootUrl } = req.query;
  res.send();
  console.log(workID, "workID");
  let levelUrls = [];
  while (true) {
    let childrenUrlsLength = 0,
      childrenUrlsArr = [],
      toContinue = true;
    //--------------------extracting data--------------------
    let currentLevelData = await getCurrentLevelData(workID);
    const polledResponse = await pollMessageFromQueue({ QueueName, workID });

    //--------------------polling tests------------------------------
    if (polledResponse == undefined) break;
    else if (polledResponse.Messages == undefined || polledResponse.Messages.length === 0) continue;

    //--------------------extract data after verifying polling request succeded ---------------
    const Messages = polledResponse.Messages;
    const QueueUrl = polledResponse.QueueUrl;

    const currentUrl = Messages[0].Body.split("$")[1];
    const parentUrl = Messages[0].Body.split("$")[2];

    //----------------------general tests------------------------
    const urlFromRedis = await isUrlExistInRedis({ currentUrl });
    if (urlFromRedis != null) {
      const responseObj = JSON.parse(urlFromRedis);
      console.log("exisitng, deathEnd?", parseInt(responseObj.workID), parseInt(workID), responseObj.myAddress);
      if (parseInt(responseObj.workID) === parseInt(workID)) await incrementDeathEndsData(workID);
      else {
        responseObj.workID = parseInt(workID);
        responseObj.position = parseInt(currentLevelData.totalUrls);
        const updatedUrl = JSON.stringify(responseObj);
        await redisClient.setexAsync(`${currentUrl}`, 3600, updatedUrl);
        const existingKeyChildrens = JSON.parse(responseObj.childrenUrlsStr);
        await incrementTotalUrlsData(workID);
        await addUrlsToNextLevelToScanData(existingKeyChildrens.length, workID);
        for (let i = 0; i < existingKeyChildrens.length; i++) {
          levelUrls.push({ url: existingKeyChildrens[i], parent: responseObj.myAddress });
        }
      }
      toContinue = false;
    }

    if (toContinue) {
      //-----------------------scrape-------------------------
      const childrenUrls = await scrapeUrl(encodeURI(currentUrl));
      if (childrenUrls == undefined) {
        await incrementUrlsInCurrentLevelScannedData(workID);
        continue;
      }
      childrenUrlsLength = childrenUrls.length;
      childrenUrlsArr = [...childrenUrls];
      for (let i = 0; i < childrenUrlsLength; i++) levelUrls.push({ url: childrenUrlsArr[i], parent: currentUrl });

      //----------------------update Redis after scrape data----------------------
      const parentAddress = rootUrl === currentUrl ? null : parentUrl;
      const position = rootUrl === currentUrl ? 0 : parseInt(currentLevelData.totalUrls);
      await incrementTotalUrlsData(workID);
      await addUrlsToNextLevelToScanData(childrenUrlsLength, workID);
      saveUrlInRedis({
        parentAddress,
        myAddress: currentUrl,
        depth: parseInt(currentLevelData.currentLevel),
        rootUrl,
        position,
        childrenUrls,
        workID: parseInt(workID),
      });
    }
    //----------------------update SQS---------------------------
    deleteMessagesFromQueue({ Messages, QueueUrl });
    //------------------------get recent data and logged it---------------
    currentLevelData = await getCurrentLevelData(workID);
    //----------------------check if the work or level done-------------------

    if (
      parseInt(currentLevelData.urlsInCurrentLevelAlreadyScanned) + parseInt(currentLevelData.currentLevelDeathEnds) + 1 >=
      parseInt(currentLevelData.urlsInCurrentLevelToScan)
    ) {
      await incrementLevelData(workID);
      for (let i = 0; i < levelUrls.length; i++)
        sendMessageToQueue({
          url: levelUrls[i].url,
          rootUrl,
          QueueUrl,
          parentUrl: levelUrls[i].parent,
          workID: parseInt(workID),
        });

      levelUrls = [];
      console.log(9, "level has done");
    }
    await incrementUrlsInCurrentLevelScannedData(workID);
  }
});

module.exports = router;
