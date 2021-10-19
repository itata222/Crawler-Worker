const express = require("express");
const { sendMessageToQueue, pollMessageFromQueue, deleteMessagesFromQueue } = require("../utils/sqs");
const {
  setWorkAsDoneInRedis,
  isUrlExistInRedis,
  saveUrlInRedis,
  getCurrentLevelData,
  incrementLevelData,
  incrementUrlsInCurrentLevelScannedData,
  getWorkDictData,
  incrementTotalUrlsData,
  addUrlsToCurrentLevelToScanData,
  addUrlsToNextLevelToScanData,
} = require("../utils/redis");
const { scrapeUrl } = require("../utils/scraper");

const router = new express.Router();
const QueueName = "Crawler-Queue1";

router.post("/crawl", async (req, res) => {
  const { rootUrl, maxDepth, maxTotalPages } = await getWorkDictData();
  console.log(maxDepth);
  let levelUrls = [],
    skippedOver = 0;
  while (true) {
    let childrenUrlsLength = 0,
      childrenUrlsArr = [],
      toContinue = true;
    //--------------------extracting data--------------------
    console.log(0);
    const currentLevelData = await getCurrentLevelData();
    const polledResponse = await pollMessageFromQueue({ QueueName, rootUrl });

    console.log(1);
    //--------------------polling tests------------------------------
    if (polledResponse == undefined) break;
    else if (polledResponse.Messages == undefined || polledResponse.Messages.length === 0) continue;

    console.log(2);
    //--------------------extract data after verifying polling request succeded ---------------
    const Messages = polledResponse.Messages;
    const QueueUrl = polledResponse.QueueUrl;
    const currentUrl = Messages[0].Body.split("$")[1];
    const parentUrl = Messages[0].Body.split("$")[2];

    console.log(
      3,
      parseInt(currentLevelData.urlsInCurrentLevelAlreadyScanned),
      parseInt(currentLevelData.urlsInCurrentLevelToScan),
      parseInt(currentLevelData.currentLevel)
    );
    //---------------------check if the whole work has need to be done------------------
    if (
      parseInt(currentLevelData.totalUrls) >= maxTotalPages ||
      (maxDepth === parseInt(currentLevelData.currentLevel) &&
        parseInt(currentLevelData.urlsInCurrentLevelAlreadyScanned) + 1 === parseInt(currentLevelData.urlsInCurrentLevelToScan))
    ) {
      await setWorkAsDoneInRedis();
      break;
    }

    console.log(4);
    //----------------------general tests------------------------
    const isExist = await isUrlExistInRedis({ currentUrl, rootUrl });
    if (isExist) {
      await deleteMessagesFromQueue({ Messages, QueueUrl });
      await incrementTotalUrlsData();
      skippedOver++;
      toContinue = false;
    }

    console.log(5);
    if (toContinue) {
      //-----------------------scrape-------------------------
      const childrenUrls = await scrapeUrl(encodeURI(currentUrl, rootUrl));
      if (childrenUrls == undefined) continue;
      childrenUrlsLength = childrenUrls.length;
      childrenUrlsArr = [...childrenUrls];
      for (let i = 0; i < childrenUrlsLength; i++) levelUrls.push(childrenUrlsArr[i]);

      console.log(6, parseInt(currentLevelData.totalUrls));
      //----------------------update Redis after scrape data----------------------
      const parentAddress = rootUrl === currentUrl ? null : parentUrl;
      const position = rootUrl === currentUrl ? 0 : parseInt(currentLevelData.totalUrls) - skippedOver;
      await incrementTotalUrlsData();
      await addUrlsToNextLevelToScanData(childrenUrlsLength);
      await deleteMessagesFromQueue({ Messages, QueueUrl });
      await saveUrlInRedis({
        parentAddress,
        myAddress: currentUrl,
        depth: parseInt(currentLevelData.currentLevel),
        rootUrl,
        position,
        childrenUrls,
      });

      console.log(7);
      //----------------------update SQS---------------------------
      await deleteMessagesFromQueue({ Messages, QueueUrl });
    }
    //----------------------check if the level done-------------------
    if (parseInt(currentLevelData.urlsInCurrentLevelAlreadyScanned) + 1 === parseInt(currentLevelData.urlsInCurrentLevelToScan)) {
      await incrementLevelData();
      if (parseInt(currentLevelData.urlsInCurrentLevelToScan) + childrenUrlsLength - 1 === 0) {
        await setWorkAsDoneInRedis();
        break;
      } else {
        console.log("level completed so we need to wait its sends all urls to queuqe in sqs");
        for (let i = 0; i < levelUrls.length; i++)
          await sendMessageToQueue({
            url: levelUrls[i],
            rootUrl,
            QueueUrl,
            parentUrl: currentUrl,
          });
      }
      levelUrls = [];
      console.log(9, "level has done");
    }
    await incrementUrlsInCurrentLevelScannedData();
    console.log(10, "iteration done");
  }
});

module.exports = router;
