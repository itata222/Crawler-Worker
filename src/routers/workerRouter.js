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
  addUrlsToNextLevelToScanData,
  incrementDeathEndsData,
} = require("../utils/redis");
const { scrapeUrl } = require("../utils/scraper");

const router = new express.Router();
const QueueName = "Crawler-Queue1";

router.post("/crawl", async (req, res) => {
  const { rootUrl, maxDepth, maxTotalPages, workID } = await getWorkDictData();
  console.log(workID, "workID");
  let levelUrls = [],
    skippedOver = 0;
  while (true) {
    let childrenUrlsLength = 0,
      childrenUrlsArr = [],
      toContinue = true;
    //--------------------extracting data--------------------
    console.log(0);
    const currentLevelData = await getCurrentLevelData();
    const polledResponse = await pollMessageFromQueue({ QueueName, workID });

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
    await deleteMessagesFromQueue({ Messages, QueueUrl });

    console.log(
      3,
      parseInt(currentLevelData.urlsInCurrentLevelAlreadyScanned),
      parseInt(currentLevelData.urlsInCurrentLevelToScan),
      parseInt(currentLevelData.currentLevel)
    );
    //---------------------check if the whole work has done or level completed------------------
    if (
      parseInt(currentLevelData.totalUrls) >= maxTotalPages ||
      (parseInt(maxDepth) === parseInt(currentLevelData.currentLevel) &&
        parseInt(currentLevelData.urlsInCurrentLevelAlreadyScanned) + parseInt(currentLevelData.totalDeathEnds) + 1 ===
          parseInt(currentLevelData.urlsInCurrentLevelToScan))
    ) {
      await setWorkAsDoneInRedis();
      break;
    }

    if (parseInt(currentLevelData.urlsInCurrentLevelAlreadyScanned) === parseInt(currentLevelData.urlsInCurrentLevelToScan)) {
      await incrementLevelData();
      continue;
    }

    console.log(4);
    //----------------------general tests------------------------
    const urlFromRedis = await isUrlExistInRedis({ currentUrl, workID });
    if (urlFromRedis != null) {
      const responseObj = JSON.parse(urlFromRedis);
      console.log("exist", responseObj);
      if (parseInt(responseObj.workID) === workID) await incrementDeathEndsData();
      else {
        // ! this is the last part i need to finish. when its not the current crawler that want the data. basically it need to do all the things that happens when it not existing
        const existingKeyChildrens = JSON.parse(responseObj.childrenUrlsStr);
        for (let i = 0; i < existingKeyChildrens.length; i++) {
          await incrementTotalUrlsData();
          await addUrlsToNextLevelToScanData(existingKeyChildrens.length);
          levelUrls.push({ url: existingKeyChildrens[i], parent: responseObj.myAddress });
        }
      }
      skippedOver++;
      toContinue = false;
    }

    console.log(5);
    if (toContinue) {
      //-----------------------scrape-------------------------
      const childrenUrls = await scrapeUrl(encodeURI(currentUrl));
      if (childrenUrls == undefined) {
        await incrementUrlsInCurrentLevelScannedData();
        continue;
      }
      childrenUrlsLength = childrenUrls.length;
      childrenUrlsArr = [...childrenUrls];
      for (let i = 0; i < childrenUrlsLength; i++) levelUrls.push({ url: childrenUrlsArr[i], parent: currentUrl });

      console.log(6, "totalUrls", parseInt(currentLevelData.totalUrls));
      //----------------------update Redis after scrape data----------------------
      const parentAddress = rootUrl === currentUrl ? null : parentUrl;
      const position = rootUrl === currentUrl ? 0 : parseInt(currentLevelData.totalUrls) - skippedOver;
      await incrementTotalUrlsData();
      await addUrlsToNextLevelToScanData(childrenUrlsLength);
      await saveUrlInRedis({
        parentAddress,
        myAddress: currentUrl,
        depth: parseInt(currentLevelData.currentLevel),
        rootUrl,
        position,
        childrenUrls,
        workID,
      });

      console.log(7);
      //----------------------update SQS---------------------------
      await deleteMessagesFromQueue({ Messages, QueueUrl });
    }
    //----------------------check if the work or level done-------------------
    console.log(
      "last check",
      parseInt(maxDepth),
      parseInt(currentLevelData.currentLevel),
      parseInt(currentLevelData.urlsInCurrentLevelAlreadyScanned) + 1,
      parseInt(currentLevelData.urlsInCurrentLevelToScan)
    );
    if (
      parseInt(currentLevelData.totalUrls) === maxTotalPages ||
      (parseInt(maxDepth) === parseInt(currentLevelData.currentLevel) &&
        parseInt(currentLevelData.urlsInCurrentLevelAlreadyScanned) + 1 + parseInt(currentLevelData.totalDeathEnds) ===
          parseInt(currentLevelData.urlsInCurrentLevelToScan))
    ) {
      await setWorkAsDoneInRedis();
      break;
    } else if (
      parseInt(currentLevelData.urlsInCurrentLevelAlreadyScanned) + parseInt(currentLevelData.totalDeathEnds) + 1 ===
      parseInt(currentLevelData.urlsInCurrentLevelToScan)
    ) {
      await incrementLevelData();
      console.log("level completed so we need to wait its sends all urls to queuqe in sqs");
      for (let i = 0; i < levelUrls.length; i++)
        sendMessageToQueue({
          url: levelUrls[i].url,
          rootUrl,
          QueueUrl,
          parentUrl: levelUrls[i].parent,
          workID,
        });

      levelUrls = [];
      console.log(9, "level has done");
    }
    await incrementUrlsInCurrentLevelScannedData();
    console.log(10, "iteration done");
  }
});

module.exports = router;
