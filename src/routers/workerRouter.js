const redisClient = require("../db/redis");
const express = require("express");
const { sendMessageToQueue, pollMessageFromQueue, deleteMessagesFromQueue } = require("../utils/sqs");
const {
  isUrlExistInRedis,
  saveUrlInRedis,
  getCurrentLevelData,
  incrementUrlsInCurrentLevelScannedData,
  incrementTotalUrlsData,
  addUrlsToNextLevelToScanData,
  incrementDeathEndsData,
  decreaseRemainnigSlots,
  incrementLevelData,
  IsJsonString,
  insertUrlsToNextLevel,
  setFirstPositionInNextLevel,
  getLatestDataFromRedis,
} = require("../utils/redis");
const { scrapeUrl } = require("../utils/scraper");

const QueueName = process.env.QUEUE_NAME;
const router = new express.Router();

router.post("/crawl", async (req, res) => {
  res.send();
  const { workID, rootUrl, maxPages } = req.query;
  while (true) {
    let toContinue = true,
      nextLevelUrls = [];
    const polledResponse = await pollMessageFromQueue({ QueueName, workID });

    if (polledResponse == undefined) break;
    else if (polledResponse.Messages == undefined || polledResponse.Messages.length === 0) continue;

    const Messages = polledResponse.Messages;
    const QueueUrl = polledResponse.QueueUrl;

    for (let i = 0; i < Messages.length; i++) {
      const currentUrl = Messages[i].Body.split("$")[1];
      const parentUrl = Messages[i].Body.split("$")[2];
      const parentPosition = Messages[i].Body.split("$")[3];
      console.log(parentPosition, "parentPosition");
      let currentLevelData = await getCurrentLevelData(workID);

      const urlFromRedis = await isUrlExistInRedis({ currentUrl });
      if (urlFromRedis != null) {
        const responseObj = JSON.parse(urlFromRedis);
        console.log("exisitng, deathEnd?", parseInt(responseObj.workID), parseInt(workID), responseObj.myAddress);
        if (parseInt(responseObj.workID) === parseInt(workID)) await incrementDeathEndsData(workID);
        else {
          const existingKeyChildrens = IsJsonString(responseObj.childrenUrlsStr)
            ? JSON.parse(responseObj.childrenUrlsStr)
            : responseObj.childrenUrlsStr;
          responseObj.workID = parseInt(workID);
          responseObj.childrenUrlsStr = JSON.stringify(existingKeyChildrens);
          saveUrlInRedis(responseObj);
          await incrementTotalUrlsData(workID);

          for (let i = 0; i < existingKeyChildrens.length; i++) {
            if (parentUrl === "null" || currentUrl !== existingKeyChildrens[i])
              nextLevelUrls.push({
                myAddress: existingKeyChildrens[i],
                depth: parseInt(currentLevelData.currentLevel) + 1,
                parentAddress: currentUrl,
                position: `${parentPosition}-${i}`,
              });
          }
          await decreaseRemainnigSlots(workID);
        }
        toContinue = false;
      }

      if (toContinue) {
        const childrenUrls = await scrapeUrl(encodeURI(currentUrl));
        await incrementUrlsInCurrentLevelScannedData(workID);
        if (childrenUrls == undefined) {
          continue;
        }
        for (let i = 0; i < childrenUrls.length; i++) {
          if (parentUrl === "null" || currentUrl !== childrenUrls[i])
            nextLevelUrls.push({
              myAddress: childrenUrls[i],
              depth: parseInt(currentLevelData.currentLevel) + 1,
              parentAddress: currentUrl,
              position: `${parentPosition}-${i}`,
            });
        }
        await addUrlsToNextLevelToScanData(nextLevelUrls.length, workID);
        await decreaseRemainnigSlots(workID);
        await incrementTotalUrlsData(workID);
        const currentUrlObj = {
          parentAddress: parentUrl,
          myAddress: currentUrl,
          childrenUrls,
          workID: parseInt(workID),
        };
        const nextLevelUrlsString = JSON.stringify(nextLevelUrls);
        await redisClient.lpushAsync(`tree:${workID}`, nextLevelUrlsString);
        saveUrlInRedis(currentUrlObj);
      }
    }

    await deleteMessagesFromQueue({ Messages, QueueUrl });

    currentLevelData = await getCurrentLevelData(workID);
    console.log(parseInt(currentLevelData.remainingSlots) - parseInt(currentLevelData.currentLevelDeathEnds));
    if (parseInt(currentLevelData.remainingSlots) - parseInt(currentLevelData.currentLevelDeathEnds) <= 0) break;
    else if (
      parseInt(currentLevelData.urlsInCurrentLevelAlreadyScanned) + parseInt(currentLevelData.currentLevelDeathEnds) + 1 >=
      parseInt(currentLevelData.urlsInCurrentLevelToScan)
    ) {
      await setFirstPositionInNextLevel(workID, currentLevelData.urlsInNextLevelToScan);
      const tree = await getLatestDataFromRedis({ workID });
      const sortedLevelUrls = tree.sort(function (a, b) {
        if (typeof a.position !== "number") a.position = `${a.position}`;
        if (typeof b.position !== "number") b.position = `${b.position}`;
        const aPos = a.position.split("-");
        const bPos = b.position.split("-");
        for (let i = 0; i < aPos.length; i++) {
          if (aPos.length < bPos.length) return -1;
          if (aPos.length > bPos.length) return 1;
          if (parseInt(aPos[i]) < parseInt(bPos[i])) return -1;
          if (parseInt(aPos[i]) > parseInt(bPos[i])) return 1;
          continue;
        }
        return 0;
      });
      for (let i = 0; i < parseInt(currentLevelData.urlsInCurrentLevelToScan); i++) sortedLevelUrls.shift();
      await incrementLevelData(workID);
      console.log(
        "currentLevelData.remainingSlots",
        parseInt(currentLevelData.remainingSlots),
        sortedLevelUrls.length,
        parseInt(currentLevelData.urlsInNextLevelToScan.length)
      );
      for (let i = 0; i < Math.min(parseInt(currentLevelData.remainingSlots), sortedLevelUrls.length); i++) {
        sendMessageToQueue({
          url: sortedLevelUrls[i].myAddress,
          parentPosition: sortedLevelUrls[i].position,
          QueueUrl,
          parentUrl: sortedLevelUrls[i].parentAddress,
          workID: parseInt(workID),
        });
      }
      nextLevelUrls = [];
      console.log(9, "level ended");
    }
    await incrementUrlsInCurrentLevelScannedData(workID);
  }
});

module.exports = router;
