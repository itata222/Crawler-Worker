const redisClient = require("../db/redis");
const express = require("express");
const { sendMessageToQueue, pollMessageFromQueue } = require("../utils/sqs");
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
  getLatestDataFromRedis,
  saveScrapedUrlChildrenInTreeList,
} = require("../utils/redis");
const { scrapeUrl } = require("../utils/scraper");

const QueueName = process.env.QUEUE_NAME;
const router = new express.Router();

router.post("/crawl", async (req, res) => {
  res.send();
  const { workID } = req.query;

  const startWorker = async () => {
    try {
      console.log("started");
      const polledResponse = await pollMessageFromQueue({ QueueName, workID });
      if (polledResponse != undefined) {
        const Messages = polledResponse.Messages;
        const QueueUrl = polledResponse.QueueUrl;
        if (Messages.length !== 0) {
          const messages = Messages.map((Message) => {
            return Message.Body;
          });
          await handleMessages(messages, QueueUrl);
          continueWork();
        }
      } else console.log("polledResponse = undefined");
    } catch (err) {
      console.log("err123", err);
    }
  };

  const continueWork = () => {
    startWorker()
      .then()
      .catch((err) => console.log({ err }));
  };

  const handleMessages = async (messages, QueueUrl) => {
    const handleMessagesPromises = messages.map((message) => handleNextMessage(message, QueueUrl));

    Promise.allSettled(handleMessagesPromises)
      .then(console.log("batch setteled"))
      .catch((e) => console.log("e", e));
  };

  const handleNextMessage = async (message, QueueUrl) => {
    const currentUrl = message.split("$")[1];
    const parentUrl = message.split("$")[2];
    const parentPosition = message.split("$")[3];

    const urlFromRedis = await isUrlExistInRedis({ currentUrl });

    return new Promise(async (resolve, reject) => {
      if (urlFromRedis != null) await handleUrlThatAlreadyExistInRedis(urlFromRedis, parentPosition, parentUrl, currentUrl);
      else await handleUrlThatNotExistingInRedis(parentPosition, currentUrl, parentUrl);

      await incrementUrlsInCurrentLevelScannedData(workID);
      const isDone = await checksAfterEachScanning(QueueUrl);
      if (isDone === true) reject();
      resolve();
    });
  };

  const getChildrensNodes = async (arrayOfChildrens, currentUrl, parentUrl, parentPosition) => {
    const currentLevelData = await getCurrentLevelData(workID);
    const nodes = arrayOfChildrens.map((url, i) => {
      if (parentUrl === "null" || currentUrl !== url)
        return {
          myAddress: url,
          depth: parseInt(currentLevelData.currentLevel) + 1,
          parentAddress: currentUrl,
          position: `${parentPosition}-${i}`,
        };
    });
    return nodes;
  };

  const handleUrlThatAlreadyExistInRedis = async (urlFromRedis, parentPosition, parentUrl, currentUrl) => {
    const responseObj = JSON.parse(urlFromRedis);
    console.log("exisitng, deathEnd?", parseInt(responseObj.workID), parseInt(workID), responseObj.myAddress);
    if (parseInt(responseObj.workID) === parseInt(workID)) await incrementDeathEndsData(workID);
    else {
      const existingKeyChildrens = IsJsonString(responseObj.childrenUrlsStr) ? JSON.parse(responseObj.childrenUrlsStr) : responseObj.childrenUrlsStr;
      responseObj.workID = parseInt(workID);
      responseObj.childrenUrls = existingKeyChildrens;
      saveUrlInRedis(responseObj);
      await incrementTotalUrlsData(workID);
      const childrensLength = existingKeyChildrens == undefined ? 0 : existingKeyChildrens.length;
      await addUrlsToNextLevelToScanData(childrensLength, workID);
      const nextLevelUrls = await getChildrensNodes(existingKeyChildrens, currentUrl, parentUrl, parentPosition);

      await saveScrapedUrlChildrenInTreeList(nextLevelUrls, workID);
      await decreaseRemainnigSlots(workID);
    }
  };

  const handleUrlThatNotExistingInRedis = async (parentPosition, currentUrl, parentUrl) => {
    let hasChild = true,
      nextLevelUrls = [];
    const childrenUrls = await scrapeUrl(encodeURI(currentUrl));
    if (childrenUrls == undefined) hasChild = false;
    if (hasChild) nextLevelUrls = await getChildrensNodes(childrenUrls, currentUrl, parentUrl, parentPosition);
    saveScrapedUrlInRedis(currentUrl, parentUrl, childrenUrls);
    await addUrlsToNextLevelToScanData(nextLevelUrls.length, workID);
    await decreaseRemainnigSlots(workID);
    await incrementTotalUrlsData(workID);
    await saveScrapedUrlChildrenInTreeList(nextLevelUrls, workID);
  };

  const saveScrapedUrlInRedis = async (currentUrl, parentUrl, childrenUrls) => {
    const currentUrlObj = {
      parentAddress: parentUrl,
      myAddress: currentUrl,
      childrenUrls: childrenUrls == undefined ? [] : childrenUrls,
      workID: parseInt(workID),
    };
    saveUrlInRedis(currentUrlObj);
  };

  const checksAfterEachScanning = async (QueueUrl) => {
    const currentLevelData = await getCurrentLevelData(workID);
    if (parseInt(currentLevelData.remainingSlots) - parseInt(currentLevelData.currentLevelDeathEnds) <= 0) return true;
    else if (
      parseInt(currentLevelData.urlsInCurrentLevelAlreadyScanned) + parseInt(currentLevelData.currentLevelDeathEnds) >=
      parseInt(currentLevelData.urlsInCurrentLevelToScan)
    ) {
      const tree = await getLatestDataFromRedis({ workID });
      for (let i = 0; i < parseInt(currentLevelData.totalUrls); i++) tree.shift();
      for (let i = 0; i < Math.min(parseInt(currentLevelData.remainingSlots), tree.length); i++) {
        sendMessageToQueue({
          url: tree[i].myAddress,
          parentPosition: tree[i].position,
          QueueUrl,
          parentUrl: tree[i].parentAddress,
          workID: parseInt(workID),
        });
      }
      await incrementLevelData(workID);
      console.log(9, "level ended");
    }
  };

  startWorker();
});

//-----------------------------------------------------------

module.exports = router;
