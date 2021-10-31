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
      } else console.log("polledResponse undefined");
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
      .then(console.log("setteled"))
      .catch((e) => console.log("e", e));
  };

  const handleNextMessage = async (message, QueueUrl) => {
    console.log("working", message);
    let nextLevelUrls = [],
      hasChild = true;
    const currentUrl = message.split("$")[1];
    const parentUrl = message.split("$")[2];
    const parentPosition = message.split("$")[3];
    const urlFromRedis = await isUrlExistInRedis({ currentUrl });
    currentLevelData = await getCurrentLevelData(workID);

    return new Promise(async (resolve, reject) => {
      if (urlFromRedis != null) {
        const responseObj = JSON.parse(urlFromRedis);
        console.log("exisitng, deathEnd?", parseInt(responseObj.workID), parseInt(workID), responseObj.myAddress);
        if (parseInt(responseObj.workID) === parseInt(workID)) await incrementDeathEndsData(workID);
        else {
          const existingKeyChildrens = IsJsonString(responseObj.childrenUrlsStr)
            ? JSON.parse(responseObj.childrenUrlsStr)
            : responseObj.childrenUrlsStr;
          responseObj.workID = parseInt(workID);
          responseObj.childrenUrls = existingKeyChildrens;
          await saveUrlInRedis(responseObj);
          await incrementTotalUrlsData(workID);
          const childrensLength = existingKeyChildrens == undefined ? 0 : existingKeyChildrens.length;
          await addUrlsToNextLevelToScanData(childrensLength, workID);

          nextLevelUrls = existingKeyChildrens.map((url, i) => {
            if (parentUrl === "null" || currentUrl !== url)
              return {
                myAddress: url,
                depth: parseInt(currentLevelData.currentLevel) + 1,
                parentAddress: currentUrl,
                position: `${parentPosition}-${i}`,
              };
          });

          const nextLevelUrlsString = JSON.stringify(nextLevelUrls);
          await redisClient.lpushAsync(`tree:${workID}`, nextLevelUrlsString);
          await decreaseRemainnigSlots(workID);
        }
      } else {
        const childrenUrls = await scrapeUrl(encodeURI(currentUrl));
        if (childrenUrls == undefined) hasChild = false;
        if (hasChild) {
          nextLevelUrls = childrenUrls.map((url, i) => {
            if (parentUrl === "null" || currentUrl !== url)
              return {
                myAddress: url,
                depth: parseInt(currentLevelData.currentLevel) + 1,
                parentAddress: currentUrl,
                position: `${parentPosition}-${i}`,
              };
          });
        }
        await addUrlsToNextLevelToScanData(nextLevelUrls.length, workID);
        await decreaseRemainnigSlots(workID);
        await incrementTotalUrlsData(workID);
        const currentUrlObj = {
          parentAddress: parentUrl,
          myAddress: currentUrl,
          childrenUrls: childrenUrls == undefined ? [] : childrenUrls,
          workID: parseInt(workID),
        };
        const nextLevelUrlsString = JSON.stringify(nextLevelUrls);
        await redisClient.lpushAsync(`tree:${workID}`, nextLevelUrlsString);
        saveUrlInRedis(currentUrlObj);
      }
      await incrementUrlsInCurrentLevelScannedData(workID);
      currentLevelData = await getCurrentLevelData(workID);
      // console.log(currentLevelData, "currentLevelData");
      if (parseInt(currentLevelData.remainingSlots) - parseInt(currentLevelData.currentLevelDeathEnds) <= 0) reject();
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
      resolve();
    });
  };

  startWorker();
});

//-----------------------------------------------------------

module.exports = router;
