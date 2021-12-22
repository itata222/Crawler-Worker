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

const startWorker = async () => {
  try {
    const polledResponse = await pollMessageFromQueue({ QueueName });
    if (!!polledResponse && polledResponse.Messages.length > 0) {
      const Messages = polledResponse.Messages;
      const QueueUrl = polledResponse.QueueUrl;
      if (Messages.length !== 0) {
        const messages = Messages.map((Message) => {
          return Message.Body;
        });
        await handleMessages(messages, QueueUrl);
        // setImmediate(() => continueWork());
        // it free the event loop for other I/O operations but it slows the performance so in our case it is not required.
        continueWork();
      }
    } else throw new Error("No Messages");
  } catch (err) {
    continueWork();
  }
};

const continueWork = () => {
  startWorker()
    .then()
    .catch((err) => console.log(err));
};

const handleMessages = async (messages, QueueUrl) => {
  const handleMessagesPromises = messages.map((message) => handleNextMessage(message, QueueUrl));

  Promise.allSettled(handleMessagesPromises)
    .then(console.log("batch setteled"))
    .catch((e) => console.log("batch setteled with error", e)); // cant be executed since in each promise i did only resolve
};

const handleNextMessage = async (message, QueueUrl) => {
  const workID = message.split("$")[0];
  const currentUrl = message.split("$")[1];
  const parentUrl = message.split("$")[2];
  const parentPosition = message.split("$")[3];

  const urlFromRedis = await isUrlExistInRedis({ currentUrl });

  return new Promise(async (resolve, reject) => {
    if (urlFromRedis != null) await handleUrlThatAlreadyExistInRedis({ workID, urlFromRedis, parentPosition, parentUrl, currentUrl });
    else await handleUrlThatNotExistingInRedis({ workID, parentPosition, currentUrl, parentUrl });

    await incrementUrlsInCurrentLevelScannedData(workID);
    await checksAfterEachScanning(workID, QueueUrl);
    resolve();
  });
};

const handleUrlThatAlreadyExistInRedis = async ({ workID, urlFromRedis, parentPosition, parentUrl, currentUrl }) => {
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
    const nextLevelUrls = await getChildrensNodes(workID, existingKeyChildrens, currentUrl, parentUrl, parentPosition);

    await saveScrapedUrlChildrenInTreeList(nextLevelUrls, workID);
    await decreaseRemainnigSlots(workID);
  }
};

const handleUrlThatNotExistingInRedis = async ({ workID, parentPosition, currentUrl, parentUrl }) => {
  let hasChild = true,
    nextLevelUrls = [];
  const childrenUrls = await scrapeUrl(encodeURI(currentUrl));
  if (childrenUrls == undefined) hasChild = false;
  if (hasChild) nextLevelUrls = await getChildrensNodes(workID, childrenUrls, currentUrl, parentUrl, parentPosition);
  saveScrapedUrlInRedis(workID, currentUrl, parentUrl, childrenUrls);
  await addUrlsToNextLevelToScanData(nextLevelUrls.length, workID);
  await decreaseRemainnigSlots(workID);
  await incrementTotalUrlsData(workID);
  await saveScrapedUrlChildrenInTreeList(nextLevelUrls, workID);
};

const checksAfterEachScanning = async (workID, QueueUrl) => {
  const currentLevelData = await getCurrentLevelData(workID);
  const currentLevelDeathEnds = parseInt(currentLevelData.currentLevelDeathEnds);
  const totalUrls = parseInt(currentLevelData.totalUrls);
  const urlsInCurrentLevelAlreadyScanned = parseInt(currentLevelData.urlsInCurrentLevelAlreadyScanned);
  const urlsInCurrentLevelToScan = parseInt(currentLevelData.urlsInCurrentLevelToScan);
  const remainingSlots = parseInt(currentLevelData.remainingSlots);

  if (urlsInCurrentLevelAlreadyScanned + currentLevelDeathEnds >= urlsInCurrentLevelToScan) {
    const tree = await getLatestDataFromRedis({ workID });
    for (let i = 0; i <= Math.min(remainingSlots, tree.length - 1); i++) {
      sendMessageToQueue({
        url: tree[i + totalUrls - 1].myAddress,
        parentPosition: tree[i + totalUrls - 1].position,
        parentUrl: tree[i + totalUrls - 1].parentAddress,
        QueueUrl,
        workID,
      });
    }
    await incrementLevelData(workID);
    console.log(9, "level ended");
  }
};

const getChildrensNodes = async (workID, arrayOfChildrens, currentUrl, parentUrl, parentPosition) => {
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

const saveScrapedUrlInRedis = async (workID, currentUrl, parentUrl, childrenUrls) => {
  const currentUrlObj = {
    parentAddress: parentUrl,
    myAddress: currentUrl,
    childrenUrls: childrenUrls == undefined ? [] : childrenUrls,
    workID: parseInt(workID),
  };
  saveUrlInRedis(currentUrlObj);
};

module.exports = startWorker;
