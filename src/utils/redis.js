const redisClient = require("../db/redis");

const isUrlExistInRedis = async ({ currentUrl }) => {
  try {
    const response = await redisClient.getAsync(`${currentUrl}`);
    return response;
  } catch (e) {
    console.log(e);
    return e;
  }
};
const getCurrentLevelData = async (workID) => {
  try {
    const response = await redisClient.hgetallAsync(`levelData-${workID}`);
    return response;
  } catch (e) {
    console.log(e);
    return e;
  }
};
function IsJsonString(str) {
  try {
    JSON.parse(str);
  } catch (e) {
    return false;
  }
  return true;
}
const incrementUrlsInCurrentLevelScannedData = async (workID) => {
  try {
    const response = await redisClient.hincrbyAsync(`levelData-${workID}`, "urlsInCurrentLevelAlreadyScanned", 1);
    return response;
  } catch (e) {
    return e;
  }
};
const getLatestDataFromRedis = async ({ workID }) => {
  try {
    // const allWorkNodes = await getAllUrlsInRedis({ workID });
    // console.log("allWorkNodes", allWorkNodes.length);
    const treeArr = [];
    const tree = await redisClient.lrangeAsync(`tree:${workID}`, 0, -1);
    tree.forEach((element) => {
      const el = JSON.parse(element);
      treeArr.push(...el);
    });
    return treeArr;
  } catch (e) {
    console.log("e", e);
  }
};
const decreaseRemainnigSlots = async (workID) => {
  try {
    const response = await redisClient.hincrbyAsync(`levelData-${workID}`, "remainingSlots", -1);
    return response;
  } catch (e) {
    return e;
  }
};
const addUrlsToNextLevelToScanData = async (childsUrlLength, workID) => {
  try {
    const levelData = await redisClient.hgetallAsync(`levelData-${workID}`);
    const childsSum = parseInt(levelData.urlsInNextLevelToScan) + childsUrlLength;
    console.log(childsSum, "childsSum");
    await redisClient.hsetAsync(`levelData-${workID}`, "urlsInNextLevelToScan", childsSum);
    return childsSum;
  } catch (e) {
    return e;
  }
};
const insertUrlsToNextLevel = async (arrOfUrls, workID) => {
  const urlsASstrings = JSON.stringify(arrOfUrls);
  const currentLevelData = await redisClient.hgetallAsync(`levelData-${workID}`);
  await redisClient.hsetAsync(`levelData-${workID}`, "nextLevelUrls", currentLevelData.nextLevelUrls + urlsASstrings);
};
const incrementTotalUrlsData = async (workID) => {
  try {
    const response = await redisClient.hincrbyAsync(`levelData-${workID}`, "totalUrls", 1);
    return response;
  } catch (e) {
    return e;
  }
};
const incrementDeathEndsData = async (workID) => {
  try {
    const response = await redisClient.hincrbyAsync(`levelData-${workID}`, "currentLevelDeathEnds", 1);
    return response;
  } catch (e) {
    return e;
  }
};

const incrementLevelData = async (workID) => {
  try {
    const levelData = await redisClient.hgetallAsync(`levelData-${workID}`);
    await redisClient.hincrbyAsync(`levelData-${workID}`, "currentLevel", 1);
    await redisClient.hsetAsync(`levelData-${workID}`, "urlsInCurrentLevelToScan", parseInt(levelData.urlsInNextLevelToScan));
    await redisClient.hsetAsync(`levelData-${workID}`, "urlsInCurrentLevelAlreadyScanned", 0);
    await redisClient.hsetAsync(`levelData-${workID}`, "currentLevelDeathEnds", 0);
    await redisClient.hsetAsync(`levelData-${workID}`, "urlsInNextLevelToScan", 0);
    const updatedStats = await redisClient.hgetallAsync(`levelData-${workID}`);
    return updatedStats;
  } catch (e) {
    return e;
  }
};
const setFirstPositionInNextLevel = async (workID, nextPosition) => {
  try {
    await redisClient.hsetAsync(`levelData-${workID}`, "firstPositionInNextLevel", nextPosition);
  } catch (e) {
    return e;
  }
};
const saveUrlInRedis = async ({ parentAddress, myAddress, childrenUrls, workID }) => {
  let address = myAddress;
  try {
    if (myAddress != undefined && !myAddress.includes("http")) address = myAddress.substring(1);
    const childrenUrlsStr = JSON.stringify(childrenUrls);
    const urlObj = {
      parentAddress,
      myAddress,
      childrenUrlsStr,
      workID,
    };
    const urlStr = JSON.stringify(urlObj);
    await redisClient.setexAsync(`${address}`, 3600, urlStr);
  } catch (e) {
    console.log(e);
    return e;
  }
};

module.exports = {
  incrementLevelData,
  getLatestDataFromRedis,
  addUrlsToNextLevelToScanData,
  isUrlExistInRedis,
  decreaseRemainnigSlots,
  getCurrentLevelData,
  saveUrlInRedis,
  insertUrlsToNextLevel,
  incrementUrlsInCurrentLevelScannedData,
  incrementTotalUrlsData,
  IsJsonString,
  setFirstPositionInNextLevel,
  incrementDeathEndsData,
};
