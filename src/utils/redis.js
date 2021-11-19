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
    const treeArr = [];
    const tree = await redisClient.lrangeAsync(`tree:${workID}`, 0, -1);
    tree.forEach((element) => {
      const el = JSON.parse(element);
      for (let item of el) {
        if (item != null) treeArr.push(item);
      }
    });
    const sortedTreeArr = treeArr.sort(function (a, b) {
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
    return sortedTreeArr;
  } catch (e) {
    console.log("e", e);
  }
};
const saveScrapedUrlChildrenInTreeList = async (nextLevelUrls, workID) => {
  const nextLevelUrlsString = JSON.stringify(nextLevelUrls);
  await redisClient.lpushAsync(`tree:${workID}`, nextLevelUrlsString);
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
    await redisClient.hsetAsync(`levelData-${workID}`, "urlsInNextLevelToScan", childsSum);
    return childsSum;
  } catch (e) {
    return e;
  }
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
  incrementUrlsInCurrentLevelScannedData,
  incrementTotalUrlsData,
  IsJsonString,
  incrementDeathEndsData,
  saveScrapedUrlChildrenInTreeList,
};
