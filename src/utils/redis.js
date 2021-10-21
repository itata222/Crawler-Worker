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
const getWorkDictData = async (workID) => {
  try {
    const response = await redisClient.hgetallAsync(`workDict-${workID}`);
    return response;
  } catch (e) {
    console.log(e);
    return e;
  }
};
const incrementUrlsInCurrentLevelScannedData = async (workID) => {
  try {
    const response = await redisClient.hincrbyAsync(`levelData-${workID}`, "urlsInCurrentLevelAlreadyScanned", 1);
    return response;
  } catch (e) {
    return e;
  }
};
const addUrlsToCurrentLevelToScanData = async (childsUrlLength, workID) => {
  try {
    const levelData = await redisClient.hgetallAsync(`levelData-${workID}`);
    const childsSum = parseInt(levelData.urlsInCurrentLevelToScan) + childsUrlLength;
    const response = await redisClient.hsetAsync(`levelData-${workID}`, "urlsInCurrentLevelToScan", childsSum);
    return response;
  } catch (e) {
    return e;
  }
};
const addUrlsToNextLevelToScanData = async (childsUrlLength, workID) => {
  try {
    const levelData = await redisClient.hgetallAsync(`levelData-${workID}`);
    const childsSum = parseInt(levelData.urlsInNextLevelToScan) + childsUrlLength;
    const response = await redisClient.hsetAsync(`levelData-${workID}`, "urlsInNextLevelToScan", childsSum);
    return response;
  } catch (e) {
    return e;
  }
};
const setNewUrlsToCurrentLevelToScanData = async (workID) => {
  try {
    const response = await redisClient.hsetAsync(`levelData-${workID}`, "urlsInCurrentLevelToScan", 0);
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
const saveUrlInRedis = async ({ parentAddress, myAddress, depth, rootUrl, position, childrenUrls, workID }) => {
  let address = myAddress;
  try {
    if (myAddress != undefined && !myAddress.includes("http")) address = myAddress.substring(1);
    const childrenUrlsStr = JSON.stringify(childrenUrls);
    const urlObj = {
      parentAddress,
      myAddress: address,
      depth,
      rootUrl,
      position,
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

const setWorkAsDoneInRedis = async (workID) => {
  console.log("work doneeeee");
  try {
    await redisClient.hsetAsync(`workDict-${workID}`, "finished", "true");

    const workDict = await redisClient.hgetallAsync(`workDict-${workID}`);

    return workDict;
  } catch (e) {
    return e;
  }
};

module.exports = {
  addUrlsToNextLevelToScanData,
  isUrlExistInRedis,
  getCurrentLevelData,
  saveUrlInRedis,
  incrementLevelData,
  setWorkAsDoneInRedis,
  addUrlsToCurrentLevelToScanData,
  setNewUrlsToCurrentLevelToScanData,
  incrementUrlsInCurrentLevelScannedData,
  getWorkDictData,
  incrementTotalUrlsData,
  incrementDeathEndsData,
};
