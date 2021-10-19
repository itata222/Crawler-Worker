const redisClient = require("../db/redis");

const isUrlExistInRedis = async ({ currentUrl, rootUrl, parentAddress }) => {
  let isExist, response;
  try {
    response = await redisClient.keysAsync(`${rootUrl}$${currentUrl}[/]$*`);
    response == null || response?.length === 0 ? (isExist = false) : (isExist = true);
    return isExist;
  } catch (e) {
    console.log(e);
    return e;
  }
};
const getCurrentLevelData = async () => {
  try {
    const response = await redisClient.hgetallAsync("levelData");
    return response;
  } catch (e) {
    console.log(e);
    return e;
  }
};
const getWorkDictData = async () => {
  try {
    const response = await redisClient.hgetallAsync("workDict");
    return response;
  } catch (e) {
    console.log(e);
    return e;
  }
};
const incrementUrlsInCurrentLevelScannedData = async () => {
  try {
    const response = await redisClient.hincrbyAsync("levelData", "urlsInCurrentLevelAlreadyScanned", 1);
    return response;
  } catch (e) {
    return e;
  }
};
const addUrlsToCurrentLevelToScanData = async (childsUrlLength) => {
  try {
    const levelData = await redisClient.hgetallAsync("levelData");
    const childsSum = parseInt(levelData.urlsInCurrentLevelToScan) + childsUrlLength;
    const response = await redisClient.hsetAsync("levelData", "urlsInCurrentLevelToScan", childsSum);
    return response;
  } catch (e) {
    return e;
  }
};
const addUrlsToNextLevelToScanData = async (childsUrlLength) => {
  try {
    const levelData = await redisClient.hgetallAsync("levelData");
    const childsSum = parseInt(levelData.urlsInNextLevelToScan) + childsUrlLength;
    const response = await redisClient.hsetAsync("levelData", "urlsInNextLevelToScan", childsSum);
    return response;
  } catch (e) {
    return e;
  }
};
const setNewUrlsToCurrentLevelToScanData = async () => {
  try {
    const response = await redisClient.hsetAsync("levelData", "urlsInCurrentLevelToScan", 0);
    return response;
  } catch (e) {
    return e;
  }
};
const incrementLevelData = async () => {
  try {
    const levelData = await redisClient.hgetallAsync("levelData");
    await redisClient.hincrbyAsync("levelData", "currentLevel", 1);
    await redisClient.hsetAsync("levelData", "urlsInCurrentLevelToScan", parseInt(levelData.urlsInNextLevelToScan));
    await redisClient.hsetAsync("levelData", "urlsInCurrentLevelAlreadyScanned", 0);
    await redisClient.hsetAsync("levelData", "urlsInNextLevelToScan", 0);
    const updatedStats = await redisClient.hgetallAsync("levelData");
    return updatedStats;
  } catch (e) {
    return e;
  }
};
const incrementTotalUrlsData = async () => {
  try {
    const response = await redisClient.hincrbyAsync("levelData", "totalUrls", 1);
    return response;
  } catch (e) {
    return e;
  }
};
const saveUrlInRedis = async ({ parentAddress, myAddress, depth, rootUrl, position, childrenUrls }) => {
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
    };
    const urlStr = JSON.stringify(urlObj);

    await redisClient.setexAsync(`${rootUrl}$${address}$${parentAddress}`, 3600, urlStr);
  } catch (e) {
    return e;
  }
};

const setWorkAsDoneInRedis = async () => {
  try {
    await redisClient.hsetAsync("workDict", "finished", "true");

    const workDict = await redisClient.hgetallAsync("workDict");

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
};
