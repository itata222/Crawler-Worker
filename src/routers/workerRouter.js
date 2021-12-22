const express = require("express");

const router = new express.Router();

router.get("/", (req, res) => {
  res.send("Worker can get API calls.");
});
//-----------------------------------------------------------

module.exports = router;
