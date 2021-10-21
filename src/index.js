const express = require("express");
const cors = require("cors");
const workerRouter = require("./routers/workerRouter");

const app = express();
const port = process.env.PORT || 8000;
require("./db/redis");

app.use(cors());
app.use(express.json());
app.use(workerRouter);

app.listen(port, () => {
  console.log("worker service on port:", port);
});
