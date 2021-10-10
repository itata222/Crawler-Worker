const express=require("express");
const cors=require('cors');

const app=express();
const port=process.env.PORT;
const workerRouter=require('./routers/workerRouter')
require('./db/redis');

app.use(express.json());
app.use(cors())
app.use(workerRouter)

app.listen(port,()=>console.log('worker service'))



