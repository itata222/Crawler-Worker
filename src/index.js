const express=require("express");
const cors=require('cors');

const app=express();
const port=process.env.PORT||8000;
const workerRouter=require('./routers/workerRouter')
require('./db/redis');

app.use(cors())
app.use(express.json());
app.use(workerRouter)

app.listen(port,()=>console.log('worker service on port:',port))



