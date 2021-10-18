const AWS=require('aws-sdk');

const sqs = new AWS.SQS({
    apiVersion: '2012-11-05',
    region:process.env.AWS_REGION
});


const sendMessageToQueue = async ({url,rootUrl,QueueUrl}) => {
    console.log(url)

    try {
        let MessageBody;
        if(!url?.includes('http')){
            url=url.substring(1)
            MessageBody = `${rootUrl}$$$${rootUrl+url}`;
        }
        else
            MessageBody = `${rootUrl}$$$${url}`;
        const {MessageId} = await sqs.sendMessage({
            QueueUrl,
            MessageBody
        }).promise();

        return MessageId

    } catch (err) {
        console.log('111',err);
    }
};

const pollMessageFromQueue = async ({QueueName,rootUrl}) => {
    try {
        const {QueueUrl}= await sqs.getQueueUrl({QueueName}).promise();
        const { Messages } = await sqs.receiveMessage({
            QueueUrl,
            MaxNumberOfMessages: 1,
            MessageAttributeNames: [
                `${rootUrl}$$$*`
            ],
            VisibilityTimeout: 30,
            WaitTimeSeconds: 10
        }).promise();
            
        return {QueueUrl,Messages}
    }catch(e){
        return undefined
    }
}

const deleteMessagesFromQueue=async({Messages,QueueUrl})=>{
    if (Messages) {
        const messagesDeleteFuncs = Messages.map(message => {
            return sqs.deleteMessage({
                QueueUrl,
                ReceiptHandle: message.ReceiptHandle
            }).promise();
        });
        
        Promise.allSettled(messagesDeleteFuncs)
        .then(data => console.log('deleteMessageFromQueue',data));
    }
};


module.exports={
    sendMessageToQueue,
    pollMessageFromQueue,
    deleteMessagesFromQueue
}

