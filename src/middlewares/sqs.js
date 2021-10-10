const AWS=require('aws-sdk');

const sqs = new AWS.SQS({
    apiVersion: '2012-11-05',
    region:process.env.AWS_REGION
});

const QueueUrl="https://sqs.eu-central-1.amazonaws.com/191422694885/crawler-queue"

const sendMessageToQueue = async ({url,rootUrl}) => {
    let MessageBody = `${rootUrl}$$$${url}`;
    try {
        await sqs.sendMessage({
            QueueUrl,
            MessageBody
        }).promise();

    } catch (err) {
        console.log('111',err);
    }
};

const pollMessagesFromQueue = async (req, res, next) => {
    const QueueUrl = req.query.queueUrl;
    try {
        const { Messages } = await sqs.receiveMessage({
            QueueUrl,
            MaxNumberOfMessages: 10,
            MessageAttributeNames: [
                "All"
            ],
            VisibilityTimeout: 30,
            WaitTimeSeconds: 10
        }).promise();

        req.messages = Messages || [];
        next();
        if (Messages) {
            const messagesDeleteFuncs = Messages.map(message => {
                return sqs.deleteMessage({
                    QueueUrl,
                    ReceiptHandle: message.ReceiptHandle
                }).promise();
            });

            Promise.allSettled(messagesDeleteFuncs)
                .then(data => console.log(data));
        }
    } catch (err) {
        console.log(err);
    }
};

module.exports={
    sendMessageToQueue,
    pollMessagesFromQueue
}