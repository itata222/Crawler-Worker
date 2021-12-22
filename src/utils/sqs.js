const AWS = require("aws-sdk");

const sqs = new AWS.SQS({
  apiVersion: "2012-11-05",
  region: process.env.AWS_REGION,
});

const sendMessageToQueue = async ({ url, workID, QueueUrl, parentUrl, parentPosition }) => {
  try {
    let MessageBody;

    MessageBody = `${workID}$${url}$${parentUrl}$${parentPosition}`;
    const { MessageId } = await sqs
      .sendMessage({
        QueueUrl,
        MessageBody,
      })
      .promise();

    return MessageId;
  } catch (err) {
    console.log("111", err.message);
  }
};

const pollMessageFromQueue = async ({ QueueName }) => {
  try {
    const { QueueUrl } = await sqs.getQueueUrl({ QueueName }).promise();
    const { Messages } = await sqs
      .receiveMessage({
        QueueUrl,
        MaxNumberOfMessages: 10,
        VisibilityTimeout: 30,
        WaitTimeSeconds: 10,
      })
      .promise();

    console.log("Messages", Messages);
    if (Messages != null) await deleteMessagesFromQueue({ Messages, QueueUrl });

    return { QueueUrl, Messages: Messages || [] };
  } catch (e) {
    console.log(e, "e");
    // console.log("Queue does not exist");
  }
};

const deleteMessagesFromQueue = async ({ Messages, QueueUrl }) => {
  const messagesDeleteFuncs = Messages.map(async (message) => {
    return sqs
      .deleteMessage({
        QueueUrl,
        ReceiptHandle: message.ReceiptHandle,
      })
      .promise();
  });
  await Promise.allSettled(messagesDeleteFuncs)
    .then()
    .catch((e) => console.log(e));
};

module.exports = {
  sendMessageToQueue,
  pollMessageFromQueue,
  deleteMessagesFromQueue,
};
