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

const pollMessageFromQueue = async ({ QueueName, workID }) => {
  try {
    const { QueueUrl } = await sqs.getQueueUrl({ QueueName }).promise();
    const { Messages } = await sqs
      .receiveMessage({
        QueueUrl,
        MaxNumberOfMessages: 10,
        MessageAttributeNames: [`${workID}$*`],
        VisibilityTimeout: 30,
        WaitTimeSeconds: 10,
      })
      .promise();

    if (Messages != null) {
      await deleteMessagesFromQueue({ Messages, QueueUrl });
    }
    return { QueueUrl, Messages: Messages || [] };
  } catch (e) {
    return undefined;
  }
};

const deleteMessagesFromQueue = async ({ Messages, QueueUrl }) => {
  if (Messages) {
    const messagesDeleteFuncs = Messages.map(async (message) => {
      return sqs
        .deleteMessage({
          QueueUrl,
          ReceiptHandle: message.ReceiptHandle,
        })
        .promise();
    });

    await Promise.allSettled(messagesDeleteFuncs).then();
  }
};

const deleteQueue = async ({ QueueUrl }) => {
  let isQueueDeleted;
  try {
    if (QueueUrl) isQueueDeleted = await sqs.deleteQueue({ QueueUrl }).promise();
    console.log("isQueueDeleted", isQueueDeleted);
  } catch (err) {
    next(err);
    console.log("112", err);
  }
};

module.exports = {
  sendMessageToQueue,
  pollMessageFromQueue,
  deleteMessagesFromQueue,
  deleteQueue,
};
