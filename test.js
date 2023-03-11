const express = require("express");
const { Kafka } = require("kafkajs");

const app = express();

// Define the configuration for the Kafka client
const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9091", "localhost:9092"],
});

console.log("Test in main  branch")

console.log("New branch changes not in main");
console.log("one more change in new branch");
console.log("only push");

// Create an instance of the Kafka producer
const producer = kafka.producer();

// Create an instance of the Kafka consumer
const consumer = kafka.consumer({ groupId: "my-group" });

// Define the route for sending a message to Kafka
app.post("/messages/:topic", async (req, res) => {
  try {
    const { topic } = req.params;
    const { message } = req.body;

    // Send the message to the specified topic
    await producer.send({
      topic,
      messages: [{ value: message }],
    });

    return res.status(200).json({ message: "Message sent successfully." });
  } catch (error) {
    console.error(error);
    return res
      .status(500)
      .json({ error: "An error occurred while sending the message." });
  }
});

// Define the route for receiving messages from Kafka
app.get("/messages/:topic", async (req, res) => {
  try {
    const { topic } = req.params;

    // Subscribe the consumer to the specified topic
    await consumer.subscribe({ topic, fromBeginning: true });

    // Listen for messages on the topic
    await consumer.run({
      eachMessage: async ({ message }) => {
        console.log(`Received message: ${message.value}`);

        // Send the message back in the response
        res.write(`Message: ${message.value}\n`);
      },
    });

    return res.end();
  } catch (error) {
    console.error(error);
    return res
      .status(500)
      .json({ error: "An error occurred while receiving the messages." });
  }
});

// Connect the producer and consumer to the Kafka cluster
Promise.all([producer.connect(), consumer.connect()])
  .then(() => {
    console.log("Kafka producer and consumer connected.");
    // Start the server
    app.listen(3000, () => {
      console.log("Server started on port 3000.");
    });
  })
  .catch(console.error);
