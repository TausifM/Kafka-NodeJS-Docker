import express from "express";
import kafka from "kafka-node";
import mongoose from "mongoose";

const app = express();
app.use(express.json());

const dbsAreRunningFine = () => {
  mongoose.connect(process.env.MONGO_URL);
  const User = new mongoose.model("user", {
    name: String,
    email: String,
    password: String,
  });
  const kafkaClient = new kafka.KafkaClient({
    kafkaHost: process.env.KAFKA_BOOTSTRAP_SERVERS,
  });
  const kafkaConsumr = new kafka.Consumer(
    kafkaClient,
    [{ topic: process.env.KAFKA_TOPIC }],
    { autoCommit: false }
  );

  kafkaConsumr.on("message", async (message) => {
    const user = await new User(JSON.parse(message.value));
    await user.save();
  });
  kafkaConsumr.on("error", (err) => console.log(err))
};
setTimeout(dbsAreRunningFine, 10000);

app.listen(process.env.PORT);
