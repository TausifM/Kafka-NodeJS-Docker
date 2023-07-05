import express from "express";
import kafka from "kafka-node";
import sequelize from "sequelize";

const app = express();
app.use(express.json());

const dbsAreRunningFine = () => {
  const db = new sequelize(process.env.POSTGRES_URL);
  const User = db.define("user", {
    name: sequelize.STRING,
    email: sequelize.STRING,
    password: sequelize.STRING,
  });
  const kafkaClient = new kafka.KafkaClient({
    kafkaHost: process.env.KAFKA_BOOTSTRAP_SERVERS,
  });
  const kafkaProducer = new kafka.Producer(kafkaClient);

  kafkaProducer.on("ready", () => {
    app.post("/", (req, res) => {
      kafkaProducer.send(
        [
          {
            topic: process.env.KAFKA_TOPIC,
            messages: JSON.stringify(req.body),
          },
        ],
        async (err, data) => {
          if (err) console.log(err);
          else {
            const user = await User.create(req.body);
            res.send(user);
          }
        }
      );
    });
  });
};
setTimeout(dbsAreRunningFine, 10000);

app.listen(process.env.PORT);
