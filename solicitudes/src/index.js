// solicitudes/solicitudes.js
const express = require('express');
const bodyParser = require('body-parser');
const { Kafka } = require('kafkajs');
const axios = require('axios');
const app = express();

app.use(bodyParser.json());

const kafka = new Kafka({
  clientId: 'solicitudes',
  brokers: ['kafka:9092']
});

const producer = kafka.producer();

let idCounter = 1;

app.post('/procesar', async (req, res) => {
  const data = req.body;
  data.id = idCounter++;
  await producer.send({
    topic: 'procesamiento',
    messages: [{ value: JSON.stringify(data) }]
  });
  res.status(200).send(data);
});

const start = async () => {
  await producer.connect();
  app.listen(3000, () => {
    console.log('Solicitudes escuchando en el puerto 3000');
  });
};

start().catch(console.error);