// procesamiento/procesamiento.js
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'procesamiento',
  brokers: ['kafka:9092']
});

const consumer = kafka.consumer({ groupId: 'procesamiento-group' });
const producer = kafka.producer();

const processMessage = async (message, currentTopic) => {
  const data = JSON.parse(message.value.toString());

  switch (currentTopic) {
    case 'procesamiento':
      //delay(2000);
      data.estado = 'recibido';
      console.log(`CONSUMER: ${JSON.stringify(data)}`);
      await producer.send({
        topic: 'recibido',
        messages: [{ value: JSON.stringify(data) }]
      });
      console.log(`PRODUCER: ${JSON.stringify(data)}`);
      break;
    case 'recibido':
      //delay(2000);
      data.estado = 'preparando';
      console.log(`CONSUMER: ${JSON.stringify(data)}`);
      await producer.send({
        topic: 'preparando',
        messages: [{ value: JSON.stringify(data) }]
      });
      console.log(`PRODUCER: ${JSON.stringify(data)}`);
      break;
    case 'preparando':
      //delay(2000);
      data.estado = 'entregando';
      console.log(`CONSUMER: ${JSON.stringify(data)}`);
      await producer.send({
        topic: 'entregando',
        messages: [{ value: JSON.stringify(data) }]
      });
      console.log(`PRODUCER: ${JSON.stringify(data)}`);
      break;
    case 'entregando':
      //delay(2000);
      data.estado = 'finalizado';
      console.log(`CONSUMER: ${JSON.stringify(data)}`);
      await producer.send({
        topic: 'finalizado',
        messages: [{ value: JSON.stringify(data) }]
      });
      console.log(`PRODUCER: ${JSON.stringify(data)}`);
      break;
    default:
      console.error(`Unhandled topic: ${currentTopic}`);
  }
};

const start = async () => {
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic: 'procesamiento', fromBeginning: true });
  await consumer.subscribe({ topic: 'recibido', fromBeginning: true });
  await consumer.subscribe({ topic: 'preparando', fromBeginning: true });
  await consumer.subscribe({ topic: 'entregando', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const data = JSON.parse(message.value.toString());
      console.log(`CONSUMER: ${JSON.stringify(data)}`);
      await processMessage(message, topic);
    },
  });
};

start().catch(console.error);