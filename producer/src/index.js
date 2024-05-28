const { Kafka, Partitioners } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'my-producer',
  brokers: [process.env.KAFKA_BROKER]
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner
});

const runProducer = async () => {
  try {
    await producer.connect();
    await producer.send({
      topic: 'my-topic',
      messages: [
        { value: 'Hello KafkaJS user!' }
      ]
    });
    console.log('Mensaje enviado con éxito');
    await producer.disconnect();
  } catch (error) {
    console.error('Error al enviar el mensaje:', error);
    process.exit(1);
  }
};

runProducer();
