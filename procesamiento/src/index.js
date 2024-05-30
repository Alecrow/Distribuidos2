// procesamiento/procesamiento.js
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'procesamiento',
  brokers: ['kafka:9092']
});

const consumer = kafka.consumer({ groupId: 'procesamiento-group' });
const producer = kafka.producer();

const start = async () => {
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: 'procesamiento', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const data = JSON.parse(message.value.toString());
      data.estado = 'recibido';
      await producer.send({
        topic: 'recibido',
        messages: [{ value: JSON.stringify(data) }]
      });
      // Simulación de procesamiento en paralelo
      setTimeout(async () => {
        data.estado = 'preparando';
        await producer.send({
          topic: 'preparando',
          messages: [{ value: JSON.stringify(data) }]
        });

        setTimeout(async () => {
          data.estado = 'entregando';
          await producer.send({
            topic: 'entregando',
            messages: [{ value: JSON.stringify(data) }]
          });

          setTimeout(async () => {
            data.estado = 'finalizado';
            await producer.send({
              topic: 'finalizado',
              messages: [{ value: JSON.stringify(data) }]
            });
          }, 1000); // Simula el tiempo de entrega
        }, 1000); // Simula el tiempo de preparación
      }, 1000); // Simula el tiempo de recepción
    },
  });
};

start().catch(console.error);