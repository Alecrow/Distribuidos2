// notificaciones/notificaciones.js
const { Kafka } = require('kafkajs');
const axios = require('axios');
const nodemailer = require('nodemailer');

const kafka = new Kafka({
  clientId: 'notificaciones',
  brokers: ['kafka:9092']
});

const consumer = kafka.consumer({ groupId: 'notificaciones-group' });

const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: 'your-email@gmail.com',
    pass: 'your-email-password'
  }
});

const sendEmail = (data) => {
  const mailOptions = {
    from: 'your-email@gmail.com',
    to: data.correo,
    subject: 'Estado de su producto',
    text: JSON.stringify(data, null, 2)
  };

  transporter.sendMail(mailOptions, function(error, info){
    if (error) {
      console.log(error);
    } else {
      console.log('Email enviado: ' + info.response);
    }
  });
};

const notifyAPI = async (data) => {
  await axios.get('http://your-api-endpoint', { params: data });
};

const start = async () => {
  await consumer.connect();

  const topics = ['recibido', 'preparando', 'entregando', 'finalizado'];
  for (const topic of topics) {
    await consumer.subscribe({ topic, fromBeginning: true });
  }

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const data = JSON.parse(message.value.toString());
      notifyAPI(data);
      sendEmail(data);
    },
  });
};

start().catch(console.error);