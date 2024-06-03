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
    user: 'tareadystry@gmail.com',
    pass: 'gjlb xbbf osvy nwvk'
  }
});

const sendEmail = (data) => {
  const mailOptions = {
    from: 'tareadystry@gmail.com',
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
  try {
    const response = await axios.get('http://localhost:3001/', { params: data });
    console.log('API Response:', response.data);
  } catch (error) {
    console.error('Error making GET request:', error);
  }
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