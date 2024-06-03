# notificaciones/Dockerfile
FROM node:14

WORKDIR /app
RUN npm install kafkajs
RUN npm install axios
RUN npm install nodemailer
COPY package.json ./
COPY package-lock.json ./
COPY . .

CMD ["node", "notificaciones.js"]