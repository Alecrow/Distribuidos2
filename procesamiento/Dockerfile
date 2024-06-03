# procesamiento/Dockerfile
FROM node:14

WORKDIR /app
RUN npm install kafkajs
COPY package.json ./
COPY package-lock.json ./
COPY . .

CMD ["node", "procesamiento.js"]