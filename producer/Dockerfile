ARG NODE_VERSION=22.1.0

FROM node:${NODE_VERSION}-alpine

ENV NODE_ENV production

WORKDIR /usr/src/app

# Copia los archivos de configuración y las dependencias
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

# Copia el resto de los archivos de la aplicación
COPY . .

USER node

EXPOSE 3000

CMD ["node", "src/index.js"]
