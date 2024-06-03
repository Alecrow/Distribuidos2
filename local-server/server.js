const express = require('express');
const app = express();
const port = 3001;

app.get('/', (req, res) => {
  console.log('Solicitud GET recibida:');
  console.log('Parámetros:', req.query);
  res.send('Solicitud GET recibida');
});

// Inicia el servidor
app.listen(port, () => {
  console.log(`Servidor escuchando en http://localhost:${port}`);
});