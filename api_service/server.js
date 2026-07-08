const express = require('express');
const cors = require('cors');
const { Kafka } = require('kafkajs');

const app = express();
const port = 3000;

app.use(cors());
app.use(express.json());

let users = [];

const kafka = new Kafka({
  clientId: 'api-service',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();
let isKafkaConnected = false;

async function connectKafka() {
  try {
    await producer.connect();
    isKafkaConnected = true;
    console.log('Kafka producer connected');
  } catch (error) {
    isKafkaConnected = false;
    console.error('Kafka producer connection failed:', error.message);
  }
}

app.post('/api/users', async (req, res) => {
  const { name, email, role } = req.body;

  if (!name || !email || !role) {
    return res.status(400).json({
      message: 'name, email, and role are required'
    });
  }

  const user = {
    id: Date.now().toString(),
    name,
    email,
    role,
    createdAt: new Date().toISOString()
  };

  users.push(user);

  const event = {
    eventType: 'USER_CREATED',
    userId: user.id,
    email: user.email,
    name: user.name,
    role: user.role,
    source: 'api-service',
    createdAt: new Date().toISOString()
  };

  if (isKafkaConnected) {
    await producer.send({
      topic: 'user-events',
      messages: [
        {
          key: user.id,
          value: JSON.stringify(event)
        }
      ]
    });
  }

  return res.status(201).json({
    message: 'User created successfully',
    data: user
  });
});

app.get('/api/users', (req, res) => {
  return res.status(200).json({
    message: 'Users retrieved successfully',
    data: users
  });
});

app.get('/api/users/:id', (req, res) => {
  const user = users.find(item => item.id === req.params.id);

  if (!user) {
    return res.status(404).json({
      message: 'User not found'
    });
  }

  return res.status(200).json({
    message: 'User retrieved successfully',
    data: user
  });
});

app.listen(port, async () => {
  console.log(`API service running on http://localhost:${port}`);
  await connectKafka();
});