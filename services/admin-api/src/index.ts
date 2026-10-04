// services/admin-api/src/index.ts
import express from 'express';

const app = express();
app.use(express.json());

// A simple health check route so Render knows the API is alive
app.get('/health', (req, res) => {
    res.status(200).json({ status: 'ok', service: 'admin-api' });
});

// The dynamic port Render assigns, falling back to 3000 for local dev
const PORT = Number(process.env.PORT) || 3000;

// Binding to '0.0.0.0' is mandatory for Render's network
app.listen(PORT, '0.0.0.0', () => {
    console.log(`Admin API Server is listening on 0.0.0.0:${PORT}`);
});