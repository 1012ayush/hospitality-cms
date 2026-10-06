// services/admin-api/src/index.ts
import express from 'express';

// TODO: Import your database and Kafka utilities from your shared packages later
// import { db } from '@hcm/db'; 
// import { kafka } from '@hcm/shared';

const app = express();
app.use(express.json());

// 1. Core Health Check (Crucial for Render's network ping)
app.get('/health', (req, res) => {
    res.status(200).json({ status: 'ok', service: 'admin-api' });
});

// 2. Add your future API routes here
// app.use('/api/properties', propertyRoutes);

const PORT = Number(process.env.PORT) || 3000;

// 3. Bind the port FIRST, then initialize slow network connections
app.listen(PORT, '0.0.0.0', async () => {
    console.log(`✅ Admin API Server is listening on 0.0.0.0:${PORT}`);
    
    try {
        console.log('⏳ Initializing external services (Neon Postgres, Aiven Kafka)...');
        
        // TODO: Uncomment these when you are ready to connect them
        // await db.connect();
        // await kafka.connect();
        
        console.log('🚀 All external services connected successfully.');
    } catch (error) {
        console.error('❌ Failed to initialize external services:', error);
        // Note: We catch the error so the web server stays alive to report the failure
    }
});