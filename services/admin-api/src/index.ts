import express from 'express';
import cors from 'cors';
import { Pool } from 'pg';
import { Kysely, PostgresDialect } from 'kysely';
import type { HcmDatabase } from '@hcm/db';

// TODO: If you have a routes.ts or controllers file in this folder, import it here!
// import { registerRoutes } from './routes.js';

const app = express();

// 1. Enable CORS so the Next.js Vercel dashboard can fetch data without being blocked
app.use(cors());
app.use(express.json());

// 2. Health Check
app.get('/health', (req, res) => {
    res.status(200).json({ status: 'ok', service: 'admin-api' });
});

const PORT = Number(process.env.PORT) || 3000;

app.listen(PORT, '0.0.0.0', async () => {
    console.log(`✅ Admin API Server is listening on 0.0.0.0:${PORT}`);
    
    try {
        console.log('⏳ Connecting to Neon Postgres...');
        
        if (!process.env.DATABASE_URL) {
            console.warn('⚠️ DATABASE_URL is missing. API will not return real data.');
            return;
        }

        const pool = new Pool({
            connectionString: process.env.DATABASE_URL,
            max: 10,
        });
        const db = new Kysely<HcmDatabase>({
            dialect: new PostgresDialect({ pool }),
        });

        // 3. Mount your actual data routes here and pass them the `db` instance
        // registerRoutes(app, db);

        console.log('🚀 Admin API connected to database successfully.');
    } catch (error) {
        console.error('❌ Failed to connect to database:', error);
    }
});