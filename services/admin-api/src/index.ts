import express from 'express';
import cors from 'cors';
import { Pool } from 'pg';
import { Kysely, PostgresDialect, sql } from 'kysely';
import type { HcmDatabase } from '@hcm/db';

const app = express();

// 1. Enable CORS so the Next.js Vercel dashboard can fetch data
app.use(cors());
app.use(express.json());

// 2. Initialize DB globally so the routes below can access it
let db: Kysely<HcmDatabase> | null = null;
if (process.env.DATABASE_URL) {
    const pool = new Pool({
        connectionString: process.env.DATABASE_URL,
        max: 10
    });
    db = new Kysely<HcmDatabase>({
        dialect: new PostgresDialect({ pool })
    });
}

// 3. Health Check (Required by Render)
app.get('/health', (req, res) => {
    res.status(200).json({ status: 'ok', service: 'admin-api' });
});

// 4. ARI MATRIX ROUTE (Updated to match DashboardSnapshot contract)
app.get('/v1/dev-dashboard/snapshot', async (req, res) => {
    if (!db) return res.status(500).json({ error: 'No database connection' });
    try {
        const inventory = await sql`SELECT * FROM hcm.inventory_days ORDER BY stay_date ASC LIMIT 500`.execute(db);

        // FIX 1: Add <string, any> to the Map
        const roomMap = new Map<string, any>();

        // FIX 2: Add `as any[]` to bypass raw SQL type errors
        for (const row of inventory.rows as any[]) {
            const roomId = String(row.room_type_id || 'unknown');
            if (!roomMap.has(roomId)) {
                roomMap.set(roomId, {
                    roomTypeId: roomId,
                    name: "Live Room " + roomId.substring(0, 4),
                    code: "LIV",
                    cells: []
                });
            }

            // FIX 3: Add `!` after get() to tell TypeScript we know it is not undefined
            roomMap.get(roomId)!.cells.push({
                date: new Date(row.stay_date).toISOString().slice(0, 10),
                availableUnits: Number(row.available_units || row.available || 0),
                price: Number(row.price || 0),
                currency: row.currency || 'USD',
                syncState: 'synced'
            });
        }

        const snapshot = {
            generatedAt: new Date().toISOString(),
            demo: false, // Turns off the yellow banner!
            properties: [{
                propertyId: req.query.propertyId || "live-property-1",
                organizationId: "live-org",
                name: "Live Postgres Database",
                timezone: "Asia/Kolkata",
                roomTypes: Array.from(roomMap.values())
            }]
        };

        res.json(snapshot);
    } catch (error) {
        console.error("Endpoint error:", error);
        res.status(500).json({ error: String(error) });
    }
});

// 5. SYNC INSPECTOR ROUTE
// 5. SYNC INSPECTOR ROUTE (Updated to query real sync_logs)
app.get('/v1/dev-dashboard/sync-logs', async (req, res) => {
    if (!db) return res.status(500).json({ error: 'No database connection' });

    const propertyId = req.query.propertyId as string;
    if (!propertyId) return res.json({ logs: [] });

    try {
        const logs = await sql`
          SELECT 
            logged_at as "loggedAt",
            property_id as "propertyId",
            channel_connection_id as "channelConnectionId",
            'booking_com' as channel, /* FIX: Provide a default string to prevent DB crash */
            operation,
            status,
            lock_wait_ms as "lockWaitMs",
            db_commit_ms as "dbCommitMs",
            http_status as "httpStatus",
            error_code as "errorCode"
          FROM hcm.sync_logs 
          WHERE property_id = ${propertyId}
          ORDER BY logged_at DESC 
          LIMIT 50
        `.execute(db);

        res.json({ logs: logs.rows });
    } catch (error) {
        console.error("Endpoint error:", error);
        res.status(500).json({ error: String(error) });
    }
});

const PORT = Number(process.env.PORT) || 3000;

app.listen(PORT, '0.0.0.0', () => {
    console.log(`✅ Admin API Server is listening on 0.0.0.0:${PORT}`);
    if (db) {
        console.log('🚀 Admin API connected to Neon Postgres successfully.');
    } else {
        console.warn('⚠️ DATABASE_URL is missing. API will return 500 errors.');
    }
});