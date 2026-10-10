import express from"express";
import{timingSafeEqual}from"node:crypto";
import{Pool}from"pg";
import{Kysely,PostgresDialect,sql}from"kysely";
import{withTenant,type HcmDatabase,type UUID}from"@hcm/db";
const app=express(),pool=process.env.DATABASE_URL?new Pool({connectionString:process.env.DATABASE_URL,max:10,application_name:"hcm-admin-api"}):undefined;
const db=pool?new Kysely<HcmDatabase>({dialect:new PostgresDialect({pool})}):undefined;
const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
app.use(express.json({limit:"32kb"}));
app.use("/v1/dev-dashboard",(req:any,res:any,next:()=>void)=>{const expected=process.env.DASHBOARD_API_TOKEN,got=(req.header("authorization")??"").replace(/^Bearer /,"");if(!expected)return res.status(503).json({error:"dashboard_api_token_not_configured"});const a=Buffer.from(expected),b=Buffer.from(got);if(a.length!==b.length||!timingSafeEqual(a,b))return res.status(401).json({error:"unauthorized"});next();});
function org(res:any):UUID|undefined{const x=process.env.DASHBOARD_ORGANIZATION_ID;if(!x||!uuid.test(x)){res.status(503).json({error:"dashboard_organization_id_not_configured"});return;}return x;}
function dateInZone(z:string){const p=new Intl.DateTimeFormat("en-CA",{timeZone:z,year:"numeric",month:"2-digit",day:"2-digit"}).formatToParts(new Date());const v=(k:string)=>p.find(x=>x.type===k)?.value??"";return v("year")+"-"+v("month")+"-"+v("day");}
function addDay(s:string,n:number){const d=new Date(s+"T00:00:00Z");d.setUTCDate(d.getUTCDate()+n);return d.toISOString().slice(0,10);}
function state(s:string|null){return s==="succeeded"||s==="acknowledged"?"synced":s==="failed"?"failed":s==="started"||s==="retrying"?"pending":"idle";}
app.get("/health",async(_q:any,res:any)=>{if(!pool)return res.status(503).json({status:"not_ready",reason:"database_not_configured"});try{await pool.query("SELECT 1");return res.json({status:"ok",service:"admin-api",database:"ok"});}catch{return res.status(503).json({status:"not_ready",reason:"database_unavailable"});}});
app.get("/v1/dev-dashboard/snapshot",async(req:any,res:any)=>{
 if(!db)return res.status(503).json({error:"database_not_configured"});const tenant=org(res),propertyId=req.query.propertyId;if(!tenant)return;
 if(propertyId!==undefined&&(typeof propertyId!=="string"||!uuid.test(propertyId)))return res.status(400).json({error:"invalid_property_id"});
 try{const data=await withTenant(db,tenant,async trx=>{
  const result=await sql<{propertyId:UUID;organizationId:UUID;propertyName:string;timezone:string;currency:string;roomTypeId:UUID|null;roomName:string|null;roomCode:string|null;date:string|null;available:number|null;price:string|null;priceCurrency:string|null;stopSell:boolean|null;cta:boolean|null;ctd:boolean|null;oversold:boolean|null;syncStatus:string|null;connectionId:UUID|null;channel:string|null;channelName:string|null;enabled:boolean|null;lastReconciledAt:Date|null}>`SELECT p.property_id AS "propertyId",p.organization_id AS "organizationId",p.name AS "propertyName",p.timezone,p.currency,
    rt.room_type_id AS "roomTypeId",rt.name AS "roomName",rt.code AS "roomCode",i.stay_date::text AS date,i.available_units AS available,
    rate.amount::text AS price,rate.currency AS "priceCurrency",(i.stop_sell OR COALESCE(rate.stop_sell,false)) AS "stopSell",
    (i.cta OR COALESCE(rate.cta,false)) AS cta,(i.ctd OR COALESCE(rate.ctd,false)) AS ctd,
    EXISTS(SELECT 1 FROM hcm.oversell_conflicts oc WHERE oc.organization_id=i.organization_id AND oc.property_id=i.property_id AND oc.room_type_id=i.room_type_id AND oc.stay_date=i.stay_date AND oc.status='open') AS oversold,
    sync.status::text AS "syncStatus",c.id AS "connectionId",c.channel,c.label AS "channelName",c.enabled,
    (SELECT max(sl.logged_at) FROM hcm.sync_logs sl WHERE sl.organization_id=p.organization_id AND sl.property_id=p.property_id AND sl.direction='reconciliation') AS "lastReconciledAt"
   FROM hcm.properties p
   LEFT JOIN LATERAL(SELECT cc.channel_connection_id AS id,ch.kind::text AS channel,ch.name AS label,cc.enabled FROM hcm.channel_connections cc
    JOIN hcm.channels ch ON ch.organization_id=cc.organization_id AND ch.channel_id=cc.channel_id
    WHERE cc.organization_id=p.organization_id AND cc.property_id=p.property_id ORDER BY cc.enabled DESC,ch.name LIMIT 1)c ON true
   LEFT JOIN hcm.room_types rt ON rt.organization_id=p.organization_id AND rt.property_id=p.property_id AND rt.active
   LEFT JOIN hcm.inventory_days i ON i.organization_id=rt.organization_id AND i.property_id=rt.property_id AND i.room_type_id=rt.room_type_id
    AND i.stay_date>=((now() AT TIME ZONE p.timezone)::date+1) AND i.stay_date<((now() AT TIME ZONE p.timezone)::date+121)
   LEFT JOIN LATERAL(SELECT rd.amount,rd.currency,rd.stop_sell,rd.cta,rd.ctd FROM hcm.rate_days rd JOIN hcm.rate_plans rp
    ON rp.organization_id=rd.organization_id AND rp.property_id=rd.property_id AND rp.rate_plan_id=rd.rate_plan_id AND rp.active
    WHERE rd.organization_id=i.organization_id AND rd.property_id=i.property_id AND rd.stay_date=i.stay_date
    AND (c.id IS NULL OR EXISTS(SELECT 1 FROM hcm.channel_mappings cm WHERE cm.organization_id=rd.organization_id AND cm.property_id=rd.property_id
      AND cm.rate_plan_id=rd.rate_plan_id AND cm.room_type_id=rt.room_type_id AND cm.channel_connection_id=c.id AND cm.active))
    ORDER BY rp.code LIMIT 1)rate ON true
   LEFT JOIN LATERAL(SELECT sl.status FROM hcm.sync_logs sl WHERE sl.organization_id=i.organization_id AND sl.property_id=i.property_id
    AND sl.direction='outbound' AND (c.id IS NULL OR sl.channel_connection_id=c.id)
    AND COALESCE(sl.details->>'roomTypeId',sl.details->>'room_type_id')=rt.room_type_id::text
    AND COALESCE(sl.details->>'stayDate',sl.details->>'stay_date')=i.stay_date::text ORDER BY sl.logged_at DESC LIMIT 1)sync ON true
   WHERE p.organization_id=$${tenant}::uuid AND p.active AND ($${propertyId??null}::uuid IS NULL OR p.property_id=$${propertyId??null}::uuid)
   ORDER BY p.name,rt.name,i.stay_date`.execute(trx);
  const connectionRows=(await sql<{propertyId:UUID;connectionId:UUID;channel:string;label:string;enabled:boolean}>`SELECT cc.property_id AS "propertyId",cc.channel_connection_id AS "connectionId",ch.kind::text AS channel,ch.name AS label,cc.enabled
    FROM hcm.channel_connections cc JOIN hcm.channels ch ON ch.organization_id=cc.organization_id AND ch.channel_id=cc.channel_id
    WHERE cc.organization_id=${tenant}::uuid AND (${propertyId??null}::uuid IS NULL OR cc.property_id=${propertyId??null}::uuid)
    ORDER BY cc.enabled DESC,ch.name`.execute(trx)).rows;
  const connectionsByProperty=new Map<string,typeof connectionRows>();
  for(const row of connectionRows){const list=connectionsByProperty.get(row.propertyId)??[];list.push(row);connectionsByProperty.set(row.propertyId,list);}
  const map=new Map<string,any>();
  for(const r of result.rows){let p=map.get(r.propertyId);if(!p){p={propertyId:r.propertyId,organizationId:r.organizationId,name:r.propertyName,timezone:r.timezone,currency:r.currency.trim(),channelConnectionId:connectionsByProperty.get(r.propertyId)?.find((x)=>x.enabled)?.connectionId??connectionsByProperty.get(r.propertyId)?.[0]?.connectionId,channel:connectionsByProperty.get(r.propertyId)?.find((x)=>x.enabled)?.channel??connectionsByProperty.get(r.propertyId)?.[0]?.channel,connections:(connectionsByProperty.get(r.propertyId)??[]).map((x)=>({channelConnectionId:x.connectionId,channel:x.channel,label:x.label,enabled:x.enabled})),activeConnectionCount:(connectionsByProperty.get(r.propertyId)??[]).filter((x)=>x.enabled).length,lastReconciledAt:r.lastReconciledAt?.toISOString()??null,roomTypes:[]};map.set(r.propertyId,p);}if(!r.roomTypeId)continue;let room=p.roomTypes.find((x:{roomTypeId:string})=>x.roomTypeId===r.roomTypeId);if(!room){room={roomTypeId:r.roomTypeId,name:r.roomName,code:r.roomCode,cells:[]};p.roomTypes.push(room);}if(r.date)room.cells.push({date:r.date,availableUnits:Number(r.available),price:r.price===null?null:Number(r.price),currency:(r.priceCurrency??r.currency).trim(),stopSell:!!r.stopSell,cta:!!r.cta,ctd:!!r.ctd,oversold:!!r.oversold,syncState:state(r.syncStatus)});}
  return{generatedAt:new Date().toISOString(),demo:false,properties:[...map.values()]};
 });return res.set("Cache-Control","no-store").json(data);}catch(e){console.error("Snapshot query failed",e);return res.status(500).json({error:"dashboard_snapshot_failed"});}
});
app.get("/v1/dev-dashboard/sync-logs",async(req:any,res:any)=>{
 if(!db)return res.status(503).json({error:"database_not_configured"});const tenant=org(res),propertyId=req.query.propertyId,connectionId=req.query.channelConnectionId,limit=Number(req.query.limit??100);if(!tenant)return;
 if(typeof propertyId!=="string"||!uuid.test(propertyId))return res.status(400).json({error:"valid_property_id_required"});
 if(connectionId!==undefined&&(typeof connectionId!=="string"||!uuid.test(connectionId)))return res.status(400).json({error:"invalid_channel_connection_id"});
 if(!Number.isInteger(limit)||limit<1)return res.status(400).json({error:"invalid_limit"});
 try{const result=await withTenant(db,tenant,async trx=>sql<{
  loggedAt:Date;propertyId:UUID;channelConnectionId:UUID|null;channel:string;operation:string;status:string;lockWaitMs:number|null;dbCommitMs:number|null;httpStatus:number|null;errorCode:string|null;durationMs:number|null;direction:string;attempt:number;correlationId:string|null;
 }>`SELECT sl.logged_at AS "loggedAt",sl.property_id AS "propertyId",sl.channel_connection_id AS "channelConnectionId",COALESCE(ch.kind::text,'system') AS channel,
  sl.operation,sl.status::text AS status,CASE WHEN sl.details->>'lockWaitMs'~'^[0-9]+$' THEN(sl.details->>'lockWaitMs')::int END AS "lockWaitMs",
  CASE WHEN sl.details->>'dbCommitMs'~'^[0-9]+$' THEN(sl.details->>'dbCommitMs')::int END AS "dbCommitMs",
  sl.http_status AS "httpStatus",sl.sanitized_error_code AS "errorCode",sl.duration_ms AS "durationMs",sl.direction::text AS direction,sl.attempt,sl.correlation_id AS "correlationId"
  FROM hcm.sync_logs sl LEFT JOIN hcm.channel_connections cc ON cc.organization_id=sl.organization_id AND cc.channel_connection_id=sl.channel_connection_id
  LEFT JOIN hcm.channels ch ON ch.organization_id=cc.organization_id AND ch.channel_id=cc.channel_id
  WHERE sl.organization_id=$${tenant}::uuid AND sl.property_id=$${propertyId}::uuid
   AND ($${connectionId??null}::uuid IS NULL OR sl.channel_connection_id=$${connectionId??null}::uuid)
  ORDER BY sl.logged_at DESC LIMIT $${Math.min(limit,200)}`.execute(trx));
  return res.set("Cache-Control","no-store").json({logs:result.rows});}catch(e){console.error("Sync logs query failed",e);return res.status(500).json({error:"dashboard_sync_logs_failed"});}
});
const port=Number(process.env.PORT)||3000,server=app.listen(port,"0.0.0.0",()=>console.log("Admin API listening on 0.0.0.0:"+port));
async function shutdown(){server.close();if(db)await db.destroy();if(pool)await pool.end();}
process.once("SIGTERM",()=>{void shutdown();});process.once("SIGINT",()=>{void shutdown();});
