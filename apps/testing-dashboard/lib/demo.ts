import type { AriCell, DashboardSnapshot, SyncLog } from "./contracts";
const org = "00000000-0000-4000-8000-000000000001";
const property = "00000000-0000-4000-8000-000000000003";
const connection = "00000000-0000-4000-8000-000000000002";

export function demoSnapshot(): DashboardSnapshot {
  const dates = Array.from({ length: 120 }, (_, i) => {
    const d = new Date();
    d.setDate(d.getDate() + i);
    return d.toISOString().slice(0, 10);
  });
  return {
    generatedAt: new Date().toISOString(),
    demo: true,
    properties: [{
      propertyId: property, organizationId: org, channelConnectionId: connection,
      name: "Demo · Northstar Hotel", timezone: "Asia/Kolkata", currency: "INR", channel: "booking_com", connections:[{channelConnectionId:connection,channel:"booking_com",label:"Booking.com sandbox"},{channelConnectionId:"00000000-0000-4000-8000-000000000004",channel:"airbnb",label:"Airbnb fixture"},{channelConnectionId:"00000000-0000-4000-8000-000000000005",channel:"pms",label:"PMS fixture"}],
      roomTypes: [0, 1].map((room) => ({
        roomTypeId: `10000000-0000-4000-8000-${String(room + 1).padStart(12, "0")}`,
        name: room ? "Garden King" : "Deluxe Queen", code: room ? "GDK" : "DLQ",
        cells: dates.map((date, i) => ({
          date,
          availableUnits: Math.max(0, (room ? 5 : 2) - (i % 8 === 2 ? 2 : i % 5 === 0 ? 1 : 0)),
          price: (room ? 189 : 149) + (i % 6) * 8,
          currency: "USD", stopSell: i % 19 === 0, cta: i % 23 === 0,
          ctd: i % 29 === 0, oversold: i % 37 === 0,
          syncState: (i % 11 === 0 ? "pending" : i % 31 === 0 ? "failed" : "synced") as AriCell["syncState"],
        })),
      })),
    }],
  };
}

export function demoLogs(): SyncLog[] {
  return Array.from({ length: 30 }, (_, i) => ({
    loggedAt: new Date(Date.now() - i * 41_000).toISOString(), propertyId: property,
    channelConnectionId: connection, channel: i % 3 ? "booking_com" : "mews",
    operation: i % 2 ? "availability.push" : "reservation.poll",
    status: i % 13 === 0 ? "retrying" : "succeeded", lockWaitMs: i % 4 ? 4 + i % 7 : null,
    dbCommitMs: 8 + i % 16, httpStatus: i % 13 === 0 ? 429 : 200,
    errorCode: i % 13 === 0 ? "PARTNER_THROTTLED" : null,
  }));
}
