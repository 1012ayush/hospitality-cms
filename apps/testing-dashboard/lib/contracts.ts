export type ChannelKind = "booking_com" | "expedia_eqc" | "airbnb" | "pms" | "rms";
export interface AriCell { date:string; availableUnits:number; price:number; currency:string; stopSell:boolean; cta:boolean; ctd:boolean; oversold:boolean; syncState:"synced"|"pending"|"failed"|"idle"; }
export interface DashboardRoomType { roomTypeId:string; name:string; code:string; cells:AriCell[]; }
export interface DashboardConnection { channelConnectionId:string; channel:ChannelKind; label:string; }
export interface DashboardProperty { propertyId:string; organizationId:string; channelConnectionId:string; connections?:DashboardConnection[]; name:string; timezone:string; channel:ChannelKind; roomTypes:DashboardRoomType[]; }
export interface DashboardSnapshot { generatedAt:string; demo:boolean; properties:DashboardProperty[]; }
export interface SyncLog { loggedAt:string; propertyId:string; channelConnectionId:string|null; channel:string; operation:string; status:string; lockWaitMs:number|null; dbCommitMs:number|null; httpStatus:number|null; errorCode:string|null; }
