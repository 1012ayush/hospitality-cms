import { z } from "zod";

export const HOSPITALITY_EVENT_TYPES = [
  "com.hospitality.webhook.received",
  "com.hospitality.event.dead_lettered",
  "com.hospitality.ari.delta",
  "com.hospitality.reservation.created",
  "com.hospitality.reservation.modified",
  "com.hospitality.reservation.cancelled",
  "com.hospitality.reservation.no_show",
  "com.hospitality.reservation.oversold",
  "com.hospitality.inventory.propagation.requested",
  "com.hospitality.inventory.propagation.failed",
] as const;

export type HospitalityEventType = (typeof HOSPITALITY_EVENT_TYPES)[number];

export const CloudEventSchema = z
  .object({
    specversion: z.literal("1.0"),
    id: z.string().min(1),
    source: z.string().min(1),
    type: z.enum(HOSPITALITY_EVENT_TYPES),
    subject: z.string().optional(),
    time: z.string().datetime({ offset: true }),
    datacontenttype: z.string().optional(),
    dataschema: z.string().optional(),
    data: z.unknown(),
  })
  .passthrough();

export type CloudEvent<
  TData = unknown,
  TType extends string = HospitalityEventType,
> = {
  specversion: "1.0";
  id: string;
  source: string;
  type: TType;
  subject?: string;
  time: string;
  datacontenttype?: string;
  dataschema?: string;
  data: TData;
  [extension: string]: unknown;
};

export type HospitalityCloudEvent<TData> = CloudEvent<
  TData,
  HospitalityEventType
>;
