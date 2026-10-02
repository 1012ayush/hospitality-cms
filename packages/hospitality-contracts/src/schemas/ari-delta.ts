import { z } from "zod";

const IsoDateSchema = z.string().regex(/^\d{4}-\d{2}-\d{2}$/).refine((value) => {
  const parsed = new Date(`${value}T00:00:00.000Z`);
  return !Number.isNaN(parsed.valueOf()) && parsed.toISOString().slice(0, 10) === value;
}, "Expected a valid ISO calendar date");

export const AriDeltaSchema = z.object({
  tenantId: z.string().uuid(),
  propertyId: z.string().uuid(),
  channelConnectionId: z.string().uuid(),
  source: z.enum(["pms", "rms", "channel_manager", "ota"]),
  roomTypeId: z.string().uuid(),
  ratePlanId: z.string().uuid().optional(),
  stay: z.object({
    startDate: IsoDateSchema,
    endDate: IsoDateSchema,
    propertyTimeZone: z.string().min(1),
  }).strict(),
  availability: z.number().int().nonnegative().optional(),
  closed: z.boolean().optional(),
  restrictions: z.object({
    minStay: z.number().int().positive().optional(),
    maxStay: z.number().int().positive().optional(),
    closedToArrival: z.boolean().optional(),
    closedToDeparture: z.boolean().optional(),
  }).strict().optional(),
  currency: z.string().length(3).optional(),
  nightlyRates: z.array(z.object({ date: IsoDateSchema, amount: z.number().nonnegative() }).strict()).optional(),
  sourceVersion: z.string().min(1),
  sourceUpdatedAt: z.string().datetime({ offset: true }).optional(),
  receivedAt: z.string().datetime({ offset: true }),
}).strict().superRefine((delta, context) => {
  if (delta.stay.endDate <= delta.stay.startDate) {
    context.addIssue({ code: z.ZodIssueCode.custom, path: ["stay", "endDate"], message: "endDate must be after startDate" });
  }
});

export type AriDelta = z.infer<typeof AriDeltaSchema>;
