import { z } from "zod";

const IsoDateSchema = z
  .string()
  .regex(/^\d{4}-\d{2}-\d{2}$/)
  .refine((value) => {
    const parsed = new Date(`${value}T00:00:00.000Z`);
    return (
      !Number.isNaN(parsed.valueOf()) &&
      parsed.toISOString().slice(0, 10) === value
    );
  }, "Expected a valid ISO calendar date");

export const ReservationSchema = z
  .object({
    tenantId: z.string().uuid(),
    propertyId: z.string().uuid(),
    channelConnectionId: z.string().uuid(),
    channel: z.enum([
      "booking_com",
      "expedia_eqc",
      "airbnb",
      "pms",
      "rms",
      "other",
    ]),
    reservationId: z.string().min(1),
    channelVersion: z.string().min(1), // Opaque source metadata; do not sort lexically.
    sourceSequence: z
      .number()
      .int()
      .nonnegative()
      .refine(Number.isSafeInteger, "sourceSequence must be a safe integer"), // Monotonic numeric ordering per reservation identity.
    lifecycle: z.enum(["new", "modified", "cancelled", "no_show"]),
    status: z.enum(["new", "confirmed", "modified", "cancelled", "no_show"]),
    // roomTypeId/ratePlanId are the legacy single-room primary line. For multi-room bookings,
    // `rooms` carries every booked room; the legacy primary line remains supported.
    roomTypeId: z.string().uuid().optional(),
    ratePlanId: z.string().uuid().optional(),
    rooms: z
      .array(
        z
          .object({
            roomTypeId: z.string().uuid(),
            ratePlanId: z.string().uuid().optional(),
            adults: z.number().int().nonnegative(),
            children: z.number().int().nonnegative().default(0),
          })
          .strict(),
      )
      .min(1)
      .optional(),
    stay: z
      .object({
        checkIn: IsoDateSchema,
        checkOut: IsoDateSchema,
        propertyTimeZone: z.string().min(1),
      })
      .strict(),
    occupancy: z
      .object({
        adults: z.number().int().nonnegative(),
        children: z.number().int().nonnegative().default(0),
      })
      .strict()
      .optional(),
    guest: z
      .object({
        givenName: z.string().optional(),
        familyName: z.string().optional(),
        email: z.string().email().optional(),
        phone: z.string().optional(),
      })
      .strict()
      .optional(),
    paymentReference: z.string().optional(), // Token/reference only; never PAN or CVV.
    currency: z.string().length(3).optional(),
    totalAmount: z.number().nonnegative().optional(),
    sourceUpdatedAt: z.string().datetime({ offset: true }).optional(),
    receivedAt: z.string().datetime({ offset: true }),
    oversold: z.boolean().default(false),
  })
  .strict()
  .superRefine((reservation, context) => {
    if (
      !reservation.rooms?.length &&
      (!reservation.roomTypeId || !reservation.occupancy)
    ) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["rooms"],
        message: "Provide rooms or a legacy roomTypeId and occupancy",
      });
    }
    if (reservation.stay.checkOut <= reservation.stay.checkIn) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["stay", "checkOut"],
        message: "checkOut must be after checkIn",
      });
    }
  });

export type Reservation = z.infer<typeof ReservationSchema>;
