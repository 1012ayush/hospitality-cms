import type { AriDelta } from "@hcm/hospitality-contracts";
import type {
  AdapterError,
  AdapterResult,
  CanonicalRateUpdate,
  CanonicalRestrictionUpdate,
  ChannelHealth,
  FullSyncRequest,
  PulledReservation,
  ReservationPullRequest,
} from "@hcm/adapter-base";
import { BaseChannelAdapter } from "@hcm/adapter-base";

/** Internal command DTO. It is deliberately not an OTA request body or a claimed Booking.com wire schema. */
export interface BookingAvailabilityCommand {
  propertyId: string;
  roomTypeId: string;
  startDate: string;
  endDateExclusive: string;
  availableUnits?: number;
  close: boolean;
}
export interface BookingRateCommand {
  propertyId: string; roomTypeId: string; ratePlanId: string; stayDate: string; amount: number; currency: string;
}
export interface BookingRestrictionCommand {
  propertyId: string; roomTypeId: string; ratePlanId?: string; stayDate: string;
  stopSell?: boolean; cta?: boolean; ctd?: boolean; minLos?: number; maxLos?: number;
}
export type BookingOperation = "availability" | "rates" | "restrictions" | "pull_reservations" | "ack_reservation" | "full_sync" | "health_check";
export interface BookingTransportError extends Error { statusCode?: number; retryAfterMs?: number; partnerCode?: string; }
export interface BookingTransport {
  /** Implemented only after partner documentation is contractually available. Secret resolution stays behind this boundary. */
  execute<T>(args: { operation: BookingOperation; credentialSecretRef: string; payload?: unknown }): Promise<T>;
}

export const BOOKING_COM_RETRY_POLICY = [
  { failure: "HTTP 429 / partner throttling", classification: "retryable", maxAttempts: 8, delay: "respect normalized retryAfterMs, otherwise exponential backoff with jitter" },
  { failure: "HTTP 5xx / transport timeout", classification: "retryable", maxAttempts: 8, delay: "exponential backoff with jitter" },
  { failure: "HTTP 401 / 403", classification: "auth", maxAttempts: 1, delay: "pause connection and rotate/revalidate secret reference" },
  { failure: "other HTTP 4xx", classification: "permanent", maxAttempts: 1, delay: "DLQ and operator correction" },
] as const;

/**
 * Booking.com connectivity is partner-gated. This class maps canonical models to internal commands;
 * the injected transport must add the exact partner wire names/endpoints only after partner approval.
 */
export class BookingComAdapter extends BaseChannelAdapter {
  constructor(connection: ConstructorParameters<typeof BaseChannelAdapter>[0], private readonly transport: BookingTransport) {
    super(connection);
    if (connection.channel !== "booking_com") throw new TypeError("BookingComAdapter requires a booking_com connection");
  }

  async pushAvailability(delta: AriDelta): Promise<AdapterResult<void>> {
    try {
      const command: BookingAvailabilityCommand = {
        propertyId: delta.propertyId,
        roomTypeId: delta.roomTypeId,
        startDate: delta.stay.startDate,
        endDateExclusive: delta.stay.endDate,
        ...(delta.availability === undefined ? {} : { availableUnits: delta.availability }),
        close: delta.closed ?? delta.restrictions?.closedToArrival ?? false,
      };
      await this.transport.execute({ operation: "availability", credentialSecretRef: this.connection.credentialSecretRef, payload: command });
      return { ok: true, value: undefined };
    } catch (error) { return { ok: false, error: this.mapError(error) }; }
  }

  async pushRates(update: CanonicalRateUpdate): Promise<AdapterResult<void>> {
    try {
      const command: BookingRateCommand = { propertyId: update.propertyId, roomTypeId: update.roomTypeId,
        ratePlanId: update.ratePlanId, stayDate: update.stayDate, amount: update.amount, currency: update.currency };
      await this.transport.execute({ operation: "rates", credentialSecretRef: this.connection.credentialSecretRef, payload: command });
      return { ok: true, value: undefined };
    } catch (error) { return { ok: false, error: this.mapError(error) }; }
  }

  async pushRestrictions(update: CanonicalRestrictionUpdate): Promise<AdapterResult<void>> {
    try {
      const command: BookingRestrictionCommand = { propertyId: update.propertyId, roomTypeId: update.roomTypeId,
        ...(update.ratePlanId ? { ratePlanId: update.ratePlanId } : {}), stayDate: update.stayDate,
        ...(update.stopSell === undefined ? {} : { stopSell: update.stopSell }),
        ...(update.closedToArrival === undefined ? {} : { cta: update.closedToArrival }),
        ...(update.closedToDeparture === undefined ? {} : { ctd: update.closedToDeparture }),
        ...(update.minStay === undefined ? {} : { minLos: update.minStay }),
        ...(update.maxStay === undefined ? {} : { maxLos: update.maxStay }),
      };
      await this.transport.execute({ operation: "restrictions", credentialSecretRef: this.connection.credentialSecretRef, payload: command });
      return { ok: true, value: undefined };
    } catch (error) { return { ok: false, error: this.mapError(error) }; }
  }

  async pullReservations(request: ReservationPullRequest): Promise<AdapterResult<PulledReservation[]>> {
    try {
      const value = await this.transport.execute<PulledReservation[]>({ operation: "pull_reservations", credentialSecretRef: this.connection.credentialSecretRef, payload: request });
      return { ok: true, value };
    } catch (error) { return { ok: false, error: this.mapError(error) }; }
  }
  async ackReservation(acknowledgementToken: string): Promise<AdapterResult<void>> {
    try {
      await this.transport.execute({ operation: "ack_reservation", credentialSecretRef: this.connection.credentialSecretRef, payload: { acknowledgementToken } });
      return { ok: true, value: undefined };
    } catch (error) { return { ok: false, error: this.mapError(error) }; }
  }
  async fullSync(request: FullSyncRequest): Promise<AdapterResult<void>> {
    try {
      await this.transport.execute({ operation: "full_sync", credentialSecretRef: this.connection.credentialSecretRef, payload: request });
      return { ok: true, value: undefined };
    } catch (error) { return { ok: false, error: this.mapError(error) }; }
  }
  async healthCheck(): Promise<AdapterResult<ChannelHealth>> {
    try {
      const value = await this.transport.execute<ChannelHealth>({ operation: "health_check", credentialSecretRef: this.connection.credentialSecretRef });
      return { ok: true, value };
    } catch (error) { return { ok: false, error: this.mapError(error) }; }
  }

  mapError(error: unknown): AdapterError {
    const normalized = error as Partial<BookingTransportError>;
    const status = normalized.statusCode;
    const classification = status === 401 || status === 403 ? "auth"
      : status === 429 || status === 408 || status === undefined || status >= 500 ? "retryable" : "permanent";
    return {
      classification,
      code: normalized.partnerCode ?? (status ? `HTTP_${status}` : "TRANSPORT_ERROR"),
      message: error instanceof Error ? error.message : "Unknown Booking.com transport error",
      ...(normalized.retryAfterMs === undefined ? {} : { retryAfterMs: normalized.retryAfterMs }),
      ...(status === undefined ? {} : { httpStatus: status }),
    };
  }
}

// All OTA URLs, external field names, auth exchanges, limits and acknowledgment semantics:
// ASSUMPTION – verify against partner docs. Expedia integrations are Partner Central/EQC (not Rapid).
// Booking.com, Expedia, and Airbnb connectivity are partner-gated; actual HTTP transport is deliberately injected.
