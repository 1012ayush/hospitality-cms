import type { AriDelta, Reservation } from "@hcm/hospitality-contracts";

export type AdapterErrorClass = "retryable" | "permanent" | "auth";
export interface AdapterError {
  classification: AdapterErrorClass;
  code: string;
  message: string;
  retryAfterMs?: number;
  httpStatus?: number;
}
export type AdapterResult<T> =
  | { ok: true; value: T; correlationId?: string }
  | { ok: false; error: AdapterError; correlationId?: string };
export interface AdapterConnection {
  organizationId: string;
  propertyId: string;
  channelConnectionId: string;
  channel: "booking_com" | "expedia_eqc" | "airbnb" | "pms" | "rms" | "other";
  credentialSecretRef: string;
}
export interface CanonicalRateUpdate {
  organizationId: string;
  propertyId: string;
  roomTypeId: string;
  ratePlanId: string;
  stayDate: string;
  amount: number;
  currency: string;
  sourceVersion: string;
}
export interface CanonicalRestrictionUpdate {
  organizationId: string;
  propertyId: string;
  roomTypeId: string;
  ratePlanId?: string;
  stayDate: string;
  stopSell?: boolean;
  closedToArrival?: boolean;
  closedToDeparture?: boolean;
  minStay?: number;
  maxStay?: number;
  sourceVersion: string;
}
export interface ReservationPullRequest { cursor?: string; limit?: number; }
export interface PulledReservation { reservation: Reservation; acknowledgementToken: string; }
export interface FullSyncRequest {
  organizationId: string;
  propertyId: string;
  channelConnectionId: string;
  roomTypeIds?: string[];
  fromDate: string;
  toDate: string;
}
export interface ChannelHealth { healthy: boolean; checkedAt: string; detail?: string; }

/**
 * The partner boundary uses canonical/internal request types. Subclasses and transports own
 * partner-specific field mapping; no unverified endpoint, credential, or OTA wire shape is assumed.
 */
export abstract class BaseChannelAdapter {
  constructor(readonly connection: AdapterConnection) {
    if (!connection.credentialSecretRef.startsWith("secret://") && !connection.credentialSecretRef.startsWith("arn:")) {
      throw new TypeError("credentialSecretRef must be a secret reference, never credential material");
    }
  }

  abstract pushAvailability(delta: AriDelta): Promise<AdapterResult<void>>;
  abstract pushRates(update: CanonicalRateUpdate): Promise<AdapterResult<void>>;
  abstract pushRestrictions(update: CanonicalRestrictionUpdate): Promise<AdapterResult<void>>;
  abstract pullReservations(request: ReservationPullRequest): Promise<AdapterResult<PulledReservation[]>>;
  abstract ackReservation(acknowledgementToken: string): Promise<AdapterResult<void>>;
  abstract fullSync(request: FullSyncRequest): Promise<AdapterResult<void>>;
  abstract healthCheck(): Promise<AdapterResult<ChannelHealth>>;
  abstract mapError(error: unknown): AdapterError;
}
