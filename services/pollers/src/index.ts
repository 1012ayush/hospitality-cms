import type { Reservation } from "@hcm/hospitality-contracts";
import { createLogger, loadConfig } from "@hcm/shared";

export interface PulledReservation {
  reservation: Reservation;
  partnerAcknowledgementToken: string;
}
export interface PullReservationAdapter {
  /** Adapter maps its documented partner API to canonical reservations. */
  fetchUnacknowledgedReservations(
    signal?: AbortSignal,
  ): Promise<PulledReservation[]>;
  /** Explicit partner acknowledgment; must only be called after apply() commits its DB transaction. */
  acknowledgeReservation(token: string): Promise<void>;
}
export interface ReservationCommitter {
  apply(
    reservation: Reservation,
  ): Promise<{
    committed: boolean;
    outcome: "applied" | "duplicate" | "stale";
  }>;
}

/** Acknowledges each pulled booking only after the canonical state transaction commits. */
export async function processPollingBatch(
  adapter: PullReservationAdapter,
  committer: ReservationCommitter,
  logger: ReturnType<typeof createLogger>,
  signal?: AbortSignal,
): Promise<{ committed: number; acknowledged: number; failed: number }> {
  const records = await adapter.fetchUnacknowledgedReservations(signal);
  let committed = 0;
  let acknowledged = 0;
  let failed = 0;
  for (const record of records) {
    if (signal?.aborted) break;
    try {
      const result = await committer.apply(record.reservation);
      if (!result.committed)
        throw new Error(
          "Reservation processor did not confirm transaction commit",
        );
      committed++;
      // Partner cursor/ack must advance only after the Postgres reservation + outbox commit.
      await adapter.acknowledgeReservation(record.partnerAcknowledgementToken);
      acknowledged++;
    } catch (error) {
      failed++;
      logger.error(
        { err: error, reservationId: record.reservation.reservationId },
        "Pulled reservation failed; leaving partner record unacknowledged",
      );
    }
  }
  return { committed, acknowledged, failed };
}

export async function runPollingWorker(
  adapter: PullReservationAdapter,
  committer: ReservationCommitter,
  logger: ReturnType<typeof createLogger>,
  options: { pollIntervalMs?: number; signal?: AbortSignal } = {},
): Promise<void> {
  const intervalMs = options.pollIntervalMs ?? 5_000;
  while (!options.signal?.aborted) {
    try {
      await processPollingBatch(adapter, committer, logger, options.signal);
    } catch (error) {
      logger.error(
        { err: error },
        "Reservation poll failed; next poll will retry",
      );
    }
    await new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, intervalMs);
      options.signal?.addEventListener(
        "abort",
        () => {
          clearTimeout(timer);
          resolve();
        },
        { once: true },
      );
    });
  }
}

// Booking.com/Expedia EQC/Airbnb integrations are partner-gated. Reservation retrieval,
// acknowledgment token shape, cursor rules, auth and rate limits: ASSUMPTION – verify against partner docs.
// OUT OF SCOPE (Part 4A): concrete partner HTTP clients and credential retrieval; verify each contract before implementation.

export async function runConfiguredWorker(
  adapter: PullReservationAdapter,
  committer: ReservationCommitter,
): Promise<void> {
  const config = loadConfig();
  const logger = createLogger({ ...config, serviceName: "pollers" });
  await runPollingWorker(adapter, committer, logger);
}
