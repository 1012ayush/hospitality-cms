import { sql, type Kysely, type Transaction } from "kysely";
import type { HcmDatabase, UUID } from "./db.types.js";

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** Run work in a DB transaction with tenant RLS context scoped to that transaction. */
export async function withTenant<T>(
  db: Kysely<HcmDatabase>,
  orgId: UUID,
  fn: (trx: Transaction<HcmDatabase>) => Promise<T> | T,
): Promise<T> {
  if (!UUID_PATTERN.test(orgId)) {
    throw new TypeError("orgId must be a UUID");
  }

  return db.transaction().execute(async (trx) => {
    // Parameterized set_config is transaction-local (third arg true), so a pooled
    // connection cannot leak tenant context into the next borrower.
    await sql`SET LOCAL ROLE hcm_app`.execute(trx);
    await sql`select set_config('app.organization_id', ${orgId}, true)`.execute(
      trx,
    );
    return fn(trx);
  });
}
