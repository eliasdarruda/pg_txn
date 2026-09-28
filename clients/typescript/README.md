# pg_txn for TypeScript

`@pg-txn/client` for Node.js 20+ and Bun, with adapters for Drizzle
(`@pg-txn/drizzle`) and Knex/Objection (`@pg-txn/knex`). node-postgres works
without an adapter.

```bash
npm install @pg-txn/client @pg-txn/drizzle
```

## Setup

```ts
import { drizzle } from "drizzle-orm/node-postgres"
import { PgTxn } from "@pg-txn/client"
import { drizzleDb } from "@pg-txn/drizzle"

export const db = drizzle(process.env.DATABASE_URL!)
export const pgtxn = new PgTxn(drizzleDb(db))
```

That is the whole setup. The first use installs the `txn` schema, and this
process starts running its spawned functions and background transactions.
On shutdown:

```ts
process.on("SIGTERM", () => pgtxn.close().then(() => process.exit(0)))
```

Other libraries:

```ts
new PgTxn(pool)             // a node-postgres Pool; tx.db is a PoolClient
new PgTxn(knexDb(knex))     // tx.db is the Knex transaction (Objection works on it)
```

## A checkout

```ts
import { eq } from "drizzle-orm"
import { orders } from "./schema"

export const checkout = (orderId: number) =>
  pgtxn.transaction(async (tx) => {
    const [order] = await tx.db.select().from(orders).where(eq(orders.id, orderId))
    if (order?.status !== "new") return null

    const payment = await tx.effect(
      (ctx) => stripe.paymentIntents.create(
        { amount: order.total, currency: "usd", confirm: true },
        { idempotencyKey: ctx.idempotencyKey },
      ),
      {
        retry: true,                                     // safe: Stripe deduplicates by the key
        compensate: (p, ctx) => stripe.refunds.create({ payment_intent: p.id }, { idempotencyKey: ctx.idempotencyKey }),
      },
    )

    await tx.db.update(orders).set({ status: "paid", paymentId: payment.id }).where(eq(orders.id, orderId))
    await tx.spawn(() => mailer.sendReceipt(orderId))    // runs iff this commits
    return payment.id
  }, { key: ["order", orderId] })                          // one checkout per order at a time
```

What happens:

1. **First run.** The function runs in a database transaction. It reaches
   the charge, which has not run yet, so that transaction is rolled back.
2. **The charge.** It is made outside of any transaction and its result is
   recorded. No lock or connection is held; the key makes a second checkout
   of the same order wait.
3. **Second run.** The function runs again on fresh data. `tx.effect`
   returns the recorded payment, and the update commits. If the order had
   been cancelled meanwhile, the function would return early instead, and
   the unused charge would be refunded.
4. **After the commit.** This process sends the receipt.
5. **If the transaction fails later,** the charge is refunded.

## API

### `pgtxn.transaction(fn, options?)`

Runs `fn(tx)` as one transaction that may include effects, and returns what
`fn` returns. If `fn` throws, nothing it wrote is committed.

| option | |
|---|---|
| `key` | transactions with the same key run one at a time; the others wait, holding nothing. A string, or JSON such as `["order", id]`. |
| `keys` | several keys, claimed all at once or none (no deadlocks), e.g. `[["account", from], ["account", to]]` |
| `id` | the transaction id |
| `isolation` | e.g. `"serializable"` |

Without a key, concurrency is optimistic. Every run re-reads your data, so
the committing run writes on current data. If data changed while an effect
ran, the re-run takes another path (the unused effect is compensated) or
calls the effect with different `deps` (a new effect; the old one is
compensated). Use a key where contention is expected, so it never costs a
compensation.

### `tx.db`

Your database transaction for this run: the Drizzle, Knex or node-postgres
transaction. Use it for all of your queries.

### `tx.effect(fn, options?)`

Calls `fn(ctx)` once for the whole transaction, outside of any database
transaction, and returns its recorded result in every run. `ctx` has
`idempotencyKey`, `attempt`, `signal` and `txId`; `idempotencyKey` is the
same across retries and crashes.

| option | default | |
|---|---|---|
| `retry` | off | `true` (5 attempts) or `{ attempts }`. Without it, `fn` is called at most once, and a failure, timeout or crash mid-call fails the effect. Turn it on only if `fn` is safe to repeat. |
| `compensate` | | `(result, ctx) => …`: undoes the effect if the transaction does not use its result. Uses the same `retry`. |
| `timeoutMs` | 30000 | per attempt; aborts `ctx.signal` |
| `deps` | | the data the call depends on, e.g. `[order.total]`: if a re-run reaches the effect with different deps, it is a new effect and the old one is compensated |
| `name` | the function's name | a label in `txn.effects` |

With `retry` on:

- `throw new PermanentError(msg)` stops the attempts.
- `throw new RetryableError(msg, { retryAfterMs })` sets the delay before the
  next attempt.

When the effect fails for good, `EffectFailedError` is thrown into `fn`.
You can catch it and still commit.

### `tx.spawn(fn, options?)`

Calls `fn(ctx)` iff the transaction commits, in this process, right after
the commit. It is recorded in `txn.effects` and has the same `retry`
default: off. It returns the effect id. More options: `delayMs`, `timeoutMs`
and `name`.

Several spawns run concurrently, in no particular order. If this process
dies between the commit and the call, the effect is marked `EffectLost`.

### `pgtxn.spawn(fn, { trx? })`

The same outside of a pg_txn transaction. With `trx` (your own Drizzle,
Knex or pg transaction), it runs iff `trx` commits:

```ts
await db.transaction(async (trx) => {
  await trx.insert(signups).values(user)
  await pgtxn.spawn(() => mailer.sendWelcome(user.email), { trx })
})
```

### Named and background transactions

```ts
const settle = pgtxn.define("settle", async (tx, { invoiceId }: { invoiceId: number }) => { … })

await settle({ invoiceId: 7 }, { key: ["invoice", 7] })          // runs here; resumed elsewhere if this process dies
const id = await pgtxn.enqueue("settle", { invoiceId: 7 }, { key: ["invoice", 7] })   // on any replica that defines it
await pgtxn.wait(id)                                    // its output
```

Pass `{ trx }` to `enqueue` to queue the transaction iff your transaction
commits.

### `tx.now()` and `tx.uuid()`

A timestamp and UUIDs that are the same in every run.

## Options

`new PgTxn(db, options)`:

| option | default | |
|---|---|---|
| `concurrency` | 16 | spawned functions, compensations and background transactions run at once |
| `leaseMs` | 30000 | lease of a transaction or effect this process drives |
| `keyWaitMs` | 300000 | how long to wait for a key another transaction holds (`KeyTimeoutError`) |
| `listen` | `true` | wake the worker with LISTEN. Use `false` or `{ connectionString }` (a direct endpoint) behind RDS Proxy. |
| `pollMs` | 250 | idle poll interval of the worker |
| `install` | `true` | install the `txn` schema if it is missing |
| `onError` | `console.error` | worker errors |

More in [docs/operations.md](../../docs/operations.md).
