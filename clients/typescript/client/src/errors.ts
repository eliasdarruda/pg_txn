/** Throw from an effect with retry on: the delay before the next attempt. */
export class RetryableError extends Error {
  retryAfterMs?: number;
  constructor(message: string, options: { retryAfterMs?: number; cause?: unknown } = {}) {
    super(message, { cause: options.cause });
    this.name = "RetryableError";
    this.retryAfterMs = options.retryAfterMs;
  }
}

/** Throw from an effect with retry on: fail it now, without more attempts. */
export class PermanentError extends Error {
  constructor(message: string, options: { cause?: unknown } = {}) {
    super(message, { cause: options.cause });
    this.name = "PermanentError";
  }
}

export type EffectError = { name: string; message: string; [k: string]: unknown };

/** Thrown into the transaction when an effect failed for good (no more retries). */
export class EffectFailedError extends Error {
  effect: string;
  error: EffectError;
  constructor(effect: string, error: EffectError) {
    super(`effect ${effect} failed: ${error?.name}: ${error?.message}`);
    this.name = "EffectFailedError";
    this.effect = effect;
    this.error = error;
  }
}

/** Another transaction held the key for longer than this one waits (keyWaitMs). */
export class KeyTimeoutError extends Error {
  key: string;
  holder: string;
  constructor(key: string, holder: string, waitedMs: number) {
    super(`key ${key} is still held by transaction ${holder} after ${waitedMs} ms`);
    this.name = "KeyTimeoutError";
    this.key = key;
    this.holder = holder;
  }
}

/** Another process took over this transaction (this one lost its lease). */
export class FencedError extends Error {
  constructor(txId: string) {
    super(`transaction ${txId} is now driven by another process`);
    this.name = "FencedError";
  }
}

/** A named or enqueued transaction ended without committing. */
export class TransactionFailedError extends Error {
  txId: string;
  status: string;
  error: unknown;
  constructor(txId: string, status: string, error: unknown) {
    const e = error as { name?: string; message?: string } | null;
    super(`transaction ${txId} ${status}${e ? `: ${e.name ?? "Error"}: ${e.message ?? ""}` : ""}`);
    this.name = "TransactionFailedError";
    this.txId = txId;
    this.status = status;
    this.error = error;
  }
}

export function errorJson(e: unknown): EffectError {
  if (e instanceof Error) {
    const out: EffectError = { name: e.name || "Error", message: e.message };
    for (const k of ["code", "status", "statusCode"]) {
      const v = (e as unknown as Record<string, unknown>)[k];
      if (typeof v === "string" || typeof v === "number") out[k] = v;
    }
    return out;
  }
  return { name: "Error", message: typeof e === "string" ? e : JSON.stringify(e) ?? String(e) };
}

export function sqlState(e: unknown): string | undefined {
  let x = e as { code?: unknown; cause?: unknown } | undefined;
  for (let i = 0; x && i < 4; i++) {
    if (typeof x.code === "string" && /^[0-9A-Z]{5}$/.test(x.code)) return x.code;
    x = x.cause as typeof x;
  }
  return undefined;
}

export function sqlDetail(e: unknown): string | undefined {
  let x = e as { detail?: unknown; cause?: unknown } | undefined;
  for (let i = 0; x && i < 4; i++) {
    if (typeof x.detail === "string") return x.detail;
    x = x.cause as typeof x;
  }
  return undefined;
}
