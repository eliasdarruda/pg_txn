// The encoding of stored values ("DJSON"): transaction inputs and outputs,
// effect keys and results. JSON with a few tagged extensions, so that values
// JSON cannot hold survive a round trip, and the same in every client.
// Object keys are sorted: the same value always produces the same bytes.
//
// Supported: string, finite number, boolean, null, undefined (as a tag inside
// arrays / at top level; omitted as object property), bigint, Uint8Array,
// Date, plain dense arrays, plain objects. Everything else is rejected, as are
// strings (values and keys) PostgreSQL jsonb cannot store: U+0000 and lone
// UTF-16 surrogates. Keys sort by UTF-16 code units (JS default sort), as in
// the Elixir client.

const TAGS = ["$bigint", "$bytes", "$date", "$undefined", "$escape"];

export class SerializationError extends Error {
  path: string;
  constructor(message: string, path: string) {
    super(`${message} at ${path || "<root>"}`);
    this.name = "SerializationError";
    this.path = path;
  }
}

const B64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

export function bytesToBase64(bytes: Uint8Array): string {
  let out = "";
  let i = 0;
  for (; i + 2 < bytes.length; i += 3) {
    const n = (bytes[i] << 16) | (bytes[i + 1] << 8) | bytes[i + 2];
    out += B64[(n >> 18) & 63] + B64[(n >> 12) & 63] + B64[(n >> 6) & 63] + B64[n & 63];
  }
  const rem = bytes.length - i;
  if (rem === 1) {
    const n = bytes[i] << 16;
    out += B64[(n >> 18) & 63] + B64[(n >> 12) & 63] + "==";
  } else if (rem === 2) {
    const n = (bytes[i] << 16) | (bytes[i + 1] << 8);
    out += B64[(n >> 18) & 63] + B64[(n >> 12) & 63] + B64[(n >> 6) & 63] + "=";
  }
  return out;
}

export function base64ToBytes(s: string): Uint8Array {
  const clean = s.replace(/=+$/, "");
  const out = new Uint8Array(Math.floor((clean.length * 3) / 4));
  let buf = 0;
  let bits = 0;
  let o = 0;
  for (let i = 0; i < clean.length; i++) {
    const v = B64.indexOf(clean[i]);
    if (v < 0) throw new SerializationError("invalid base64", "");
    buf = (buf << 6) | v;
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      out[o++] = (buf >> bits) & 0xff;
    }
  }
  return out.subarray(0, o);
}

// Own data property; plain assignment of "__proto__" would set the prototype.
export function defineKey(o: Record<string, unknown>, k: string, v: unknown): void {
  if (k === "__proto__") Object.defineProperty(o, k, { value: v, enumerable: true, writable: true, configurable: true });
  else o[k] = v;
}

// U+0000 or an unpaired surrogate (both rejected by PostgreSQL jsonb)
function badString(s: string): string | null {
  if (!/[\u0000\uD800-\uDFFF]/.test(s)) return null;
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c === 0) return "U+0000";
    if (c >= 0xd800 && c <= 0xdbff) {
      const n = i + 1 < s.length ? s.charCodeAt(i + 1) : 0;
      if (n >= 0xdc00 && n <= 0xdfff) {
        i++;
        continue;
      }
      return "a lone surrogate";
    }
    if (c >= 0xdc00 && c <= 0xdfff) return "a lone surrogate";
  }
  return null;
}

function checkString(s: string, path: string, what: string): void {
  const bad = badString(s);
  if (bad) throw new SerializationError(`${what} contains ${bad}, which PostgreSQL jsonb cannot store`, path);
}

function isPlainObject(v: object): boolean {
  const p = Object.getPrototypeOf(v);
  return p === Object.prototype || p === null;
}

function describe(v: unknown): string {
  if (v === null) return "null";
  if (typeof v !== "object" && typeof v !== "function") return typeof v;
  const ctor = (v as { constructor?: { name?: string } }).constructor;
  return ctor && ctor.name ? ctor.name : typeof v;
}

// Convert a JS value into its tagged JSON-compatible form.
export function toTagged(value: unknown, path = "", seen: Set<object> = new Set()): unknown {
  switch (typeof value) {
    case "string":
      checkString(value, path, "string");
      return value;
    case "boolean":
      return value;
    case "number":
      if (!Number.isFinite(value)) throw new SerializationError(`non-finite number ${value}`, path);
      return Object.is(value, -0) ? 0 : value;
    case "bigint":
      return { $bigint: value.toString() };
    case "undefined":
      return { $undefined: true };
    case "function":
      throw new SerializationError(`functions are not durable values (${describe(value)})`, path);
    case "symbol":
      throw new SerializationError("symbols are not durable values", path);
  }
  if (value === null) return null;
  const obj = value as object;
  if (seen.has(obj)) throw new SerializationError("cyclic structure", path);
  if (obj instanceof Uint8Array) return { $bytes: bytesToBase64(obj) };
  if (obj instanceof Date) {
    if (Number.isNaN(obj.getTime())) throw new SerializationError("invalid Date", path);
    return { $date: obj.toISOString() };
  }
  if (typeof (obj as { then?: unknown }).then === "function") {
    throw new SerializationError("a running Promise is not a durable value", path);
  }
  seen.add(obj);
  try {
    if (Array.isArray(obj)) {
      if (Object.getPrototypeOf(obj) !== Array.prototype) {
        throw new SerializationError(`non-plain array ${describe(obj)}`, path);
      }
      const arr = obj as unknown[];
      const out: unknown[] = new Array(arr.length);
      for (let i = 0; i < arr.length; i++) {
        if (!(i in arr)) throw new SerializationError("sparse array (hole)", `${path}[${i}]`);
        out[i] = toTagged(arr[i], `${path}[${i}]`, seen);
      }
      return out;
    }
    if (!isPlainObject(obj)) {
      throw new SerializationError(`non-serializable ${describe(obj)} instance`, path);
    }
    const out: Record<string, unknown> = {};
    const emitted: string[] = [];
    for (const k of Object.keys(obj).sort()) {
      const v = (obj as Record<string, unknown>)[k];
      if (v === undefined) continue;
      const p = path ? `${path}.${k}` : k;
      checkString(k, p, "object key");
      defineKey(out, k, toTagged(v, p, seen));
      emitted.push(k);
    }
    // decided on the keys actually emitted: {$date: "...", x: undefined}
    // encodes like {$date: "..."} and must be escaped the same way
    if (emitted.length === 1 && TAGS.includes(emitted[0])) return { $escape: out };
    return out;
  } finally {
    seen.delete(obj);
  }
}

export function fromTagged(value: unknown): unknown {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(fromTagged);
  const o = value as Record<string, unknown>;
  const keys = Object.keys(o);
  if (keys.length === 1) {
    switch (keys[0]) {
      case "$bigint":
        return BigInt(o.$bigint as string);
      case "$bytes":
        return base64ToBytes(o.$bytes as string);
      case "$date":
        return new Date(o.$date as string);
      case "$undefined":
        return undefined;
      case "$escape": {
        const inner = o.$escape as Record<string, unknown>;
        const r: Record<string, unknown> = {};
        for (const k of Object.keys(inner)) defineKey(r, k, fromTagged(inner[k]));
        return r;
      }
    }
  }
  const r: Record<string, unknown> = {};
  for (const k of keys) defineKey(r, k, fromTagged(o[k]));
  return r;
}

// Canonical JSON text (sorted keys, no whitespace).
export function canonicalJson(v: unknown): string {
  if (v === null || typeof v !== "object") return JSON.stringify(v);
  if (Array.isArray(v)) return "[" + v.map(canonicalJson).join(",") + "]";
  const o = v as Record<string, unknown>;
  return (
    "{" +
    Object.keys(o)
      .sort()
      .filter((k) => o[k] !== undefined)
      .map((k) => JSON.stringify(k) + ":" + canonicalJson(o[k]))
      .join(",") +
    "}"
  );
}

export function serialize(value: unknown): string {
  return canonicalJson(toTagged(value));
}

export function deserialize(text: string): unknown {
  return fromTagged(JSON.parse(text));
}
