import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  serialize, deserialize, toTagged, fromTagged, canonicalJson, SerializationError, bytesToBase64, base64ToBytes,
} from "../src/serialize.ts";

// shared with the Elixir client (byte-compatibility vectors)
const VECTORS = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../../djson-vectors.json");

type Spec = { t: string; v: any };
function decodeSpec(s: Spec): unknown {
  switch (s.t) {
    case "json": return s.v;
    case "bigint": return BigInt(s.v);
    case "bytes": return new Uint8Array(s.v);
    case "date": return new Date(s.v);
    case "float": return Number(s.v);
    case "list": return s.v.map(decodeSpec);
    case "obj": return Object.fromEntries(Object.entries(s.v).map(([k, x]) => [k, decodeSpec(x as Spec)]));
  }
  throw new Error(s.t);
}

// deterministic PRNG for property tests
function rng(seed: number) {
  let s = seed >>> 0;
  return () => {
    s = (s * 1664525 + 1013904223) >>> 0;
    return s / 2 ** 32;
  };
}

function randomValue(r: () => number, depth = 0): unknown {
  const k = Math.floor(r() * (depth > 3 ? 7 : 10));
  switch (k) {
    case 0: return null;
    case 1: return r() < 0.5;
    case 2: return Math.floor((r() - 0.5) * 1e9) / (r() < 0.5 ? 1 : 1000);
    case 3: return Array.from({ length: Math.floor(r() * 12) }, () => String.fromCharCode(32 + Math.floor(r() * 90000) % 55000)).join("");
    case 4: return BigInt(Math.floor(r() * 1e15)) * (r() < 0.5 ? -1n : 1n) * 1000003n;
    case 5: return new Uint8Array(Array.from({ length: Math.floor(r() * 40) }, () => Math.floor(r() * 256)));
    case 6: return new Date(Math.floor(r() * 4e12));
    case 7: return Array.from({ length: Math.floor(r() * 5) }, () => randomValue(r, depth + 1));
    default: {
      const o: Record<string, unknown> = {};
      const n = Math.floor(r() * 5);
      for (let i = 0; i < n; i++) {
        const key = r() < 0.1 ? ["$bigint", "$bytes", "$date", "$escape", "$undefined"][Math.floor(r() * 5)] : `k${Math.floor(r() * 100)}`;
        o[key] = randomValue(r, depth + 1);
      }
      return o;
    }
  }
}

function normalize(v: unknown): unknown {
  if (v instanceof Uint8Array) return { bytes: [...v] };
  if (v instanceof Date) return { date: v.toISOString() };
  if (typeof v === "bigint") return { big: v.toString() };
  if (Array.isArray(v)) return v.map(normalize);
  if (v && typeof v === "object") {
    const o: Record<string, unknown> = {};
    for (const k of Object.keys(v).sort()) o[k] = normalize((v as Record<string, unknown>)[k]);
    return o;
  }
  return v;
}

describe("DJSON serialization", () => {
  test("round-trips 5000 random durable values exactly", () => {
    const r = rng(42);
    for (let i = 0; i < 5000; i++) {
      const v = randomValue(r);
      const s = serialize(v);
      assert.deepEqual(normalize(deserialize(s)), normalize(v), s);
    }
  });

  test("encoding is canonical: key order and object identity never change bytes", () => {
    const r = rng(7);
    for (let i = 0; i < 1000; i++) {
      const v = randomValue(r);
      const again = deserialize(serialize(v));
      assert.equal(serialize(again), serialize(v));
    }
    assert.equal(serialize({ b: 1, a: 2 }), serialize({ a: 2, b: 1 }));
    assert.equal(serialize({ a: 1, u: undefined }), '{"a":1}');
    assert.equal(serialize(-0), "0");
  });

  test("tagged values", () => {
    assert.equal(serialize(10n), '{"$bigint":"10"}');
    assert.equal(serialize(new Uint8Array([1, 2, 3])), '{"$bytes":"AQID"}');
    assert.equal(serialize(new Date("2020-01-01T00:00:00Z")), '{"$date":"2020-01-01T00:00:00.000Z"}');
    assert.equal(serialize(undefined), '{"$undefined":true}');
    assert.equal(serialize({ $bigint: "x" }), '{"$escape":{"$bigint":"x"}}');
    assert.deepEqual(deserialize('{"$escape":{"$bigint":"x"}}'), { $bigint: "x" });
    assert.deepEqual(deserialize(serialize([undefined, null])), [undefined, null]);
  });

  test("non-durable values are rejected with a path", () => {
    class Socket {}
    const cases: [unknown, RegExp][] = [
      [() => 1, /functions are not durable/],
      [Symbol("x"), /symbols/],
      [NaN, /non-finite/],
      [Infinity, /non-finite/],
      [new Socket(), /non-serializable Socket instance/],
      [Promise.resolve(1), /running Promise/],
      [new Map(), /non-serializable Map/],
      [{ a: { b: [1, new Set()] } }, /a\.b\[1\]/],
      [new Date(NaN), /invalid Date/],
    ];
    for (const [v, re] of cases) assert.throws(() => serialize(v), (e: Error) => e instanceof SerializationError && re.test(e.message));
    const cyc: Record<string, unknown> = {};
    cyc.self = cyc;
    assert.throws(() => toTagged(cyc), /cyclic/);
    // shared (non-cyclic) references are fine
    const shared = { x: 1 };
    assert.equal(serialize({ a: shared, b: shared }), '{"a":{"x":1},"b":{"x":1}}');
    // null-prototype objects are plain
    const np = Object.create(null);
    np.k = 1;
    assert.equal(serialize(np), '{"k":1}');
  });

  test("base64 matches Node's implementation", () => {
    const r = rng(99);
    for (let i = 0; i < 500; i++) {
      const bytes = new Uint8Array(Array.from({ length: Math.floor(r() * 70) }, () => Math.floor(r() * 256)));
      const b = bytesToBase64(bytes);
      assert.equal(b, Buffer.from(bytes).toString("base64"));
      assert.deepEqual([...base64ToBytes(b)], [...bytes]);
    }
    assert.throws(() => base64ToBytes("!!!"), /invalid base64/);
  });

  test("shared cross-language vectors (TypeScript is the reference)", () => {
    const vectors = JSON.parse(readFileSync(VECTORS, "utf8")) as { value: Spec; expect?: string; error?: string }[];
    assert.ok(vectors.some((v) => v.error), "rejection vectors present");
    assert.ok(vectors.some((v) => v.expect && /[\u{10000}-\u{10FFFF}]/u.test(v.expect)), "non-BMP vectors present");
    for (const v of vectors) {
      const value = decodeSpec(v.value);
      if (v.error) {
        assert.throws(() => serialize(value), (e: Error) => e instanceof SerializationError && e.message.includes(v.error!), JSON.stringify(v));
        continue;
      }
      assert.equal(serialize(value), v.expect, JSON.stringify(v));
      assert.equal(serialize(deserialize(v.expect!)), v.expect);
    }
  });

  test("keys sort by UTF-16 code units (not code points)", () => {
    // U+1F600 is D83D DE00 in UTF-16, which sorts before U+FB01
    assert.equal(serialize({ "\ufb01": 1, "😀": 2, "\ue000": 3 }), '{"😀":2,"\ue000":3,"ﬁ":1}');
  });

  test("tag escaping is decided on the keys actually emitted", () => {
    const s = serialize({ $date: "2020-01-01T00:00:00.000Z", x: undefined });
    assert.equal(s, '{"$escape":{"$date":"2020-01-01T00:00:00.000Z"}}');
    const back = deserialize(s);
    assert.ok(!(back instanceof Date));
    assert.deepEqual(back, { $date: "2020-01-01T00:00:00.000Z" });
    assert.equal(serialize({ $bigint: "1", u: undefined }), '{"$escape":{"$bigint":"1"}}');
    assert.equal(serialize({ $bigint: "1", n: null }), '{"$bigint":"1","n":null}');
  });

  test("strings PostgreSQL jsonb cannot store are rejected with a path", () => {
    const cases: [unknown, RegExp][] = [
      ["a\u0000b", /string contains U\+0000.* at <root>/],
      [{ a: ["ok", "\ud800"] }, /string contains a lone surrogate.* at a\[1\]/],
      [{ a: "x\udc00y" }, /lone surrogate.* at a$/],
      [{ a: "\ud83d" }, /lone surrogate/],
      [{ ["k\u0000"]: 1 }, /object key contains U\+0000/],
      [{ ["\udfff"]: 1 }, /object key contains a lone surrogate/],
    ];
    for (const [v, re] of cases) assert.throws(() => serialize(v), (e: Error) => e instanceof SerializationError && re.test(e.message), String(re));
    assert.equal(serialize("😀\u0001"), '"😀\\u0001"');
  });

  test("sparse arrays are rejected", () => {
    // eslint-disable-next-line no-sparse-arrays
    assert.throws(() => serialize([1, , 3]), (e: Error) => e instanceof SerializationError && /sparse array.* at \[1\]/.test(e.message));
    assert.throws(() => serialize({ a: new Array(2) }), /sparse array.* at a\[0\]/);
    assert.equal(serialize([undefined, 1]), '[{"$undefined":true},1]');
  });

  test("__proto__ keys are kept as own data and never touch prototypes", () => {
    const own = JSON.parse('{"__proto__":{"polluted":true},"b":1}');
    const s = serialize(own);
    assert.equal(s, '{"__proto__":{"polluted":true},"b":1}');
    const back = deserialize(s) as Record<string, unknown>;
    assert.equal(Object.getPrototypeOf(back), Object.prototype);
    assert.deepEqual(Object.keys(back), ["__proto__", "b"]);
    assert.equal(({} as Record<string, unknown>).polluted, undefined);
    assert.equal((back as { polluted?: unknown }).polluted, undefined);
    const esc = fromTagged({ $escape: { __proto__: 1 } }) as object;
    assert.equal(Object.getPrototypeOf(esc), Object.prototype);
    assert.equal(serialize(deserialize('{"$escape":{"__proto__":{"$bigint":"5"}}}')), '{"__proto__":{"$bigint":"5"}}');
    assert.equal(serialize(toTagged(own)), s);
  });

  test("canonicalJson sorts keys recursively", () => {
    assert.equal(canonicalJson({ z: [{ b: 1, a: 2 }], a: null }), '{"a":null,"z":[{"a":2,"b":1}]}');
  });
});
