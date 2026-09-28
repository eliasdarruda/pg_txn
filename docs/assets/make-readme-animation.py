#!/usr/bin/env python3
"""Generates docs/assets/outbox-vs-pgtxn.svg: a looping, icon-based animation
of the same checkout done with a transactional outbox and with pg_txn.

Every network hop the data makes is an animated packet, numbered on its edge,
and a counter on each side ticks up with the hops:

  outbox   API service -> PostgreSQL -> relay -> broker -> worker -> Redis,
           Payments API, PostgreSQL again, and on failure the retry / DLQ
           topic and a DLQ handler: 10 hops, 5 services + Redis
  pg_txn   a SQL schema plus a library in the app, on stock PostgreSQL. The
           transaction takes key order:42 in txn.start (a row in txn.keys,
           not a lock), then run 1 reads order 42 in a short DB transaction,
           reaches the effect and rolls back (no lock, transaction or
           connection held); the app charges the Payments API with the effect
           id as idempotency key and records the result; run 2 writes 'paid',
           spawns the receipt, releases the key and commits atomically; the
           app's in-process worker sends the receipt: 6 hops, 1 service

Pure SVG + SMIL (no scripts), so it animates when embedded in a GitHub README
(<img>), and follows the viewer's light/dark preference.

    python3 docs/assets/make-readme-animation.py
"""
from pathlib import Path
import math

DUR = 17.2          # seconds per loop
FADE = 0.2
W, H = 1000, 800
PH = 740            # panel height
TOP = 44            # the panels sit below the one-line header
CAPY, RULEY, CODEY, SUMY = 532, 562, 596, 720   # captions, rule, code, summary (both panels)

# hop slots (seconds): slot i starts at T0 + i * STEP, a packet travels for TRAVEL
T0, STEP, TRAVEL = 0.5, 1.3, 0.85
END = 16.6          # everything fades out, then the loop restarts


def slot(i):
    return T0 + i * STEP


def arrive(i):
    return slot(i) + TRAVEL


def kt(ts):
    return ";".join(f"{max(0.0, min(1.0, t / DUR)):.4f}" for t in ts)


def visible(t0, t1):
    """opacity animation: hidden, fade in at t0, fade out at t1 (loops)."""
    pts = [(0, 0)]
    if t0 > 0:
        pts += [(t0, 0)]
    pts += [(min(t0 + FADE, DUR), 1)]
    if t1 < DUR:
        pts += [(t1, 1), (min(t1 + FADE, DUR), 0)]
    pts += [(DUR, pts[-1][1])]
    return (f'<animate attributeName="opacity" dur="{DUR}s" repeatCount="indefinite" '
            f'values="{";".join(str(v) for _, v in pts)}" keyTimes="{kt([t for t, _ in pts])}"/>')


def dot(a, b, t0, cls, bounce=False, r=6.5, t1=None):
    """a packet travelling from a to b starting at t0 (a round trip if bounce)."""
    (x1, y1), (x2, y2) = a, b
    t1 = t1 or t0 + TRAVEL
    if bounce:
        mid = (t0 + t1) / 2
        motion = (f'<animateMotion dur="{DUR}s" repeatCount="indefinite" path="M{x1},{y1} L{x2},{y2}" '
                  f'keyPoints="0;0;0.8;0;0" keyTimes="{kt([0, t0, mid, t1, DUR])}" calcMode="linear"/>')
    else:
        motion = (f'<animateMotion dur="{DUR}s" repeatCount="indefinite" path="M{x1},{y1} L{x2},{y2}" '
                  f'keyPoints="0;0;1;1" keyTimes="{kt([0, t0, t1, DUR])}" calcMode="linear"/>')
    return (f'<circle r="{r}" class="{cls}" opacity="0">{motion}'
            f'<animate attributeName="opacity" dur="{DUR}s" repeatCount="indefinite" values="0;0;1;1;0;0" '
            f'keyTimes="{kt([0, t0 - 0.05, t0, t1, t1 + 0.12, DUR])}"/></circle>')


def text(x, y, s, cls="", anchor="middle", extra=""):
    return f'<text x="{x}" y="{y}" class="{cls}" text-anchor="{anchor}" {extra}>{s}</text>'


def link(a, b, cls="link"):
    return f'<line x1="{a[0]}" y1="{a[1]}" x2="{b[0]}" y2="{b[1]}" class="{cls}"/>'


def hot(a, b, cls, t0):
    """an edge lit up from the moment its hop starts."""
    return f'<line x1="{a[0]}" y1="{a[1]}" x2="{b[0]}" y2="{b[1]}" class="hot {cls}" opacity="0">{visible(t0, END)}</line>'


def hop_marker(a, b, n, cls, t0, side=1, frac=0.5, off=14):
    """the hop's number, next to its edge, from the moment the hop starts."""
    (x1, y1), (x2, y2) = a, b
    dx, dy = x2 - x1, y2 - y1
    ln = math.hypot(dx, dy)
    nx, ny = -dy / ln * side, dx / ln * side
    x, y = x1 + dx * frac + nx * off, y1 + dy * frac + ny * off
    return (f'<g opacity="0">{visible(t0, END)}<circle cx="{x:.1f}" cy="{y:.1f}" r="9.5" class="{cls}"/>'
            f'{text(f"{x:.1f}", f"{y + 4:.1f}", n, "num")}</g>')


# ---------------------------------------------------------------- icons

def icon_app(x, y, label):
    return f'''<g transform="translate({x},{y})">
  <rect x="-38" y="-28" width="76" height="52" rx="7" class="card"/>
  <rect x="-38" y="-28" width="76" height="12" rx="7" class="bar"/>
  <circle cx="-29" cy="-22" r="2.2" class="dotc"/><circle cx="-22" cy="-22" r="2.2" class="dotc"/>
  {text(0, 10, "&lt;/&gt;", "mono big")}
  {text(0, 44, label, "label")}
</g>'''


def gear(r, spin=True):
    """a gear of outer radius ~r centred on the origin."""
    k = r / 31
    teeth = "".join(f'<rect x="{-5 * k:.1f}" y="{-31 * k:.1f}" width="{10 * k:.1f}" height="{12 * k:.1f}" rx="{2 * k:.1f}" '
                    f'class="gearc" transform="rotate({a})"/>' for a in range(0, 360, 45))
    anim = (f'<animateTransform attributeName="transform" type="rotate" from="0" to="360" dur="{DUR}s" '
            f'repeatCount="indefinite"/>') if spin else ""
    return f'<g>{teeth}<circle r="{22 * k:.1f}" class="gearc"/><circle r="{9 * k:.1f}" class="hole"/>{anim}</g>'


def icon_gear(x, y, label):
    return f'''<g transform="translate({x},{y})">{gear(31)}
  {text(0, 50, label, "label")}
</g>'''


def icon_store(x, y, label, rx=46, h=68, lh=None):
    """a cylinder: PostgreSQL, or a smaller one for Redis."""
    ry = rx * 13 / 46
    lh = lh if lh is not None else h / 2 + ry + 17
    return f'''<g transform="translate({x},{y})">
  <path d="M{-rx},{-h / 2} v{h} a{rx},{ry} 0 0 0 {2 * rx},0 v{-h}" class="card"/>
  <ellipse cx="0" cy="{-h / 2}" rx="{rx}" ry="{ry}" class="bar"/>
  <path d="M{-rx},{-h / 2 + h * 0.38:.1f} a{rx},{ry} 0 0 0 {2 * rx},0" class="rim"/>
  <path d="M{-rx},{-h / 2 + h * 0.74:.1f} a{rx},{ry} 0 0 0 {2 * rx},0" class="rim"/>
  {text(0, lh, label, "label")}
</g>'''


def icon_broker(x, y, label):
    """a queue: a tube of messages."""
    msgs = "".join(f'<rect x="{-30 + i * 16}" y="-9" width="11" height="18" rx="2" class="chipc"/>' for i in range(4))
    return f'''<g transform="translate({x},{y})">
  <rect x="-42" y="-22" width="84" height="44" rx="22" class="card"/>
  {msgs}
  {text(0, 44, label, "label")}
</g>'''


def icon_api(x, y):
    return f'''<g transform="translate({x},{y})">
  <rect x="-40" y="-26" width="80" height="52" rx="8" class="card"/>
  <rect x="-40" y="-14" width="80" height="10" class="bar"/>
  <rect x="-30" y="6" width="26" height="8" rx="2" class="chipc"/>
  {text(0, 46, "Payments API", "label")}
</g>'''


def books():
    """a small library glyph: three book spines, the last one leaning."""
    return ('<g class="libc"><rect x="-9" y="-8" width="4.6" height="16" rx="1"/>'
            '<rect x="-3.4" y="-8" width="4.6" height="16" rx="1"/>'
            '<rect x="2.6" y="-7.4" width="4.6" height="15" rx="1" transform="rotate(-16 4.9 7.6)"/></g>')


def icon_app_with_lib(x, y):
    """the application process, with the pg_txn library inside it."""
    return f'''<g transform="translate({x},{y})">
  <rect x="-70" y="-48" width="140" height="96" rx="9" class="card"/>
  <path d="M-70,-30 v-9 a9,9 0 0 1 9,-9 h122 a9,9 0 0 1 9,9 v9 z" class="bar"/>
  <circle cx="-60" cy="-39" r="2.2" class="dotc"/><circle cx="-53" cy="-39" r="2.2" class="dotc"/>
  {text(6, -35, "App process", "label plain")}
  {text(0, -6, "&lt;/&gt;", "mono big")}
  <rect x="-50" y="8" width="100" height="30" rx="15" class="libchip"/>
  <g transform="translate(-30,23)">{books()}</g>
  {text(10, 28, "pg_txn", "libt")}
</g>'''


def icon_mail(x, y, label):
    """an API that sends email: a card with an envelope flap."""
    return f'''<g transform="translate({x},{y})">
  <rect x="-40" y="-26" width="80" height="52" rx="8" class="card"/>
  <path d="M-38,-22 L0,6 L38,-22" class="flap"/>
  {text(0, 46, label, "label")}
</g>'''


def key_glyph(x, y, cls, t0, t1):
    """a transaction key: a row in txn.keys, held until the commit; no lock."""
    return (f'<g opacity="0" transform="translate({x},{y})">{visible(t0, t1)}'
            f'<circle cx="-5" cy="0" r="4.5" class="keyc {cls}"/>'
            f'<path d="M-0.5,0 H10 M6,0 v4 M9,0 v3" class="keyc {cls}"/></g>')


def card(x, y, w, lines, cls, t0, t1):
    """a multi-line badge."""
    h = len(lines) * 17 + 10
    out = [f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" class="{cls}"/>']
    out += [text(x + w / 2, y + 22 + i * 17, ln, "badge") for i, ln in enumerate(lines)]
    return f'<g opacity="0">{visible(t0, t1)}{"".join(out)}</g>'


def badge(x, y, s, cls, t0, t1, w=None):
    w = w or (len(s) * 7.2 + 20)
    return (f'<g opacity="0">{visible(t0, t1)}<rect x="{x - w / 2}" y="{y - 13}" width="{w}" height="24" rx="12" class="{cls}"/>'
            f'{text(x, y + 4, s, "badge")}</g>')


def lock(x, y, t0, t1):
    return (f'<g opacity="0" transform="translate({x},{y})">{visible(t0, t1)}'
            f'<path d="M-6,-3 v-5 a6,6 0 0 1 12,0 v5" class="lockarc"/>'
            f'<rect x="-9" y="-3" width="18" height="14" rx="3" class="lockbody"/></g>')


def caption(x, y, s, t0, t1, cls="cap", num=None, numcls=""):
    """a step caption, left-aligned, led by the same numbered marker as its edge."""
    mark = ""
    if num:
        mark = f'<circle cx="{x + 10}" cy="{y - 5}" r="9.5" class="{numcls}"/>{text(x + 10, y - 1, num, "num")}'
        x += 26
    lines = "".join(text(x, y + i * 18, ln, cls, "start") for i, ln in enumerate(s.split("\n")))
    return f'<g opacity="0">{visible(t0, t1)}{mark}{lines}</g>'


def counter(x, y, hops, cls):
    """the running hop count: [(value, t0, t1)]."""
    out = [text(x, y + 18, "network hops", "tiny", "end")]
    for v, t0, t1 in hops:
        out.append(f'<g opacity="0">{visible(t0, t1)}{text(x, y, v, f"count {cls}", "end")}</g>')
    return "".join(out)


def row(x, y, w, label, states):
    """a table row with a status chip that changes: [(label, class, t0, t1, chip width)]."""
    first = states[0][2]
    parts = [f'<g opacity="0">{visible(first, END)}<rect x="{x - w / 2}" y="{y - 14}" width="{w}" height="26" rx="6" class="row"/>'
             f'{text(x - w / 2 + 10, y + 4, label, "mono small", "start")}</g>']
    for s, cls, t0, t1, cw in states:
        parts.append(badge(x + w / 2 - cw / 2 - 6, y, s, cls, t0, t1, cw))
    return "".join(parts)


def chips(x0, y, items, cls):
    parts, x = [], x0
    for label, t0, t1 in items:
        w = len(label) * 6.9 + 22
        parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="26" rx="6" class="file"/>')
        parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="26" rx="6" class="file-on {cls}" opacity="0">{visible(t0, t1)}</rect>')
        parts.append(text(x + w / 2, y + 17, label, "mono small"))
        x += w + 8
    return "".join(parts)


# ---------------------------------------------------------------- outbox

def panel_outbox():
    pw = 610
    app, relay, broker, worker, redis = (60, 150), (190, 150), (315, 150), (440, 150), (555, 150)
    pg, dlq, api = (120, 335), (315, 335), (490, 335)
    p = [f'<rect x="0" y="0" width="{pw}" height="{PH}" rx="14" class="panel"/>',
         text(24, 38, "Transactional outbox", "h o", "start"),
         text(24, 60, "an API service, a relay, a broker, a worker, a DLQ handler and Redis", "sub", "start")]

    edges = [(app, pg), (pg, relay), (relay, broker), (broker, worker), (worker, redis),
             (worker, api), (worker, pg)]
    p += [link(a, b) for a, b in edges]
    p.append(link(broker, dlq, "link warnl"))

    hops = [  # (n, a, b, slot, bounce, marker side, marker frac)
        ("1", app, pg, 0, False, 1, 0.5),         # order + outbox row, one transaction
        ("2", relay, pg, 1, True, -1, 0.34),      # relay polls the outbox table
        ("3", relay, broker, 2, False, -1, 0.5),  # publish
        ("4", relay, pg, 3, False, -1, 0.66),     # mark the outbox row sent
        ("5", broker, worker, 4, False, -1, 0.5),  # consume
        ("6", worker, redis, 5, True, -1, 0.5),   # dedup check
        ("7", worker, api, 6, True, -1, 0.5),     # charge, idempotency key it manages
        ("8", worker, pg, 7, False, 1, 0.42),     # write the result back, version check
    ]
    p += [hot(a, b, "o", slot(s)) for _, a, b, s, *_ in hops]
    p.append(hot(worker, broker, "w", slot(9)))
    p.append(hot(broker, dlq, "w", slot(10)))

    p.append(icon_app(*app, "API service"))
    p.append(icon_store(*pg, "PostgreSQL"))
    p.append(icon_gear(*relay, "Relay / CDC"))
    p.append(icon_broker(*broker, "Broker"))
    p.append(icon_gear(*worker, "Worker"))
    p.append(icon_store(*redis, "Redis (dedup)", rx=24, h=40, lh=44))
    p.append(icon_gear(*dlq, "Retry / DLQ handler"))
    p.append(icon_api(*api))

    # rows inside PostgreSQL
    rx = pg[0]
    p.append(row(rx, 426, 172, "order 42", [("new", "chip-neutral", arrive(0), arrive(7), 50),
                                            ("paid", "chip-good", arrive(7), END, 50)]))
    p.append(row(rx, 456, 172, "outbox #17", [("pending", "chip-neutral", arrive(0), arrive(3), 70),
                                              ("sent", "chip-good", arrive(3), END, 70)]))

    # the hops, in order
    for n, a, b, s, bounce, side, frac in hops:
        p.append(hop_marker(a, b, n, "numo", slot(s), side, frac))
    for n, a, b, s, bounce, side, frac in hops:
        p.append(dot(a, b, slot(s), "do", bounce))

    # on failure instead: the charge times out, the message goes to the retry / DLQ topic
    p.append(dot(worker, api, slot(8), "dwarn", True))
    p.append(badge(552, 282, "timeout", "chip-warn", slot(8) + 0.4, END, 70))
    p.append(hop_marker(worker, broker, "9", "numw", slot(9), -1, 0.5))
    p.append(dot(worker, broker, slot(9), "dwarn"))
    p.append(hop_marker(broker, dlq, "10", "numw", slot(10), -1, 0.72))
    p.append(dot(broker, dlq, slot(10), "dwarn"))
    p.append(badge(worker[0], 96, "idempotency key: yours to manage", "chip-warn", slot(6), slot(8), 236))
    p.append(badge(248, 270, "version check", "chip-warn", slot(7) + 0.3, slot(8), 112))
    p.append(badge(402, 300, "dead letter", "chip-bad", arrive(10), END, 96))

    p.append(counter(pw - 24, 44, [("0", 0, slot(0))] + [(str(i + 1), slot(i), slot(i + 1)) for i in range(7)]
                     + [("8", slot(7), slot(9)), ("9", slot(9), slot(10)), ("10", slot(10), END)], "o"))

    # captions
    caps = [  # (slot, hop number, text)
        (0, "1", "the API writes the order and an outbox row in one transaction"),
        (1, "2", "the relay polls the outbox table (or CDC tails the WAL)"),
        (2, "3", "the relay publishes OrderPlaced to the broker (Kafka, SQS)"),
        (3, "4", "the relay marks the outbox row sent"),
        (4, "5", "the worker service consumes the message"),
        (5, "6", "the worker asks Redis: already processed?"),
        (6, "7", "the worker charges, with an idempotency key it manages"),
        (7, "8", "the worker writes 'paid' back, guarded by a version check"),
        (8, None, "if the charge fails instead: timeout"),
        (9, "9", "the worker nacks: the message goes to the retry / DLQ topic"),
        (10, "10", "a DLQ handler consumes it: re-publish later, or page someone"),
    ]
    for i, (s, n, c) in enumerate(caps):
        t1 = slot(caps[i + 1][0]) if i + 1 < len(caps) else END
        bad = s >= 8
        p.append(caption(24, CAPY, c, slot(s), t1, "cap warn" if bad else "cap", n, "numw" if bad else "numo"))

    # locality of behaviour: where the logic of this checkout lives
    p.append(f'<line x1="24" y1="{RULEY}" x2="{pw - 24}" y2="{RULEY}" class="rule"/>')
    p.append(text(24, RULEY + 22, "your code for this checkout", "tiny", "start"))
    p.append(chips(24, CODEY, [("checkout.ts", slot(0), slot(1)), ("relay.ts", slot(1), slot(4)),
                             ("worker.ts", slot(4), slot(10)), ("dlq-handler.ts", slot(10), END)], "o"))
    p.append(text(24, CODEY + 56, "+ an outbox table, topics, a DLQ and Redis keys to keep in step", "tiny", "start"))
    p.append(text(24, CODEY + 76, "+ a relay, a broker and a worker service to deploy, scale and watch", "tiny", "start"))
    p.append(text(24, SUMY, "10 hops · 5 services + Redis · your code in 4 places", "sum bad", "start"))
    return f'<g transform="translate(10,{TOP})">{"".join(p)}</g>'


# ---------------------------------------------------------------- pg_txn

def panel_pgtxn():
    pw = 360
    app, api, mail, pg = (95, 160), (275, 140), (275, 252), (95, 340)
    app_b, pg_top = (95, 208), (95, 306)          # the app's bottom edge, the top of PostgreSQL
    app_pay, api_l = (165, 140), (235, 140)       # the app's right edge, towards Payments
    app_mail, mail_l = (165, 196), (235, 252)     # ... and towards the Email API
    p = [f'<rect x="0" y="0" width="{pw}" height="{PH}" rx="14" class="panel"/>',
         text(24, 38, "pg_txn", "h t", "start"),
         text(24, 60, "SQL schema + in-app library", "sub", "start")]
    p += [link(app_b, pg_top), link(app_pay, api_l), link(app_mail, mail_l)]
    hops = [  # (n, from, to, slot, bounce, marker side, marker frac)
        ("1", app_b, pg_top, 0, True, 1, 0.3),    # txn.start takes key order:42; run 1 reads order 42
        ("2", app_b, pg_top, 1, False, -1, 0.3),  # the effect has not run: ROLLBACK
        ("3", app_pay, api_l, 2, True, 1, 0.5),   # charge, effect id = idempotency key
        ("4", app_b, pg_top, 3, False, -1, 0.72), # record the result
        ("5", app_b, pg_top, 4, False, 1, 0.72),  # run 2: 'paid' + spawn + COMMIT
        ("6", app_mail, mail_l, 5, True, 1, 0.5), # after the commit: the receipt
    ]
    p += [hot(a, b, "t", slot(s)) for n, a, b, s, *_ in hops]
    p.append(icon_app_with_lib(*app))
    p.append(icon_api(*api))
    p.append(icon_mail(*mail, "Email API"))
    p.append(icon_store(*pg, "stock PostgreSQL"))
    p.append(text(pg[0], pg[1] - 4, "txn schema", "mono schema"))

    # what is not there
    p.append(text(214, 406, "not needed:", "tiny", "start"))
    for i, s_ in enumerate(["a relay", "a broker", "a worker service", "a dedup store", "a DLQ handler",
                            "a Postgres extension"]):
        p.append(text(214, 424 + i * 16, s_, "tiny gone", "start"))

    # rows inside PostgreSQL: the order is held under key order:42 (a row in txn.keys, not a lock)
    # from txn.start to the commit
    rx, rw = 110, 172
    p.append(row(rx, 432, rw, "order 42", [("new", "chip-neutral", arrive(0), arrive(4), 50),
                                           ("paid", "chip-good", arrive(4), END, 50)]))
    p.append(key_glyph(rx + 6, 431, "t", slot(0) + 0.3, arrive(4)))
    p.append(row(rx, 462, rw, "effect #1", [("recorded", "chip-good", arrive(3), END, 74)]))
    p.append(row(rx, 492, rw, "receipt", [("committed", "chip-good", arrive(4), END, 80)]))

    for n, a, b, s, bounce, side, frac in hops:
        p.append(hop_marker(a, b, n, "numt", slot(s), side, frac))
    for n, a, b, s, bounce, side, frac in hops:
        p.append(dot(a, b, slot(s), "dt", bounce))

    p.append(badge(app[0], 92, "run 1", "chip-t", slot(0), arrive(1), 58))
    p.append(badge(app[0], 92, "run 2", "chip-t", slot(4), arrive(4) + 0.3, 58))
    p.append(badge(api[0], 92, "key = effect id", "chip-good", slot(2), slot(4), 120))
    p.append(badge(172, 266, "key order:42", "chip-t", slot(0) + 0.3, arrive(4), 100))
    zero = ["0 open transactions", "0 row locks", "0 connections held"]
    p.append(card(203, 314, 146, zero, "chip-good", arrive(1), slot(3)))
    p.append(badge(mail[0], 318, "receipt sent", "chip-good", arrive(5), slot(8), 104))

    # on failure instead: the charge times out, the app retries with the same key,
    # and the retry is recorded in PostgreSQL
    p.append(dot(app_pay, api_l, slot(8), "dwarn", True))
    p.append(badge(api[0], 92, "timeout", "chip-warn", slot(8) + 0.4, slot(10), 70))
    p.append(badge(172, 266, "key order:42", "chip-t", slot(8) + 0.3, END, 100))
    p.append(card(203, 314, 146, zero, "chip-good", slot(8) + 0.3, slot(9)))
    p.append(dot(app_b, pg_top, slot(9), "dwarn"))
    p.append(badge(262, 350, "retry kept in Postgres", "chip-warn", arrive(9), END, 166))
    p.append(dot(app_pay, api_l, slot(10), "dt", True))
    p.append(badge(api[0], 92, "same key", "chip-good", slot(10), END, 84))

    p.append(counter(pw - 24, 44, [("0", 0, slot(0))] + [(str(i + 1), slot(i), slot(i + 1)) for i in range(5)]
                     + [("6", slot(5), END)], "t"))

    caps = [  # (slot, hop number, text)
        (0, "1", "key order:42 taken; run 1, a short\nDB transaction: read order 42"),
        (1, "2", "the charge hasn't run yet: ROLLBACK.\nnothing is held while it runs"),
        (2, "3", "the app charges, keyed by the effect id"),
        (3, "4", "the result is recorded in Postgres"),
        (4, "5", "run 2: 'paid' + spawn + COMMIT,\nkey released, in one DB transaction"),
        (5, "6", "after the commit, the app's own\nworker sends the receipt"),
        (6, None, "done: one function, nothing else runs"),
        (8, None, "if the charge fails instead: timeout"),
        (9, "4", "the failure is recorded in Postgres,\nwith the retry"),
        (10, "3", "the app retries with the same key:\ncharged once"),
    ]
    for i, (s, n, c) in enumerate(caps):
        t1 = slot(caps[i + 1][0]) if i + 1 < len(caps) else END
        cls = "cap warn" if s in (8, 9) else ("cap good" if s in (6, 10) else "cap")
        p.append(caption(24, CAPY, c, slot(s), t1, cls, n, "numw" if s in (8, 9) else "numt"))

    p.append(f'<line x1="24" y1="{RULEY}" x2="{pw - 24}" y2="{RULEY}" class="rule"/>')
    p.append(text(24, RULEY + 22, "your code for this checkout: checkout.ts", "tiny", "start"))
    code = ["await pgtxn.transaction(async (tx) =&gt; {",
            "  const [order] = await tx.db.select()…",
            "  const p = await tx.effect(() =&gt; charge(order))",
            "  await tx.db.update(orders)…",
            "  await tx.spawn(() =&gt; sendReceipt(id))",
            "}, { key: [\"order\", id] })"]
    lh, h = 14, 6 * 14 + 14
    p.append(f'<rect x="20" y="{CODEY}" width="{pw - 40}" height="{h}" rx="6" class="file"/>')
    p.append(f'<rect x="20" y="{CODEY}" width="{pw - 40}" height="{h}" rx="6" class="file-on t" opacity="0">'
             f'{visible(slot(0), END)}</rect>')
    marks = [(0, 1, slot(0), slot(1)), (5, 5, slot(0), slot(1)), (2, 2, slot(1), slot(4)), (0, 5, slot(4), slot(5)),
             (4, 4, slot(5), slot(6)), (2, 2, slot(8), END)]
    for a, b, t0, t1 in marks:
        p.append(f'<rect x="24" y="{CODEY + 6 + a * lh}" width="{pw - 48}" height="{(b - a + 1) * lh + 1}" rx="3" '
                 f'class="hl t" opacity="0">{visible(t0, t1)}</rect>')
    for i, line in enumerate(code):
        p.append(text(28, CODEY + 17 + i * lh, line, "mono code", "start", 'xml:space="preserve"'))
    p.append(text(24, SUMY, "6 hops · 1 service · one function", "sum good", "start"))
    return f'<g transform="translate(630,{TOP})">{"".join(p)}</g>'


STYLE = """
:root { --bg:#F3F6F5; --panel:#FFFFFF; --ink:#16262B; --muted:#5B6E72; --line:#CBD6D4; --soft:#EDF2F1;
  --o:#B8690B; --t:#0B7A70; --bad:#C23B2E; --warn:#B8690B; --good:#0B7A70; }
@media (prefers-color-scheme: dark) { :root { --bg:#0E1517; --panel:#152023; --ink:#E3ECEA; --muted:#93A6A8; --line:#2E4246; --soft:#101A1C;
  --o:#E39A3B; --t:#3CC2B3; --bad:#F07565; --warn:#E39A3B; --good:#3CC2B3; } }
.bg { fill: var(--bg); } .panel { fill: var(--panel); stroke: var(--line); }
text { font-family: system-ui, -apple-system, "Segoe UI", Helvetica, Arial, sans-serif; fill: var(--ink); }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
.h { font-size: 22px; font-weight: 700; } .h.o { fill: var(--o); } .h.t { fill: var(--t); }
.sub { font-size: 13px; fill: var(--muted); } .label.plain { stroke: none; } .label { font-size: 12px; fill: var(--muted); font-weight: 600; paint-order: stroke; stroke: var(--panel); stroke-width: 5px; stroke-linejoin: round; }
.big { font-size: 17px; font-weight: 700; } .small { font-size: 11.5px; } .code { font-size: 10.5px; } .tiny { font-size: 12px; fill: var(--muted); }
.gone { text-decoration: line-through; opacity: .8; }
.cap { font-size: 13.5px; font-weight: 600; } .bad { fill: var(--bad); } .good { fill: var(--good); } .warn { fill: var(--warn); }
.sum { font-size: 14px; font-weight: 700; }
.count { font-size: 30px; font-weight: 800; } .count.o { fill: var(--o); } .count.t { fill: var(--t); }
.badge { font-size: 11.5px; font-weight: 700; fill: #fff; }
.num { font-size: 10.5px; font-weight: 800; fill: #fff; }
.numo { fill: var(--o); } .numt { fill: var(--t); } .numw { fill: var(--bad); }
.card { fill: var(--panel); stroke: var(--ink); stroke-width: 2; } .bar { fill: var(--soft); stroke: var(--ink); stroke-width: 2; }
.rim { fill: none; stroke: var(--ink); stroke-width: 1.5; opacity: .5; } .dotc { fill: var(--muted); }
.gearc { fill: var(--muted); } .hole { fill: var(--panel); } .chipc { fill: var(--muted); }
.libchip { fill: var(--soft); stroke: var(--t); stroke-width: 2; }
.libt { font-size: 12.5px; font-weight: 700; fill: var(--t); } .libc rect { fill: var(--t); }
.flap { fill: none; stroke: var(--ink); stroke-width: 2; stroke-linejoin: round; }
.schema { font-size: 10.5px; fill: var(--muted); font-weight: 600; }
.keyc { fill: none; stroke-width: 2.4; stroke-linecap: round; } .keyc.t { stroke: var(--t); } .chip-t { fill: var(--t); }
.hl { stroke: none; fill-opacity: .2; } .hl.t { fill: var(--t); }
.head { font-size: 13.5px; font-weight: 600; } .head .ht { fill: var(--t); font-weight: 800; }
.link { stroke: var(--line); stroke-width: 2; stroke-dasharray: 5 6; } .warnl { stroke-dasharray: 2 5; }
.hot { stroke-width: 2.5; } .hot.o { stroke: var(--o); } .hot.t { stroke: var(--t); } .hot.w { stroke: var(--bad); stroke-dasharray: 5 5; }
.row { fill: var(--soft); stroke: var(--line); }
.chip-neutral { fill: var(--muted); } .chip-warn { fill: var(--warn); } .chip-bad { fill: var(--bad); } .chip-good { fill: var(--good); }
.lockbody { fill: var(--t); } .lockarc { fill: none; stroke: var(--t); stroke-width: 2.4; }
.do { fill: var(--o); } .dt { fill: var(--t); } .dbad { fill: var(--bad); } .dwarn { fill: var(--bad); } .dgood { fill: var(--good); }
.rule { stroke: var(--line); } .file { fill: var(--soft); stroke: var(--line); }
.file-on { fill: none; stroke-width: 2.5; } .file-on.o { stroke: var(--o); } .file-on.t { stroke: var(--t); }
"""


HEADER = ('<text x="500" y="30" class="head" text-anchor="middle"><tspan class="ht">pg_txn</tspan>: the transactional outbox, '
          'local to your transaction, plus effects in the middle of it that hold no locks or connections</text>')


def main():
    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img"
  aria-label="Animation: the same checkout with a transactional outbox and with pg_txn. Outbox: the API service writes the order and an outbox row, a relay polls it, publishes to a broker and marks it sent, a worker consumes it, checks Redis for duplicates, charges the payment API and writes the result back with a version check, and on failure a retry / DLQ topic and a DLQ handler take over: 10 network hops across 5 services plus Redis. pg_txn, a SQL schema plus a library in the app on stock PostgreSQL: the transaction takes key order:42, a row in txn.keys rather than a lock, then run 1 reads order 42 in a short DB transaction, reaches the charge and rolls back, so no transaction, lock or connection is held while the app charges the payment API with the effect id as idempotency key; the result is recorded in PostgreSQL; run 2 writes paid, spawns the receipt, releases the key and commits atomically in one DB transaction; after the commit the app's in-process worker sends the receipt. On failure the app retries the charge with the same key and the retry is recorded in PostgreSQL: 6 hops, 1 service, one function.">
<title>Transactional outbox vs pg_txn</title>
<style>{STYLE}</style>
<rect width="{W}" height="{H}" class="bg"/>
{HEADER}
{panel_outbox()}
{panel_pgtxn()}
</svg>
'''
    out = Path(__file__).with_name("outbox-vs-pgtxn.svg")
    out.write_text(svg)
    print(f"wrote {out} ({len(svg)} bytes)")


if __name__ == "__main__":
    main()
