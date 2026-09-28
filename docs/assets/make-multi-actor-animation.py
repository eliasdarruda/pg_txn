#!/usr/bin/env python3
"""Generates docs/assets/multi-actor-transfer.svg: a looping, icon-based
animation of the same two-account transfer (read A and B, call a slow fraud
check, move 30 from A to B) done in plain PostgreSQL and with pg_txn, side by
side and in lockstep.

  plain PostgreSQL   BEGIN; SELECT ... FOR UPDATE on both rows, then the HTTP
                     call inside the open transaction: a second transfer that
                     locks in the opposite order deadlocks, the locks are held
                     for the whole call, a deposit blocks until lock_timeout,
                     and every waiting session holds a transaction open.
  pg_txn             a SQL schema plus a library in the app, on stock
                     PostgreSQL. Run 1 owns account A and B together (all or
                     nothing, a claim, not a lock) in a short DB transaction,
                     reaches the fraud check and rolls back: the call runs
                     from the app with no transaction, row lock or connection
                     held. T2 finds the rows owned and waits outside any
                     transaction, so nothing can deadlock; a plain UPDATE on
                     A fails at once with 55P03. The result is recorded, run 2
                     writes both balances and commits atomically in one DB
                     transaction, then T2 runs.

Pure SVG + SMIL (no scripts), so it animates when embedded in a GitHub README
(<img>), and follows the viewer's light/dark preference. Same visual language
as make-readme-animation.py.

    python3 docs/assets/make-multi-actor-animation.py
"""
from pathlib import Path
import math

DUR = 21.4          # seconds per loop
FADE = 0.2
W, H = 1000, 788
PW, PH = 485, 758   # panel width and height
TRAVEL = 0.85       # a packet's travel time

# step slots (seconds), the same on both sides
S = [0.5, 2.6, 4.7, 7.8, 10.1, 12.4, 14.7, 17.4]
END = 20.8          # everything fades out, then the loop restarts
DL = S[2] + 1.9     # plain PostgreSQL: the deadlock detector aborts T2
LT = S[5] + 0.9     # plain PostgreSQL: the deposit hits lock_timeout
RET = S[6] + TRAVEL           # the fraud check's answer is back in the app
CM = RET + TRAVEL             # plain PostgreSQL: the final writes commit
ACQ = S[7] + 0.35             # plain PostgreSQL: T2 gets both locks
REC = RET + TRAVEL            # pg_txn: the fraud check's result is recorded
CMT = REC + TRAVEL            # pg_txn: run 2 commits both balances
T2S = S[7] + 0.25             # pg_txn: T2 is woken up and runs
OWN2 = T2S + TRAVEL / 2       # pg_txn: T2 owns both accounts


def arr(t):
    return t + TRAVEL


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


def dot(a, b, t0, cls, bounce=False, r=6.5, t1=None, hold=None):
    """a packet travelling from a to b starting at t0 (a round trip if bounce);
    with hold, it stays at b until then (a session waiting there)."""
    (x1, y1), (x2, y2) = a, b
    t1 = t1 or t0 + TRAVEL
    if bounce:
        mid = (t0 + t1) / 2
        motion = (f'<animateMotion dur="{DUR}s" repeatCount="indefinite" path="M{x1},{y1} L{x2},{y2}" '
                  f'keyPoints="0;0;0.8;0;0" keyTimes="{kt([0, t0, mid, t1, DUR])}" calcMode="linear"/>')
    else:
        motion = (f'<animateMotion dur="{DUR}s" repeatCount="indefinite" path="M{x1},{y1} L{x2},{y2}" '
                  f'keyPoints="0;0;1;1" keyTimes="{kt([0, t0, t1, DUR])}" calcMode="linear"/>')
    gone = hold if hold else t1
    return (f'<circle r="{r}" class="{cls}" opacity="0">{motion}'
            f'<animate attributeName="opacity" dur="{DUR}s" repeatCount="indefinite" values="0;0;1;1;0;0" '
            f'keyTimes="{kt([0, t0 - 0.05, t0, gone, gone + 0.12, DUR])}"/></circle>')


def text(x, y, s, cls="", anchor="middle", extra=""):
    return f'<text x="{x}" y="{y}" class="{cls}" text-anchor="{anchor}" {extra}>{s}</text>'


def link(a, b, cls="link"):
    return f'<line x1="{a[0]}" y1="{a[1]}" x2="{b[0]}" y2="{b[1]}" class="{cls}"/>'


def hot(a, b, cls, t0):
    """an edge lit up from the moment it is first used."""
    return f'<line x1="{a[0]}" y1="{a[1]}" x2="{b[0]}" y2="{b[1]}" class="hot {cls}" opacity="0">{visible(t0, END)}</line>'


def marker(a, b, n, cls, t0, side=1, frac=0.5, off=14):
    """the step's number, next to its edge, from the moment the step starts."""
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
  <rect x="-38" y="-26" width="76" height="52" rx="7" class="card"/>
  <rect x="-38" y="-26" width="76" height="12" rx="7" class="bar"/>
  <circle cx="-29" cy="-20" r="2.2" class="dotc"/><circle cx="-22" cy="-20" r="2.2" class="dotc"/>
  {text(0, 12, "&lt;/&gt;", "mono big")}
  {text(0, 44, label, "label")}
</g>'''


def icon_store(x, y, label, rx=46, h=68):
    """a cylinder: PostgreSQL."""
    ry = rx * 13 / 46
    lh = h / 2 + ry + 17
    return f'''<g transform="translate({x},{y})">
  <path d="M{-rx},{-h / 2} v{h} a{rx},{ry} 0 0 0 {2 * rx},0 v{-h}" class="card"/>
  <ellipse cx="0" cy="{-h / 2}" rx="{rx}" ry="{ry}" class="bar"/>
  <path d="M{-rx},{-h / 2 + h * 0.38:.1f} a{rx},{ry} 0 0 0 {2 * rx},0" class="rim"/>
  <path d="M{-rx},{-h / 2 + h * 0.74:.1f} a{rx},{ry} 0 0 0 {2 * rx},0" class="rim"/>
  {text(0, lh, label, "label")}
</g>'''


def icon_api(x, y, label):
    return f'''<g transform="translate({x},{y})">
  <rect x="-40" y="-26" width="80" height="52" rx="8" class="card"/>
  <rect x="-40" y="-14" width="80" height="10" class="bar"/>
  <rect x="-30" y="6" width="26" height="8" rx="2" class="chipc"/>
  {text(0, 46, label, "label")}
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


def card(x, y, w, lines, cls, t0, t1):
    """a multi-line badge."""
    h = len(lines) * 17 + 10
    out = [f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" class="{cls}"/>']
    out += [text(x + w / 2, y + 22 + i * 17, ln, "badge") for i, ln in enumerate(lines)]
    return f'<g opacity="0">{visible(t0, t1)}{"".join(out)}</g>'


def badge(x, y, s, cls, t0, t1, w=None):
    w = w or (len(s) * 7.2 + 20)
    return (f'<g opacity="0">{visible(t0, t1)}<rect x="{x - w / 2}" y="{y - 13}" width="{w}" height="24" rx="12" class="{cls}"/>'
            f'{text(x, y + 4, s, "badge")}</g>')


def lock(x, y, cls, t0, t1):
    """a row lock, in the colour of the transaction holding it."""
    return (f'<g opacity="0" transform="translate({x},{y})">{visible(t0, t1)}'
            f'<path d="M-6,-3 v-5 a6,6 0 0 1 12,0 v5" class="lockarc {cls}"/>'
            f'<rect x="-9" y="-3" width="18" height="14" rx="3" class="lockbody {cls}"/></g>')


def crown(x, y, cls, t0, t1):
    """ownership of an actor, in the colour of the owning transaction."""
    return (f'<g opacity="0" transform="translate({x},{y})">{visible(t0, t1)}'
            f'<path d="M-10,7 L-10,-5 L-5,1 L0,-9 L5,1 L10,-5 L10,7 Z" class="crownc {cls}"/></g>')


def owner(x, y, s, cls, t0, t1):
    return f'<g opacity="0">{visible(t0, t1)}{text(x, y + 4, s, f"owner {cls}", "start")}</g>'


def caption(x, y, s, t0, t1, cls="cap", num=None, numcls=""):
    """a step caption, left-aligned, led by the same numbered marker as its edge."""
    mark = ""
    if num:
        mark = f'<circle cx="{x + 10}" cy="{y - 5}" r="9.5" class="{numcls}"/>{text(x + 10, y - 1, num, "num")}'
        x += 26
    lines = "".join(text(x, y + i * 18, ln, cls, "start") for i, ln in enumerate(s.split("\n")))
    return f'<g opacity="0">{visible(t0, t1)}{mark}{lines}</g>'


def counter(x, y, values, cls, label):
    """a running count: [(value, t0, t1)]."""
    out = [text(x, y + 18, label, "tiny", "end")]
    for v, t0, t1 in values:
        out.append(f'<g opacity="0">{visible(t0, t1)}{text(x, y, v, f"count {cls}", "end")}</g>')
    return "".join(out)


def small_counter(x, y, label, values, cls):
    out = [text(x - 16, y, label, "tiny", "end")]
    for v, t0, t1 in values:
        out.append(f'<g opacity="0">{visible(t0, t1)}{text(x, y, v, f"scount {cls}", "end")}</g>')
    return "".join(out)


def row_bg(x, y, w, label, t0=0, t1=END):
    return (f'<g opacity="0">{visible(t0, t1)}<rect x="{x - w / 2}" y="{y - 14}" width="{w}" height="26" rx="6" class="row"/>'
            f'{text(x - w / 2 + 10, y + 4, label, "mono small", "start")}</g>')


def lane(x, y, w, label, who, states):
    """a session / transaction lane: a coloured id, a label and a status chip
    that changes: [(label, class, t0, t1)]."""
    first = states[0][2]
    parts = [f'<g opacity="0">{visible(first, END)}<rect x="{x - w / 2}" y="{y - 14}" width="{w}" height="26" rx="6" class="row"/>'
             f'<circle cx="{x - w / 2 + 16}" cy="{y - 1}" r="9" class="{who}"/>'
             f'{text(x - w / 2 + 16, y + 3, label[0], "num")}'
             f'{text(x - w / 2 + 32, y + 4, label[1], "mono small", "start")}</g>']
    for s, cls, t0, t1 in states:
        cw = len(s) * 7.0 + 22
        parts.append(badge(x + w / 2 - cw / 2 - 6, y, s, cls, t0, t1, cw))
    return "".join(parts)


def code_block(y, lines, marks, cls):
    """a code listing with the currently running lines highlighted:
    marks = [(first line, last line, t0, t1)]."""
    lh = 13
    h = CODEN * lh + 14
    p = [f'<rect x="20" y="{y}" width="{PW - 40}" height="{h}" rx="6" class="file"/>']
    for a, b, t0, t1 in marks:
        p.append(f'<rect x="24" y="{y + 5 + a * lh}" width="{PW - 48}" height="{(b - a + 1) * lh + 2}" rx="3" '
                 f'class="hl {cls}" opacity="0">{visible(t0, t1)}</rect>')
    for i, ln in enumerate(lines):
        p.append(text(30, y + 17 + i * lh, ln, "mono code", "start", 'xml:space="preserve"'))
    return "".join(p)


# ---------------------------------------------------------------- shared layout

FRAUD = (400, 150)
PG = (240, 322)
OTHER = (72, 322)
ROWX, ROWW = 386, 172          # the accounts table, right of PostgreSQL
ROWA, ROWB = 302, 334
LANEX, LANEW = PW / 2, PW - 40
LANES = (438, 468, 498)
CAPY = 540
CODEY = 584
CODEN = 8                      # lines in the taller listing
SUMY = 726
BODY_DY = -18                  # the body sits a little closer to the header


def header(p, title, cls, sub):
    p += [f'<rect x="0" y="0" width="{PW}" height="{PH}" rx="14" class="panel"/>',
          text(24, 38, title, f"h {cls}", "start"),
          text(24, 60, sub, "sub", "start")]


def panel(x, p, top):
    """the panel frame and title (p[:3]) and the top counter stay put; the body moves up by BODY_DY."""
    return (f'<g transform="translate({x},20)">{"".join(p[:3])}{top}'
            f'<g transform="translate(0,{BODY_DY})">{"".join(p[3:])}</g></g>')


def accounts(p, cm):
    """the two rows, their balances (100 / 50 -> 70 / 80) and the conserved total."""
    p.append(row_bg(ROWX, ROWA, ROWW, "account A"))
    p.append(row_bg(ROWX, ROWB, ROWW, "account B"))
    bx = ROWX + ROWW / 2 - 25
    p.append(badge(bx, ROWA, "100", "chip-neutral", 0, cm, 38))
    p.append(badge(bx, ROWA, "70", "chip-good", cm, END, 38))
    p.append(badge(bx, ROWB, "50", "chip-neutral", 0, cm, 38))
    p.append(badge(bx, ROWB, "80", "chip-good", cm, END, 38))
    p.append(f'<g opacity="0">{visible(0, END)}{text(ROWX + ROWW / 2 - 6, ROWB + 32, "total: 150", "tiny", "end")}</g>')


def timer(p, cls, ok_until):
    """the fraud check: the same seconds on both sides."""
    x0, x1, y = 186, 336, 212
    p.append(f'<g opacity="0">{visible(S[3], RET)}'
             f'{text(x0, y - 8, "fraud check: waiting…", "tiny", "start")}'
             f'<rect x="{x0}" y="{y}" width="{x1 - x0}" height="8" rx="4" class="track"/>'
             f'<rect x="{x0}" y="{y}" width="0" height="8" rx="4" class="fill {cls}">'
             f'<animate attributeName="width" dur="{DUR}s" repeatCount="indefinite" values="0;0;{x1 - x0};{x1 - x0}" '
             f'keyTimes="{kt([0, S[3] + TRAVEL, S[6], DUR])}"/></rect></g>')
    p.append(badge(FRAUD[0], FRAUD[1] - 44, "ok", "chip-good", S[6], ok_until, 40))


# ---------------------------------------------------------------- plain PostgreSQL

def panel_plain():
    app = (80, 150)
    app_b, app_r = (80, 176), (118, 150)
    pg_top, pg_l = (224, 290), (194, 322)
    other_r = (110, 322)
    p = []
    header(p, "Plain PostgreSQL", "o", "FOR UPDATE on both rows, then the HTTP call")
    p += [link(app_b, pg_top), link(app_r, FRAUD), link(other_r, pg_l)]
    p.append(hot(app_b, pg_top, "o", S[0]))
    p.append(hot(app_r, FRAUD, "o", S[3]))
    p.append(hot(other_r, pg_l, "w", S[4]))
    p.append(icon_app(*app, "App"))
    p.append(icon_api(*FRAUD, "Fraud check API"))
    p.append(icon_store(*PG, "PostgreSQL"))
    p.append(icon_app(*OTHER, "another client"))
    accounts(p, CM)
    timer(p, "o", CM + 0.4)

    # row locks, in the colour of the holder
    lx, tx = ROWX + 6, ROWX + 19
    p.append(lock(lx, ROWA - 3, "o", arr(S[0]), CM))
    p.append(owner(tx, ROWA, "T1", "o", arr(S[0]), CM))
    p.append(lock(lx, ROWB - 3, "p", arr(S[1]), DL))
    p.append(owner(tx, ROWB, "T2", "p", arr(S[1]), DL))
    p.append(lock(lx, ROWB - 3, "o", DL + 0.3, CM))
    p.append(owner(tx, ROWB, "T1", "o", DL + 0.3, CM))
    p.append(lock(lx, ROWA - 3, "p", ACQ, END))
    p.append(owner(tx, ROWA, "T2", "p", ACQ, END))
    p.append(lock(lx, ROWB - 3, "p", ACQ, END))
    p.append(owner(tx, ROWB, "T2", "p", ACQ, END))

    p.append(badge(320, 250, "T1 waits for T2, T2 waits for T1", "chip-warn", arr(S[2] + 0.1), DL, 236))
    p.append(badge(320, 250, "ERROR: deadlock detected", "chip-bad", DL, S[3], 200))
    p.append(badge(320, 250, "T1 idle in transaction · 2 row locks", "chip-warn", S[3] + 0.4, CM, 262))

    # step markers on the edges
    p.append(marker(app_b, pg_top, "1", "numo", S[0], 1, 0.5))
    p.append(marker(app_r, FRAUD, "4", "numo", S[3], 1, 0.5))
    p.append(marker(other_r, pg_l, "5", "numw", S[4], 1, 0.5))

    # packets
    p.append(dot(app_b, pg_top, S[0], "do"))                               # T1: FOR UPDATE A
    p.append(dot(app_b, pg_top, S[1], "dp"))                               # T2: FOR UPDATE B
    p.append(dot(app_b, pg_top, S[2], "do", hold=DL + 0.3))                # T1: FOR UPDATE B, waits
    p.append(dot(app_b, pg_top, S[2] + 0.1, "dp", hold=DL))                # T2: FOR UPDATE A, waits
    p.append(dot(app_r, FRAUD, S[3], "do", hold=S[6]))                     # the HTTP call
    p.append(dot(FRAUD, app_r, S[6], "do"))                                # its answer
    p.append(dot(app_b, pg_top, RET, "do"))                                # UPDATE A, B; COMMIT
    p.append(dot(other_r, pg_l, S[4], "dn", hold=LT))                      # the deposit, waiting
    p.append(dot(app_b, pg_top, S[4] + 0.9, "dp", hold=ACQ))               # T2 retried, waiting
    p.append(dot(app_r, FRAUD, ACQ + 0.4, "dp", hold=END))                 # T2's own slow call

    top = counter(PW - 24, 44, [("0", 0, arr(S[0])), ("1", arr(S[0]), arr(S[1])), ("2", arr(S[1]), DL),
                                   ("1", DL, DL + 0.3), ("2", DL + 0.3, CM), ("0", CM, ACQ), ("2", ACQ, END)],
                     "o", "row locks held")

    # sessions: every one of them keeps a transaction (and a connection) open
    p.append(text(24, 412, "sessions", "tiny", "start"))
    p.append(small_counter(PW - 24, 412, "open transactions / pinned connections",
                           [("0", 0, arr(S[0])), ("1", arr(S[0]), arr(S[1])), ("2", arr(S[1]), DL),
                            ("1", DL, arr(S[4])), ("2", arr(S[4]), arr(S[4] + 0.9)), ("3", arr(S[4] + 0.9), LT),
                            ("2", LT, CM), ("1", CM, END)], "o"))
    p.append(lane(LANEX, LANES[0], LANEW, ("1", "T1 transfer A→B"), "numo", [
        ("locks A", "chip-o", arr(S[0]), arr(S[2])),
        ("waits for B", "chip-warn", arr(S[2]), DL + 0.3),
        ("locks A, B", "chip-o", DL + 0.3, S[3]),
        ("idle in transaction", "chip-warn", S[3], CM),
        ("committed", "chip-good", CM, END)]))
    p.append(lane(LANEX, LANES[1], LANEW, ("2", "T2 transfer B→A"), "nump", [
        ("locks B", "chip-p", arr(S[1]), arr(S[2] + 0.1)),
        ("waits for A", "chip-warn", arr(S[2] + 0.1), DL),
        ("deadlock: aborted", "chip-bad", DL, arr(S[4] + 0.9)),
        ("retried: waits for B", "chip-warn", arr(S[4] + 0.9), ACQ),
        ("locks B, A", "chip-p", ACQ, END)]))
    p.append(lane(LANEX, LANES[2], LANEW, ("D", "UPDATE A (deposit)"), "numn", [
        ("waits for A", "chip-warn", arr(S[4]), LT),
        ("lock timeout", "chip-bad", LT, END)]))

    caps = [  # (slot, number, text, class)
        (0, "1", "T1: BEGIN; SELECT … FOR UPDATE locks account A", "cap"),
        (1, "2", "T2 (B→A) locks in the opposite order: B first", "cap"),
        (2, "3", "each waits for the other's row:\ndeadlock detected, T2 is aborted", "cap warn"),
        (3, "4", "T1 locks B, then calls the fraud check,\nholding both row locks for seconds", "cap"),
        (4, "5", "a deposit to A blocks, T2's retry blocks:\ntransactions and connections pile up", "cap warn"),
        (5, "6", "the provider is slow: the deposit hits lock_timeout", "cap warn"),
        (6, "7", "ok: T1 updates A and B, commits, frees the locks", "cap"),
        (7, "8", "T2 takes both locks, for its own slow call", "cap"),
    ]
    for i, (s, n, c, cls) in enumerate(caps):
        t1 = S[caps[i + 1][0]] if i + 1 < len(caps) else END
        p.append(caption(24, CAPY, c, S[s], t1, cls, n, "numw" if "warn" in cls else "numo"))

    p.append(f'<line x1="24" y1="570" x2="{PW - 24}" y2="570" class="rule"/>')
    code = ["BEGIN;",
            "SELECT * FROM accounts WHERE id = 'A' FOR UPDATE;",
            "SELECT * FROM accounts WHERE id = 'B' FOR UPDATE;",
            "-- POST /fraud-check: seconds, minutes, locks held",
            "UPDATE accounts SET balance = balance - 30 WHERE id = 'A';",
            "UPDATE accounts SET balance = balance + 30 WHERE id = 'B';",
            "COMMIT;"]
    p.append(code_block(CODEY, code, [(0, 1, S[0], S[2]), (2, 2, S[2], S[3]), (3, 3, S[3], RET),
                                      (4, 6, RET, S[7])], "o"))
    for i, ln in enumerate(["locks held across the network call", "blocked writers · pinned connections",
                            "deadlock"]):
        p.append(text(24, SUMY + i * 18, ln, "sum bad", "start"))
    return panel(10, p, top)


# ---------------------------------------------------------------- pg_txn

def panel_pgtxn():
    app = (100, 150)
    app_b, lib_r = (100, 198), (150, 173)
    pg_top, pg_l = (224, 290), (194, 322)
    other_r = (110, 322)
    fraud_l = (FRAUD[0] - 40, 173)
    p = []
    header(p, "pg_txn", "t", "owns both rows, holds no locks or connections")
    p += [link(app_b, pg_top), link(lib_r, fraud_l), link(other_r, pg_l)]
    p.append(hot(app_b, pg_top, "t", S[0]))
    p.append(hot(lib_r, fraud_l, "t", S[3]))
    p.append(hot(other_r, pg_l, "w", S[4]))
    p.append(icon_app_with_lib(*app))
    p.append(icon_api(*FRAUD, "Fraud check API"))
    p.append(icon_store(*PG, "stock PostgreSQL"))
    p.append(icon_app(*OTHER, "another client"))
    accounts(p, CMT)
    timer(p, "t", CMT + 0.4)

    # ownership: both rows claimed at once in run 1, kept across the rollback, released at the commit
    own1 = S[0] + TRAVEL / 2
    cx, tx = ROWX + 6, ROWX + 19
    for y in (ROWA, ROWB):
        p.append(crown(cx, y - 1, "t", own1, CMT))
        p.append(owner(tx, y, "T1", "t", own1, CMT))
        p.append(crown(cx, y - 1, "p", OWN2, END))
        p.append(owner(tx, y, "T2", "p", OWN2, END))

    p.append(badge(331, 250, "T2 waits outside any transaction", "chip-p", S[2] + TRAVEL / 2, S[3], 240))
    p.append(card(200, 226, 262, ["open transactions: 0 · row locks: 0", "connections held: 0"],
                  "chip-good", S[3] + 0.4, RET))
    p.append(badge(OTHER[0] + 6, OTHER[1] - 44, "55P03 at once", "chip-bad", S[4] + 0.45, S[6], 116))

    p.append(marker(app_b, pg_top, "1", "numt", S[0], 1, 0.5))
    p.append(marker(lib_r, fraud_l, "4", "numt", S[3], -1, 0.5))
    p.append(marker(other_r, pg_l, "5", "numw", S[4], 1, 0.5))

    p.append(dot(app_b, pg_top, S[0], "dt", bounce=True))                  # run 1: own A, own B
    p.append(dot(app_b, pg_top, S[1], "dt"))                               # the effect: ROLLBACK
    p.append(dot(app_b, pg_top, S[2], "dp", bounce=True))                  # T2: owned, so it waits
    p.append(dot(lib_r, fraud_l, S[3], "dt", hold=S[6]))                   # the effect, from the app
    p.append(dot(fraud_l, lib_r, S[6], "dt"))                              # its result
    p.append(dot(app_b, pg_top, RET, "dt"))                                # the result is recorded
    p.append(dot(app_b, pg_top, REC, "dt"))                                # run 2: both writes, COMMIT
    p.append(dot(other_r, pg_l, S[4], "dwarn", bounce=True, t1=S[4] + 0.6))  # UPDATE A: 55P03
    p.append(dot(app_b, pg_top, T2S, "dp", bounce=True))                   # T2's run 1 owns A and B
    p.append(dot(lib_r, fraud_l, T2S + 1.0, "dp", hold=END))              # T2's own effect

    # a DB transaction is open only for the short runs; run 2's UPDATEs lock the rows until its COMMIT
    top = counter(PW - 24, 44, [("0", 0, REC + 0.3), ("2", REC + 0.3, CMT), ("0", CMT, END)], "t", "row locks held")

    p.append(text(24, 412, "transactions", "tiny", "start"))
    p.append(small_counter(PW - 24, 412, "open transactions / pinned connections",
                           [("0", 0, S[0]), ("1", S[0], arr(S[1])), ("0", arr(S[1]), S[2]),
                            ("1", S[2], arr(S[2])), ("0", arr(S[2]), RET), ("1", RET, CMT),
                            ("0", CMT, T2S), ("1", T2S, arr(T2S)), ("0", arr(T2S), END)], "t"))
    p.append(lane(LANEX, LANES[0], LANEW, ("1", "T1 transfer A→B"), "numt", [
        ("run 1: owns A, B", "chip-t", own1, arr(S[1])),
        ("rolled back · owns A, B", "chip-t", arr(S[1]), S[3]),
        ("effect: nothing held", "chip-t", S[3], RET),
        ("run 2: one commit", "chip-t", RET, CMT),
        ("committed", "chip-good", CMT, END)]))
    p.append(lane(LANEX, LANES[1], LANEW, ("2", "T2 transfer B→A"), "nump", [
        ("waits outside any transaction", "chip-neutral", S[2] + TRAVEL / 2, OWN2),
        ("owns A, B", "chip-p", OWN2, END)]))
    p.append(lane(LANEX, LANES[2], LANEW, ("D", "UPDATE A (deposit)"), "numn", [
        ("fails fast: 55P03", "chip-bad", S[4] + 0.45, END)]))

    caps = [
        (0, "1", "run 1: T1 owns A and B together, all or nothing:\ncrowns, not locks", "cap"),
        (1, "2", "T1 reaches the fraud check: ROLLBACK.\nthe claims stay, nothing is held", "cap"),
        (2, "3", "T2 (B→A) finds A and B owned: it waits outside\nany transaction, so no deadlock", "cap"),
        (3, "4", "the fraud check is called from the app:\n0 transactions, 0 row locks, 0 connections", "cap"),
        (4, "5", "a plain UPDATE on account A fails at once: 55P03", "cap warn"),
        (5, "6", "slow provider: A and B stay owned, still 0 locks", "cap"),
        (6, "7", "result recorded; run 2 writes both balances\nand commits atomically in one DB transaction", "cap good"),
        (7, "8", "A and B are released: T2 owns them and runs", "cap good"),
    ]
    for i, (s, n, c, cls) in enumerate(caps):
        t1 = S[caps[i + 1][0]] if i + 1 < len(caps) else END
        p.append(caption(24, CAPY, c, S[s], t1, cls, n, "numw" if "warn" in cls else "numt"))

    p.append(f'<line x1="24" y1="570" x2="{PW - 24}" y2="570" class="rule"/>')
    code = ["await pgtxn.transaction(async (tx) =&gt; {",
            "  const a = await tx.own(\"accounts\", \"A\")",
            "  const b = await tx.own(\"accounts\", \"B\")",
            "  const verdict = await tx.effect(() =&gt; fraudCheck(a, b, 30))",
            "  if (verdict.blocked) throw new Error(\"transfer refused\")",
            "  await tx.db.query(\"UPDATE accounts SET balance=balance-30 WHERE id='A'\")",
            "  await tx.db.query(\"UPDATE accounts SET balance=balance+30 WHERE id='B'\")",
            "})"]
    p.append(code_block(CODEY, code, [(0, 2, S[0], S[1]), (3, 3, S[1], RET), (1, 7, RET, S[7])], "t"))
    for i, ln in enumerate(["owned across the call · nothing held in Postgres",
                            "SQL fails fast (55P03) · T2 waits, never deadlocks",
                            "both balances commit in one DB transaction"]):
        p.append(text(24, SUMY + i * 18, ln, "sum good", "start"))
    return panel(PW + 20, p, top)


STYLE = """
:root { --bg:#F3F6F5; --panel:#FFFFFF; --ink:#16262B; --muted:#5B6E72; --line:#CBD6D4; --soft:#EDF2F1;
  --o:#B8690B; --t:#0B7A70; --p:#5B5BC4; --bad:#C23B2E; --warn:#B8690B; --good:#0B7A70; }
@media (prefers-color-scheme: dark) { :root { --bg:#0E1517; --panel:#152023; --ink:#E3ECEA; --muted:#93A6A8; --line:#2E4246; --soft:#101A1C;
  --o:#E39A3B; --t:#3CC2B3; --p:#9D9DF2; --bad:#F07565; --warn:#E39A3B; --good:#3CC2B3; } }
.bg { fill: var(--bg); } .panel { fill: var(--panel); stroke: var(--line); }
text { font-family: system-ui, -apple-system, "Segoe UI", Helvetica, Arial, sans-serif; fill: var(--ink); }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
.h { font-size: 22px; font-weight: 700; } .h.o { fill: var(--o); } .h.t { fill: var(--t); }
.sub { font-size: 13px; fill: var(--muted); } .label.plain { stroke: none; } .label { font-size: 12px; fill: var(--muted); font-weight: 600; paint-order: stroke; stroke: var(--panel); stroke-width: 5px; stroke-linejoin: round; }
.big { font-size: 17px; font-weight: 700; } .small { font-size: 11.5px; } .code { font-size: 9.6px; } .tiny { font-size: 12px; fill: var(--muted); }
.cap { font-size: 13.5px; font-weight: 600; } .bad { fill: var(--bad); } .good { fill: var(--good); } .warn { fill: var(--warn); }
.sum { font-size: 14px; font-weight: 700; }
.count { font-size: 30px; font-weight: 800; } .count.o { fill: var(--o); } .count.t { fill: var(--t); }
.scount { font-size: 15px; font-weight: 800; } .scount.o { fill: var(--o); } .scount.t { fill: var(--t); }
.badge { font-size: 11.5px; font-weight: 700; fill: #fff; }
.num { font-size: 10.5px; font-weight: 800; fill: #fff; }
.numo { fill: var(--o); } .numt { fill: var(--t); } .numw { fill: var(--bad); } .nump { fill: var(--p); } .numn { fill: var(--muted); }
.owner { font-size: 11px; font-weight: 800; } .owner.o { fill: var(--o); } .owner.t { fill: var(--t); } .owner.p { fill: var(--p); }
.card { fill: var(--panel); stroke: var(--ink); stroke-width: 2; } .bar { fill: var(--soft); stroke: var(--ink); stroke-width: 2; }
.rim { fill: none; stroke: var(--ink); stroke-width: 1.5; opacity: .5; } .dotc { fill: var(--muted); }
.chipc { fill: var(--muted); }
.libchip { fill: var(--soft); stroke: var(--t); stroke-width: 2; }
.libt { font-size: 12.5px; font-weight: 700; fill: var(--t); } .libc rect { fill: var(--t); }
.link { stroke: var(--line); stroke-width: 2; stroke-dasharray: 5 6; }
.hot { stroke-width: 2.5; } .hot.o { stroke: var(--o); } .hot.t { stroke: var(--t); } .hot.w { stroke: var(--bad); stroke-dasharray: 5 5; }
.row { fill: var(--soft); stroke: var(--line); }
.chip-neutral { fill: var(--muted); } .chip-warn { fill: var(--warn); } .chip-bad { fill: var(--bad); } .chip-good { fill: var(--good); }
.chip-o { fill: var(--o); } .chip-t { fill: var(--t); } .chip-p { fill: var(--p); }
.lockbody.o { fill: var(--o); } .lockbody.p { fill: var(--p); } .lockarc { fill: none; stroke-width: 2.4; }
.lockarc.o { stroke: var(--o); } .lockarc.p { stroke: var(--p); }
.crownc.t { fill: var(--t); } .crownc.p { fill: var(--p); }
.do { fill: var(--o); } .dt { fill: var(--t); } .dp { fill: var(--p); } .dn { fill: var(--muted); } .dwarn { fill: var(--bad); }
.track { fill: var(--soft); stroke: var(--line); } .fill.o { fill: var(--o); } .fill.t { fill: var(--t); }
.rule { stroke: var(--line); } .file { fill: var(--soft); stroke: var(--line); }
.hl { stroke: none; fill-opacity: .2; } .hl.o { fill: var(--o); } .hl.t { fill: var(--t); }
"""


def main():
    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img"
  aria-label="Animation: a transfer of 30 from account A to account B that reads both accounts, calls a slow fraud check and then moves the money, in plain PostgreSQL and with pg_txn. Plain PostgreSQL: T1 locks A with SELECT FOR UPDATE while a second transfer T2 from B to A locks B; each waits for the other's row and T2 is aborted with deadlock detected. T1 then holds both row locks, idle in transaction, for the whole fraud check; a deposit to A blocks until lock_timeout and T2's retry blocks too, each holding a transaction and a connection open. T1 commits and T2 takes both locks for its own slow call. pg_txn, a SQL schema plus a library in the app on stock PostgreSQL: run 1 owns A and B together, all or nothing, without row locks, reaches the fraud check and rolls back; T2 finds both rows owned and waits outside any transaction, so there is no deadlock; the fraud check runs from the app with 0 open transactions, 0 row locks and 0 connections held; a plain UPDATE on account A fails at once with SQLSTATE 55P03; the result is recorded and run 2 writes both balances, A 100 to 70 and B 50 to 80, committing atomically in one DB transaction; then T2 owns both accounts and runs.">
<title>Two-account transfer: plain PostgreSQL vs pg_txn</title>
<style>{STYLE}</style>
<rect width="{W}" height="{H}" class="bg"/>
{panel_plain()}
{panel_pgtxn()}
</svg>
'''
    out = Path(__file__).with_name("multi-actor-transfer.svg")
    out.write_text(svg)
    print(f"wrote {out} ({len(svg)} bytes)")


if __name__ == "__main__":
    main()
