-- pg_txn: transactions that include side effects.
--
-- Pure SQL (plpgsql), no C and no preloaded library: install it with
-- CREATE EXTENSION pg_txn, run it as a migration, or let an SDK install it
-- on first use. Runs on any PostgreSQL 14+ (including managed services).
--
-- The protocol (SDKs in any language follow it; see docs/protocol.md):
--
--   A logical transaction is a function the application runs. Each run is
--   one ordinary, short PostgreSQL transaction ("attempt"):
--     txn.attempt(tx, owner)              marks the session as running tx
--     txn.effect_lookup(tx, seq, name, input)
--                                         memoized result of effect #seq, or 'missing'
--     txn.own(table, key)                 reads a row the transaction will depend on
--   When an effect is missing, the SDK rolls the attempt back (nothing is
--   held: no transaction, no lock, no connection) and, outside any
--   transaction:
--     txn.prepare_effects(...)            claims owned rows (all or nothing,
--                                         version-checked) and records intents
--     <calls the effect>                  in the application process
--     txn.effect_done(...)                records the result
--   then runs the function again: effects already done return their
--   recorded results. The run that reaches the end commits with
--     txn.finish(tx, owner, consumed)     orphaned effects -> compensation,
--                                         owned rows released
--   so every write, the spawned effects and the outcome commit atomically.
--
--   txn.spawn(owner, name, id)            an effect that runs iff the surrounding
--                                         transaction commits (built-in outbox),
--                                         run right after it by the process owner
--   txn.enqueue(name, input)              a named transaction that runs in the
--                                         background iff the surrounding commits

CREATE TABLE txn.meta (
    version integer NOT NULL
);
INSERT INTO txn.meta VALUES (1);

-- ---------------------------------------------------------------------------
-- durable state

CREATE TABLE txn.transactions (
    id          uuid PRIMARY KEY,
    name        text,                   -- NULL: inline (not resumable by another process)
    input       jsonb,                  -- named transactions: their input
    status      text NOT NULL DEFAULT 'running'
                CHECK (status IN ('running', 'committed', 'failed', 'abandoned')),
    owner       uuid,                   -- the process driving it (NULL: waiting for a worker)
    generation  bigint NOT NULL DEFAULT 0,
    lease_until timestamptz,
    runs        integer NOT NULL DEFAULT 0,
    output      jsonb,
    error       jsonb,
    created_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz
);
CREATE INDEX transactions_running ON txn.transactions (lease_until) WHERE status = 'running';

CREATE TABLE txn.effects (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tx_id            uuid REFERENCES txn.transactions ON DELETE CASCADE,
    kind             text NOT NULL CHECK (kind IN ('call', 'spawn', 'compensation')),
    seq              integer,                -- calls: position in the transaction
    name             text NOT NULL,
    input            jsonb NOT NULL DEFAULT '{}',
    input_hash       text,
    status           text NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'running', 'retry_wait', 'succeeded', 'failed', 'orphaned')),
    result           jsonb,
    error            jsonb,
    attempts         integer NOT NULL DEFAULT 0,
    -- nothing is called twice unless the application opts into retries
    max_attempts     integer NOT NULL DEFAULT 1 CHECK (max_attempts BETWEEN 1 AND 1000),
    delivery         text NOT NULL DEFAULT 'at-most-once' CHECK (delivery IN ('at-least-once', 'at-most-once')),
    compensation     text,                   -- calls: label of the compensation run if the effect is orphaned
    compensates      uuid,                   -- compensations: the orphaned call
    local_owner      uuid,                   -- spawns and compensations: the process that has their code
    next_attempt_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_owner      uuid,
    lease_generation bigint NOT NULL DEFAULT 0,
    lease_until      timestamptz,
    leased_at        timestamptz,
    created_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at     timestamptz
);
CREATE UNIQUE INDEX effects_call ON txn.effects (tx_id, seq, input_hash) WHERE kind = 'call';
CREATE INDEX effects_due ON txn.effects (local_owner, next_attempt_at) WHERE kind <> 'call' AND status IN ('pending', 'retry_wait');
CREATE INDEX effects_leased ON txn.effects (lease_until) WHERE kind <> 'call' AND status = 'running';
CREATE INDEX effects_tx ON txn.effects (tx_id);

CREATE TABLE txn.effect_attempts (
    id          bigserial PRIMARY KEY,
    effect_id   uuid NOT NULL REFERENCES txn.effects ON DELETE CASCADE,
    attempt     integer NOT NULL,
    outcome     text NOT NULL CHECK (outcome IN ('succeeded', 'retry', 'failed', 'lease_expired', 'stale', 'ambiguous')),
    error       jsonb,
    owner       uuid,
    started_at  timestamptz,
    finished_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX effect_attempts_effect ON txn.effect_attempts (effect_id);

-- rows owned by a running transaction: nobody else may change them
CREATE TABLE txn.owned_rows (
    rel    regclass NOT NULL,
    key    jsonb NOT NULL,
    tx_id  uuid NOT NULL REFERENCES txn.transactions ON DELETE CASCADE,
    PRIMARY KEY (rel, key)
);
CREATE INDEX owned_rows_tx ON txn.owned_rows (tx_id);

CREATE TABLE txn.guarded_tables (
    rel         regclass PRIMARY KEY,
    key_columns text[] NOT NULL
);

-- processes running SDK workers (for txn.doctor)
CREATE TABLE txn.workers (
    owner    uuid PRIMARY KEY,
    info     jsonb,
    seen_at  timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- ---------------------------------------------------------------------------
-- helpers

CREATE FUNCTION txn._now() RETURNS timestamptz LANGUAGE sql VOLATILE AS 'SELECT clock_timestamp()';

CREATE FUNCTION txn._hash(p_name text, p_input jsonb) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    -- jsonb has one canonical text form: the same input hashes the same in every language
    SELECT pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(p_name || ':' || p_input::text, 'UTF8')), 'hex')
$$;

CREATE FUNCTION txn._current() RETURNS uuid
LANGUAGE sql STABLE AS $$
    SELECT nullif(pg_catalog.current_setting('txn.current', true), '')::uuid
$$;

CREATE FUNCTION txn._backoff_ms(p_attempts integer) RETURNS integer
LANGUAGE sql IMMUTABLE AS $$
    SELECT least(60000, (200 * pg_catalog.power(2, greatest(0, p_attempts - 1)))::integer)
$$;

CREATE FUNCTION txn._pk(p_rel regclass) RETURNS text[]
LANGUAGE plpgsql STABLE AS $$
DECLARE
    cols text[];
BEGIN
    SELECT pg_catalog.array_agg(a.attname::text ORDER BY pg_catalog.array_position(i.indkey::int2[], a.attnum))
      INTO cols
      FROM pg_catalog.pg_index i
      JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
     WHERE i.indrelid = p_rel AND i.indisprimary;
    IF cols IS NULL THEN
        RAISE EXCEPTION 'pg_txn: table % has no primary key', p_rel USING ERRCODE = 'invalid_parameter_value';
    END IF;
    RETURN cols;
END $$;

-- The canonical key of a row: its primary key columns as a jsonb object, in
-- the columns' own types (so 42 and '42' name the same row of a bigint key).
CREATE FUNCTION txn._key(p_rel regclass, p_key jsonb) RETURNS jsonb
LANGUAGE plpgsql STABLE AS $$
DECLARE
    cols text[] := txn._pk(p_rel);
    obj jsonb;
    full_row jsonb;
BEGIN
    IF pg_catalog.jsonb_typeof(p_key) = 'object' THEN
        obj := p_key;
    ELSIF pg_catalog.cardinality(cols) = 1 THEN
        obj := pg_catalog.jsonb_build_object(cols[1], p_key);
    ELSE
        RAISE EXCEPTION 'pg_txn: % has a composite primary key (%); pass the key as an object', p_rel, cols
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    EXECUTE pg_catalog.format('SELECT pg_catalog.to_jsonb(r) FROM pg_catalog.jsonb_populate_record(NULL::%s, $1) r', p_rel)
        INTO full_row USING obj;
    SELECT pg_catalog.jsonb_object_agg(c, full_row -> c) INTO obj FROM pg_catalog.unnest(cols) c;
    IF EXISTS (SELECT 1 FROM pg_catalog.jsonb_each(obj) e WHERE e.value = 'null'::jsonb) THEN
        RAISE EXCEPTION 'pg_txn: key % does not name a row of % (primary key: %)', p_key, p_rel, cols
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    RETURN obj;
END $$;

CREATE FUNCTION txn._key_predicate(p_cols text[]) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    SELECT pg_catalog.string_agg(pg_catalog.format('t.%I = k.%I', c, c), ' AND ')
      FROM pg_catalog.unnest(p_cols) c
$$;

-- ---------------------------------------------------------------------------
-- ownership guard: changing a row owned by another running transaction
-- fails at once (55P03), instead of blocking or silently racing

CREATE FUNCTION txn._guard() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, txn, pg_temp AS $$
DECLARE
    cols text[];
    k jsonb;
    holder uuid;
BEGIN
    SELECT key_columns INTO cols FROM txn.guarded_tables WHERE rel = TG_RELID;
    IF cols IS NOT NULL THEN
        SELECT pg_catalog.jsonb_object_agg(c, pg_catalog.to_jsonb(OLD) -> c) INTO k FROM pg_catalog.unnest(cols) c;
        SELECT tx_id INTO holder FROM txn.owned_rows WHERE rel = TG_RELID AND key = k;
        IF holder IS NOT NULL AND holder IS DISTINCT FROM txn._current() THEN
            RAISE EXCEPTION 'pg_txn: % row % is owned by transaction %', TG_RELID::regclass, k, holder
                USING ERRCODE = 'lock_not_available', DETAIL = 'owner=' || holder::text,
                      HINT = 'The transaction that owns it is waiting for an external call; retry after it commits.';
        END IF;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END $$;

-- Installs the guard on a table (done automatically the first time one of
-- its rows is owned; call it in a migration if the application role does
-- not own the table).
CREATE FUNCTION txn.guard(p_table regclass) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM txn.guarded_tables WHERE rel = p_table) THEN
        RETURN;
    END IF;
    EXECUTE pg_catalog.format(
        'CREATE OR REPLACE TRIGGER pg_txn_guard BEFORE UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION txn._guard()',
        p_table);
    INSERT INTO txn.guarded_tables (rel, key_columns) VALUES (p_table, txn._pk(p_table))
        ON CONFLICT (rel) DO NOTHING;
END $$;

-- ---------------------------------------------------------------------------
-- inside an attempt (the application's own transaction)

-- Marks this database transaction as a run of logical transaction p_tx: rows
-- it owns become writable here. Fails if another process drives it now.
CREATE FUNCTION txn.attempt(p_tx uuid, p_owner uuid) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    t txn.transactions;
BEGIN
    PERFORM pg_catalog.set_config('txn.current', p_tx::text, true);
    SELECT * INTO t FROM txn.transactions WHERE id = p_tx;
    IF FOUND THEN
        IF t.status <> 'running' OR t.owner IS DISTINCT FROM p_owner THEN
            -- it ended, or another process took it over after this one's lease expired
            RAISE EXCEPTION 'pg_txn: transaction % is % and driven by another process', p_tx, t.status
                USING ERRCODE = 'lock_not_available', DETAIL = 'fenced';
        END IF;
        UPDATE txn.transactions SET runs = runs + 1, updated_at = txn._now() WHERE id = p_tx;
    END IF;
END $$;

-- The memoized outcome of effect #p_seq, if it ran with exactly this name and
-- input; status 'missing' otherwise.
CREATE FUNCTION txn.effect_lookup(p_tx uuid, p_seq integer, p_name text, p_input jsonb)
RETURNS TABLE (effect_id uuid, status text, result jsonb, error jsonb)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
        SELECT e.id, e.status, e.result, e.error FROM txn.effects e
         WHERE e.tx_id = p_tx AND e.kind = 'call' AND e.seq = p_seq AND e.input_hash = txn._hash(p_name, p_input);
    IF NOT FOUND THEN
        RETURN QUERY SELECT NULL::uuid, 'missing'::text, NULL::jsonb, NULL::jsonb;
    END IF;
END $$;

-- Reads a row the transaction depends on: returns it with its version. The
-- SDK claims it, version-checked, before the transaction's first effect.
-- Fails with 55P03 if another running transaction owns it.
CREATE FUNCTION txn.own(p_table regclass, p_key jsonb)
RETURNS TABLE ("row" jsonb, version text, key jsonb, rel oid)
LANGUAGE plpgsql AS $$
DECLARE
    k jsonb := txn._key(p_table, p_key);
    holder uuid;
BEGIN
    SELECT o.tx_id INTO holder FROM txn.owned_rows o WHERE o.rel = p_table AND o.key = k;
    IF holder IS NOT NULL AND holder IS DISTINCT FROM txn._current() THEN
        RAISE EXCEPTION 'pg_txn: % row % is owned by transaction %', p_table, k, holder
            USING ERRCODE = 'lock_not_available', DETAIL = 'owner=' || holder::text;
    END IF;
    RETURN QUERY EXECUTE pg_catalog.format(
        'SELECT pg_catalog.to_jsonb(t), t.xmin::text, $1, $2 FROM %s t, pg_catalog.jsonb_populate_record(NULL::%s, $1) k WHERE %s',
        p_table, p_table, txn._key_predicate(txn._pk(p_table)))
        USING k, p_table::oid;
END $$;

-- An effect that runs iff the surrounding transaction commits (a built-in
-- transactional outbox). Its code is a function in the process p_owner
-- (the SDK passes its own identity), which runs it right after the commit.
CREATE FUNCTION txn.spawn(p_owner uuid, p_name text, p_id uuid DEFAULT NULL, p_max_attempts integer DEFAULT 1,
                          p_delivery text DEFAULT 'at-most-once', p_delay_ms integer DEFAULT 0)
RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE
    id uuid := coalesce(p_id, pg_catalog.gen_random_uuid());
    tx uuid;
BEGIN
    SELECT t.id INTO tx FROM txn.transactions t WHERE t.id = txn._current();
    INSERT INTO txn.effects (id, tx_id, kind, name, input, max_attempts, delivery, next_attempt_at, local_owner)
    VALUES (id, tx, 'spawn', p_name, '{}', p_max_attempts, p_delivery,
            txn._now() + pg_catalog.make_interval(secs => p_delay_ms / 1000.0), p_owner)
    ON CONFLICT ON CONSTRAINT effects_pkey DO NOTHING;
    RETURN id;
END $$;

-- A named transaction that runs in the background (on any process that
-- defines it) iff the surrounding transaction commits.
CREATE FUNCTION txn.enqueue(p_name text, p_input jsonb DEFAULT '{}', p_id uuid DEFAULT NULL)
RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE
    id uuid := coalesce(p_id, pg_catalog.gen_random_uuid());
BEGIN
    INSERT INTO txn.transactions (id, name, input, owner, lease_until)
    VALUES (id, p_name, coalesce(p_input, '{}'), NULL, txn._now())
    ON CONFLICT ON CONSTRAINT transactions_pkey DO NOTHING;
    PERFORM pg_catalog.pg_notify('txn_effects', p_name);
    RETURN id;
END $$;

-- Final step of the run that commits: effects recorded for this transaction
-- but not used by this run are orphaned (their compensations are scheduled),
-- owned rows are released, and the outcome is stored, atomically with the
-- run's own writes.
CREATE FUNCTION txn.finish(p_tx uuid, p_owner uuid, p_consumed uuid[], p_output jsonb DEFAULT NULL)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    t txn.transactions;
    n integer := 0;
BEGIN
    SELECT * INTO t FROM txn.transactions WHERE id = p_tx FOR UPDATE;
    IF NOT FOUND THEN
        RETURN 0;
    END IF;
    IF t.status <> 'running' OR t.owner IS DISTINCT FROM p_owner THEN
        RAISE EXCEPTION 'pg_txn: transaction % is % and driven by another process', p_tx, t.status
            USING ERRCODE = 'lock_not_available', DETAIL = 'fenced';
    END IF;
    n := txn._orphan(p_tx, p_consumed, NULL);
    DELETE FROM txn.owned_rows WHERE tx_id = p_tx;
    UPDATE txn.transactions
       SET status = 'committed', output = p_output, owner = NULL, lease_until = NULL,
           updated_at = txn._now(), finished_at = txn._now()
     WHERE id = p_tx;
    PERFORM pg_catalog.pg_notify('txn_done', p_tx::text);
    RETURN n;
END $$;

-- Effects of p_tx not in p_consumed become orphaned; succeeded ones with a
-- compensation get a compensation effect. Returns the number scheduled.
CREATE FUNCTION txn._orphan(p_tx uuid, p_consumed uuid[], p_reason jsonb) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    e record;
    n integer := 0;
BEGIN
    FOR e IN
        UPDATE txn.effects
           SET status = 'orphaned', updated_at = txn._now(),
               error = coalesce(p_reason, error)
         WHERE tx_id = p_tx AND kind = 'call' AND status <> 'orphaned'
           AND NOT (id = ANY (coalesce(p_consumed, '{}')))
        RETURNING id, name, input, result, compensation, max_attempts, delivery, (result IS NOT NULL) AS succeeded
    LOOP
        IF e.compensation IS NOT NULL AND e.succeeded THEN
            -- run by the process driving the transaction, which has the
            -- function, with the retry policy of the effect it undoes
            INSERT INTO txn.effects (tx_id, kind, name, input, compensates, max_attempts, delivery, local_owner)
            VALUES (p_tx, 'compensation', e.compensation,
                    pg_catalog.jsonb_build_object('effect', e.name, 'input', e.input, 'result', e.result),
                    e.id, e.max_attempts, e.delivery, (SELECT t.owner FROM txn.transactions t WHERE t.id = p_tx));
            n := n + 1;
        END IF;
    END LOOP;
    IF n > 0 THEN
        PERFORM pg_catalog.pg_notify('txn_effects', 'compensation');
    END IF;
    RETURN n;
END $$;

-- ---------------------------------------------------------------------------
-- outside any transaction (autocommit), around the external calls

-- Takes (or keeps) the lease on the transaction, claims the rows it owns --
-- all or nothing, each unchanged since it was read -- and records the
-- intents of the effects it is about to call. Returns what to do per effect:
--   {"conflict": {...}}          nothing claimed or recorded: re-run later
--   {"effects": [{"seq", "id", "action": execute|wait|done, "attempt", "wait_ms"}]}
CREATE FUNCTION txn.prepare_effects(p_tx uuid, p_owner uuid, p_lease_ms integer,
                                    p_effects jsonb, p_claims jsonb DEFAULT '[]',
                                    p_name text DEFAULT NULL, p_input jsonb DEFAULT NULL)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
    t txn.transactions;
    c record;
    cur text;
    holder uuid;
    e jsonb;
    er txn.effects;
    res jsonb := '[]';
    action text;
    wait_ms integer;
BEGIN
    INSERT INTO txn.transactions AS x (id, name, input, owner, generation, lease_until)
    VALUES (p_tx, p_name, p_input, p_owner, 1, txn._now() + pg_catalog.make_interval(secs => p_lease_ms / 1000.0))
    ON CONFLICT (id) DO UPDATE
       SET owner = p_owner,
           generation = x.generation + CASE WHEN x.owner IS DISTINCT FROM p_owner THEN 1 ELSE 0 END,
           lease_until = EXCLUDED.lease_until, updated_at = txn._now()
     WHERE x.status = 'running' AND (x.owner = p_owner OR x.owner IS NULL OR x.lease_until < txn._now())
    RETURNING * INTO t;
    IF NOT FOUND THEN
        RETURN pg_catalog.jsonb_build_object('conflict', pg_catalog.jsonb_build_object('reason', 'fenced'));
    END IF;

    -- claims: lock the rows briefly in a fixed order, check each is unchanged
    -- since the run read it and not owned by another transaction; only if all
    -- pass, record them (no one waits while holding anything: no deadlocks).
    -- The ownership check comes after the row lock: a claim racing this one
    -- either committed before the lock was granted (and is visible now) or
    -- waits for this transaction.
    FOR c IN
        -- tables by oid (txn.own returns it): names would resolve in this function's search_path
        SELECT (x->>'rel')::oid::regclass AS rel, x->'key' AS key, x->>'version' AS version
          FROM pg_catalog.jsonb_array_elements(coalesce(p_claims, '[]')) x
         ORDER BY (x->>'rel')::oid, (x->'key')::text
    LOOP
        EXECUTE pg_catalog.format(
            'SELECT t.xmin::text FROM %s t, pg_catalog.jsonb_populate_record(NULL::%s, $1) k WHERE %s FOR UPDATE OF t',
            c.rel, c.rel, txn._key_predicate(txn._pk(c.rel)))
            INTO cur USING c.key;
        IF cur IS DISTINCT FROM c.version THEN
            RETURN pg_catalog.jsonb_build_object('conflict', pg_catalog.jsonb_build_object(
                'reason', CASE WHEN cur IS NULL THEN 'gone' ELSE 'changed' END, 'table', c.rel::text, 'key', c.key));
        END IF;
        SELECT o.tx_id INTO holder FROM txn.owned_rows o WHERE o.rel = c.rel AND o.key = c.key;
        IF holder IS NOT NULL AND holder <> p_tx THEN
            RETURN pg_catalog.jsonb_build_object('conflict', pg_catalog.jsonb_build_object(
                'reason', 'owned', 'table', c.rel::text, 'key', c.key, 'owner', holder));
        END IF;
    END LOOP;
    FOR c IN
        SELECT (x->>'rel')::oid::regclass AS rel, x->'key' AS key
          FROM pg_catalog.jsonb_array_elements(coalesce(p_claims, '[]')) x
    LOOP
        PERFORM txn.guard(c.rel);
        INSERT INTO txn.owned_rows (rel, key, tx_id) VALUES (c.rel, c.key, p_tx)
            ON CONFLICT (rel, key) DO NOTHING;
        SELECT o.tx_id INTO holder FROM txn.owned_rows o WHERE o.rel = c.rel AND o.key = c.key;
        IF holder IS DISTINCT FROM p_tx THEN
            -- lost a race after all: undo this call's claims and intents
            RAISE EXCEPTION USING ERRCODE = 'lock_not_available', MESSAGE = 'pg_txn claim race',
                  DETAIL = 'owner=' || holder::text;
        END IF;
    END LOOP;

    FOR e IN SELECT * FROM pg_catalog.jsonb_array_elements(coalesce(p_effects, '[]')) LOOP
        INSERT INTO txn.effects (tx_id, kind, seq, name, input, input_hash, max_attempts, delivery, compensation)
        VALUES (p_tx, 'call', (e->>'seq')::integer, e->>'name', e->'input', txn._hash(e->>'name', e->'input'),
                coalesce((e->>'max_attempts')::integer, 1),
                coalesce(e->>'delivery', 'at-most-once'), e->>'compensation')
        ON CONFLICT (tx_id, seq, input_hash) WHERE kind = 'call' DO NOTHING;
        SELECT * INTO er FROM txn.effects x
         WHERE x.tx_id = p_tx AND x.kind = 'call' AND x.seq = (e->>'seq')::integer
           AND x.input_hash = txn._hash(e->>'name', e->'input')
         FOR UPDATE;
        action := 'execute';
        wait_ms := 0;
        IF er.status IN ('succeeded', 'failed', 'orphaned') THEN
            action := 'done';
        ELSIF er.status = 'retry_wait' AND er.next_attempt_at > txn._now() THEN
            action := 'wait';
            wait_ms := pg_catalog.ceil(EXTRACT(EPOCH FROM er.next_attempt_at - txn._now()) * 1000)::integer;
        ELSIF er.status = 'running' THEN
            -- the process that was calling it is gone (we hold the lease now)
            IF er.delivery = 'at-most-once' THEN
                UPDATE txn.effects
                   SET status = 'failed', completed_at = txn._now(), updated_at = txn._now(),
                       error = pg_catalog.jsonb_build_object('name', 'AmbiguousEffectOutcome',
                               'message', 'the process running this at-most-once effect stopped mid-call; it was not retried')
                 WHERE id = er.id;
                INSERT INTO txn.effect_attempts (effect_id, attempt, outcome, owner, started_at)
                VALUES (er.id, er.attempts, 'ambiguous', er.lease_owner, er.leased_at);
                action := 'done';
            ELSE
                INSERT INTO txn.effect_attempts (effect_id, attempt, outcome, owner, started_at)
                VALUES (er.id, er.attempts, 'lease_expired', er.lease_owner, er.leased_at);
            END IF;
        END IF;
        IF action = 'execute' THEN
            UPDATE txn.effects
               SET status = 'running', attempts = attempts + 1, lease_owner = p_owner,
                   leased_at = txn._now(), updated_at = txn._now()
             WHERE id = er.id
            RETURNING * INTO er;
        END IF;
        res := res || pg_catalog.jsonb_build_object('seq', er.seq, 'id', er.id, 'action', action,
                                                    'attempt', er.attempts, 'wait_ms', wait_ms);
    END LOOP;
    RETURN pg_catalog.jsonb_build_object('effects', res, 'generation', t.generation);
END $$;

-- Records how an effect call ended. Fenced: only the process driving the
-- transaction may record it (a stalled one gets 'stale').
CREATE FUNCTION txn.effect_done(p_effect uuid, p_owner uuid, p_ok boolean, p_result jsonb DEFAULT NULL,
                                p_error jsonb DEFAULT NULL, p_retryable boolean DEFAULT true,
                                p_retry_after_ms integer DEFAULT NULL)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
    e txn.effects;
    t txn.transactions;
    delay integer;
BEGIN
    SELECT * INTO e FROM txn.effects WHERE id = p_effect FOR UPDATE;
    IF NOT FOUND THEN
        RETURN pg_catalog.jsonb_build_object('status', 'unknown');
    END IF;
    SELECT * INTO t FROM txn.transactions WHERE id = e.tx_id;
    IF e.status <> 'running' OR t.owner IS DISTINCT FROM p_owner OR t.status <> 'running' THEN
        INSERT INTO txn.effect_attempts (effect_id, attempt, outcome, error, owner, started_at)
        VALUES (e.id, e.attempts, 'stale', p_error, p_owner, e.leased_at);
        RETURN pg_catalog.jsonb_build_object('status', 'stale');
    END IF;
    IF p_ok THEN
        UPDATE txn.effects
           SET status = 'succeeded', result = p_result, error = NULL, completed_at = txn._now(), updated_at = txn._now()
         WHERE id = e.id;
        INSERT INTO txn.effect_attempts (effect_id, attempt, outcome, owner, started_at)
        VALUES (e.id, e.attempts, 'succeeded', p_owner, e.leased_at);
        RETURN pg_catalog.jsonb_build_object('status', 'succeeded');
    END IF;
    IF p_retryable AND e.attempts < e.max_attempts THEN
        delay := coalesce(p_retry_after_ms, txn._backoff_ms(e.attempts));
        UPDATE txn.effects
           SET status = 'retry_wait', error = p_error, updated_at = txn._now(),
               next_attempt_at = txn._now() + pg_catalog.make_interval(secs => delay / 1000.0)
         WHERE id = e.id;
        INSERT INTO txn.effect_attempts (effect_id, attempt, outcome, error, owner, started_at)
        VALUES (e.id, e.attempts, 'retry', p_error, p_owner, e.leased_at);
        RETURN pg_catalog.jsonb_build_object('status', 'retry_wait', 'wait_ms', delay);
    END IF;
    UPDATE txn.effects
       SET status = 'failed', error = p_error, completed_at = txn._now(), updated_at = txn._now()
     WHERE id = e.id;
    INSERT INTO txn.effect_attempts (effect_id, attempt, outcome, error, owner, started_at)
    VALUES (e.id, e.attempts, 'failed', p_error, p_owner, e.leased_at);
    RETURN pg_catalog.jsonb_build_object('status', 'failed');
END $$;

-- Extends the lease of a transaction the process is driving (during long calls).
CREATE FUNCTION txn.heartbeat(p_tx uuid, p_owner uuid, p_lease_ms integer) RETURNS boolean
LANGUAGE sql AS $$
    UPDATE txn.transactions
       SET lease_until = txn._now() + pg_catalog.make_interval(secs => p_lease_ms / 1000.0), updated_at = txn._now()
     WHERE id = p_tx AND owner = p_owner AND status = 'running'
    RETURNING true
$$;

-- The transaction failed (the function threw): its effects are orphaned and
-- compensated, its rows released.
CREATE FUNCTION txn.fail_transaction(p_tx uuid, p_owner uuid, p_error jsonb) RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM txn.transactions WHERE id = p_tx AND owner = p_owner AND status = 'running' FOR UPDATE;
    IF NOT FOUND THEN
        RETURN false;
    END IF;
    PERFORM txn._orphan(p_tx, '{}', NULL);
    DELETE FROM txn.owned_rows WHERE tx_id = p_tx;
    UPDATE txn.transactions
       SET status = 'failed', error = p_error, owner = NULL, lease_until = NULL,
           updated_at = txn._now(), finished_at = txn._now()
     WHERE id = p_tx;
    PERFORM pg_catalog.pg_notify('txn_done', p_tx::text);
    RETURN true;
END $$;

-- Starts a named transaction driven by this process (resumable by any
-- process that defines it if this one stops).
CREATE FUNCTION txn.start(p_tx uuid, p_name text, p_input jsonb, p_owner uuid, p_lease_ms integer) RETURNS timestamptz
LANGUAGE sql AS $$
    INSERT INTO txn.transactions (id, name, input, owner, generation, lease_until)
    VALUES (p_tx, p_name, p_input, p_owner, 1, txn._now() + pg_catalog.make_interval(secs => p_lease_ms / 1000.0))
    RETURNING created_at
$$;

-- ---------------------------------------------------------------------------
-- SDK workers: spawned effects, compensations, background and resumed
-- transactions

-- Leases due spawned effects and compensations of this process (one
-- set-based statement on the due-work index).
CREATE FUNCTION txn.lease_effects(p_owner uuid, p_max integer DEFAULT 16, p_lease_ms integer DEFAULT 30000)
RETURNS TABLE (id uuid, kind text, name text, input jsonb, attempt integer, generation bigint, tx_id uuid,
               delivery text, compensates uuid)
LANGUAGE sql AS $$
    WITH due AS (
        SELECT x.id FROM txn.effects x
         WHERE x.kind <> 'call' AND x.local_owner = p_owner
           AND x.status IN ('pending', 'retry_wait') AND x.next_attempt_at <= txn._now()
         ORDER BY x.next_attempt_at
         LIMIT p_max
         FOR UPDATE SKIP LOCKED
    )
    UPDATE txn.effects e
       SET status = 'running', attempts = e.attempts + 1, lease_owner = p_owner,
           lease_generation = e.lease_generation + 1, leased_at = txn._now(),
           lease_until = txn._now() + pg_catalog.make_interval(secs => p_lease_ms / 1000.0),
           updated_at = txn._now()
      FROM due WHERE e.id = due.id
    RETURNING e.id, e.kind, e.name, e.input, e.attempts, e.lease_generation, e.tx_id, e.delivery, e.compensates
$$;

-- Spawned effects and compensations whose process is gone (not seen for a
-- minute): nobody else has their code, so they fail as EffectLost (see
-- txn.doctor). For work that must survive its process, enqueue a defined
-- transaction instead: any process that defines it runs it.
CREATE FUNCTION txn.fail_lost_effects() RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    n integer;
BEGIN
    WITH lost AS (
        UPDATE txn.effects e
           SET status = 'failed', lease_owner = NULL, lease_until = NULL, completed_at = txn._now(),
               updated_at = txn._now(),
               error = pg_catalog.jsonb_build_object('name', 'EffectLost',
                       'message', 'the process that had this effect''s function stopped before it completed')
         WHERE e.kind <> 'call'
           AND e.status IN ('pending', 'retry_wait', 'running')
           AND e.created_at < txn._now() - interval '1 minute'
           AND NOT EXISTS (SELECT 1 FROM txn.workers w
                            WHERE w.owner = e.local_owner AND w.seen_at > txn._now() - interval '1 minute')
        RETURNING e.id, e.attempts
    ), hist AS (
        INSERT INTO txn.effect_attempts (effect_id, attempt, outcome, error)
        SELECT id, attempts, 'failed', pg_catalog.jsonb_build_object('name', 'EffectLost') FROM lost
    )
    SELECT pg_catalog.count(*) INTO n FROM lost;
    RETURN n;
END $$;

-- Spawned effects whose worker stopped mid-call (lease expired): run again
-- (at-least-once) or fail as ambiguous (at-most-once). Workers call it
-- periodically.
CREATE FUNCTION txn.expire_effect_leases() RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    e txn.effects;
    n integer := 0;
BEGIN
    FOR e IN
        SELECT * FROM txn.effects x
         WHERE x.kind <> 'call' AND x.status = 'running' AND x.lease_until < txn._now()
         FOR UPDATE SKIP LOCKED
    LOOP
        INSERT INTO txn.effect_attempts (effect_id, attempt, outcome, owner, started_at)
        VALUES (e.id, e.attempts, CASE WHEN e.delivery = 'at-most-once' THEN 'ambiguous' ELSE 'lease_expired' END,
                e.lease_owner, e.leased_at);
        IF e.delivery = 'at-most-once' OR e.attempts >= e.max_attempts THEN
            UPDATE txn.effects
               SET status = 'failed', lease_owner = NULL, lease_until = NULL, completed_at = txn._now(),
                   updated_at = txn._now(),
                   error = pg_catalog.jsonb_build_object('name', CASE WHEN e.delivery = 'at-most-once'
                           THEN 'AmbiguousEffectOutcome' ELSE 'LeaseExpired' END,
                           'message', 'the process running it stopped mid-call')
             WHERE txn.effects.id = e.id;
        ELSE
            UPDATE txn.effects
               SET status = 'retry_wait', lease_owner = NULL, lease_until = NULL,
                   next_attempt_at = txn._now(), updated_at = txn._now()
             WHERE txn.effects.id = e.id;
        END IF;
        n := n + 1;
    END LOOP;
    IF n > 0 THEN
        PERFORM pg_catalog.pg_notify('txn_effects', 'expired');
    END IF;
    RETURN n;
END $$;

CREATE FUNCTION txn.heartbeat_effect(p_id uuid, p_owner uuid, p_generation bigint, p_lease_ms integer)
RETURNS boolean LANGUAGE sql AS $$
    UPDATE txn.effects
       SET lease_until = txn._now() + pg_catalog.make_interval(secs => p_lease_ms / 1000.0)
     WHERE id = p_id AND lease_owner = p_owner AND lease_generation = p_generation AND status = 'running'
    RETURNING true
$$;

CREATE FUNCTION txn.complete_effect(p_id uuid, p_owner uuid, p_generation bigint, p_result jsonb)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    e txn.effects;
BEGIN
    SELECT * INTO e FROM txn.effects WHERE id = p_id FOR UPDATE;
    IF NOT FOUND THEN
        RETURN false;
    END IF;
    IF e.status <> 'running' OR e.lease_owner IS DISTINCT FROM p_owner OR e.lease_generation <> p_generation THEN
        INSERT INTO txn.effect_attempts (effect_id, attempt, outcome, owner, started_at)
        VALUES (p_id, e.attempts, 'stale', p_owner, e.leased_at);
        RETURN false;
    END IF;
    UPDATE txn.effects
       SET status = 'succeeded', result = p_result, error = NULL, lease_owner = NULL, lease_until = NULL,
           completed_at = txn._now(), updated_at = txn._now()
     WHERE id = p_id;
    INSERT INTO txn.effect_attempts (effect_id, attempt, outcome, owner, started_at)
    VALUES (p_id, e.attempts, 'succeeded', p_owner, e.leased_at);
    RETURN true;
END $$;

CREATE FUNCTION txn.fail_effect(p_id uuid, p_owner uuid, p_generation bigint, p_error jsonb,
                                p_retryable boolean DEFAULT true, p_retry_after_ms integer DEFAULT NULL)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    e txn.effects;
    delay integer;
BEGIN
    SELECT * INTO e FROM txn.effects WHERE id = p_id FOR UPDATE;
    IF NOT FOUND THEN
        RETURN 'unknown';
    END IF;
    IF e.status <> 'running' OR e.lease_owner IS DISTINCT FROM p_owner OR e.lease_generation <> p_generation THEN
        INSERT INTO txn.effect_attempts (effect_id, attempt, outcome, error, owner, started_at)
        VALUES (p_id, e.attempts, 'stale', p_error, p_owner, e.leased_at);
        RETURN 'stale';
    END IF;
    IF p_retryable AND e.attempts < e.max_attempts THEN
        delay := coalesce(p_retry_after_ms, txn._backoff_ms(e.attempts));
        UPDATE txn.effects
           SET status = 'retry_wait', error = p_error, lease_owner = NULL, lease_until = NULL, updated_at = txn._now(),
               next_attempt_at = txn._now() + pg_catalog.make_interval(secs => delay / 1000.0)
         WHERE id = p_id;
        INSERT INTO txn.effect_attempts (effect_id, attempt, outcome, error, owner, started_at)
        VALUES (p_id, e.attempts, 'retry', p_error, p_owner, e.leased_at);
        RETURN 'retry_wait';
    END IF;
    UPDATE txn.effects
       SET status = 'failed', error = p_error, lease_owner = NULL, lease_until = NULL,
           completed_at = txn._now(), updated_at = txn._now()
     WHERE id = p_id;
    INSERT INTO txn.effect_attempts (effect_id, attempt, outcome, error, owner, started_at)
    VALUES (p_id, e.attempts, 'failed', p_error, p_owner, e.leased_at);
    RETURN 'failed';
END $$;

-- Leases named transactions this process defines that nobody drives: queued
-- ones (txn.enqueue) and ones whose process stopped (lease expired). The
-- process runs them again; recorded effects are reused.
CREATE FUNCTION txn.lease_transactions(p_owner uuid, p_names text[], p_max integer DEFAULT 16,
                                       p_lease_ms integer DEFAULT 30000)
RETURNS TABLE (id uuid, name text, input jsonb, runs integer, created_at timestamptz)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    WITH due AS (
        SELECT t.id FROM txn.transactions t
         WHERE t.status = 'running' AND t.name = ANY (p_names)
           AND (t.owner IS NULL OR t.lease_until < txn._now())
           -- a live process keeps driving its own transactions
           AND t.owner IS DISTINCT FROM p_owner
         ORDER BY t.created_at
         LIMIT p_max
         FOR UPDATE SKIP LOCKED
    )
    UPDATE txn.transactions t
       SET owner = p_owner, generation = t.generation + 1, updated_at = txn._now(),
           lease_until = txn._now() + pg_catalog.make_interval(secs => p_lease_ms / 1000.0)
      FROM due WHERE t.id = due.id
    RETURNING t.id, t.name, t.input, t.runs, t.created_at;
END $$;

-- Inline (unnamed) transactions whose process stopped cannot be resumed:
-- they are abandoned, their effects orphaned and compensated, their rows
-- released.
CREATE FUNCTION txn.abandon_expired(p_grace_ms integer DEFAULT 5000) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    t record;
    n integer := 0;
BEGIN
    FOR t IN
        SELECT x.id FROM txn.transactions x
         WHERE x.status = 'running' AND x.name IS NULL
           AND x.lease_until < txn._now() - pg_catalog.make_interval(secs => p_grace_ms / 1000.0)
         FOR UPDATE SKIP LOCKED
    LOOP
        UPDATE txn.effects
           SET status = 'failed', completed_at = txn._now(), updated_at = txn._now(),
               error = pg_catalog.jsonb_build_object('name', 'AbandonedTransaction',
                       'message', 'the process running this transaction stopped')
         WHERE tx_id = t.id AND kind = 'call' AND status IN ('pending', 'running', 'retry_wait');
        PERFORM txn._orphan(t.id, '{}', NULL);
        DELETE FROM txn.owned_rows WHERE tx_id = t.id;
        UPDATE txn.transactions
           SET status = 'abandoned', owner = NULL, lease_until = NULL, updated_at = txn._now(),
               finished_at = txn._now(),
               error = pg_catalog.jsonb_build_object('name', 'AbandonedTransaction',
                       'message', 'the process running this inline transaction stopped before it committed')
         WHERE id = t.id;
        PERFORM pg_catalog.pg_notify('txn_done', t.id::text);
        n := n + 1;
    END LOOP;
    RETURN n;
END $$;

CREATE FUNCTION txn.worker_seen(p_owner uuid, p_info jsonb DEFAULT NULL) RETURNS void
LANGUAGE sql AS $$
    INSERT INTO txn.workers (owner, info, seen_at) VALUES (p_owner, p_info, txn._now())
    ON CONFLICT (owner) DO UPDATE SET info = EXCLUDED.info, seen_at = EXCLUDED.seen_at;
    DELETE FROM txn.workers WHERE seen_at < txn._now() - interval '1 hour';
$$;

-- ---------------------------------------------------------------------------
-- observability and administration

CREATE FUNCTION txn.status(p_tx uuid)
RETURNS TABLE (status text, output jsonb, error jsonb, runs integer, created_at timestamptz)
LANGUAGE sql STABLE AS $$
    SELECT status, output, error, runs, created_at FROM txn.transactions WHERE id = p_tx
$$;

CREATE VIEW txn.running_transactions AS
    SELECT id, name, owner, runs, lease_until, created_at, clock_timestamp() - created_at AS age
      FROM txn.transactions WHERE status = 'running';

CREATE VIEW txn.owned AS
    SELECT o.rel AS "table", o.key, o.tx_id, t.name, t.created_at
      FROM txn.owned_rows o JOIN txn.transactions t ON t.id = o.tx_id;

CREATE VIEW txn.effect_errors AS
    SELECT a.effect_id, e.tx_id, e.kind, e.name, a.attempt, a.outcome, a.error,
           a.error ->> 'name' AS error_name, a.error ->> 'message' AS error_message,
           a.started_at, a.finished_at, a.finished_at - a.started_at AS duration
      FROM txn.effect_attempts a JOIN txn.effects e ON e.id = a.effect_id
     WHERE a.outcome <> 'succeeded';

CREATE VIEW txn.pending_effects AS
    SELECT id, tx_id, kind, name, status, attempts, max_attempts, next_attempt_at, error
      FROM txn.effects WHERE status IN ('pending', 'running', 'retry_wait');

CREATE FUNCTION txn.doctor() RETURNS TABLE (check_name text, status text, detail text)
LANGUAGE plpgsql AS $$
DECLARE
    n bigint;
BEGIN
    RETURN QUERY SELECT 'schema'::text, 'ok'::text, pg_catalog.format('pg_txn schema version %s', (SELECT m.version FROM txn.meta m));

    SELECT pg_catalog.count(*) INTO n FROM txn.workers w
     WHERE w.seen_at > txn._now() - interval '1 minute';
    RETURN QUERY SELECT 'workers',
        CASE WHEN n > 0 THEN 'ok' ELSE 'warning' END,
        CASE WHEN n > 0 THEN pg_catalog.format('%s worker(s) active', n)
             ELSE 'no SDK worker seen in the last minute: spawned effects and background transactions wait' END;

    SELECT pg_catalog.count(*) INTO n FROM txn.effects e
     WHERE e.kind <> 'call' AND e.status IN ('pending', 'retry_wait') AND e.next_attempt_at < txn._now() - interval '1 minute';
    RETURN QUERY SELECT 'effects due', CASE WHEN n = 0 THEN 'ok' ELSE 'warning' END,
        CASE WHEN n = 0 THEN 'no effect has been due for over a minute'
             ELSE pg_catalog.format('%s effect(s) due for over a minute: their processes are saturated or stopped', n) END;

    SELECT pg_catalog.count(*) INTO n FROM txn.effects e
     WHERE e.kind <> 'call' AND e.status = 'failed' AND e.error ->> 'name' = 'EffectLost'
       AND e.updated_at > txn._now() - interval '1 day';
    IF n > 0 THEN
        RETURN QUERY SELECT 'lost effects', 'warning',
            pg_catalog.format('%s spawned effect(s) or compensation(s) lost in the last day: their process stopped before they completed; see txn.effect_errors', n);
    END IF;

    SELECT pg_catalog.count(*) INTO n FROM txn.transactions t
     WHERE t.status = 'running' AND t.lease_until < txn._now() - interval '1 minute';
    IF n > 0 THEN
        RETURN QUERY SELECT 'stalled transactions', 'warning',
            pg_catalog.format('%s transaction(s) not driven by any process for over a minute (no worker defines them?)', n);
    END IF;

    SELECT pg_catalog.count(*) INTO n FROM txn.effects e WHERE e.kind = 'call' AND e.status = 'orphaned'
       AND e.compensation IS NULL AND e.result IS NOT NULL AND e.updated_at > txn._now() - interval '1 day';
    IF n > 0 THEN
        RETURN QUERY SELECT 'orphaned effects', 'warning',
            pg_catalog.format('%s effect(s) ran for a transaction that did not use their result (no compensation declared) in the last day; see txn.effects WHERE status = ''orphaned''', n);
    END IF;
END $$;

-- Deletes finished transactions and completed effects older than p_older_than
-- (at most p_batch of each per call; repeat while it returns full batches).
CREATE FUNCTION txn.purge(p_older_than interval DEFAULT interval '30 days', p_batch integer DEFAULT 10000)
RETURNS TABLE (deleted_transactions bigint, deleted_effects bigint)
LANGUAGE plpgsql AS $$
DECLARE
    nt bigint;
    ne bigint;
BEGIN
    WITH d AS (
        DELETE FROM txn.transactions WHERE id IN (
            SELECT t.id FROM txn.transactions t
             WHERE t.status <> 'running' AND t.finished_at < txn._now() - p_older_than
               AND NOT EXISTS (SELECT 1 FROM txn.effects e WHERE e.tx_id = t.id
                                AND e.status IN ('pending', 'running', 'retry_wait'))
             LIMIT p_batch)
        RETURNING 1)
    SELECT pg_catalog.count(*) INTO nt FROM d;
    WITH d AS (
        DELETE FROM txn.effects WHERE id IN (
            SELECT e.id FROM txn.effects e
             WHERE e.tx_id IS NULL AND e.status IN ('succeeded', 'failed', 'orphaned')
               AND e.completed_at < txn._now() - p_older_than
             LIMIT p_batch)
        RETURNING 1)
    SELECT pg_catalog.count(*) INTO ne FROM d;
    RETURN QUERY SELECT nt, ne;
END $$;

-- For installations by another role (e.g. CREATE EXTENSION as superuser):
-- lets p_role use pg_txn.
CREATE FUNCTION txn.grant_to(p_role regrole) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE pg_catalog.format('GRANT USAGE ON SCHEMA txn TO %s', p_role);
    EXECUTE pg_catalog.format('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA txn TO %s', p_role);
    EXECUTE pg_catalog.format('GRANT USAGE ON ALL SEQUENCES IN SCHEMA txn TO %s', p_role);
    EXECUTE pg_catalog.format('GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA txn TO %s', p_role);
END $$;

-- every function resolves names only in pg_catalog and txn
DO $$
DECLARE
    f regprocedure;
BEGIN
    FOR f IN SELECT p.oid::regprocedure FROM pg_catalog.pg_proc p
              WHERE p.pronamespace = 'txn'::regnamespace AND p.prokind = 'f'
                AND p.oid::regprocedure::text <> 'txn._guard()'
    LOOP
        EXECUTE pg_catalog.format('ALTER FUNCTION %s SET search_path = pg_catalog, txn, pg_temp', f);
    END LOOP;
END $$;
