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
--   When an effect is missing, the SDK rolls the attempt back (nothing is
--   held: no transaction, no lock, no connection) and, outside any
--   transaction:
--     txn.prepare_effects(...)            takes the lease and records intents
--     <calls the effect>                  in the application process
--     txn.effect_done(...)                records the result
--   then runs the function again: effects already done return their
--   recorded results. The run that reaches the end commits with
--     txn.finish(tx, owner, consumed)     orphaned effects -> compensation
--   so every write, the spawned effects and the outcome commit atomically.
--
--   txn.spawn(owner, name, id)            an effect that runs iff the surrounding
--                                         transaction commits (built-in outbox),
--                                         run right after it by the process owner
--   txn.enqueue(name, input)              a named transaction that runs in the
--                                         background iff the surrounding commits
--
--   A transaction may hold keys (txn.start, txn.enqueue): transactions
--   sharing a key run one at a time, like advisory locks that hold nothing
--   while effects run. All of a transaction's keys are claimed at once or
--   none: nobody waits holding a key, so there are no deadlocks.

CREATE TABLE txn.meta (
    version integer NOT NULL
);
INSERT INTO txn.meta VALUES (1);

-- ---------------------------------------------------------------------------
-- durable state

CREATE TABLE txn.transactions (
    id          uuid PRIMARY KEY,
    name        text,                   -- NULL: inline (not resumable by another process)
    keys        text[],                 -- the keys it runs under (see txn.keys)
    isolation   text                    -- its runs' isolation level (NULL: the database default)
                CHECK (isolation IN ('read committed', 'repeatable read', 'serializable')),
    input       jsonb,                  -- named transactions: their input
    status      text NOT NULL DEFAULT 'running'
                CHECK (status IN ('running', 'committed', 'failed', 'abandoned')),
    owner       uuid,                   -- the process driving it (NULL: waiting for a worker)
    generation  bigint NOT NULL DEFAULT 0,
    runs        integer NOT NULL DEFAULT 0,
    output      jsonb,
    error       jsonb,
    created_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz
);
CREATE INDEX transactions_running ON txn.transactions (created_at) WHERE status = 'running';

-- the lease of the process driving a transaction, renewed by its heartbeat;
-- apart from txn.transactions so that heartbeats never touch the row a run
-- updates (under repeatable read or serializable, that run would fail)
CREATE TABLE txn.leases (
    tx_id       uuid PRIMARY KEY REFERENCES txn.transactions ON DELETE CASCADE,
    lease_until timestamptz NOT NULL
);
CREATE INDEX leases_until ON txn.leases (lease_until);
-- keys held by running transactions
CREATE TABLE txn.keys (
    key   text PRIMARY KEY,
    tx_id uuid NOT NULL REFERENCES txn.transactions ON DELETE CASCADE
);
CREATE INDEX keys_tx ON txn.keys (tx_id);

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
-- a process's unfinished spawns and compensations (fail_lost_effects, close, sweeps)
CREATE INDEX effects_open ON txn.effects (local_owner, created_at)
    WHERE kind <> 'call' AND status IN ('pending', 'retry_wait', 'running');
CREATE INDEX effects_compensates ON txn.effects (compensates) WHERE compensates IS NOT NULL;

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

-- ---------------------------------------------------------------------------
-- inside an attempt (the application's own transaction)

-- Sets the lease of p_tx to p_lease_ms from now.
CREATE FUNCTION txn._lease(p_tx uuid, p_lease_ms integer) RETURNS void
LANGUAGE sql AS $$
    INSERT INTO txn.leases (tx_id, lease_until)
    VALUES (p_tx, txn._now() + pg_catalog.make_interval(secs => p_lease_ms / 1000.0))
    ON CONFLICT (tx_id) DO UPDATE SET lease_until = EXCLUDED.lease_until
$$;

-- Whether nobody holds the lease of p_tx (none yet, or it expired p_grace_ms ago).
CREATE FUNCTION txn._lease_expired(p_tx uuid, p_grace_ms integer DEFAULT 0) RETURNS boolean
LANGUAGE sql STABLE AS $$
    SELECT coalesce((SELECT l.lease_until < txn._now() - pg_catalog.make_interval(secs => p_grace_ms / 1000.0)
                       FROM txn.leases l WHERE l.tx_id = p_tx), true)
$$;

-- Marks this database transaction as a run of logical transaction p_tx.
-- Fails if another process drives it now.
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
    IF p_owner IS NULL THEN
        RAISE EXCEPTION 'pg_txn: spawn needs the owner process that has its function' USING ERRCODE = 'null_value_not_allowed';
    END IF;
    SELECT t.id INTO tx FROM txn.transactions t WHERE t.id = txn._current();
    INSERT INTO txn.effects (id, tx_id, kind, name, input, max_attempts, delivery, next_attempt_at, local_owner)
    VALUES (id, tx, 'spawn', p_name, '{}', p_max_attempts, p_delivery,
            txn._now() + pg_catalog.make_interval(secs => p_delay_ms / 1000.0), p_owner)
    ON CONFLICT ON CONSTRAINT effects_pkey DO NOTHING;
    RETURN id;
END $$;

-- A named transaction that runs in the background (on any process that
-- defines it) iff the surrounding transaction commits.
CREATE FUNCTION txn.enqueue(p_name text, p_input jsonb DEFAULT '{}', p_id uuid DEFAULT NULL,
                            p_keys text[] DEFAULT NULL, p_isolation text DEFAULT NULL)
RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE
    id uuid := coalesce(p_id, pg_catalog.gen_random_uuid());
BEGIN
    IF p_name IS NULL THEN
        RAISE EXCEPTION 'pg_txn: enqueue needs the name of a defined transaction' USING ERRCODE = 'null_value_not_allowed';
    END IF;
    PERFORM txn._check_keys(p_keys);
    INSERT INTO txn.transactions (id, name, input, keys, isolation, owner)
    VALUES (id, p_name, coalesce(p_input, '{}'), p_keys, p_isolation, NULL)
    ON CONFLICT ON CONSTRAINT transactions_pkey DO NOTHING;
    PERFORM pg_catalog.pg_notify('txn_effects', p_name);
    RETURN id;
END $$;

-- Final step of the run that commits: effects recorded for this transaction
-- but not used by this run are orphaned (their compensations are scheduled)
-- and the outcome is stored (releasing its keys), atomically with the run's
-- own writes.
-- p_runs: how many runs this process made (runs are rolled back, so they
-- cannot count themselves).
CREATE FUNCTION txn.finish(p_tx uuid, p_owner uuid, p_consumed uuid[], p_output jsonb DEFAULT NULL,
                           p_runs integer DEFAULT 1)
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
    DELETE FROM txn.keys WHERE tx_id = p_tx;
    UPDATE txn.transactions
       SET status = 'committed', output = p_output, owner = NULL, runs = runs + p_runs,
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

-- Takes (or keeps) the lease on the transaction and records the intents of
-- the effects it is about to call. Returns what to do per effect:
--   {"conflict": {"reason": "fenced"}}   another process drives it now: stop
--   {"effects": [{"seq", "id", "action": execute|wait|done, "attempt", "wait_ms"}]}
CREATE FUNCTION txn.prepare_effects(p_tx uuid, p_owner uuid, p_lease_ms integer,
                                    p_effects jsonb)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
    t txn.transactions;
    e jsonb;
    er txn.effects;
    res jsonb := '[]';
    action text;
    wait_ms integer;
BEGIN
    INSERT INTO txn.transactions AS x (id, owner, generation)
    VALUES (p_tx, p_owner, 1)
    ON CONFLICT (id) DO UPDATE
       SET owner = p_owner,
           generation = x.generation + CASE WHEN x.owner IS DISTINCT FROM p_owner THEN 1 ELSE 0 END,
           updated_at = txn._now()
     WHERE x.status = 'running' AND (x.owner = p_owner OR x.owner IS NULL OR txn._lease_expired(x.id))
    RETURNING * INTO t;
    IF NOT FOUND THEN
        RETURN pg_catalog.jsonb_build_object('conflict', pg_catalog.jsonb_build_object('reason', 'fenced'));
    END IF;
    PERFORM txn._lease(p_tx, p_lease_ms);

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
-- Never touches txn.transactions, so it can run while a run is open.
CREATE FUNCTION txn.heartbeat(p_tx uuid, p_owner uuid, p_lease_ms integer) RETURNS boolean
LANGUAGE sql AS $$
    UPDATE txn.leases l
       SET lease_until = txn._now() + pg_catalog.make_interval(secs => p_lease_ms / 1000.0)
      FROM txn.transactions t
     WHERE l.tx_id = p_tx AND t.id = p_tx AND t.owner = p_owner AND t.status = 'running'
    RETURNING true
$$;

-- The transaction failed (the function threw): its effects are orphaned and
-- compensated.
CREATE FUNCTION txn.fail_transaction(p_tx uuid, p_owner uuid, p_error jsonb, p_runs integer DEFAULT 1) RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM txn.transactions WHERE id = p_tx AND owner = p_owner AND status = 'running' FOR UPDATE;
    IF NOT FOUND THEN
        RETURN false;
    END IF;
    PERFORM txn._orphan(p_tx, '{}', NULL);
    DELETE FROM txn.keys WHERE tx_id = p_tx;
    UPDATE txn.transactions
       SET status = 'failed', error = p_error, owner = NULL, runs = runs + p_runs,
           updated_at = txn._now(), finished_at = txn._now()
     WHERE id = p_tx;
    PERFORM pg_catalog.pg_notify('txn_done', p_tx::text);
    RETURN true;
END $$;

CREATE FUNCTION txn._check_keys(p_keys text[]) RETURNS void
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF p_keys IS NOT NULL AND pg_catalog.array_position(p_keys, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'pg_txn: a transaction key is NULL' USING ERRCODE = 'null_value_not_allowed';
    END IF;
END $$;

-- Claims all of p_keys for p_tx, or none: returns NULL when they are held
-- (already by p_tx included), or the transaction holding one of them.
-- Claims of a key are serialized by a transaction-level advisory lock on it
-- (in pg_txn's own lock space), taken for every key in sorted order before
-- any insert: p_wait false (background leasing) never waits for one and
-- returns the all-zero uuid when a key is being claimed right now, so no
-- two claims ever wait for each other in a cycle.
CREATE FUNCTION txn._claim(p_tx uuid, p_keys text[], p_wait boolean DEFAULT true) RETURNS uuid
LANGUAGE plpgsql AS $$
DECLARE
    k text;
    holder uuid;
    ks text[] := ARRAY(SELECT DISTINCT x FROM pg_catalog.unnest(p_keys) x ORDER BY x);
BEGIN
    FOREACH k IN ARRAY ks LOOP
        IF p_wait THEN
            PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('pg_txn keys'), pg_catalog.hashtext(k));
        ELSIF NOT pg_catalog.pg_try_advisory_xact_lock(pg_catalog.hashtext('pg_txn keys'), pg_catalog.hashtext(k)) THEN
            RETURN '00000000-0000-0000-0000-000000000000';
        END IF;
    END LOOP;
    <<again>>
    LOOP
        FOREACH k IN ARRAY ks LOOP
            INSERT INTO txn.keys (key, tx_id) VALUES (k, p_tx) ON CONFLICT (key) DO NOTHING;
            IF NOT FOUND THEN
                SELECT h.tx_id INTO holder FROM txn.keys h WHERE h.key = k;
                IF holder IS NULL THEN
                    -- released meanwhile: start over
                    DELETE FROM txn.keys WHERE tx_id = p_tx AND key = ANY (ks);
                    CONTINUE again;
                END IF;
                IF holder <> p_tx THEN
                    DELETE FROM txn.keys WHERE tx_id = p_tx AND key = ANY (ks);
                    RETURN holder;
                END IF;
            END IF;
        END LOOP;
        RETURN NULL;
    END LOOP;
END $$;

-- Starts a transaction driven by this process: a named one (resumable by
-- any process that defines it if this one stops), or an inline one with
-- keys or a caller-chosen id. If another transaction holds one of the keys,
-- nothing is started and holder is that transaction: wait for it to end,
-- then start again. If a transaction with this id exists already (it ran,
-- or runs now: idempotent ids), nothing is started and existing is true:
-- wait for its outcome instead of running it again.
CREATE FUNCTION txn.start(p_tx uuid, p_name text, p_input jsonb, p_owner uuid, p_lease_ms integer,
                          p_keys text[] DEFAULT NULL, p_isolation text DEFAULT NULL)
RETURNS TABLE (created_at timestamptz, holder uuid, existing boolean)
LANGUAGE plpgsql AS $$
DECLARE
    t txn.transactions;
    h uuid;
BEGIN
    PERFORM txn._check_keys(p_keys);
    INSERT INTO txn.transactions (id, name, input, keys, isolation, owner, generation)
    VALUES (p_tx, p_name, p_input, p_keys, p_isolation, p_owner, 1)
    ON CONFLICT (id) DO NOTHING
    RETURNING * INTO t;
    IF NOT FOUND THEN
        RETURN QUERY SELECT NULL::timestamptz, NULL::uuid, true;
        RETURN;
    END IF;
    PERFORM txn._lease(p_tx, p_lease_ms);
    IF p_keys IS NOT NULL THEN
        h := txn._claim(p_tx, p_keys);
        IF h IS NOT NULL THEN
            DELETE FROM txn.transactions x WHERE x.id = p_tx;
            RETURN QUERY SELECT NULL::timestamptz, h, false;
            RETURN;
        END IF;
    END IF;
    RETURN QUERY SELECT t.created_at, NULL::uuid, false;
END $$;

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
RETURNS TABLE (id uuid, name text, input jsonb, runs integer, created_at timestamptz, isolation text)
LANGUAGE plpgsql AS $$
DECLARE
    c record;
    n integer := 0;
BEGIN
    FOR c IN
        SELECT t.id, t.keys FROM txn.transactions t
         WHERE t.status = 'running' AND t.name = ANY (p_names)
           AND (t.owner IS NULL OR txn._lease_expired(t.id))
           -- a live process keeps driving its own transactions
           AND t.owner IS DISTINCT FROM p_owner
           -- queued under a key another transaction holds: not a candidate,
           -- so blocked ones never crowd out runnable ones
           AND NOT EXISTS (SELECT 1 FROM txn.keys k WHERE k.key = ANY (t.keys) AND k.tx_id <> t.id)
         ORDER BY t.created_at
         LIMIT p_max * 4
         FOR UPDATE SKIP LOCKED
    LOOP
        EXIT WHEN n >= p_max;
        BEGIN
            -- a key claimed by another transaction right now: its turn comes later
            CONTINUE WHEN c.keys IS NOT NULL AND txn._claim(c.id, c.keys, false) IS NOT NULL;
            PERFORM txn._lease(c.id, p_lease_ms);
            RETURN QUERY
            UPDATE txn.transactions t
               SET owner = p_owner, generation = t.generation + 1, updated_at = txn._now()
             WHERE t.id = c.id
            RETURNING t.id, t.name, t.input, t.runs, t.created_at, t.isolation;
            n := n + 1;
        EXCEPTION WHEN OTHERS THEN
            -- a transaction that cannot be started fails; it never blocks the others
            UPDATE txn.transactions t
               SET status = 'failed', owner = NULL, updated_at = txn._now(), finished_at = txn._now(),
                   error = pg_catalog.jsonb_build_object('name', 'StartFailed', 'message', SQLERRM)
             WHERE t.id = c.id;
        END;
    END LOOP;
END $$;

-- Inline (unnamed) transactions whose process stopped cannot be resumed:
-- they are abandoned (releasing their keys), their effects orphaned and
-- compensated.
CREATE FUNCTION txn.abandon_expired(p_grace_ms integer DEFAULT 5000) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    t record;
    n integer := 0;
BEGIN
    FOR t IN
        SELECT x.id FROM txn.transactions x
         WHERE x.status = 'running' AND x.name IS NULL
           AND txn._lease_expired(x.id, p_grace_ms)
         FOR UPDATE SKIP LOCKED
    LOOP
        UPDATE txn.effects
           SET status = 'failed', completed_at = txn._now(), updated_at = txn._now(),
               error = pg_catalog.jsonb_build_object('name', 'AbandonedTransaction',
                       'message', 'the process running this transaction stopped')
         WHERE tx_id = t.id AND kind = 'call' AND status IN ('pending', 'running', 'retry_wait');
        PERFORM txn._orphan(t.id, '{}', NULL);
        DELETE FROM txn.keys WHERE tx_id = t.id;
        UPDATE txn.transactions
           SET status = 'abandoned', owner = NULL, updated_at = txn._now(),
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
    SELECT t.id, t.name, t.keys, t.owner, t.runs, l.lease_until, t.created_at, clock_timestamp() - t.created_at AS age
      FROM txn.transactions t LEFT JOIN txn.leases l ON l.tx_id = t.id WHERE t.status = 'running';

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
     WHERE t.status = 'running' AND t.created_at < txn._now() - interval '1 minute'
       AND txn._lease_expired(t.id, 60000);
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
    LOOP
        EXECUTE pg_catalog.format('ALTER FUNCTION %s SET search_path = pg_catalog, txn, pg_temp', f);
    END LOOP;
END $$;
