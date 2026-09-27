-- 0002_partition_epoch: fencing at the effect (spec 7.2, G14; design-notes 8.7).
-- A partition lease lives in Redis, and the lease store cannot close the window
-- between Redis forgetting the key and the old owner's next renewal tick: a new
-- owner acquires the partition at once with a higher fencing token while the
-- old one keeps reading and applying. The token is therefore checked where the
-- effect happens. A consumer transaction records its token here
-- (pg.Tx.FencePartition) before the inbox insert; a token below the stored
-- epoch is rejected, so a stale owner cannot commit after a newer owner has,
-- and the row lock serializes the two owners' transactions.

CREATE TABLE mediator_partition_epoch (
    consumer_group TEXT        NOT NULL,
    topic          TEXT        NOT NULL,
    partition      SMALLINT    NOT NULL,
    epoch          BIGINT      NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, topic, partition)
);

-- down
DROP TABLE IF EXISTS mediator_partition_epoch;
