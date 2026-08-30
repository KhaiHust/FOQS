--liquibase formatted sql

--changeset foqs:001-create-queue-messages-table
CREATE TABLE IF NOT EXISTS queue_messages (
                                              id BINARY(16) NOT NULL,
    topic VARCHAR(64) NOT NULL,
    priority INT NOT NULL DEFAULT 0,
    payload BLOB NOT NULL,
    status TINYINT NOT NULL DEFAULT 0,         -- 0: READY, 1: LEASED, 2: COMPLETED
    deliver_after TIMESTAMP(3) NOT NULL,
    lease_until TIMESTAMP(3) NULL,
    retry_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    INDEX idx_fetch_priority (topic, status, deliver_after, priority ASC, id ASC),
    INDEX idx_reclaim (status, lease_until)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--rollback DROP TABLE IF EXISTS queue_messages;