-- 本地对象仓库的 SQLite 元数据模式。
--
-- 对象 (objects) 与文件 (objects/ab/cdef...) 一一对应；
-- 快照 (snapshots) 是不可变根集合；租约 (leases) 是带到期时间的临时根。
-- gc_runs / gc_candidates 记录两阶段回收，candidates 刻意不使用外键，
-- 以便对象行被删除后仍保留审计记录。

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT OR IGNORE INTO schema_meta(key, value) VALUES ('version', '1');

CREATE TABLE IF NOT EXISTS objects (
    id          TEXT PRIMARY KEY CHECK (length(id) = 64),
    size        INTEGER NOT NULL CHECK (size >= 0),
    created_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    id         TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshot_objects (
    snapshot_id TEXT NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
    object_id   TEXT NOT NULL REFERENCES objects(id)   ON DELETE RESTRICT,
    PRIMARY KEY (snapshot_id, object_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS leases (
    id         TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS lease_objects (
    lease_id  TEXT NOT NULL REFERENCES leases(id) ON DELETE CASCADE,
    object_id TEXT NOT NULL REFERENCES objects(id) ON DELETE RESTRICT,
    PRIMARY KEY (lease_id, object_id)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_leases_expires_at ON leases(expires_at);

CREATE TABLE IF NOT EXISTS gc_runs (
    id           TEXT PRIMARY KEY,
    started_at   INTEGER NOT NULL,
    finished_at  INTEGER,
    status       TEXT NOT NULL CHECK (status IN ('marked', 'swept'))
);

CREATE TABLE IF NOT EXISTS gc_candidates (
    run_id       TEXT NOT NULL REFERENCES gc_runs(id) ON DELETE CASCADE,
    object_id    TEXT NOT NULL,
    state        TEXT NOT NULL CHECK (state IN ('candidate', 'reclaimed', 'rescued')),
    marked_at    INTEGER NOT NULL,
    reclaimed_at INTEGER,
    PRIMARY KEY (run_id, object_id)
) WITHOUT ROWID;
