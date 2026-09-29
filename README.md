# 本地对象仓库（local-object-repo）

一个纯 Python 标准库实现的本地对象仓库：

- **Python API**：内容寻址对象、不可变快照、导出租约；
- **SQLite 元数据**：对象、快照引用、租约、GC 运行状态全部持久化；
- **两阶段垃圾回收器**：先“标记候选”，再“复核后删除文件”；两阶段之间
  若对象被新快照或未到期租约重新引用，删除自动放弃；进程中断后可安全
  重试，**绝不删除数据库仍引用的文件，也绝不让数据库指向已删除文件**。

只依赖 Python 3.11 标准库；运行测试需要 `pytest`（见 `requirements.txt`）。

---

## 快速开始

```python
from local_repo import ObjectRepository

repo = ObjectRepository("./my-repo")

# 1) 写入内容寻址对象（相同内容幂等）
oid_a = repo.put_object(b"hello")
oid_b = repo.put_object(b"world")

# 2) 创建不可变快照并发布
snap = repo.create_snapshot([oid_a, oid_b])
repo.publish_snapshot(snap)          # -> True；重复发布 -> False

# 3) 导出租约：未到期租约同样保护对象
lease = repo.add_lease(oid_a, ttl=3600)

# 4) 删除快照只是撤销根引用（对象仍在，直到 GC 确认不可达）
repo.delete_snapshot(snap)           # -> True；重复删除 -> False

# 5) 两阶段垃圾回收
report = repo.gc_collect()
print(report.deleted, report.kept)   # 被删数 / 被新引用救下数
```

### 可控时钟

租约到期判定需要时间。`ObjectRepository(root, clock=...)` 接受任何
返回浮点数秒的可调用对象；测试中使用单调可控时钟：

```python
class FakeClock:
    def __init__(self): self.t = 1000.0
    def __call__(self): return self.t
    def advance(self, s): self.t += s; return self.t

clock = FakeClock()
repo = ObjectRepository("./r", clock=clock)
repo.add_lease(oid, ttl=10)
clock.advance(10)          # 租约到期
```

---

## 存储布局

```
root/
├── repo.sqlite3            # SQLite 元数据（WAL 模式）
├── objects/
│   └── ab/abcd...（64 hex）# 按 SHA-256 前两位分片的对象文件
├── tmp/                    # put_object 落盘暂存（崩溃后清理）
└── graveyard/<run_id>/     # GC 第二阶段“先移出、后物理删除”的过渡目录
```

元数据表：

| 表 | 作用 |
| --- | --- |
| `objects` | 对象 OID、大小、创建时间 |
| `snapshots` / `snapshot_rows` | 不可变快照及其对象引用（根） |
| `export_leases` | 导出租约（OID + 到期时刻） |
| `gc_runs` / `gc_candidates` | GC 运行（marking/sweeping/done）与冻结的候选集 |

---

## 根引用与安全模型

**什么能保住对象**：被任意快照引用，或被未到期导出租约引用，即“可达”。

- 快照一经创建即不可变（没有修改内容的 API）。所有快照（含发布前的
  草稿）都持有根引用；`delete_snapshot` 才撤销引用。这样保证在 GC
  面前数据库永远不会指向一个已被删除的文件。`published` 标记只表示
  “发布”这一确定状态（发布幂等、并发发布结果确定）。
- 导出租约在创建时即保护对象；到期租约会在下一次 GC 标记时被清除，
  `release_lease` 可提前撤销。到期判定为闭区间：`expires_at <= now`
  即到期。

### 两阶段 GC

`gc_collect()` 在单个 `BEGIN IMMEDIATE` 事务 + 进程内锁的保护下工作：

1. **标记候选（已持久化）**：清除到期租约，把当前不可达的对象写入
   `gc_candidates`，运行状态置为 `sweeping` 并提交。候选集就此冻结。
2. **逐个复核后删除**：对每个候选重新判定当前可达性——
   - 已被新快照/未到期租约重新引用 → 标记 `kept`，**删除放弃**，
     文件原样保留；
   - 仍不可达 → 在同一事务中删除元数据行并标记 `deleted`，**提交后**
     才把文件移到 `graveyard/<run_id>/`。
   运行收尾时再统一物理删除 graveyard，最后清除无元数据的游离文件。

**阶段屏障**（测试钩子，锁外调用，避免与 API 死锁）：

- `on_after_mark(run_id, candidates)`：标记提交后、第二阶段前触发；
- `on_after_delete(run_id, oid)`：每删完一个对象后触发，用于在第二阶段
  中途加入引用——后续候选仍会被逐个重新复核。

```python
def after_mark(run_id, candidates):
    snap = repo.create_snapshot([candidates[0]])
    repo.publish_snapshot(snap)      # 两阶段之间救回对象

ObjectRepository(root, on_after_mark=after_mark).gc_collect()
# -> GCReport(kept=1, deleted=0)
```

### 崩溃序次与恢复

关键不变量有两条：

> A. 任何数据库行提交时，其对象文件都已经在 `objects/` 中；
> B. 任何文件的物理删除都发生在对应数据库变更提交之后。

- `put_object`：先写 `tmp/` 并 fsync，移入 `objects/` 后才提交元数据。
  最坏的崩溃结果是“有文件、无行”的游离文件（重开时清除），绝不会
  “有行、无文件”。
- 删除对象：先在事务中删 `objects` 行并提交，再移动文件到 graveyard，
  最后物理删除。
- 重开仓库时执行恢复 `_reconcile_graveyard()`：
  - graveyard 中**数据库仍引用**的文件 → 还原回 `objects/`
    （对应“删行事务尚未提交”的崩溃）；
  - **数据库已无引用**的残留 → 物理删除（对应“已提交、未清理”的崩溃）；
  - 同时清除 `tmp/` 暂存与 `objects/` 下游离文件。

因此中断后直接再次调用 `gc_collect()` 即可：它会接管处于
`sweeping` 的运行（`GCReport.resumed=True`），沿用冻结候选继续复核，
不会重复标记。

### 确定性行为汇总

| 情形 | 结果 |
| --- | --- |
| 获取不存在的对象 | 抛 `ObjectNotFound` |
| 操作不存在的快照 | 抛 `SnapshotNotFound`（删除返回 `False`） |
| 查询不存在的租约 | 抛 `LeaseNotFound`（释放返回 `False`） |
| 租约到期 | 下次 GC 标记时清除，对象随之可能回收 |
| 重复回收 | 每次是新运行；中断中的运行先被接管完成 |
| 并发发布 | 状态翻转只有一个赢家：首次 `True`，其余 `False` |
| 两阶段间重新引用 | 候选被 `kept`，文件绝不删除 |
| 外部删文件/内容损坏 | 读写与 `assert_consistent()` 抛 `CorruptionError` |

---

## 一致性自检

`repo.assert_consistent()` 核对数据库与文件目录：

1. 每个数据库对象都有内容（SHA-256）正确的文件；
2. `objects/` 下每个文件都有元数据行（GC 运行中途可传
   `gc_in_progress=True` 放宽 graveyard 检查）；
3. 快照行、租约引用的 OID 都存在；
4. 非 GC 期间 graveyard 为空。

测试在每个关键步骤之后都调用该自检。

---

## 测试

测试使用可控时钟与阶段屏障复现竞争，而不是依赖不稳定的真实竞态：

- `tests/test_api.py`：对象/快照/租约 API 与确定异常；
- `tests/test_gc.py`：两阶段 GC、到期租约、游离文件、重复回收；
- `tests/test_concurrency.py`：阶段屏障（含真实线程 + `Event`）复现
  “两阶段间发布/加租约”“第二阶段中途加租约”；
- `tests/test_recovery.py`：模拟标记后崩溃、提交前后崩溃、重开恢复、
  连续中断重试、外部损坏检测。

本仓库另外提供了一个**零依赖**冒烟脚本（无 pytest 时也可验证核心行为）：

```bash
python smoke_test.py
```

---

## Docker 验收

固定验收依次执行三条命令：

```bash
docker compose config --quiet      # 1. 校验 Compose 配置
docker compose build               # 2. 构建镜像
docker compose run --rm verify     # 3. 运行一次性 verify 测试服务
```

`verify` 服务在 `python:3.11-slim` 镜像中运行 `python -m pytest -v`，
结束即退出，`--rm` 自动清理。`docker-compose.yml` 同时定义了一个
`app` 服务，默认命令用于确认镜像可正常导入本包。

---

## API 速览

```python
ObjectRepository(root, clock=None, on_after_mark=None, on_after_delete=None)

# 对象
put_object(data: bytes) -> oid: str
get_object(oid) -> bytes                 # ObjectNotFound / CorruptionError
has_object(oid) -> bool
list_objects() -> list[str]

# 快照（不可变）
create_snapshot(oids) -> snapshot_id
publish_snapshot(snapshot_id) -> bool    # 幂等
is_published(snapshot_id) -> bool
read_snapshot(snapshot_id) -> list[str]
list_snapshots(include_drafts=True) -> list[str]
delete_snapshot(snapshot_id) -> bool     # 撤销根引用，幂等

# 导出租约
add_lease(oid, ttl) -> lease_id
get_lease(lease_id) -> LeaseInfo         # .is_live / .is_live_at(now)
release_lease(lease_id) -> bool
list_leases() -> list[LeaseInfo]

# 垃圾回收与自检
gc_collect() -> GCReport                 # resumed/candidates/deleted/kept/...
active_gc_run() -> str | None
assert_consistent(gc_in_progress=False)
close()
```
