# 本地对象仓库（objstore）

一个基于 **Python API + SQLite 元数据 + 内容寻址文件目录** 的本地对象仓库，核心提供：

- **不可变快照**：快照引用一组对象，发布后即根（root），快照的对象不会被回收；
- **导出租约（lease）**：带到期时间的临时根，未到期租约保护其对象，到期自动失效；
- **两阶段垃圾回收器**：先「标记候选」，再「复核后删除文件」，两阶段之间被新快照或
  未到期租约重新引用的候选一律放弃删除；
- **崩溃安全**：删除顺序严格为「先提交元数据删除，再删文件」。进程在任意时刻被杀死后
  都可以安全重试，绝不会删掉数据库仍引用的文件，也绝不会让数据库指向已删除的文件；
- **确定性行为**：对象不存在、租约过期、重复回收、与回收并发的发布都有明确结果。

## 目录布局

```
objstore/
  __init__.py        # 公共 API
  repo.py            # Repository：对象/快照/租约/两阶段 GC
  clocks.py          # Clock 抽象（SystemClock / 测试用可控时钟）
  hooks.py           # GC 阶段钩子（屏障、崩溃注入），不参与正确性
  exceptions.py      # 确定性异常类型
  diagnostics.py     # 数据库 ↔ 文件目录一致性核对
  schema.sql         # SQLite 模式
tests/
  conftest.py                # FakeClock、带阶段屏障的 ScriptedHooks
  test_basic.py              # 基础语义与错误结果
  test_gc.py                 # 两阶段回收与并发竞争
  test_crash_recovery.py     # 子进程 SIGKILL 后的安全重试
  crash_worker.py            # 一次性崩溃工作进程
Dockerfile
docker-compose.yml         # 唯一服务 verify：一次性 pytest
requirements.txt
```

## API 快速上手

```python
from datetime import timedelta
from objstore import Repository, SystemClock

repo = Repository("./data", clock=SystemClock())

a = repo.put(b"hello")                 # 内容寻址：SHA-256 即对象 id
repo.get(a)                            # b"hello"
repo.exists(a)

sid = repo.publish_snapshot([a])       # 发布不可变快照（相同集合幂等）
repo.delete_snapshot(sid)              # 只撤销根引用，不删对象

lid = repo.grant_lease([a], ttl=timedelta(hours=1))   # 导出租约
repo.revoke_lease(lid)                 # 提前撤销（也可等其自动到期）

mark = repo.gc_mark()                  # 阶段一：标记候选（不删文件）
mark.candidate_ids
result = repo.gc_sweep(mark.run_id)    # 阶段二：逐个复核后删除
result.reclaimed                       # 确认无引用、已删除
result.rescued                         # 两阶段间被重新引用、放弃删除

repo.gc()                              # 便捷方法：标记+复核一把跑完
```

### 确定性错误

| 情况 | 结果 |
| --- | --- |
| 读取/引用不存在的对象 | `ObjectNotFoundError`（不写入任何数据） |
| 删除不存在的快照 | `SnapshotNotFoundError` |
| 使用不存在的租约 | `LeaseNotFoundError`；重复 `revoke_lease` 返回 `False` |
| 对不存在的 run 复核 | `UnknownGCRunError`；重复 run id 标记 → `DuplicateGCRunError` |
| 对同一 run 重复 `gc_sweep` | 幂等：已删除的不再触碰，结果不变 |

## 垃圾回收的两阶段与崩溃安全

**阶段一 `gc_mark`**（单事务）：清理已到期租约 → 计算存活集
（所有已发布快照 ∪ 所有未到期租约引用的对象）→ 其余对象作为候选写入
`gc_candidates`，状态 `candidate`。此阶段不删除任何文件。

**阶段二 `gc_sweep`**：对每个候选开启一个 `BEGIN IMMEDIATE` 写事务，按
**标记时刻** 的存活集语义重新复核：

1. 被任何快照引用（快照没有时间限制）、或在标记时刻仍未到期的租约引用、或
   两阶段之间/本次扫描期间新获得的根引用 → 置为 `rescued`，绝不删除；
   （标记时有效的租约即使在复核窗口内到期也不会删除对象，到期清理留给下一次
   回收——GC 绝不删除“标记时仍被根引用”的对象。）
2. 复核通过 → 在事务内删除 `objects` 行（外键 `ON DELETE RESTRICT` 兜底）、
   候选置 `reclaimed` → **提交**；
3. 提交之后才删除对象文件。

这样，崩溃的后果只有两种，且都可由重试修复：

- 崩溃在提交之前：事务整体回滚，文件与数据库都保持原状；
- 崩溃在提交之后、删文件之前：只剩一个**数据库不再引用**的待清理文件
  （由 `reclaimed` 候选跟踪），重试 sweep 时补删。

**并发发布的确定性**：在删除窗口并发的 `publish_snapshot` 必须等待 IMMEDIATE 写锁；
拿到锁时元数据删除已经提交、对象行不可见，因此发布确定性地收到
`ObjectNotFoundError`，且数据库里不会出现指向已删除文件的快照引用。若发布发生在
复核之前（两阶段之间），候选转为 `rescued` 而被保留。

## 一致性核对

`objstore.check_consistency(repo)` 双向核对数据库与 `objects/` 目录：

- 数据库有行但文件缺失（指向已删除文件）；
- 文件存在但没有数据库行（且不在 reclaimed 待清理集合中的孤儿文件）；
- 快照/租约引用了不存在的对象；
- 回收审计状态矛盾、残留临时文件等。

测试在**每个关键步骤之后**都调用该检查。

## 测试设计：可控时钟与阶段屏障

- `FakeClock`：冻结/可步进时钟，复现「租约在标记与复核之间到期」而无需真实睡眠；
- `ScriptedHooks`：在 `mark_begin / mark_end / sweep_begin / before_reclaim /
  after_reclaim` 注入回调；测试用 `threading.Barrier` 让并发线程恰好在
  「两阶段之间」或「删除窗口」与回收线程会合；
- `crash_worker.py`：真实子进程在标记前、标记后、元数据删除提交前、文件删除前
  被 `SIGKILL`，随后由新进程打开仓库核对一致性并重试。

## 验收（固定三步，依次执行）

```bash
docker compose config --quiet     # 1. 校验 compose 文件
docker compose build              # 2. 构建 verify 镜像
docker compose run --rm verify    # 3. 一次性运行全部 pytest，退出码即验收结果
```

`verify` 服务是一次性的：pytest 全部通过则容器退出码为 0，`--rm` 自动清理。

本地不使用 Docker 时也可直接运行：

```bash
pip install -r requirements.txt
python -m pytest -v
```
