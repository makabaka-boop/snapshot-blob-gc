"""仓库抛出的确定性异常。"""


class RepositoryError(Exception):
    """所有仓库异常的基类。"""


class ObjectNotFound(RepositoryError):
    """请求的对象不存在（数据库与文件目录中均无该对象）。"""


class SnapshotNotFound(RepositoryError):
    """请求的快照不存在。"""


class LeaseNotFound(RepositoryError):
    """请求的租约不存在（已释放或已到期清除）。"""


class CorruptionError(RepositoryError):
    """数据库与文件目录之间出现不可调和的不一致。

    正常流程（含垃圾回收中断、并发发布）不会产生这种状态；它表示
    外部干预或存储介质故障，例如数据库仍引用的文件丢失、文件内容
    与内容寻址散列不符。
    """
