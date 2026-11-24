"""工作进程监管与换版编排（与 FastAPI 同进程）。

换版流程（有界分批交接，监管进程额外 fd 峰值不超过 :data:`FD_BUDGET`）
=====================================================================
1. 向旧工作进程发 ``SNAPSHOT``；旧进程冻结全部连接 I/O，交回**纯状态**快照
   （半行/未发回执/版号，分块记录，不附带任何 fd）。
2. 容量预检：监管进程自身可用 fd 余量至少 :data:`FD_BUDGET` (32)；
   新工作进程的 nofile 须容得下全部在线连接加预留余量。不满足则发
   ``RESUME`` 让旧进程立即解冻，接口返回 503，旧版服务不受影响。
3. fork+exec 新工作进程，连接 fd 按每批 :data:`FD_BATCH` (31) 个接力中转：
   令旧进程发一批 -> 本进程收一个立即转发给新进程并关闭中转副本 ->
   批末收 ``FDS_DONE``。任一时刻本进程额外持有的 fd 至多为
   1 个新控制通道 + 31 个连接 fd = 32，与在线连接总数无关。
4. 全部 fd 与状态交接完、新进程回 ``READY`` 后：令旧进程 ``QUIT`` 退出并
   回收，再发 ``GO`` 放行新进程，保证任一时刻同一连接只有一个进程在操作。
5. 交接中断（超时/新进程接管前退出/协议错误）：杀掉新进程、关闭全部中转
   fd 副本、发 ``RESUME`` 并等旧进程回 ``RESUMED``；旧进程沿原连接按原版
   恢复服务，本轮资源释放干净，可再次发起换版。
"""
from __future__ import annotations

import os
import resource
import socket
import sys
import threading
import time
from dataclasses import dataclass

from .netutil import control_pair, recv_fd, send_fd

HANDSHAKE_TIMEOUT = 10.0

# 换版期间监管进程最多额外占用的 fd 数，也是换版要求的最小可用余量
FD_BUDGET = 32
# 单批接力中转的连接 fd 数：31 个中转 fd + 1 个新控制通道 = 32
FD_BATCH = 31
# 状态快照/移交单条记录承载的连接条目数（每条目约数百字节，远小于
# SEQPACKET 套接字缓冲，避免大记录被 EAGAIN 拆分）
STATE_CHUNK = 8
# 新工作进程除 N 个连接 fd 外的预留（stdio/控制通道/监听/epoll/新接入余量）
WORKER_FD_RESERVE = 16


class ReloadFailed(RuntimeError):
    """新工作进程启动失败，旧进程仍在服务。"""


@dataclass
class WorkerHandle:
    pid: int
    control: socket.socket
    offset: int
    revision: int
    started_at: float
    alive: bool = True
    exitcode: int | None = None
    superseded: bool = False  # 换版成功后旧进程置 True，退出属预期


class Supervisor:
    def __init__(self, host: str, tcp_port: int, initial_offset: int = 0):
        self.host = host
        self.tcp_port = tcp_port
        self.offset = initial_offset
        self.revision = 1
        self.lock = threading.RLock()
        self.worker: WorkerHandle | None = None
        self._listen_sock: socket.socket | None = None
        # 测试/部署可调：监管进程换版要求的 fd 余量地板（默认 32）
        self.fd_floor = int(os.environ.get("FD_HEADROOM_FLOOR", str(FD_BUDGET)))
        # 上一轮换版中本进程额外占用 fd 的高水位（对外可观测，便于自测）
        self.reload_extra_fd_peak = 0
        self._apply_supervisor_rlimit()

    # ---------------- rlimit / fd 计量 ----------------
    @staticmethod
    def _apply_supervisor_rlimit() -> None:
        """``SUPERVISOR_NOFILE`` 存在时压低本进程软 nofile（受限环境模拟/调优）。"""
        target = int(os.environ.get("SUPERVISOR_NOFILE", "0"))
        if target <= 0:
            return
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(
            resource.RLIMIT_NOFILE, (min(target, hard), hard)
        )

    @staticmethod
    def _open_fd_count() -> int:
        """当前进程打开的 fd 数。Linux 优先读 /proc，失败再线性探测。"""
        try:
            return len(os.listdir("/proc/self/fd"))
        except OSError:
            soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
            if soft == resource.RLIM_INFINITY:
                soft = 1 << 20
            count = 0
            for fd in range(soft):
                try:
                    os.fstat(fd)
                except OSError:
                    continue
                count += 1
            return count

    @classmethod
    def _available_fds(cls) -> int:
        soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft == resource.RLIM_INFINITY:
            return 1 << 30
        return max(0, soft - cls._open_fd_count())

    @staticmethod
    def _worker_soft_limit() -> int:
        """新工作进程握手前会生效的软 nofile。

        工作进程可经 WORKER_NOFILE_HARD/SOFT 调整自身限制；这里按同样的
        规则推导，保证容量预检与子进程实际生效值一致。
        """
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        hard_target = int(os.environ.get("WORKER_NOFILE_HARD", "0"))
        if hard_target > 0 and hard_target < hard:
            hard = hard_target
            soft = min(soft, hard)
        soft_target = int(os.environ.get("WORKER_NOFILE_SOFT", "0"))
        if soft_target > 0:
            soft = min(soft_target, hard)
        return soft

    def _check_worker_capacity(self, n_conns: int) -> None:
        """新工作进程容量预检：nofile 须容得下 ``n_conns`` 个连接加预留。"""
        worker_soft = self._worker_soft_limit()
        need = n_conns + WORKER_FD_RESERVE
        if worker_soft < need:
            raise ReloadFailed(
                f"worker fd capacity {worker_soft} < {need} "
                f"for {n_conns} live conns"
            )

    # ---------------- 生命周期 ----------------
    def start(self) -> None:
        listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listen.bind((self.host, self.tcp_port))
        listen.listen(128)
        self._listen_sock = listen
        self.worker = self._spawn(
            offset=self.offset, revision=self.revision, entries=[]
        )
        self._start_watcher(self.worker)

    def shutdown(self) -> None:
        with self.lock:
            worker = self.worker
            if worker is None:
                return
            worker.superseded = True
            try:
                send_fd(worker.control, {"type": "SHUTDOWN"}, None)
            except OSError:
                pass
            worker.control.close()
            self.worker = None

    # ---------------- 查询 ----------------
    def status(self) -> dict:
        with self.lock:
            w = self.worker
            soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
            return {
                "pid": w.pid if w else None,
                "alive": bool(w and w.alive),
                "offset": self.offset,
                "revision": self.revision,
                "tcp_port": self.tcp_port,
                "started_at": w.started_at if w else None,
                "fd_soft_limit": None if soft == resource.RLIM_INFINITY else soft,
                "fd_open": self._open_fd_count(),
                "fd_headroom": self._available_fds(),
                "reload_extra_fd_peak": self.reload_extra_fd_peak,
            }

    # ---------------- 换版 ----------------
    def reload(self, new_offset: int) -> dict:
        with self.lock:
            old = self.worker
            if old is None:
                raise ReloadFailed("no active worker")
            new_revision = self.revision + 1

            # 0) 监管进程余量预检：不足则在冻结旧进程之前直接拒绝，
            #    线上服务完全不受影响（旧进程不会被冻住、旧连接不会停）。
            if self._available_fds() < self.fd_floor:
                raise ReloadFailed(
                    f"supervisor fd headroom {self._available_fds()} "
                    f"< required {self.fd_floor}"
                )

            # 1) 纯状态快照（不传 fd，本进程不额外占用连接 fd）
            try:
                entries = self._recv_snapshot(old)
            except (OSError, EOFError, TimeoutError) as exc:
                # 旧进程可能已进入冻结态：主动解冻，避免其冻到自我超时
                self._resume_old(old)
                raise ReloadFailed(f"snapshot failed: {exc!r}") from exc

            # 2) 新进程容量预检（用快照得到的准确连接数）：新工作进程的
            #    nofile 须容得下这批连接加预留。旧进程能稳定服务这批连接，
            #    本身即证明其容量足够，无需对旧进程预检。
            try:
                self._check_worker_capacity(len(entries))
            except ReloadFailed:
                self._resume_old(old)
                raise

            # 3) 拉起新进程并分批接力 fd，任何失败都回退旧进程
            peak_base = self._open_fd_count()
            self.reload_extra_fd_peak = 0
            try:
                new = self._spawn(
                    offset=new_offset,
                    revision=new_revision,
                    entries=entries,
                    fd_source=old,
                    go_now=False,
                    peak_base=peak_base,
                )
            except ReloadFailed:
                self._resume_old(old)
                raise

            # 4) 旧进程退出 -> 回收 -> 放行新进程。
            #    严格串行，任何时刻同一连接 fd 只有一个工作进程在操作。
            old.superseded = True
            try:
                old.control.settimeout(HANDSHAKE_TIMEOUT)
                send_fd(old.control, {"type": "QUIT"}, None)
            except OSError:
                pass
            old.control.close()
            self._wait_exit(old.pid, timeout=3.0)
            try:
                new.control.settimeout(HANDSHAKE_TIMEOUT)
                send_fd(new.control, {"type": "GO"}, None)
                new.control.setblocking(False)
            except OSError as exc:
                # 极端情况：放行失败，新进程由 watcher 兜底重拉
                raise ReloadFailed(f"GO failed: {exc!r}") from exc

            self.offset = new_offset
            self.revision = new_revision
            self.worker = new
            self._start_watcher(new)
            return {
                "old_pid": old.pid,
                "pid": new.pid,
                "offset": new_offset,
                "revision": new_revision,
                "inherited_conns": len(entries),
            }

    def _recv_snapshot(self, old: WorkerHandle) -> list[dict]:
        """接收旧进程的分块纯状态快照，返回与 fd 顺序一一对应的条目列表。"""
        old.control.settimeout(HANDSHAKE_TIMEOUT)
        send_fd(old.control, {"type": "SNAPSHOT"}, None)
        entries: list[dict] = []
        n = None
        while True:
            msg, stray_fd = recv_fd(old.control)
            if stray_fd is not None:  # 防御：快照阶段不应携带 fd
                os.close(stray_fd)
            if msg.get("type") != "SNAPSHOT_STATE":
                raise ReloadFailed(f"bad snapshot header: {msg!r}")
            n = int(msg.get("n", -1))
            entries.extend(msg.get("conns", []))
            if not msg.get("more"):
                break
        if n != len(entries):
            raise ReloadFailed(
                f"snapshot count mismatch: n={n} entries={len(entries)}"
            )
        return entries

    def _resume_old(self, old: WorkerHandle) -> None:
        """令冻结中的旧进程解冻并确认；旧进程沿原连接按原版恢复服务。"""
        try:
            old.control.settimeout(HANDSHAKE_TIMEOUT)
            send_fd(old.control, {"type": "RESUME"}, None)
            try:
                msg, stray_fd = recv_fd(old.control)  # 期望 RESUMED
                if stray_fd is not None:
                    os.close(stray_fd)
                if msg.get("type") != "RESUMED":
                    sys.stderr.write(
                        f"[supervisor] unexpected resume reply: {msg!r}\n"
                    )
            except (OSError, EOFError, TimeoutError):
                # 旧进程可能已失联：交由 watcher 的崩溃恢复兜底
                pass
            old.control.setblocking(False)
        except OSError:
            pass

    @staticmethod
    def _wait_exit(pid: int, timeout: float) -> None:
        """有界等待子进程退出；watcher 线程可能已 waitpid 收走。"""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                reaped, _ = os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                return
            if reaped == pid:
                return
            time.sleep(0.02)

    # ---------------- 进程拉起与握手 ----------------
    def _spawn(
        self,
        offset: int,
        revision: int,
        entries: list[dict],
        fd_source: WorkerHandle | None = None,
        go_now: bool = True,
        peak_base: int | None = None,
    ) -> WorkerHandle:
        """fork+exec 工作进程并完成握手。

        ``entries`` 为连接状态条目（顺序即连接序号）；``fd_source`` 为冻结中的
        旧进程时，连接 fd 按 :data:`FD_BATCH` 分批接力中转；为 None 时（初次
        启动/崩溃重拉）没有连接需要继承。
        """
        assert self._listen_sock is not None
        n = len(entries)
        parent_ctrl, child_ctrl = control_pair()
        env = dict(os.environ)
        env.update(
            {
                "WORKER_CONTROL_FD": str(child_ctrl.fileno()),
                "WORKER_OFFSET": str(offset),
                "WORKER_REVISION": str(revision),
            }
        )
        # 确保 exec 出的 worker 能 import 到与本进程相同的依赖路径
        # （Docker 镜像内为系统 site-packages，此项为空；本地 /target 安装时必需）
        extra = [p for p in sys.path if p and os.path.isdir(p)]
        if extra:
            env["PYTHONPATH"] = os.pathsep.join(
                [env.get("PYTHONPATH", "")] + extra
            ).strip(os.pathsep)

        pid = os.fork()
        if pid == 0:  # 子进程
            self._child_exec(child_ctrl, parent_ctrl, env)

        # 父进程
        parent_ctrl.settimeout(HANDSHAKE_TIMEOUT)
        child_ctrl.close()
        handle = WorkerHandle(
            pid=pid,
            control=parent_ctrl,
            offset=offset,
            revision=revision,
            started_at=time.time(),
        )
        held: list[int] = []  # 本进程当前持有的中转 fd，异常时确保全部释放
        try:
            send_fd(
                parent_ctrl,
                {"type": "HELLO", "conns": n},
                self._listen_sock.fileno(),
            )

            # 3a) 分批接力（逐批流控，防 ETOOMANYREFS）：
            #     每批从旧进程收 FD，收一个立即转发给新进程并关闭本进程副本，
            #     批末收旧进程 FDS_DONE，并等新进程回 FDS_ACK（确认这批 fd 已
            #     被 recvmsg 取走、不再积压在 unix socket 缓冲）后才发下一批。
            #     因此在途/中转 fd 恒 <= FD_BATCH，本进程额外占用与连接总数
            #     无关，也不会触及内核对在途 SCM_RIGHTS 引用数的上限。
            if fd_source is not None and n:
                for start in range(0, n, FD_BATCH):
                    count = min(FD_BATCH, n - start)
                    send_fd(
                        fd_source.control,
                        {"type": "GIVE_FDS", "start": start, "count": count},
                        None,
                    )
                    for _ in range(count):
                        msg, fd = recv_fd(fd_source.control)
                        if msg.get("type") != "FD" or fd is None:
                            raise ReloadFailed(
                                f"bad fd in relay batch start={start}: {msg!r}"
                            )
                        held.append(fd)
                        if peak_base is not None:
                            extra = self._open_fd_count() - peak_base
                            self.reload_extra_fd_peak = max(
                                self.reload_extra_fd_peak, extra
                            )
                        send_fd(parent_ctrl, {"type": "FD"}, fd)
                        os.close(fd)
                        held.pop()
                    done, stray = recv_fd(fd_source.control)
                    if stray is not None:
                        os.close(stray)
                    if done.get("type") != "FDS_DONE":
                        raise ReloadFailed(f"bad FDS_DONE: {done!r}")
                    ack, stray2 = recv_fd(parent_ctrl)
                    if stray2 is not None:
                        os.close(stray2)
                    if ack.get("type") != "FDS_ACK":
                        raise ReloadFailed(f"bad FDS_ACK: {ack!r}")

            # 3b) 状态分块移交（无 fd，记录顺序与上面的 fd 序号一一对应）
            if n == 0:
                send_fd(
                    parent_ctrl,
                    {"type": "STATE", "n": 0, "chunk": 0, "more": False,
                     "conns": []},
                    None,
                )
            else:
                for i in range(0, n, STATE_CHUNK):
                    part = entries[i : i + STATE_CHUNK]
                    send_fd(
                        parent_ctrl,
                        {
                            "type": "STATE",
                            "n": n,
                            "chunk": i // STATE_CHUNK,
                            "more": i + STATE_CHUNK < n,
                            "conns": part,
                        },
                        None,
                    )

            msg, _ = recv_fd(parent_ctrl)
            if msg.get("type") != "READY":
                raise ReloadFailed(f"bad READY: {msg!r}")
            # 初次启动/崩溃重拉直接 GO；换版时由 reload() 在旧进程退出后再 GO，
            # 保证同一连接 fd 不会被两个进程同时操作。
            if go_now:
                send_fd(parent_ctrl, {"type": "GO"}, None)
        except (OSError, EOFError, TimeoutError) as exc:
            self._abort_child(handle)
            for fd in held:  # 异常路径兜底：释放所有中转副本
                try:
                    os.close(fd)
                except OSError:
                    pass
            raise ReloadFailed(f"worker handshake failed: {exc!r}") from exc
        finally:
            try:
                parent_ctrl.setblocking(False)
            except OSError:
                pass
        return handle

    @staticmethod
    def _child_exec(
        child_ctrl: socket.socket, parent_ctrl: socket.socket, env: dict
    ) -> None:
        try:
            parent_ctrl.close()
            # PEP 446：所有 socket fd 默认 CLOEXEC，execve 时自动关闭，
            # 因此只需把控制通道标记为可继承；监听/连接 fd 一律经
            # SCM_RIGHTS 在 exec 之后的新进程握手阶段显式转交。
            os.set_inheritable(child_ctrl.fileno(), True)
            os.execve(
                sys.executable,
                [sys.executable, "-m", "app.main", "worker"],
                env,
            )
        except BaseException:  # pragma: no cover - 子进程兜底
            os._exit(127)

    @staticmethod
    def _abort_child(handle: WorkerHandle) -> None:
        try:
            handle.control.close()
        except OSError:
            pass
        try:
            pid, _ = os.waitpid(handle.pid, os.WNOHANG)
            if pid == 0:
                import signal

                os.kill(handle.pid, signal.SIGKILL)
                os.waitpid(handle.pid, 0)
        except ChildProcessError:
            pass
        handle.alive = False

    # ---------------- 崩溃看护（预期外退出时同版重拉） ----------------
    def _start_watcher(self, handle: WorkerHandle) -> None:
        t = threading.Thread(
            target=self._watch, args=(handle,), name=f"watch-{handle.pid}", daemon=True
        )
        t.start()

    def _watch(self, handle: WorkerHandle) -> None:
        try:
            _, status = os.waitpid(handle.pid, 0)
        except ChildProcessError:
            return
        handle.alive = False
        handle.exitcode = os.waitstatus_to_exitcode(status)
        if handle.superseded:
            return
        sys.stderr.write(
            f"[supervisor] worker pid={handle.pid} died unexpectedly "
            f"(exit={handle.exitcode}), respawning same revision\n"
        )
        with self.lock:
            if self.worker is not handle:
                return
            try:
                fresh = self._spawn(
                    offset=handle.offset,
                    revision=handle.revision,
                    entries=[],
                )
                self.worker = fresh
                self._start_watcher(fresh)
            except ReloadFailed as exc:
                sys.stderr.write(f"[supervisor] respawn failed: {exc!r}\n")
