"""温度校正工作进程。

协议与缓冲
----------
1. 探头通过 TCP 长连接上报整数温度，每行至多 32 字节（不含 ``\\n``）。
2. 每条连接维护独立的应用层接收/发送缓冲，各上限 :data:`MAX_BUFFER` (128B)。
   接收缓冲满时只暂停该连接的读入（poll 摘掉 POLLIN），不影响其他连接。
3. 完整一行（收到 ``\\n``）生成且只生成一次回执
   ``校正后整数温度,版号\\n``，校正值 = 上报整数 + 当前偏移。

换版语义（与 supervisor 配合，有界分批交接）
---------------------------------------------
- SNAPSHOT 时工作进程冻结全部连接 I/O，**只回传应用层缓冲状态**
  （半行 recv_buf、未发 send_buf、带版号的 pending 队列），分块发送、
  不附带任何 fd，因此监管进程在快照阶段不额外占用连接 fd。
- 之后旧进程保持冻结，按监管进程的 ``GIVE_FDS`` 指令**分批**经 SCM_RIGHTS
  交连接 fd（每批 31 个，批末回 ``FDS_DONE``）；连接对象与 fd 始终保留在
  旧进程中，直到收到 ``QUIT`` 才退出。
- 收到 ``RESUME``（新进程失败/容量不足/交接中断）：重新注册监听与全部
  存活连接、解冻 I/O，并回 ``RESUMED`` 确认；沿原连接按原版继续服务。
  由此满足：
    * 换版前已收到换行符的行，回执保留旧版号；
    * 只收到半行的，缓冲状态随交接交给新进程，补齐后按新版回执；
    * 探头停读或只发半行都不阻塞换版——旧进程直接退出，
      内核 socket 与应用缓冲分别由 fd 继承和状态移交保留；
    * 探头无需重连，其他探头照常收回执。
"""
from __future__ import annotations

import base64
import os
import re
import resource
import select
import socket
import sys
import time
from dataclasses import dataclass, field

from .netutil import recv_fd, send_fd

MAX_LINE_BODY = 32          # 每行至多 32 字节（不含换行符）
MAX_BUFFER = 128            # 每条连接应用层收/发缓冲各最多 128 字节
HANDSHAKE_TIMEOUT = 10.0
FROZEN_VERDICT_DEADLINE = 30.0  # 冻结后等待 supervisor 裁决的总时限
# 与 supervisor.FD_BATCH / STATE_CHUNK 对应的协议常量
FD_BATCH = 31
STATE_CHUNK = 8
_INT_RE = re.compile(rb"^[+-]?\d+$")


@dataclass
class Conn:
    sock: socket.socket
    recv_buf: bytearray = field(default_factory=bytearray)
    send_buf: bytearray = field(default_factory=bytearray)
    # 已从 recv_buf 切出、等待进入发送缓冲的回执：(回执字节, 生成时版号)
    pending: list[tuple[bytes, int]] = field(default_factory=list)
    read_paused: bool = False
    eof: bool = False
    closed: bool = False

    def __post_init__(self) -> None:
        # 缓存 fd：socket.close() 后 fileno() 会返回 -1，而关闭路径
        # （_reap/_poll_modify）仍需用稳定的整数 key 操作 poll 与 conns 字典
        self._fd = self.sock.fileno()

    @property
    def fd(self) -> int:
        return self._fd

    def mask(self) -> int:
        if self.closed:
            return 0
        mask = 0
        if not self.read_paused and not self.eof:
            mask |= select.POLLIN
        if self.send_buf:
            mask |= select.POLLOUT
        return mask


def _parse_int(line: bytes) -> int | None:
    try:
        text = line.strip()
    except Exception:  # pragma: no cover
        return None
    if not _INT_RE.match(text):  # 严格整数：可选符号 + 纯数字，拒绝 1_0、abc 等
        return None
    try:
        return int(text)
    except ValueError:
        return None


class Worker:
    def __init__(
        self,
        listen_sock: socket.socket,
        control: socket.socket,
        offset: int,
        revision: int,
    ):
        self.offset = offset
        self.revision = revision
        self.control = control
        self.listen_sock = listen_sock
        self.poller = select.poll()
        self.conns: dict[int, Conn] = {}
        self.frozen = False
        self.poller.register(self.control.fileno(), select.POLLIN)
        self.poller.register(self.listen_sock.fileno(), select.POLLIN)

    # ---------------- 主循环 ----------------
    def run(self) -> None:
        while True:
            for fd, events in self.poller.poll(500):
                self._dispatch(fd, events)
            self._reap()

    def _dispatch(self, fd: int, events: int) -> None:
        if fd == self.control.fileno():
            self._on_control()
            return
        if events & (select.POLLERR | select.POLLHUP) and not (
            events & select.POLLIN
        ):
            # 无数据可读且对端挂断/出错：尽力发完缓冲后关闭
            if fd in self.conns:
                conn = self.conns[fd]
                if not self._flush_send(conn):
                    conn.closed = True
            return
        if fd == self.listen_sock.fileno():
            self._on_accept()
        elif fd in self.conns:
            try:
                self._on_conn(self.conns[fd], events)
            except (ConnectionError, OSError) as exc:
                # 单个探头的 RST/错误只关闭该连接，绝不影响工作进程与其他探头
                conn = self.conns.get(fd)
                self._log(f"fd={fd} connection error: {exc!r}")
                if conn is not None:
                    conn.closed = True

    # ---------------- 控制通道 ----------------
    def _on_control(self) -> None:
        try:
            msg, _ = recv_fd(self.control)
        except EOFError:
            os._exit(0)
        kind = msg.get("type")
        if kind in ("QUIT", "SHUTDOWN"):
            self._log(f"{kind} -> exit")
            os._exit(0)
        if kind == "SNAPSHOT":
            self._handle_snapshot()
        else:
            self._log(f"unknown control message: {msg!r}")

    def _handle_snapshot(self) -> None:
        # 冻结：先停 accept，再逐连接停 poll 并排空内核中已到达的数据。
        # 这样“换版前已收到换行符”的行一定在旧进程生成旧版回执；
        # 排空之后才到达的字节属于换版后，半行补齐按新版回执。
        self.frozen = True
        self.control.setblocking(True)
        self.poller.unregister(self.listen_sock.fileno())

        live: list[Conn] = []
        entries: list[dict] = []
        for conn in list(self.conns.values()):
            if conn.closed:
                continue
            self._poll_unregister(conn.fd)
            # 已摘除 poll，不会再有事件驱动读入；显式排到 EAGAIN 为止
            self._read_conn(conn)
            self._enqueue_lines(conn)
            live.append(conn)
            entries.append(
                {
                    "recv": base64.b64encode(bytes(conn.recv_buf)).decode(),
                    "send": base64.b64encode(bytes(conn.send_buf)).decode(),
                    "pending": [
                        [base64.b64encode(reply).decode(), rev]
                        for reply, rev in conn.pending
                    ],
                    "eof": conn.eof,
                    "paused": conn.read_paused,
                }
            )

        # 纯状态分块快照：不附带 fd，监管进程快照阶段不额外占用连接 fd
        n = len(live)
        if n == 0:
            send_fd(
                self.control,
                {"type": "SNAPSHOT_STATE", "n": 0, "more": False, "conns": []},
                None,
            )
        else:
            for i in range(0, n, STATE_CHUNK):
                part = entries[i : i + STATE_CHUNK]
                send_fd(
                    self.control,
                    {
                        "type": "SNAPSHOT_STATE",
                        "n": n,
                        "more": i + STATE_CHUNK < n,
                        "conns": part,
                    },
                    None,
                )
        self._log(f"snapshot state sent: {n} live conns (no fds)")

        # 保持冻结，等待裁决。期间监管进程按批索要 fd；QUIT 表示新版就绪，
        # RESUME 表示交接失败、解冻恢复。设总时限，supervisor 异常时旧进程
        # 能自我解冻，不会永久冻结停服。
        self.control.settimeout(5.0)
        deadline = time.monotonic() + FROZEN_VERDICT_DEADLINE
        verdict: dict | None = None
        while verdict is None and time.monotonic() < deadline:
            try:
                msg, _ = recv_fd(self.control)
                kind = msg.get("type")
                if kind in ("QUIT", "RESUME"):
                    verdict = msg
                    break
                if kind == "GIVE_FDS":
                    self._give_fd_batch(live, msg)
                else:
                    self._log(f"frozen loop unknown message: {msg!r}")
            except TimeoutError:
                continue
            except EOFError:
                os._exit(0)
            except (OSError, ReloadAbort) as exc:
                # 控制通道故障/协议区间非法：交接已不可能，直接退出，
                # supervisor 的 watcher 会同版重拉；不会带着半交接状态服务
                self._log(f"frozen handover broken: {exc!r}, exit")
                os._exit(2)
        if verdict is None:
            self._log("verdict timeout, self-resume")
            verdict = {"type": "RESUME"}
        if verdict["type"] == "QUIT":
            self._log("superseded by new worker -> exit")
            os._exit(0)

        # RESUME：重新接管监听与全部存活连接，沿原连接按原版恢复服务
        self.poller.register(self.listen_sock.fileno(), select.POLLIN)
        for conn in live:
            if not conn.closed:
                self.poller.register(conn.fd, conn.mask())
        self.frozen = False
        self.control.setblocking(False)
        try:
            send_fd(self.control, {"type": "RESUMED", "conns": len(live)}, None)
        except OSError:
            pass
        self._log(f"resumed after failed handover: {len(live)} conns reattached")

    def _give_fd_batch(self, live: list[Conn], msg: dict) -> None:
        """按监管进程指定的区间批量交 fd，序号与状态条目顺序一一对应。"""
        start = int(msg.get("start", -1))
        count = int(msg.get("count", 0))
        end = start + count
        if start < 0 or end > len(live) or count > FD_BATCH:
            raise ReloadAbort(f"bad GIVE_FDS range: start={start} count={count}")
        for conn in live[start:end]:
            send_fd(self.control, {"type": "FD"}, conn.fd)
        send_fd(self.control, {"type": "FDS_DONE", "start": start,
                               "count": count}, None)

    # ---------------- accept ----------------
    def _on_accept(self) -> None:
        while True:
            try:
                sock, addr = self.listen_sock.accept()
            except BlockingIOError:
                return
            sock.setblocking(False)
            conn = Conn(sock=sock)
            self.conns[conn.fd] = conn
            self.poller.register(conn.fd, select.POLLIN)
            self._log(f"accepted fd={conn.fd} addr={addr}")

    # ---------------- 连接读写 ----------------
    def _on_conn(self, conn: Conn, events: int) -> None:
        if conn.closed:
            return
        if events & select.POLLOUT:
            self._flush_send(conn)
        if events & select.POLLIN:
            self._read_conn(conn)
        self._enqueue_lines(conn)
        if conn.send_buf:
            self._flush_send(conn)
        if conn.eof and not conn.send_buf and not conn.pending and not conn.recv_buf:
            conn.closed = True

    def _read_conn(self, conn: Conn) -> None:
        while True:
            space = MAX_BUFFER - len(conn.recv_buf)
            if space <= 0:  # 应用层接收缓冲满：只暂停该连接读入
                # 缓冲满且没有任何换行 -> 是一个超过 128B 的超长半行，
                # 永远无法成为合法行（行体上限 32B），丢弃并恢复读入，
                # 避免该连接被永久暂停。
                if b"\n" not in conn.recv_buf:
                    self._log(
                        f"fd={conn.fd} unterminated overlong half-line, "
                        f"drop {len(conn.recv_buf)}B"
                    )
                    conn.recv_buf.clear()
                    conn.read_paused = False
                    self._poll_modify(conn)
                    continue
                conn.read_paused = True
                self._poll_modify(conn)
                return
            try:
                chunk = conn.sock.recv(space)
            except BlockingIOError:
                return
            except InterruptedError:
                continue
            if chunk == b"":
                conn.eof = True
                return
            conn.recv_buf.extend(chunk)
            if len(chunk) < space:
                return

    def _enqueue_lines(self, conn: Conn) -> None:
        buf = conn.recv_buf
        while b"\n" in buf:
            nl = buf.index(10)
            line = bytes(buf[:nl])
            del buf[: nl + 1]  # 行一旦切出就不会被重复回执
            if len(line) > MAX_LINE_BODY:
                self._log(f"fd={conn.fd} overlong line {len(line)}B, dropped")
                continue
            value = _parse_int(line)
            if value is None:
                self._log(f"fd={conn.fd} non-integer line {line!r}, dropped")
                continue
            reply = f"{value + self.offset},{self.revision}\n".encode("ascii")
            if len(reply) > MAX_BUFFER:
                self._log(f"fd={conn.fd} reply exceeds buffer cap, dropped")
                continue
            if len(reply) > MAX_BUFFER - len(conn.send_buf):
                conn.pending.append((reply, self.revision))  # FIFO，记下版号
                continue
            conn.send_buf.extend(reply)
        if conn.read_paused and len(conn.recv_buf) < MAX_BUFFER:
            conn.read_paused = False
        self._drain_pending(conn)
        self._poll_modify(conn)

    def _drain_pending(self, conn: Conn) -> None:
        while conn.pending:
            reply, _rev = conn.pending[0]
            if len(reply) > MAX_BUFFER - len(conn.send_buf):
                return
            conn.send_buf.extend(reply)
            conn.pending.pop(0)

    def _flush_send(self, conn: Conn) -> bool:
        """尽量发送发送缓冲。返回是否已全部发完。"""
        buf = conn.send_buf
        while buf:
            try:
                sent = conn.sock.send(buf)
            except BlockingIOError:
                break
            except InterruptedError:
                continue
            except OSError:
                conn.closed = True
                return False
            if sent <= 0:
                break
            del buf[:sent]
        self._drain_pending(conn)
        if conn.read_paused and len(conn.recv_buf) < MAX_BUFFER:
            conn.read_paused = False
        self._poll_modify(conn)
        return not buf

    # ---------------- 状态恢复 ----------------
    def load_state(self, state_chunks: list[dict],
                   conn_socks: list[socket.socket]) -> None:
        conns_items: list[dict] = []
        n_total = int(state_chunks[0].get("n", 0))
        for chunk in state_chunks:
            conns_items.extend(chunk.get("conns", []))
        if n_total != len(conns_items) or len(conns_items) != len(conn_socks):
            raise ValueError("state/fd count mismatch")
        for sock, item in zip(conn_socks, conns_items):
            sock.setblocking(False)
            conn = Conn(
                sock=sock,
                recv_buf=bytearray(base64.b64decode(item["recv"])),
                send_buf=bytearray(base64.b64decode(item["send"])),
                pending=[
                    (base64.b64decode(reply), rev)
                    for reply, rev in item.get("pending", [])
                ],
                eof=bool(item.get("eof")),
                read_paused=bool(item.get("paused")),
            )
            # 缓冲上限不变量校验，防御异常快照
            if len(conn.recv_buf) > MAX_BUFFER or len(conn.send_buf) > MAX_BUFFER:
                raise ValueError("inherited state exceeds buffer cap")
            self.conns[conn.fd] = conn
            self.poller.register(conn.fd, conn.mask())

    # ---------------- 杂项 ----------------
    def _poll_modify(self, conn: Conn) -> None:
        mask = conn.mask()
        if mask:
            self._poll_register(conn.fd, mask)
        else:
            self._poll_unregister(conn.fd)

    def _poll_register(self, fd: int, mask: int) -> None:
        try:
            self.poller.modify(fd, mask)
        except FileNotFoundError:  # 未注册过 -> 新注册
            try:
                self.poller.register(fd, mask)
            except FileNotFoundError:
                pass

    def _poll_unregister(self, fd: int) -> None:
        try:
            self.poller.unregister(fd)
        except (FileNotFoundError, KeyError):
            pass

    def _reap(self) -> None:
        for conn in [c for c in self.conns.values() if c.closed]:
            self._poll_unregister(conn.fd)
            try:
                conn.sock.close()
            except OSError:
                pass
            self.conns.pop(conn.fd, None)

    def _log(self, msg: str) -> None:
        sys.stderr.write(
            f"[worker pid={os.getpid()} rev={self.revision}] {msg}\n"
        )
        sys.stderr.flush()


class ReloadAbort(RuntimeError):
    """冻结交接协议出错，旧工作进程退出（由 supervisor 走崩溃恢复）。"""


# ---------------- 进程入口（fresh start 与 exec 后共用） ----------------
def _apply_worker_rlimit() -> None:
    """按测试/部署环境变量调整工作进程软 nofile。

    - ``WORKER_NOFILE_HARD``：先把 hard 压到该值（须不低于当前 soft，
      因此需要在工作进程一启动、尚未打开任何 fd 时设置），用于模拟
      受限容器里工作进程整体 fd 容量更小的场景；
    - ``WORKER_NOFILE_SOFT``：把 soft 抬/压到该值（不超过 hard）。
    """
    hard_target = int(os.environ.get("WORKER_NOFILE_HARD", "0"))
    if hard_target > 0:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if hard_target < hard:
            resource.setrlimit(
                resource.RLIMIT_NOFILE,
                (min(soft, hard_target), hard_target),
            )
    soft_target = int(os.environ.get("WORKER_NOFILE_SOFT", "0"))
    if soft_target > 0:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(
            resource.RLIMIT_NOFILE, (min(soft_target, hard), hard)
        )


def worker_entry() -> None:
    # 测试用：模拟新工作进程失败。
    # - WORKER_FAIL_MARKER 文件存在即失败；
    # - WORKER_FAIL_AFTER_FDS=N（可选）：在收到 N 个连接 fd 之后才退出，
    #   用于模拟“接管前中途退出”，验证旧进程 RESUME 回退与资源释放。
    # 必须在任何对外应答之前退出，这样旧工作进程仍安全冻结在交接阶段。
    marker = os.environ.get("WORKER_FAIL_MARKER")
    fail_after = int(os.environ.get("WORKER_FAIL_AFTER_FDS", "0"))
    fail_now = bool(marker and os.path.exists(marker) and fail_after == 0)
    if fail_now:
        sys.stderr.write("[worker] boot failure injected\n")
        sys.stderr.flush()
        os._exit(2)

    _apply_worker_rlimit()

    control_fd = int(os.environ["WORKER_CONTROL_FD"])
    offset = int(os.environ["WORKER_OFFSET"])
    revision = int(os.environ["WORKER_REVISION"])

    control = socket.socket(fileno=control_fd)
    control.setblocking(True)
    control.settimeout(HANDSHAKE_TIMEOUT)

    hello, listen_fd = recv_fd(control)
    if hello.get("type") != "HELLO" or listen_fd is None:
        sys.stderr.write("[worker] bad HELLO\n")
        os._exit(2)
    n = int(hello.get("conns", 0))

    # 连接 fd 与后续 STATE 分块严格按协议顺序到达。
    # 每收满 FD_BATCH 个回一个 FDS_ACK，做逐批流控：让 supervisor 确认这批
    # fd 已被本进程 recvmsg 取走后再发下一批，避免在 unix socket 缓冲里
    # 积压过多 SCM_RIGHTS 引用触发 ETOOMANYREFS。
    conn_socks: list[socket.socket] = []
    while len(conn_socks) < n:
        msg, fd = recv_fd(control)
        if msg.get("type") != "FD" or fd is None:
            sys.stderr.write("[worker] bad FD record\n")
            os._exit(2)
        conn_socks.append(socket.socket(fileno=fd))
        if (
            marker
            and os.path.exists(marker)
            and fail_after > 0
            and len(conn_socks) >= fail_after
        ):
            sys.stderr.write(
                f"[worker] injected failure after {len(conn_socks)} fds\n"
            )
            sys.stderr.flush()
            os._exit(2)
        if len(conn_socks) % FD_BATCH == 0 or len(conn_socks) == n:
            send_fd(
                control,
                {"type": "FDS_ACK", "count": len(conn_socks)},
                None,
            )

    state_chunks: list[dict] = []
    while True:
        chunk, _ = recv_fd(control)
        if chunk.get("type") != "STATE":
            sys.stderr.write("[worker] bad STATE record\n")
            os._exit(2)
        state_chunks.append(chunk)
        if not chunk.get("more"):
            break

    listen_sock = socket.socket(fileno=listen_fd)
    listen_sock.setblocking(False)
    worker = Worker(listen_sock, control, offset, revision)
    try:
        worker.load_state(state_chunks, conn_socks)
    except ValueError as exc:
        sys.stderr.write(f"[worker] load_state failed: {exc!r}\n")
        os._exit(2)

    send_fd(control, {"type": "READY", "pid": os.getpid()}, None)
    go, _ = recv_fd(control)  # supervisor 确认接管后才放行
    if go.get("type") != "GO":
        os._exit(2)

    control.setblocking(False)
    worker._log(
        f"ready offset={offset} inherited_conns={len(conn_socks)}"
    )
    worker.run()
