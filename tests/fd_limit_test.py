"""描述符受限与交接中断的端到端自测（真实子进程 + 真实 TCP/HTTP，无 mock）。

覆盖题目要求：
1. 监管进程 nofile 受限、在线连接数超过监管进程余量时，换版仍成功；
   且换版期间监管进程额外占用的 fd 峰值 <= 32，全部探头连接不中断。
2. 监管进程余量 < 32：换版在冻结旧进程之前直接 503，旧服务不停、
   不出现旧连接冻住（回执应立刻可读，而不是等自我恢复超时）。
3. 新工作进程 fd 容量装不下这批连接：503 + 旧进程按原版恢复。
4. 交接中断——新进程在接管前中途退出（已收到部分连接 fd）：
   503、存活旧进程沿原连接按原版恢复、无需重连、释放资源后可再次换版。
5. 连续多轮换版/失败后无 fd 泄漏（监管进程打开 fd 数回落，峰值有界）。

受限手段（仅测试用环境变量，不设则行为与默认部署一致）：
- SUPERVISOR_NOFILE   监管进程启动后压低自身软 nofile
- WORKER_NOFILE_SOFT  工作进程握手前把软 nofile 抬/压到此值（不超过 hard）
- FD_HEADROOM_FLOOR   换版要求的监管进程余量地板（默认 32）
- WORKER_FAIL_MARKER / WORKER_FAIL_AFTER_FDS 新进程故障注入
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, "/workspace")
sys.path.insert(0, "/tmp/pylibs")

from tests.integration_test import Server, free_port, recv_line

ROOT = Path(__file__).resolve().parent.parent
_MARKER_SEQ = 0


class LimitedServer(Server):
    """带 rlimit / 故障注入环境变量的服务进程。"""

    def __init__(self, extra_env: dict[str, str] | None = None) -> None:
        # 每个实例使用独立的故障注入标记，彻底避免跨用例/跨进程残留
        global _MARKER_SEQ
        _MARKER_SEQ += 1
        self.fail_marker = Path(
            f"/tmp/worker-fail-marker-fdtest-{os.getpid()}-{_MARKER_SEQ}"
        )
        self.fail_marker.unlink(missing_ok=True)
        self.http_port = free_port()
        self.tcp_port = free_port()
        env = dict(os.environ)
        env["HTTP_PORT"] = str(self.http_port)
        env["TCP_PORT"] = str(self.tcp_port)
        env["WORKER_FAIL_MARKER"] = str(self.fail_marker)
        env["PYTHONUNBUFFERED"] = "1"
        if extra_env:
            env.update(extra_env)
        import subprocess

        self.proc = subprocess.Popen(
            [sys.executable, "-m", "app.main"],
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.base = f"http://127.0.0.1:{self.http_port}"
        self._wait_ready()

    def stop(self) -> None:
        super().stop()
        self.fail_marker.unlink(missing_ok=True)


results: list[tuple[str, bool, str]] = []


def check(name: str, fn) -> None:
    try:
        fn()
        results.append((name, True, ""))
        print(f"PASS  {name}")
    except Exception as exc:  # noqa: BLE001
        results.append((name, False, repr(exc)))
        print(f"FAIL  {name}: {exc!r}")
        import traceback

        traceback.print_exc()


# ---------------- 用例 ----------------
def t_reload_succeeds_when_conns_exceed_supervisor_headroom() -> None:
    # 监管进程软 nofile=52：稳态约用掉 7~8 个 fd，余量 ~44；
    # 40 条在线连接远大于“余量-32”的旧式全量中转能力，但分批中转只需 32。
    # 工作进程（新旧都一样）nofile 抬到 512，保证装得下这批连接——
    # 题目要求“新旧工作进程都容得下这批连接、只有监管进程余量受限”。
    srv = LimitedServer(
        {"SUPERVISOR_NOFILE": "52", "WORKER_NOFILE_SOFT": "512"}
    )
    try:
        n = 40  # 40 > 31，至少触发两批 GIVE_FDS
        socks = [srv.connect() for _ in range(n)]
        for i, s in enumerate(socks):
            s.sendall(f"{i}\n".encode())
        # 先排空换版前回执（停读探头的缓冲交接由其他用例专门覆盖）
        for i, s in enumerate(socks):
            assert recv_line(s, timeout=3) == f"{i},1\n".encode()
        time.sleep(0.3)

        st_before = srv.status()
        assert st_before["fd_soft_limit"] == 52, st_before

        r = srv.reload(10)
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["inherited_conns"] == n, body
        assert body["revision"] == 2

        st_after = srv.status()
        # 关键不变量：换版期间监管进程额外 fd 峰值不超过 32
        assert st_after["reload_extra_fd_peak"] <= 32, st_after
        # 交接完成后中转 fd 全部释放，监管进程打开 fd 数回落到换版前量级
        assert st_after["fd_open"] <= st_before["fd_open"] + 2, st_after
        print(
            f"    监管进程 nofile=52、{n} 连接：换版成功，"
            f"额外 fd 峰值={st_after['reload_extra_fd_peak']}，"
            f"换版前 fd_open={st_before['fd_open']}，换版后={st_after['fd_open']}"
        )

        # 全部探头连接未断、无需重连；换版后的新行按新版回执
        for s in socks:
            assert s.getpeername()
        socks[0].sendall(b"5\n")
        assert recv_line(socks[0]) == b"15,2\n"
        # 半行跨换版补齐仍按新版
        socks[1].sendall(b"9")
        time.sleep(0.2)
        socks[1].sendall(b"9\n")
        assert recv_line(socks[1]) == b"109,2\n"
        for s in socks:
            s.close()
    finally:
        srv.stop()


def t_headroom_below_32_fails_fast_without_freezing() -> None:
    # 软 nofile=40：稳态后余量 < 32，换版必须在冻结旧进程之前拒绝
    srv = LimitedServer({"SUPERVISOR_NOFILE": "40"})
    try:
        socks = [srv.connect() for _ in range(24)]
        time.sleep(0.3)
        st = srv.status()
        assert st["fd_headroom"] < 32, st

        socks[0].sendall(b"1\n")
        assert recv_line(socks[0]) == b"1,1\n"

        t0 = time.time()
        r = srv.reload(10)
        elapsed = time.time() - t0
        assert r.status_code == 503, r.text
        assert "fd headroom" in r.text, r.text
        assert elapsed < 3, f"预检没有在冻结前短路，耗时 {elapsed:.1f}s"

        # 旧进程未被冻结：新行立刻得到旧版回执（回归点：旧实现要冻 30s）
        socks[1].sendall(b"2\n")
        t0 = time.time()
        assert recv_line(socks[1], timeout=3) == b"2,1\n"
        assert time.time() - t0 < 2

        st2 = srv.status()
        assert st2["revision"] == 1 and st2["alive"], st2
        for s in socks:
            s.close()
    finally:
        srv.stop()


def t_worker_capacity_insufficient_falls_back() -> None:
    # 监管进程余量充足；工作进程整体 nofile 被压到 128。
    # 旧工作进程稳态 fd 用量 ≈ 连接数 + 6（stdio/控制/监听/poll 等），
    # 113 条连接 ≈ 119 <= 128，旧进程能稳定服务；
    # 但新进程预检口径 = 连接数 + 预留 16 = 129 > 128，换版被拒绝。
    # 先证明连接数较少时（40 条，56 <= 128）同配置下换版可以成功。
    srv = LimitedServer(
        {"SUPERVISOR_NOFILE": "512", "WORKER_NOFILE_HARD": "128"}
    )
    try:
        ok_socks = [srv.connect() for _ in range(40)]
        time.sleep(0.5)
        r = srv.reload(10)
        assert r.status_code == 200, r.text
        ok_socks[0].sendall(b"3\n")
        assert recv_line(ok_socks[0]) == b"13,2\n"
        for s in ok_socks:
            s.close()
        time.sleep(0.5)

        n = 113  # 113 + 预留 16 = 129 > 新进程容量 128 -> 预检拒绝
        socks = [srv.connect() for _ in range(n)]
        time.sleep(1.0)
        r = srv.reload(20)
        assert r.status_code == 503, r.text
        assert "worker fd capacity" in r.text, r.text

        # 旧进程按原版（当前 rev=2）恢复服务，全部连接仍在、未被冻结
        t0 = time.time()
        socks[0].sendall(b"3\n")
        assert recv_line(socks[0], timeout=3) == b"13,2\n"
        assert time.time() - t0 < 2
        assert srv.status()["revision"] == 2
        for s in socks:
            s.close()

        # 释放连接使新进程装得下后，再次换版成功（资源释放后可重试）
        few = [srv.connect() for _ in range(4)]
        r = srv.reload(10)
        assert r.status_code == 200, r.text
        few[0].sendall(b"3\n")
        assert recv_line(few[0]) == b"13,3\n"
        for s in few:
            s.close()
    finally:
        srv.stop()


def t_handover_interrupted_new_worker_exits_mid_relay() -> None:
    # 新进程在收到 8 个连接 fd 后、READY 之前退出（接管前死亡）
    srv = LimitedServer(
        {"SUPERVISOR_NOFILE": "256", "WORKER_FAIL_AFTER_FDS": "8"}
    )
    try:
        n = 40  # 两批以上，确保死亡发生在中转进行中
        socks = [srv.connect() for _ in range(n)]
        for s in socks:
            s.sendall(b"7\n")
        time.sleep(0.6)

        srv.fail_marker.write_text("x")
        try:
            t0 = time.time()
            r = srv.reload(10)
            elapsed = time.time() - t0
            assert r.status_code == 503, r.text
            assert elapsed < 12, elapsed
        finally:
            srv.fail_marker.unlink(missing_ok=True)

        # 存活的旧进程沿原连接按原版恢复：连接不断、无需重连、回执立即可读
        st = srv.status()
        assert st["revision"] == 1 and st["alive"], st
        for s in socks:
            assert s.getpeername()
        for s in socks:
            assert recv_line(s, timeout=4) == b"7,1\n"
        socks[0].sendall(b"4\n")
        assert recv_line(socks[0]) == b"4,1\n"
        print("    交接中断后旧进程原版恢复，40 条连接全部存活、回执不丢不重")

        # 本轮资源已释放：再次换版成功，且仍继承全部连接
        r = srv.reload(10)
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["revision"] == 2 and body["inherited_conns"] == n, body
        socks[1].sendall(b"4\n")
        assert recv_line(socks[1]) == b"14,2\n"
        # 半行跨换版补齐仍按新版
        socks[2].sendall(b"2")
        time.sleep(0.2)
        socks[2].sendall(b"0\n")
        assert recv_line(socks[2]) == b"30,2\n"
        for s in socks:
            s.close()
    finally:
        srv.stop()


def t_repeated_failures_release_every_fd() -> None:
    # 连续 3 次交接中断 + 2 次成功换版：监管进程 fd 不得累积泄漏
    srv = LimitedServer(
        {"SUPERVISOR_NOFILE": "256", "WORKER_FAIL_AFTER_FDS": "5"}
    )
    try:
        socks = [srv.connect() for _ in range(35)]
        time.sleep(0.3)
        baseline = srv.status()["fd_open"]

        srv.fail_marker.write_text("x")
        try:
            for k in range(3):
                r = srv.reload(k + 1)
                assert r.status_code == 503, r.text
                # 失败后旧服务即刻可用
                socks[k].sendall(b"1\n")
                assert recv_line(socks[k]) == b"1,1\n"
        finally:
            srv.fail_marker.unlink(missing_ok=True)

        mid = srv.status()
        # 三次失败后没有中转 fd 残留
        assert mid["fd_open"] <= baseline + 2, (baseline, mid)

        r = srv.reload(10)
        assert r.status_code == 200, r.text
        r = srv.reload(20)
        assert r.status_code == 200, r.text
        end = srv.status()
        assert end["revision"] == 3, end
        assert end["fd_open"] <= baseline + 2, (baseline, end)
        assert end["reload_extra_fd_peak"] <= 32, end
        socks[0].sendall(b"1\n")
        assert recv_line(socks[0]) == b"21,3\n"
        for s in socks:
            s.close()
    finally:
        srv.stop()


def t_large_conn_count_batched_relay_no_deadlock() -> None:
    # 200 条连接（7 批）+ 监管 nofile=80：验证逐批流控不会死锁、
    # 不触发 ETOOMANYREFS，在途 fd 有界，全部连接继承、回执语义保持。
    srv = LimitedServer(
        {"SUPERVISOR_NOFILE": "80", "WORKER_NOFILE_SOFT": "512"}
    )
    try:
        n = 200
        socks = [srv.connect() for _ in range(n)]
        for s in socks:
            s.sendall(b"1\n")
        time.sleep(1.0)  # 旧进程排空，换版前的行按旧版入回执队列

        t0 = time.time()
        r = srv.reload(10)
        elapsed = time.time() - t0
        assert r.status_code == 200, r.text
        assert r.json()["inherited_conns"] == n
        assert elapsed < 15, f"批量交接卡住 {elapsed:.1f}s"

        st = srv.status()
        assert st["reload_extra_fd_peak"] <= 32, st
        for s in socks:  # 全部连接存活、无需重连
            assert s.getpeername()

        # 换版前完整行 -> 旧版回执；换版后新行 -> 新版；半行补齐 -> 新版
        for s in (socks[100], socks[150]):
            assert recv_line(s, timeout=5) == b"1,1\n"
        socks[100].sendall(b"5\n")
        assert recv_line(socks[100], timeout=5) == b"15,2\n"
        socks[150].sendall(b"2")
        time.sleep(0.2)
        socks[150].sendall(b"0\n")
        assert recv_line(socks[150], timeout=5) == b"30,2\n"
        for s in socks:
            s.close()
    finally:
        srv.stop()


def main() -> int:
    cases = [
        ("reload ok when live conns exceed supervisor headroom (peak extra fd <= 32)",
         t_reload_succeeds_when_conns_exceed_supervisor_headroom),
        ("headroom < 32 -> fast 503, old worker never frozen",
         t_headroom_below_32_fails_fast_without_freezing),
        ("worker fd capacity insufficient -> 503 + old revision serves",
         t_worker_capacity_insufficient_falls_back),
        ("handover interrupted (new worker exits mid-relay) -> resume & retry",
         t_handover_interrupted_new_worker_exits_mid_relay),
        ("repeated interrupted handovers leak no fd",
         t_repeated_failures_release_every_fd),
        ("200 conns batched relay: no deadlock / ETOOMANYREFS, peak <= 32",
         t_large_conn_count_batched_relay_no_deadlock),
    ]
    for name, fn in cases:
        check(name, fn)
    failed = [n for n, ok, _ in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
