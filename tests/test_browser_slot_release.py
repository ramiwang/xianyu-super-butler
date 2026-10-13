"""浏览器槽位与扫码会话的释放。

browser_limit 的槽位全局只有一个，漏掉一次之后所有浏览器任务（滑块、扫码、
商品抓取、Cookie 刷新）都要卡满 300 秒超时才动得了。扫码会话里存着完整登录
Cookie，同样不能只靠"用户恰好还在轮询"来回收。
"""

import asyncio
import time
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from utils import browser_limit
from utils.qr_login import QRLoginManager, QRLoginSession
from utils.xianyu_slider_stealth import XianyuSliderStealth


class SliderSlotReleaseTests(unittest.TestCase):
    """滑块实例初始化失败时必须归还槽位。"""

    def _make_instance(self):
        instance = XianyuSliderStealth.__new__(XianyuSliderStealth)
        instance.user_id = "acc"
        instance.pure_user_id = "acc"
        instance._browser_slot_held = True
        instance.page = None
        instance.context = None
        instance.browser = None
        instance.playwright = None
        return instance

    def test_init_failure_cleanup_returns_the_slot(self):
        instance = self._make_instance()

        with patch.object(browser_limit, "release_slot") as release:
            instance._cleanup_on_init_failure()

        # 浏览器启动失败时只清进程不还槽位，槽位就永久漏掉
        release.assert_called_once()
        self.assertFalse(instance._browser_slot_held)

    def test_cleanup_is_idempotent(self):
        instance = self._make_instance()

        with patch.object(browser_limit, "release_slot") as release:
            instance._cleanup_on_init_failure()
            instance._cleanup_on_init_failure()

        # 重复清理不能把别人的槽位也还掉
        self.assertEqual(release.call_count, 1)

    def test_cleanup_without_slot_releases_nothing(self):
        instance = self._make_instance()
        instance._browser_slot_held = False

        with patch.object(browser_limit, "release_slot") as release:
            instance._cleanup_on_init_failure()

        release.assert_not_called()


class PasswordLoginSlotReleaseTests(unittest.TestCase):
    """浏览器启动失败时，密码登录同样不能漏槽位。"""

    def test_slot_is_released_when_browser_fails_to_start(self):
        instance = XianyuSliderStealth.__new__(XianyuSliderStealth)
        instance.user_id = "acc"
        instance.pure_user_id = "acc"

        # 占槽在浏览器启动之前，而正常释放它的 finally 属于更靠后的内层 try：
        # 启动本身失败时会跳过那个 finally
        with (
            patch.object(browser_limit, "acquire_slot") as acquire,
            patch.object(browser_limit, "release_slot") as release,
            patch(
                "utils.xianyu_slider_stealth.sync_playwright",
                side_effect=RuntimeError("Executable doesn't exist"),
            ),
        ):
            result = instance.login_with_password_playwright("user", "pass")

        self.assertIsNone(result)
        self.assertEqual(acquire.call_count, release.call_count, "占用与归还次数不一致")
        release.assert_called_once()


class QrSessionReleaseTests(unittest.IsolatedAsyncioTestCase):
    """监控结束后会话要自清，不能等前端来触发。"""

    async def test_session_is_discarded_after_monitor_finishes(self):
        manager = QRLoginManager()
        manager.sessions["s1"] = QRLoginSession("s1")

        await manager._discard_session_later("s1", delay=0)
        await asyncio.sleep(0)

        # 会话里存着完整登录 Cookie，用户关掉弹窗后不能一直留在内存
        self.assertNotIn("s1", manager.sessions)

    async def test_discarding_a_missing_session_is_safe(self):
        manager = QRLoginManager()

        await manager._discard_session_later("nope", delay=0)

        self.assertEqual(manager.sessions, {})

    async def test_session_survives_long_enough_for_frontend_to_read_result(self):
        manager = QRLoginManager()
        session = QRLoginSession("s2")
        session.status = "success"
        manager.sessions["s2"] = session

        task = asyncio.create_task(manager._discard_session_later("s2", delay=5))
        await asyncio.sleep(0)

        # 删除前要留窗口期，否则前端最后一次轮询会拿到 not_found
        self.assertIn("s2", manager.sessions)
        task.cancel()


class LeakedBrowserProcessTests(unittest.TestCase):
    """关不掉的浏览器进程必须被清理，而不是一直留在容器里累积。"""

    def setUp(self):
        browser_limit._active_jobs.clear()
        browser_limit._recent_jobs.clear()

    def tearDown(self):
        browser_limit._active_jobs.clear()
        browser_limit._recent_jobs.clear()

    def test_slot_release_records_pids_for_later_reaping(self):
        job_id = browser_limit._register_job("扫码登录")
        browser_limit._bind_job_pids(job_id)

        browser_limit._unregister_job(job_id, "扫码登录", "browser.close")

        # 归还槽位时记下的进程号，是关不掉时按图索骥的唯一依据
        self.assertIn("扫码登录", browser_limit._recent_jobs)

    def test_kill_recent_pids_terminates_recorded_processes(self):
        browser_limit._recent_jobs["扫码登录"] = {
            "pids": {4242},
            "finished_at": __import__("time").monotonic(),
        }

        killed = []

        class FakePsutil:
            STATUS_ZOMBIE = "zombie"

            class Process:
                def __init__(self, pid):
                    self.pid = pid

                def is_running(self):
                    return True

                def status(self):
                    return "running"

                def kill(self):
                    killed.append(self.pid)

        with patch.dict("sys.modules", {"psutil": FakePsutil}):
            count = browser_limit.kill_recent_pids("扫码登录")

        self.assertEqual(count, 1)
        self.assertEqual(killed, [4242])

    def test_reaper_ignores_stale_records(self):
        browser_limit._recent_jobs["扫码登录"] = {
            "pids": {4242},
            "finished_at": __import__("time").monotonic() - 10_000,
        }

        killed = browser_limit.kill_recent_pids("扫码登录", max_age_seconds=300)

        # 10 分钟前的记录说明那次任务早就结束，误杀会伤到新起的浏览器
        self.assertEqual(killed, 0)

    def test_reaper_kills_only_unowned_processes(self):
        live = MagicMock()
        live.info = {
            "pid": 111,
            "name": "chrome-headless-shell",
            "cmdline": ["chrome-headless-shell"],
            "create_time": 0,
        }
        orphan = MagicMock()
        orphan.info = {
            "pid": 222,
            "name": "chrome-headless-shell",
            "cmdline": ["chrome-headless-shell"],
            "create_time": 0,
        }
        unrelated = MagicMock()
        unrelated.info = {
            "pid": 333,
            "name": "python",
            "cmdline": ["python", "app.py"],
            "create_time": 0,
        }

        job_id = browser_limit._register_job("定时Cookie刷新")
        browser_limit._active_jobs[job_id]["pids"] = {111}

        class FakePsutil:
            @staticmethod
            def process_iter(_attrs):
                return [live, orphan, unrelated]

        with patch.dict("sys.modules", {"psutil": FakePsutil}):
            count = browser_limit.reap_orphan_browsers(min_age_seconds=1)

        # 在册任务正在用的 111 不能杀，333 不是浏览器，只有 222 该收
        self.assertEqual(count, 1)
        orphan.kill.assert_called_once()
        live.kill.assert_not_called()
        unrelated.kill.assert_not_called()


class QrRescanCounterTests(unittest.TestCase):
    """「我到底扫了几次」必须有答案，不能让用户靠猜。"""

    def setUp(self):
        from utils import qr_rescan

        self.qr_rescan = qr_rescan
        qr_rescan.reset()

    def tearDown(self):
        self.qr_rescan.reset()

    def test_first_scan_has_no_notice(self):
        result = self.qr_rescan.record_attempt("acc1")

        self.assertEqual(result["count"], 1)
        self.assertEqual(result["notice"], "")

    def test_third_scan_warns(self):
        for _ in range(3):
            result = self.qr_rescan.record_attempt("acc1")

        self.assertEqual(result["count"], 3)
        self.assertIn("3", result["notice"])

    def test_fourth_scan_warns_about_risk_control(self):
        for _ in range(4):
            result = self.qr_rescan.record_attempt("acc1")

        self.assertIn("风控", result["notice"])

    def test_counter_is_scoped_per_account(self):
        for _ in range(3):
            self.qr_rescan.record_attempt("acc1")

        other = self.qr_rescan.record_attempt("acc2")

        self.assertEqual(other["count"], 1)

    def test_old_scans_expire_out_of_the_window(self):
        now = time.time()
        self.qr_rescan.record_attempt("acc1", now=now - 3600)
        self.qr_rescan.record_attempt("acc1", now=now - 1801)

        result = self.qr_rescan.record_attempt("acc1", now=now)

        self.assertEqual(result["count"], 1)


if __name__ == "__main__":
    unittest.main()
