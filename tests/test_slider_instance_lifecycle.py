"""密码登录不能永久占住滑块实例槽位，也不能阻塞异步心跳。"""

import ast
import asyncio
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from utils import xianyu_slider_stealth as slider


class InstanceLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.manager = slider.concurrency_manager
        self.assertEqual(self.manager.get_stats()['active_count'], 0)

    def make_instance(self):
        instance = slider.XianyuSliderStealth('lifecycle-test', enable_learning=False)
        self.addCleanup(instance.close_browser)
        return instance

    def test_password_startup_failure_allows_next_login(self):
        instance = self.make_instance()
        with (
            patch.object(slider.browser_limit, 'acquire_slot'),
            patch.object(slider.browser_limit, 'release_slot'),
            patch.object(slider, 'sync_playwright', side_effect=RuntimeError('startup failed')),
        ):
            self.assertIsNone(instance.login_with_password_playwright('user', 'pass'))
        self.assertEqual(self.manager.get_stats()['active_count'], 0)
        self.make_instance()  # 同一个账号可以再次占槽，不会卡到超时。

    def test_invalid_credentials_also_release_instance(self):
        instance = self.make_instance()
        self.assertIsNone(instance.login_with_password_playwright('', ''))
        self.assertEqual(self.manager.get_stats()['active_count'], 0)

    def test_old_cleanup_does_not_unregister_new_owner(self):
        old = self.make_instance()
        old.close_browser()
        new = self.make_instance()
        old.close_browser()
        self.assertIs(self.manager.active_instances[new.user_id]['instance'], new)

    def test_constructor_failure_releases_reservation(self):
        with patch.object(slider.tempfile, 'mkdtemp', side_effect=OSError('disk full')):
            with self.assertRaises(OSError):
                self.make_instance()
        self.assertEqual(self.manager.get_stats()['active_count'], 0)

    def test_concurrent_acquisition_respects_limit(self):
        barrier = threading.Barrier(2)
        acquired = []

        def acquire(user_id):
            barrier.wait()
            acquired.append(self.manager.wait_for_slot(user_id, timeout=0.05, instance=object()))

        with patch.object(slider.time, 'sleep', side_effect=lambda _: threading.Event().wait(0.005)):
            threads = [threading.Thread(target=acquire, args=(f'thread-{i}',)) for i in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(2)
        try:
            self.assertEqual(sorted(acquired), [False, True])
            self.assertEqual(self.manager.get_stats()['active_count'], 1)
        finally:
            for user_id in list(self.manager.active_instances):
                self.manager.unregister_instance(user_id)


class CaptchaWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_waiting_for_constructor_keeps_event_loop_responsive(self):
        # 提取真实方法，避免导入完整服务及数据库；执行其线程调度和清理代码。
        tree = ast.parse(Path('XianyuAutoAsync.py').read_text())
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'XianyuLive')
        method = next(n for n in cls.body if isinstance(n, ast.AsyncFunctionDef) and n.name == '_handle_captcha_verification')
        from types import SimpleNamespace
        from loguru import logger
        namespace = dict(asyncio=asyncio, os=__import__('os'), logger=logger)
        exec(compile(ast.Module(body=[method], type_ignores=[]), 'captcha-test', 'exec'), namespace)
        entered, release = threading.Event(), threading.Event()
        instances = []

        class FakeSlider:
            def __init__(self, **kwargs):
                entered.set()
                if not release.wait(2):
                    raise RuntimeError('event loop blocked during construction')
                self.closed = False
                instances.append(self)

            def run(self, url, cookies):
                raise asyncio.CancelledError()

            def close_browser(self):
                self.closed = True

        live = SimpleNamespace(cookie_id='acc', cookies_str='test')
        with patch.object(slider, 'XianyuSliderStealth', FakeSlider):
            task = asyncio.create_task(namespace['_handle_captcha_verification'](live, {'data': {'url': 'https://example.test'}}))
            try:
                for _ in range(100):
                    if entered.is_set():
                        break
                    await asyncio.sleep(0.005)
                self.assertTrue(entered.is_set())
            finally:
                release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertTrue(instances[0].closed)
