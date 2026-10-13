"""浏览器并发限流。

背景：项目里有多处需要拉起 headless Chromium（扫码登录、Cookie 刷新、
账号资料与商品详情抓取）。这些调用点原先各自独立启动浏览器，信号量也都建在
账号实例内部，于是挂几个账号就可能同时冒出几个 Chromium —— 多个账号的定时
Cookie 刷新一旦撞在同一分钟，这种情况几乎必然发生。

一个 headless Chromium 冷启动就要占满一个核数十秒。在 J1800 这类双核低压
CPU 上，两三个并存就足以把整机拖到 99%，启动变慢又导致任务继续堆积，形成雪崩。

这里提供一个进程内全局信号量，所有浏览器启动都从这里取槽位，用完自动归还。
槽位数按机器实际配置推算，弱机强制单开。
"""
import asyncio
import os
import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, Optional

from loguru import logger

# 允许通过环境变量强制指定，方便排查问题或应对推算不准的机器
_ENV_KEY = 'MAX_CONCURRENT_BROWSERS'

# 底层用线程信号量而不是 asyncio 版本：滑块验证走的是同步 Playwright，
# 跑在独立线程里，与异步任务共用同一台机器的 CPU。两边各建一套信号量的话，
# 总数就会翻倍，限流也就失去意义。
_semaphore: Optional[threading.Semaphore] = None
_semaphore_lock = threading.Lock()
_limit: Optional[int] = None
_active_lock = threading.Lock()
_active_jobs: Dict[int, Dict[str, Any]] = {}
_next_job_id = 0

# 槽位归还后仍记下这次的进程号：浏览器关不掉时，正是靠这份记录去收尸。
_recent_jobs: Dict[str, Dict[str, Any]] = {}

# 等待空闲浏览器的上限。低配机器上一次浏览器任务可能要一两分钟，
# 排队本身是正常的；但等过这个时间基本说明有任务卡死了，
# 与其无限期挂着，不如报错让上层记录并重试。
_ACQUIRE_TIMEOUT = 300


def _process_snapshot() -> str:
    """返回 Chromium/Playwright 进程摘要，避免日志里只能看到槽位超时。"""
    try:
        import psutil
        names = []
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            info = proc.info
            text = ' '.join(info.get('cmdline') or [])
            name = info.get('name') or ''
            if any(key in f'{name} {text}'.lower() for key in ('chrome', 'chromium', 'playwright')):
                names.append(f"{info.get('pid')}:{name}")
        return f"count={len(names)} pids={','.join(names[:20]) or '-'}"
    except Exception as exc:
        return f"unavailable={type(exc).__name__}"


def _register_job(purpose: str) -> int:
    global _next_job_id
    with _active_lock:
        _next_job_id += 1
        job_id = _next_job_id
        _active_jobs[job_id] = {'purpose': purpose, 'started_at': time.monotonic()}
        logger.info(f"浏览器槽位获取成功: job={job_id}, purpose={purpose}, active={len(_active_jobs)}/{get_limit()}, {_process_snapshot()}")
        return job_id


def _bind_job_pids(job_id: Optional[int]) -> None:
    """把当前 Chromium 进程登记到这个槽位名下。

    只有登记过的进程才算有主的活进程；关不掉的残留会以孤儿身份被清理。否则
    「关不掉」和「正在用」在进程列表里长得一模一样，没法区分。
    """
    if job_id is None:
        return
    pids = _chromium_pids()
    with _active_lock:
        item = _active_jobs.get(job_id)
        if item is not None:
            item['pids'] = pids


def bind_current_pids(purpose: str) -> None:
    """按用途登记当前进程（滑块验证这类自行启动浏览器的场景用）。"""
    with _active_lock:
        job_id = next(
            (key for key, item in _active_jobs.items() if item['purpose'] == purpose),
            None,
        )
    _bind_job_pids(job_id)


def _chromium_pids() -> set:
    """当前可见的 Chromium/Playwright 进程 PID 集合。"""
    try:
        import psutil
        pids = set()
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            info = proc.info
            name = info.get('name') or ''
            text = ' '.join(info.get('cmdline') or [])
            if any(key in f'{name} {text}'.lower() for key in ('chrome', 'chromium', 'playwright')):
                pids.add(info.get('pid'))
        return pids
    except Exception:
        return set()


def describe_active_jobs() -> str:
    """在册槽位摘要，用于超时日志指出到底是谁占着不放。"""
    with _active_lock:
        if not _active_jobs:
            return '无在册浏览器任务'
        return '; '.join(
            f"job={job_id} purpose={item['purpose']} "
            f"已运行={time.monotonic() - item['started_at']:.0f}s"
            for job_id, item in sorted(_active_jobs.items())
        )


def reap_orphan_browsers(min_age_seconds: int = 60) -> int:
    """清掉不属于任何在册槽位、且已存活够久的 Chromium 进程。

    Playwright 的浏览器关不掉时进程会一直留在容器里：启动前就有残留、槽位被
    占满、滑块验证越来越慢，而日志里只能看到 count 一直涨。这里按「有没有
    主人」判定——在册槽位的进程不动，其余超过 min_age_seconds 的直接终止。
    """
    try:
        import psutil
    except Exception as exc:
        logger.warning(f"清理残留浏览器失败: psutil 不可用 ({type(exc).__name__})")
        return 0

    try:
        with _active_lock:
            protected = set()
            for item in _active_jobs.values():
                protected |= set(item.get('pids') or set())

        now = time.time()
        killed = []
        for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'create_time']):
            info = proc.info
            pid = info.get('pid')
            if pid in protected or pid == os.getpid():
                continue
            name = info.get('name') or ''
            text = ' '.join(info.get('cmdline') or [])
            if not any(key in f'{name} {text}'.lower() for key in ('chrome', 'chromium', 'playwright')):
                continue
            try:
                created_at = info.get('create_time')
                if created_at is not None and now - created_at < min_age_seconds:
                    continue
                proc.kill()
                killed.append(f'{pid}:{name}')
            except Exception as exc:
                logger.debug(f"清理进程 {pid} 失败: {type(exc).__name__}")

        if killed:
            logger.warning(
                f"清理残留浏览器进程: killed={len(killed)}, "
                f"protected={len(protected)}, pids={','.join(killed[:20])}"
            )
        else:
            logger.info(f"检查残留浏览器进程: 无需清理, protected={len(protected)}")
        return len(killed)
    except Exception as exc:
        logger.warning(f"清理残留浏览器失败: {type(exc).__name__}: {exc}")
        return 0


_reaper_started = False


def _start_reaper(interval_seconds: int = 300) -> None:
    """后台定期回收残留浏览器进程，随进程内信号量一起懒启动。"""
    global _reaper_started
    with _active_lock:
        if _reaper_started:
            return
        _reaper_started = True

    def _loop() -> None:
        while True:
            time.sleep(interval_seconds)
            try:
                reap_orphan_browsers()
            except Exception as exc:
                logger.warning(f"残留浏览器清理线程异常: {type(exc).__name__}: {exc}")

    threading.Thread(target=_loop, name='browser-reaper', daemon=True).start()
    logger.info(f"残留浏览器清理线程已启动: interval={interval_seconds}s")


def _unregister_job(job_id: Optional[int], purpose: str, reason: str) -> None:
    if job_id is None:
        return
    with _active_lock:
        item = _active_jobs.pop(job_id, None)
        elapsed = time.monotonic() - item['started_at'] if item else 0
        if item and item.get('pids'):
            _recent_jobs[purpose] = {
                'pids': set(item['pids']),
                'finished_at': time.monotonic(),
            }
    logger.info(f"浏览器槽位释放: job={job_id}, purpose={purpose}, reason={reason}, elapsed={elapsed:.1f}s, active={len(_active_jobs)}/{get_limit()}, {_process_snapshot()}")


def kill_recent_pids(purpose: str, max_age_seconds: int = 300) -> int:
    """终止某用途最近一次浏览器任务遗留的进程，返回终止数量。

    Playwright 关闭超时时，close() 没生效但槽位已经归还，进程就这么留在容器里。
    这里按记录的 PID 精确清理，不动别人的浏览器。
    """
    try:
        import psutil
    except Exception:
        return 0

    with _active_lock:
        record = _recent_jobs.get(purpose)
    if not record:
        return 0
    if time.monotonic() - record['finished_at'] > max_age_seconds:
        return 0

    killed = 0
    for pid in sorted(record['pids']):
        try:
            proc = psutil.Process(pid)
            if proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE:
                proc.kill()
                killed += 1
        except Exception:
            continue
    with _active_lock:
        _recent_jobs.pop(purpose, None)
    return killed

def _detect_total_memory_gb() -> Optional[float]:
    """物理内存（GB）。取不到时返回 None，由调用方只按 CPU 判断。"""
    try:
        import psutil
        return psutil.virtual_memory().total / (1024 ** 3)
    except Exception:
        return None


def get_limit() -> int:
    """允许同时运行的浏览器数量。

    浏览器是这里最重的资源消耗方，一个 headless Chromium 冷启动就要吃满一个核
    数十秒；并发拉起既拖慢彼此，也让滑块验证更容易失败。所以按机器实际配置放行，
    弱机一律单开——排队只是慢一点，挤爆了整机卡死，连正常的消息收发都会受影响。

    推算不准时可用环境变量 MAX_CONCURRENT_BROWSERS 覆盖。
    """
    global _limit
    if _limit is not None:
        return _limit

    forced = (os.getenv(_ENV_KEY) or '').strip()
    if forced:
        try:
            _limit = max(1, int(forced))
            logger.info(f"浏览器并发上限由 {_ENV_KEY} 指定为 {_limit}")
            return _limit
        except ValueError:
            logger.warning(f"{_ENV_KEY} 取值无效: {forced!r}，改用自动推算")

    cpu_count = os.cpu_count() or 2
    memory_gb = _detect_total_memory_gb()

    # 双核或 4G 以内：只能单开。这类配置多见于低价 NAS 和入门小主机，
    # 同时跑两个浏览器就会明显卡顿，甚至把整机拖到无法响应。
    if cpu_count <= 2 or (memory_gb is not None and memory_gb <= 4.5):
        _limit = 1
    elif cpu_count <= 4 or (memory_gb is not None and memory_gb <= 8.5):
        _limit = 2
    else:
        _limit = 3

    memory_desc = f"{memory_gb:.1f}G" if memory_gb is not None else "未知"
    logger.info(f"浏览器并发上限: {_limit}（CPU {cpu_count} 核，内存 {memory_desc}）")
    return _limit


def _get_semaphore() -> threading.Semaphore:
    global _semaphore
    if _semaphore is None:
        with _semaphore_lock:
            if _semaphore is None:
                _semaphore = threading.Semaphore(get_limit())
    _start_reaper()
    return _semaphore


class LimitedBrowser:
    """给浏览器对象套一层，close() 时把槽位还回去。

    这样调用方原有的 `finally: await browser.close()` 不用改写，
    也就不会出现「忘记归还槽位」导致后续启动被永久卡住的情况。
    其余属性一律透传，用起来与原对象无差别。
    """

    def __init__(self, browser: Any, semaphore: threading.Semaphore, purpose: str, job_id: int):
        self._browser = browser
        self._semaphore = semaphore
        self._purpose = purpose
        self._job_id = job_id
        self._released = False
        self._release_lock = threading.Lock()

    def _release(self) -> None:
        with self._release_lock:
            if not self._released:
                self._released = True
                self._semaphore.release()
                _unregister_job(self._job_id, self._purpose, 'browser.close')

    async def close(self, *args, **kwargs):
        try:
            return await self._browser.close(*args, **kwargs)
        finally:
            self._release()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._browser, name)


async def launch_browser(
    playwright: Any,
    launch_options: Optional[Dict[str, Any]] = None,
    purpose: str = '浏览器任务',
) -> LimitedBrowser:
    """取到槽位后再启动浏览器；没有空位就等着。

    调用方必须像以前一样在 finally 里 close()，槽位随之归还。
    """
    semaphore = _get_semaphore()

    # 在线程里等待，避免占着事件循环不放导致消息收发一起卡住
    started = time.monotonic()
    logger.info(f"等待浏览器槽位: purpose={purpose}, limit={get_limit()}, {_process_snapshot()}")
    acquired = await asyncio.to_thread(semaphore.acquire, True, _ACQUIRE_TIMEOUT)
    if not acquired:
        logger.error(f"浏览器槽位等待超时: purpose={purpose}, waited={time.monotonic() - started:.1f}s, active={len(_active_jobs)}/{get_limit()}, 占用者=[{describe_active_jobs()}], {_process_snapshot()}")
        raise TimeoutError(
            f"{purpose}: 等待浏览器空闲超过 {_ACQUIRE_TIMEOUT} 秒。"
            "可能有浏览器任务卡住未退出，或机器性能不足以支撑当前账号数量。"
        )

    try:
        job_id = _register_job(purpose)
        browser = await playwright.chromium.launch(**(launch_options or {}))
        _bind_job_pids(job_id)
    except BaseException:
        # 启动失败要立刻归还，否则槽位会被永久占住
        semaphore.release()
        _unregister_job(locals().get('job_id'), purpose, 'launch_failed')
        raise

    logger.info(f"浏览器启动完成: job={job_id}, purpose={purpose}, {_process_snapshot()}")
    return LimitedBrowser(browser, semaphore, purpose, job_id)


@contextmanager
def browser_slot(purpose: str = '浏览器任务'):
    """同步版槽位。供滑块验证这类跑在线程里的同步 Playwright 使用。

    用法::

        with browser_slot("滑块验证"):
            browser = playwright.chromium.launch(...)
            ...
    """
    semaphore = _get_semaphore()
    started = time.monotonic()
    logger.info(f"等待浏览器槽位: purpose={purpose}, limit={get_limit()}, {_process_snapshot()}")
    acquired = semaphore.acquire(True, _ACQUIRE_TIMEOUT)
    if not acquired:
        logger.error(f"浏览器槽位等待超时: purpose={purpose}, waited={time.monotonic() - started:.1f}s, active={len(_active_jobs)}/{get_limit()}, 占用者=[{describe_active_jobs()}], {_process_snapshot()}")
        raise TimeoutError(
            f"{purpose}: 等待浏览器空闲超过 {_ACQUIRE_TIMEOUT} 秒。"
            "可能有浏览器任务卡住未退出，或机器性能不足以支撑当前账号数量。"
        )
    try:
        job_id = _register_job(purpose)
        yield
    finally:
        semaphore.release()
        _unregister_job(locals().get('job_id'), purpose, 'context_exit')


def acquire_slot(purpose: str = '浏览器任务') -> None:
    """同步取槽位。用于滑块验证这类跑在线程里、启动与关闭分处两个方法的场景。

    取到之后必须调用 release_slot 归还，否则后续任务会一直排队。
    """
    semaphore = _get_semaphore()
    started = time.monotonic()
    logger.info(f"等待浏览器槽位: purpose={purpose}, limit={get_limit()}, {_process_snapshot()}")
    if not semaphore.acquire(True, _ACQUIRE_TIMEOUT):
        logger.error(f"浏览器槽位等待超时: purpose={purpose}, waited={time.monotonic() - started:.1f}s, active={len(_active_jobs)}/{get_limit()}, 占用者=[{describe_active_jobs()}], {_process_snapshot()}")
        raise TimeoutError(
            f"{purpose}: 等待浏览器空闲超过 {_ACQUIRE_TIMEOUT} 秒。"
            "可能有浏览器任务卡住未退出，或机器性能不足以支撑当前账号数量。"
        )


def release_slot(purpose: str = '浏览器任务') -> None:
    """归还 acquire_slot 取得的槽位。重复调用会被忽略调用方自行保证配对。"""
    try:
        _get_semaphore().release()
        with _active_lock:
            job_id = next((key for key, item in _active_jobs.items() if item['purpose'] == purpose), None)
        _unregister_job(job_id, purpose, 'explicit_release')
    except ValueError:
        # 归还次数多于取用次数，说明配对有误；记录但不影响主流程
        logger.warning(f"{purpose}: 浏览器槽位重复归还，已忽略")
