"""扫码重频统计。

登录态一失效，界面上只提示「重新扫码」，用户往往不知道自己是第几次扫了。
而平台看到的是短时间内多次登录尝试——反复重扫本身就会让风控升级，于是变成
「越登录不上越要扫，越扫越登不上」。这个模块把次数显式记下来，日志、风控
日志和前端提示共用同一个口径，用户不必再靠猜。

单独成模块而不是塞在 reply_server 里，是为了能脱离 FastAPI 依赖单独测试。
"""
import time
from typing import Dict, List, Optional

# 半小时是「连续重试」和「隔天正常换 Cookie」的分界：超过这个窗口，
# 平台侧看到的也是两次独立登录，混在一起计数只会误报。
WINDOW_SECONDS = 60 * 30

_history: Dict[str, List[float]] = {}


def record_attempt(account_id: str, now: Optional[float] = None) -> dict:
    """记录一次扫码登录成功，返回 30 分钟内次数与提示文案。"""
    now = time.time() if now is None else now
    history = _history.setdefault(account_id, [])
    history[:] = [ts for ts in history if now - ts <= WINDOW_SECONDS]
    history.append(now)
    count = len(history)

    if count > 3:
        notice = (
            f"该账号 30 分钟内已扫码 {count} 次。频繁重扫会触发平台风控，"
            "建议先停止重试，确认 Cookie 与滑块是否正常后再登录。"
        )
    elif count == 3:
        notice = "该账号 30 分钟内第 3 次扫码登录，请避免继续重试。"
    else:
        notice = ''
    return {'count': count, 'notice': notice}


def recent_count(account_id: str, now: Optional[float] = None) -> int:
    """只查不记，供状态查询使用。"""
    now = time.time() if now is None else now
    return len([ts for ts in _history.get(account_id, []) if now - ts <= WINDOW_SECONDS])


def reset(account_id: Optional[str] = None) -> None:
    """清空统计。account_id 为空时全部清空，供测试与运维使用。"""
    if account_id is None:
        _history.clear()
    else:
        _history.pop(account_id, None)
