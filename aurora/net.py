from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Callable, Dict, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter

try:  # urllib3 ships with requests, but stay defensive
    from urllib3.util.retry import Retry
except Exception:  # pragma: no cover - defensive
    Retry = None  # type: ignore[assignment]

log = logging.getLogger(__name__)


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except Exception:
        return default


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return default


DEFAULT_TIMEOUT = _float_env("HTTP_TIMEOUT_SECONDS", 20.0)
POOL_SIZE = max(2, _int_env("HTTP_POOL_SIZE", 8))
RETRY_TOTAL = max(0, _int_env("HTTP_RETRY_TOTAL", 2))
STALE_TTL_SECONDS = max(0, _int_env("HTTP_STALE_TTL_SECONDS", 6 * 3600))
USER_AGENT = os.getenv(
    "HTTP_USER_AGENT",
    "AuroraAlertsBot/2.0 (+https://github.com/Ne-k/Aurora-Alerts)",
)

_session_lock = threading.Lock()
_session: Optional[requests.Session] = None

_scraper_lock = threading.Lock()
_scraper: Any = None
_scraper_failed = False

_cache_lock = threading.Lock()
_cache: Dict[str, Tuple[float, float, Any]] = {}  # key -> (fresh_until, stale_until, value)
_key_locks: Dict[str, threading.Lock] = {}


def _build_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update({"User-Agent": USER_AGENT})
    if Retry is not None:
        try:
            retry = Retry(
                total=RETRY_TOTAL,
                connect=RETRY_TOTAL,
                read=RETRY_TOTAL,
                backoff_factor=0.6,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=frozenset({"GET", "POST"}),
                raise_on_status=False,
            )
        except TypeError:  # older urllib3 spells it method_whitelist
            retry = Retry(total=RETRY_TOTAL, backoff_factor=0.6)
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=POOL_SIZE,
            pool_maxsize=POOL_SIZE,
            pool_block=True,
        )
    else:  # pragma: no cover - defensive
        adapter = HTTPAdapter(
            pool_connections=POOL_SIZE, pool_maxsize=POOL_SIZE, pool_block=True
        )
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess


def session() -> requests.Session:
    """The one shared Session. Sockets are capped at POOL_SIZE per host."""
    global _session
    with _session_lock:
        if _session is None:
            _session = _build_session()
        return _session


def scraper() -> Any:
    """A single cloudscraper instance, or the shared Session if unavailable.

    cloudscraper builds a Session subclass; creating one per call was a second
    source of leaked connection pools.
    """
    global _scraper, _scraper_failed
    with _scraper_lock:
        if _scraper is not None or _scraper_failed:
            return _scraper or session()
        try:
            import cloudscraper  # type: ignore

            _scraper = cloudscraper.create_scraper()
            _scraper.headers.setdefault("User-Agent", USER_AGENT)
        except Exception as exc:
            _scraper_failed = True
            log.info("cloudscraper unavailable (%s); falling back to plain session", exc)
            return session()
        return _scraper


def get(url: str, **kwargs) -> requests.Response:
    kwargs.setdefault("timeout", DEFAULT_TIMEOUT)
    return session().get(url, **kwargs)


def post(url: str, **kwargs) -> requests.Response:
    kwargs.setdefault("timeout", DEFAULT_TIMEOUT)
    return session().post(url, **kwargs)


def log_fetch_error(label: str, exc: BaseException) -> None:
    """One line per failure. Full traceback only when DEBUG is on."""
    log.warning("%s fetch failed: %s: %s", label, type(exc).__name__, exc)
    log.debug("%s traceback", label, exc_info=exc)


def _lock_for(key: str) -> threading.Lock:
    with _cache_lock:
        lock = _key_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _key_locks[key] = lock
        return lock


def _peek(key: str, now: float, allow_stale: bool) -> Tuple[bool, Any]:
    with _cache_lock:
        entry = _cache.get(key)
        if not entry:
            return False, None
        fresh_until, stale_until, value = entry
        if now < fresh_until:
            return True, value
        if allow_stale and now < stale_until:
            return True, value
        return False, None


def cached(key: str, ttl: float, producer: Callable[[], Any], label: str = "") -> Any:
    """Return ``producer()``, memoized for ``ttl`` seconds.

    On producer failure (exception or ``None``) a previously cached value is
    served for up to ``STALE_TTL_SECONDS`` so one bad fetch does not wipe the
    embed. Concurrent callers for the same key wait rather than stampede.
    """
    now = time.monotonic()
    hit, value = _peek(key, now, allow_stale=False)
    if hit:
        return value

    with _lock_for(key):
        # Another thread may have filled it while we waited.
        now = time.monotonic()
        hit, value = _peek(key, now, allow_stale=False)
        if hit:
            return value
        try:
            produced = producer()
        except Exception as exc:
            log_fetch_error(label or key, exc)
            produced = None
        if produced is not None:
            now = time.monotonic()
            with _cache_lock:
                _cache[key] = (now + ttl, now + ttl + STALE_TTL_SECONDS, produced)
            return produced
        stale_hit, stale_value = _peek(key, time.monotonic(), allow_stale=True)
        if stale_hit:
            log.info("%s: serving cached value after failed refresh", label or key)
            return stale_value
        return None


def cached_json(key: str, url: str, ttl: float, label: str = "", **kwargs) -> Any:
    def _produce():
        resp = get(url, **kwargs)
        resp.raise_for_status()
        return resp.json()

    return cached(key, ttl, _produce, label or url)


def cached_text(key: str, url: str, ttl: float, label: str = "", **kwargs) -> Optional[str]:
    def _produce():
        resp = get(url, **kwargs)
        resp.raise_for_status()
        return resp.text

    return cached(key, ttl, _produce, label or url)


def cache_stats() -> Dict[str, int]:
    with _cache_lock:
        return {"entries": len(_cache)}


def clear_cache() -> None:
    with _cache_lock:
        _cache.clear()


def close() -> None:
    """Release the shared session and scraper (used on shutdown / in tests)."""
    global _session, _scraper
    with _session_lock:
        if _session is not None:
            try:
                _session.close()
            except Exception:
                pass
            _session = None
    with _scraper_lock:
        if _scraper is not None:
            try:
                _scraper.close()
            except Exception:
                pass
            _scraper = None
