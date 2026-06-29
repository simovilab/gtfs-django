"""Tiny, dependency-free terminal progress UI for the dataset builder.

Goals: make a long build *feel* alive (so it never looks hung) while staying
minimal. No third-party deps. Degrades gracefully to plain line output when
stdout is not a TTY (piped logs, ``docker compose exec -T``) or ``NO_COLOR``
is set.

API:
    banner(title, lines=...)         # framed header
    note(text)                       # dim one-liner
    with step(title) as s:           # animated spinner for indeterminate work
        s.detail("123 rows")         #   -> "✓ title — 123 rows (1.2s)"
    with bar(total, desc) as b:      # progress bar for counted loops
        for x in xs: ...; b.advance(detail="...")
    summary(title, rows=[(k, v)...]) # framed key/value footer
"""

from __future__ import annotations

import itertools
import os
import sys
import threading
import time
from contextlib import contextmanager
from typing import Iterable, Optional, Sequence, Tuple

_FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"


def _tty() -> bool:
    return sys.stdout.isatty() and os.environ.get("NO_COLOR") is None


class _C:
    """ANSI styles, blanked out when color is unsupported."""

    def __init__(self, on: bool):
        self.reset = "\033[0m" if on else ""
        self.bold = "\033[1m" if on else ""
        self.dim = "\033[2m" if on else ""
        self.cyan = "\033[36m" if on else ""
        self.green = "\033[32m" if on else ""
        self.red = "\033[31m" if on else ""
        self.blue = "\033[34m" if on else ""


def _fmt_secs(s: float) -> str:
    if s < 60:
        return f"{s:.1f}s"
    m, sec = divmod(int(s), 60)
    if m < 60:
        return f"{m}m{sec:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m"


def banner(title: str, lines: Sequence[str] = ()) -> None:
    c = _C(_tty())
    width = max([len(title)] + [len(x) for x in lines]) + 2
    bar_ = "─" * width
    print(f"{c.cyan}╭{bar_}╮{c.reset}")
    print(f"{c.cyan}│{c.reset} {c.bold}{title.ljust(width - 1)}{c.reset}{c.cyan}│{c.reset}")
    for ln in lines:
        print(f"{c.cyan}│{c.reset} {c.dim}{ln.ljust(width - 1)}{c.reset}{c.cyan}│{c.reset}")
    print(f"{c.cyan}╰{bar_}╯{c.reset}")


def note(text: str) -> None:
    c = _C(_tty())
    print(f"{c.dim}  · {text}{c.reset}")


class _Step:
    def __init__(self, title: str, spinner: bool = True):
        self.title = title
        self._spinner = spinner
        self._detail = ""
        self._c = _C(_tty())
        self._tty = _tty()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._t0 = 0.0

    def detail(self, text: str) -> None:
        self._detail = text

    def _spin(self) -> None:
        for frame in itertools.cycle(_FRAMES):
            if self._stop.is_set():
                break
            tail = f" {self._c.dim}{self._detail}{self._c.reset}" if self._detail else ""
            sys.stdout.write(f"\r{self._c.cyan}{frame}{self._c.reset} {self.title}{tail}  ")
            sys.stdout.flush()
            time.sleep(0.08)

    def __enter__(self) -> "_Step":
        self._t0 = time.time()
        if self._tty and self._spinner:
            self._thread = threading.Thread(target=self._spin, daemon=True)
            self._thread.start()
        else:
            # Non-animated: print a start line now; any sub-output appears below,
            # then the ✓ summary line on exit. Used when the wrapped work prints.
            print(f"  {self._c.blue}▶{self._c.reset} {self.title} …", flush=True)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if self._thread is not None:
            self._stop.set()
            self._thread.join()
        elapsed = _fmt_secs(time.time() - self._t0)
        tail = f" {self._c.dim}— {self._detail}{self._c.reset}" if self._detail else ""
        mark = f"{self._c.green}✓{self._c.reset}" if exc_type is None else f"{self._c.red}✗{self._c.reset}"
        line = f"{mark} {self.title}{tail} {self._c.dim}({elapsed}){self._c.reset}"
        if self._tty:
            sys.stdout.write("\r\033[K" + line + "\n")  # clear spinner line
            sys.stdout.flush()
        else:
            print(line)
        return False  # never suppress exceptions


def step(title: str, spinner: bool = True) -> _Step:
    """Indeterminate-work reporter. Set ``spinner=False`` when the wrapped work
    prints its own output (an animated line would get clobbered)."""
    return _Step(title, spinner=spinner)


def heading(text: str) -> None:
    """Styled section divider (lighter than a full banner)."""
    c = _C(_tty())
    print(f"\n{c.blue}▌{c.reset} {c.bold}{text}{c.reset}")


class _Bar:
    def __init__(self, total: int, desc: str, width: int = 26):
        self.total = max(int(total), 1)
        self.desc = desc
        self.width = width
        self.n = 0
        self._detail = ""
        self._c = _C(_tty())
        self._tty = _tty()
        self._t0 = 0.0
        self._last = 0.0

    def __enter__(self) -> "_Bar":
        self._t0 = self._last = time.time()
        if not self._tty:
            print(f"  ▸ {self.desc} (0/{self.total}) …", flush=True)
        return self

    def advance(self, n: int = 1, detail: Optional[str] = None) -> None:
        self.n += n
        if detail is not None:
            self._detail = detail
        now = time.time()
        if self._tty and (now - self._last >= 0.08 or self.n >= self.total):
            self._last = now
            self._render(now)

    def _render(self, now: float) -> None:
        frac = min(self.n / self.total, 1.0)
        filled = int(frac * self.width)
        bar = (
            f"{self._c.green}{'━' * filled}{self._c.reset}"
            f"{self._c.dim}{'━' * (self.width - filled)}{self._c.reset}"
        )
        rate = self.n / max(now - self._t0, 1e-6)
        eta = (self.total - self.n) / rate if rate > 0 else 0
        tail = f" {self._c.dim}{self._detail}{self._c.reset}" if self._detail else ""
        sys.stdout.write(
            f"\r {self.desc} {bar} {self._c.bold}{int(frac * 100):3d}%{self._c.reset} "
            f"{self._c.dim}{self.n:,}/{self.total:,} · eta {_fmt_secs(eta)}{self._c.reset}{tail}\033[K"
        )
        sys.stdout.flush()

    def __exit__(self, exc_type, exc, tb) -> bool:
        elapsed = _fmt_secs(time.time() - self._t0)
        mark = f"{self._c.green}✓{self._c.reset}" if exc_type is None else f"{self._c.red}✗{self._c.reset}"
        tail = f" {self._c.dim}— {self._detail}{self._c.reset}" if self._detail else ""
        line = f"{mark} {self.desc}{tail} {self._c.dim}({self.n:,} in {elapsed}){self._c.reset}"
        if self._tty:
            sys.stdout.write("\r\033[K" + line + "\n")
            sys.stdout.flush()
        else:
            print(line)
        return False


def bar(total: int, desc: str) -> _Bar:
    return _Bar(total, desc)


def track(iterable: Iterable, total: int, desc: str, detail=None):
    """Wrap a counted iterable in a progress bar.

    ``detail`` is an optional zero-arg callable returning a status string,
    evaluated after each item (e.g. ``lambda: f"{len(rows):,} samples"``).
    """
    with bar(total, desc) as b:
        for item in iterable:
            yield item
            b.advance(detail=detail() if detail is not None else None)


def summary(title: str, rows: Sequence[Tuple[str, str]]) -> None:
    c = _C(_tty())
    klen = max((len(k) for k, _ in rows), default=0)
    body = [f"{k.rjust(klen)}  {c.bold}{v}{c.reset}" for k, v in rows]
    width = max([len(title)] + [klen + 2 + len(v) for _, v in rows]) + 2
    bar_ = "─" * width
    print(f"{c.green}╭{bar_}╮{c.reset}")
    print(f"{c.green}│{c.reset} {c.bold}{title}{c.reset}")
    for ln in body:
        # ljust on raw text is awkward with ANSI; keep it simple and unpadded
        print(f"{c.green}│{c.reset} {ln}")
    print(f"{c.green}╰{bar_}╯{c.reset}")
