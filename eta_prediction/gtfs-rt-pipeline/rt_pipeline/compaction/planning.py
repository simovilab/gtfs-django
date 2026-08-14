"""Discover the units of work: closed leaf partitions and closed staging days.

Both discovery functions use only delimited (non-recursive) listings -- the
data files themselves are never listed one by one here; DuckDB resolves each
leaf's files itself, via `glob()`, at compaction time. A single MBTA day can
hold hundreds of thousands of objects in the legacy layout, so that
distinction matters.

Neither function ever returns the current UTC day: it is still being written
to by the collectors, and touching it risks losing an in-flight write.
"""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass

from .feeds import Feed
from .storage import Storage

_SEG_RE = re.compile(r"^(?:year|month|day)=(\d+)$")
_PATH_DATE_RE = re.compile(r"year=(\d+)/month=(\d+)/day=(\d+)")


def parse_partition_date(year: str, month: str, day: str) -> dt.date:
    def val(seg: str) -> int:
        m = _SEG_RE.match(seg)
        if not m:
            raise ValueError(f"unexpected partition segment {seg!r}")
        return int(m.group(1))

    return dt.date(val(year), val(month), val(day))


def date_from_key(key: str) -> dt.date:
    """Recover a partition's date from any key containing year=/month=/day= segments
    (used during crash recovery, where only the leaf path is available)."""
    m = _PATH_DATE_RE.search(key)
    if not m:
        raise ValueError(f"no year/month/day in {key!r}")
    return dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))


def _in_bounds(
    date: dt.date, today: dt.date, since: dt.date | None, until: dt.date | None
) -> bool:
    if date >= today:
        return False  # open / future day -- never touch
    if since is not None and date < since:
        return False
    if until is not None and date > until:
        return False
    return True


@dataclass(frozen=True)
class Leaf:
    """A closed partition in the legacy in-place layout: `.../day=D[/route_id=R]`."""

    feed: Feed
    key: str  # bucket-relative prefix of the leaf
    date: dt.date


def discover_leaves(
    store: Storage,
    feed: Feed,
    today: dt.date,
    *,
    since: dt.date | None = None,
    until: dt.date | None = None,
) -> list[Leaf]:
    leaves: list[Leaf] = []
    for y in store.child_dirs(feed.curated_prefix):
        for m in store.child_dirs(f"{feed.curated_prefix}/{y}"):
            for d in store.child_dirs(f"{feed.curated_prefix}/{y}/{m}"):
                try:
                    date = parse_partition_date(y, m, d)
                except ValueError:
                    continue
                if not _in_bounds(date, today, since, until):
                    continue
                day_prefix = f"{feed.curated_prefix}/{y}/{m}/{d}"
                if feed.route_level:
                    for r in store.child_dirs(day_prefix):
                        leaves.append(Leaf(feed, f"{day_prefix}/{r}", date))
                else:
                    leaves.append(Leaf(feed, day_prefix, date))
    return leaves


@dataclass(frozen=True)
class StagingDay:
    """A closed day of hourly-staging objects awaiting repartition into the
    curated layout."""

    feed: Feed
    staging_key: str  # .../<staging_prefix>/year=Y/month=M/day=D (flat hourly objects)
    curated_key: str  # .../<curated_prefix>/year=Y/month=M/day=D
    date: dt.date


def discover_staging_days(
    store: Storage,
    feed: Feed,
    today: dt.date,
    *,
    since: dt.date | None = None,
    until: dt.date | None = None,
) -> list[StagingDay]:
    if not feed.staging_prefix:
        return []
    days: list[StagingDay] = []
    for y in store.child_dirs(feed.staging_prefix):
        for m in store.child_dirs(f"{feed.staging_prefix}/{y}"):
            for d in store.child_dirs(f"{feed.staging_prefix}/{y}/{m}"):
                try:
                    date = parse_partition_date(y, m, d)
                except ValueError:
                    continue
                if not _in_bounds(date, today, since, until):
                    continue
                days.append(
                    StagingDay(
                        feed,
                        f"{feed.staging_prefix}/{y}/{m}/{d}",
                        f"{feed.curated_prefix}/{y}/{m}/{d}",
                        date,
                    )
                )
    return days
