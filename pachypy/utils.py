import os
from fnmatch import fnmatch
from glob import glob
from pathlib import Path
from typing import List, Dict, Iterable, Union, Optional

import pandas as pd


WildcardFilter = Optional[Union[str, Iterable[str]]]
FileGlob = Optional[Union[str, Path, Iterable[Union[str, Path]]]]


def wildcard_filter(x: Iterable[str], pattern: WildcardFilter) -> List[str]:
    if pattern is None or pattern == '*':
        return list(x)
    else:
        return [i for i in x if wildcard_match(i, pattern)]


def wildcard_match(x: str, pattern: WildcardFilter) -> bool:
    if pattern is None or pattern == '*':
        return True
    elif isinstance(pattern, str):
        return fnmatch(x, pattern)
    else:
        return any([wildcard_match(x, m) for m in pattern])


def expand_files(files: FileGlob) -> List[str]:
    if not files:
        return []
    if isinstance(files, str) or isinstance(files, Path):
        files = [files]
    return sorted(list(set.union(*[set(glob(os.path.expanduser(f))) for f in files])))


def invert_dict(d: Dict[str, str]) -> Dict[str, List[str]]:
    inverted: Dict[str, List[str]] = {}
    for key, value in d.items():
        inverted.setdefault(value, []).append(key)
    return inverted


def to_timestamp(seconds: int, nanos: int) -> Optional[pd.Timestamp]:
    if seconds > 0:
        return pd.Timestamp(float(f'{seconds}.{nanos}'), unit='s', tz='utc')
    else:
        return None


def to_timedelta(seconds: int, nanos: int) -> pd.Timedelta:
    return pd.Timedelta(float(f'{seconds}.{nanos}'), unit='s')
