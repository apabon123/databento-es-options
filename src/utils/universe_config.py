from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import yaml


ROLL_CODE_METADATA = {
    ".c.": {
        "roll_strategy": "calendar",
        "roll_rule_desc": "databento_calendar",
        "folder": "calendar",
    },
    ".v.": {
        "roll_strategy": "volume",
        "roll_rule_desc": "databento_volume",
        "folder": "volume",
    },
    ".o.": {
        "roll_strategy": "open-interest",
        "roll_rule_desc": "databento_open_interest",
        "folder": "open-interest",
    },
}


@dataclass(frozen=True)
class RootUniverse:
    root: str
    roll_code: str
    ranks: List[int]
    comment: Optional[str]
    optional: bool
    roll_strategy: str
    roll_rule_desc: str
    folder: str

    def symbols(self) -> List[str]:
        return [f"{self.root}{self.roll_code}{rank}" for rank in self.ranks]


@dataclass(frozen=True)
class DownloadUniverseConfig:
    dataset: str
    schema: str
    stype_in: str
    default_start: Optional[str]
    default_end: Optional[str]
    roots: Dict[str, RootUniverse]

    def selected_roots(self, include_optionals: bool = True, filter_roots: Optional[Iterable[str]] = None) -> List[RootUniverse]:
        roots = self.roots.values()
        if not include_optionals:
            roots = [r for r in roots if not r.optional]
        if filter_roots:
            wanted = {r.upper() for r in filter_roots}
            roots = [r for r in roots if r.root.upper() in wanted]
        return sorted(roots, key=lambda r: r.root)


def parse_ranks(raw) -> List[int]:
    if raw is None:
        raise ValueError("Ranks value is required in download universe config.")
    if isinstance(raw, int):
        return [raw]
    if isinstance(raw, list):
        ranks: List[int] = []
        for item in raw:
            ranks.extend(parse_ranks(item))
        return sorted(set(ranks))
    if isinstance(raw, str):
        ranks: List[int] = []
        for part in raw.split(","):
            part = part.strip()
            if not part:
                continue
            if "-" in part:
                start_str, end_str = part.split("-", 1)
                start = int(start_str)
                end = int(end_str)
                if start > end:
                    raise ValueError(f"Invalid rank range '{part}' (start > end).")
                ranks.extend(range(start, end + 1))
            else:
                ranks.append(int(part))
        return sorted(set(ranks))
    raise TypeError(f"Unsupported ranks type: {type(raw)}")


def _load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def load_download_universe_config(path: Path) -> DownloadUniverseConfig:
    cfg = _load_yaml(path)

    dataset = cfg.get("dataset", "GLBX.MDP3")
    schema = cfg.get("schema", "ohlcv-1d")
    stype_in = cfg.get("stype_in", "continuous")
    default_start = cfg.get("default_start")
    default_end = cfg.get("default_end")

    roots_section = cfg.get("roots")
    if not roots_section:
        raise ValueError("download_universe config requires a 'roots' mapping.")

    roots: Dict[str, RootUniverse] = {}
    for root, meta in roots_section.items():
        roll_code = meta.get("roll_rule")
        if not roll_code:
            raise ValueError(f"Root '{root}' is missing 'roll_rule'.")
        if roll_code not in ROLL_CODE_METADATA:
            raise ValueError(f"Unsupported roll rule '{roll_code}' for root '{root}'.")

        metadata = ROLL_CODE_METADATA[roll_code]
        ranks = parse_ranks(meta.get("ranks"))
        comment = meta.get("comment")
        optional = bool(meta.get("optional", False))

        roots[root] = RootUniverse(
            root=root,
            roll_code=roll_code,
            ranks=ranks,
            comment=comment,
            optional=optional,
            roll_strategy=metadata["roll_strategy"],
            roll_rule_desc=metadata["roll_rule_desc"],
            folder=metadata["folder"],
        )

    return DownloadUniverseConfig(
        dataset=dataset,
        schema=schema,
        stype_in=stype_in,
        default_start=default_start,
        default_end=default_end,
        roots=roots,
    )

