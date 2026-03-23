from __future__ import annotations

import hashlib
import json
from typing import Any


def payload_hash(payload: dict[str, Any]) -> str:
    compact = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(compact.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"
