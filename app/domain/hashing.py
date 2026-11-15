"""Content identity.

We normalise before hashing *and store the normalised text*, so the stored
``content`` always hashes to the stored ``content_hash``. Normalisation is
deliberately minimal (line endings + Unicode NFC): it removes transport noise
(CRLF from Windows clients, composed vs. decomposed accents) without changing
anything the summariser could observe.
"""

from __future__ import annotations

import hashlib
import unicodedata


def normalize_content(raw: str) -> str:
    unified = raw.replace("\r\n", "\n").replace("\r", "\n")
    return unicodedata.normalize("NFC", unified)


def content_hash(normalized: str) -> str:
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
