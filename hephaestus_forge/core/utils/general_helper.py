# --General Libraries--
import uuid
import re
import base58
import hashlib
import numpy as np
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Optional
from pathlib import Path


# --Core Functions--
def deterministic_code(text: str, max_length: Optional[int] = 16) -> str:
    code = hashlib.sha256(text.encode('utf-8')).digest()
    code_b58 = base58.b58encode(code).decode('utf-8')
    return code_b58[:max_length]


def random_code(max_length: Optional[int] = 8) -> str:
    code = uuid.uuid4().hex
    return code[:max_length].zfill(max_length)


def json_cleaner(record):
    """
    Limpia y serializa valores complejos antes de convertir a JSONB.

    - Elimina NUL (\x00) y caracteres de control no imprimibles (incluyendo 0x7F).
    - Limpia tanto claves como valores.
    - Convierte tipos especiales: Enum, datetime, UUID, Decimal, numpy.nan, etc.
    - Maneja dataclasses, pydantic models, Path, bytes.
    - Evita referencias circulares.
    """

    # Compilamos un regex para todos los control chars
    # (ASCII 0–31 y DEL 127; preserva \n, \r, \t)
    control_chars = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]")

    def clean_str(s: str) -> str:
        """Elimina caracteres no imprimibles o nulos."""
        if not isinstance(s, str):
            return s
        # Normaliza y remueve control chars
        return control_chars.sub("", s)

    seen = set()

    def clean_value(v):
        """Convierte un valor a algo seguro para JSON/JSONB."""
        if id(v) in seen:
            return None  # evita referencias circulares
        seen.add(id(v))

        try:
            if isinstance(v, Enum):
                return v.value
            elif isinstance(v, (datetime, date)):
                return v.isoformat()
            elif isinstance(v, uuid.UUID):
                return str(v)
            elif isinstance(v, Decimal):
                return float(v)
            elif isinstance(v, float) and np.isnan(v):
                return None
            elif isinstance(v, (int, float, bool)) or v is None:
                return v
            elif isinstance(v, Path):
                return str(v)
            elif hasattr(v, "__dataclass_fields__"):
                # dataclass -> dict limpio
                return {k: clean_value(getattr(v, k)) for k in v.__dataclass_fields__}
            elif hasattr(v, "model_dump"):  # Pydantic v2
                return clean_value(v.model_dump())
            elif hasattr(v, "dict"):  # Pydantic v1
                return clean_value(v.dict())
            elif isinstance(v, dict):
                return {clean_str(str(k)): clean_value(val) for k, val in v.items()}
            elif isinstance(v, (list, tuple, set)):
                return [clean_value(item) for item in v]
            elif isinstance(v, bytes):
                try:
                    v = v.decode("utf-8", "ignore")
                except Exception:
                    v = str(v)
                return clean_str(v)
            elif isinstance(v, str):
                return clean_str(v)
            else:
                # Fallback final para tipos no serializables
                return clean_str(str(v))
        finally:
            seen.discard(id(v))

    if not isinstance(record, dict):
        # si llega algo que no sea dict, lo intento limpiar igual
        return clean_value(record)

    return {clean_str(str(k)): clean_value(v) for k, v in record.items()}

