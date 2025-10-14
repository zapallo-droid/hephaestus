# --General Libraries--
from datetime import datetime, timezone
import getpass
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import DateTime, Text, func
from typing import Any, Optional


# --Core Classes--
class Base(DeclarativeBase):
    created_at = mapped_column(DateTime(timezone=True), server_default=func.now())

    created_by: Mapped[str] = mapped_column(Text,
                                            nullable=False,
                                            default=getpass.getuser)

    modified_by: Mapped[str] = mapped_column(Text,
                                             nullable=False,
                                             default=getpass.getuser,
                                             onupdate=getpass.getuser)

    updated_at = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        values = ', '.join(f"{k}={repr(v)}" for k, v in self.__dict__.items())
        return f"{self.__class__.__name__}({values})"

    def to_dict(self, exclude_fields: Optional[list[str]] = None) -> dict[str, Any]:
        data = self.__dict__.copy()
        if exclude_fields:
            for field in exclude_fields:
                data.pop(field, None)
        data.pop("_sa_instance_state", None)
        return data