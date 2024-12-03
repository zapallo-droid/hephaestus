import ulid
from sqlalchemy.types import TypeDecorator, Text


class ULIDType(TypeDecorator):
    """
    SQLAlchemy custom type for applying ULID codes.
    Stores the ULID code as Text (UlID codes contains 26 characters)
    """
    