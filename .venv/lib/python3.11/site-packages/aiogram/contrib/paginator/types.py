from typing import Any, TypeVar, Union

from magic_filter import MagicFilter

T = TypeVar("T", bound=Any)
MaybeMagic = Union[T, MagicFilter]
