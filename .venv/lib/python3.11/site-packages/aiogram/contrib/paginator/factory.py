from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional, cast

from magic_filter import AttrDict, MagicFilter

from aiogram import BaseMiddleware, Router
from aiogram.filters.callback_data import CallbackData
from aiogram.types import CallbackQuery, TelegramObject

from .callback_data import CDPagination, CDStub
from .paginator import Paginator
from .types import MaybeMagic, T


def resolve(value: MaybeMagic[T], data: AttrDict[str, Any]) -> T:
    if isinstance(value, MagicFilter):
        return cast(T, value.resolve(data))
    return value


async def stub_handler(query: CallbackQuery) -> Any:
    await query.answer()


@dataclass
class PaginationFactory(BaseMiddleware):
    paginator_cls: type[Paginator] = Paginator
    left_button_text: MaybeMagic[str] = "<<"
    right_button_text: MaybeMagic[str] = ">>"
    page_button_text: MaybeMagic[str] = "{}/{}"
    page_button_data: type[CDPagination] = CDPagination
    stub_callback_data: type[CallbackData] = CDStub
    rows_per_page: MaybeMagic[int] = 10
    default_row_size: MaybeMagic[int] = 1

    def register(self, router: Router, *event_types: str) -> None:
        for event_type in event_types:
            router.observers[event_type].middleware(self)
        router.callback_query.register(stub_handler, self.stub_callback_data.filter())

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        callback_data: Optional[CallbackData] = data.get("callback_data")
        attr_data: AttrDict[str, Any] = AttrDict(data)
        data["paginator"] = self.paginator_cls(
            left_button_text=resolve(self.left_button_text, attr_data),
            right_button_text=resolve(self.right_button_text, attr_data),
            page_button_text=resolve(self.page_button_text, attr_data),
            page_button_data=self.page_button_data,
            stub_callback_data=self.stub_callback_data(),
            offset=getattr(callback_data, "offset", 0),
            rows_per_page=resolve(self.rows_per_page, attr_data),
            default_row_size=resolve(self.default_row_size, attr_data),
        )
        return await handler(event, data)
