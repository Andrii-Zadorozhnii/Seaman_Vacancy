from __future__ import annotations

import math
from dataclasses import dataclass, field
from math import ceil
from typing import Callable, Optional

from aiogram.filters.callback_data import CallbackData
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from .callback_data import CDPagination, CDStub
from .types import T
from .utils import count_pages


@dataclass
class Paginator:
    left_button_text: str = "<<"
    right_button_text: str = ">>"
    page_button_text: str = "{}/{}"
    page_button_data: type[CDPagination] = CDPagination
    stub_callback_data: CallbackData = field(default_factory=CDStub)
    offset: int = 0
    total_count: int = 0
    rows_per_page: int = 10
    default_row_size: int = 1

    @property
    def current_page(self) -> int:
        return ceil(self.offset / self.rows_per_page) + 1

    def recalculate_offset(
        self,
        total_count: int,
        offset: Optional[int] = None,
        rows_per_page: Optional[int] = None,
    ) -> None:
        if offset is not None:
            self.offset = offset
        if rows_per_page is not None:
            self.rows_per_page = rows_per_page
        self.total_count = total_count
        total_pages = math.ceil(total_count / self.rows_per_page)
        if self.offset >= total_count:
            self.offset = 0
        elif self.offset < 0:
            self.offset = (total_pages - 1) * self.rows_per_page

    def get_keyboard(
        self,
        objects: list[T],
        button_getter: Callable[[T], InlineKeyboardButton],
        menu_type: str,
        attach: Optional[InlineKeyboardBuilder] = None,
        attach_above_arrows: Optional[InlineKeyboardBuilder] = None,
        row_size: Optional[int] = None,
        pagination_button_data: Optional[type[CDPagination]] = None,
    ) -> InlineKeyboardMarkup:
        if pagination_button_data is None:
            pagination_button_data = self.page_button_data

        objects_builder: InlineKeyboardBuilder = InlineKeyboardBuilder()
        pagination_builder: InlineKeyboardBuilder = InlineKeyboardBuilder()
        summarized_builder: InlineKeyboardBuilder = InlineKeyboardBuilder()

        objects_builder.row(*[button_getter(obj) for obj in objects])
        total_pages: int = count_pages(
            count=self.total_count,
            rows_per_page=self.rows_per_page,
        )

        if total_pages >= 2:
            pagination_builder.button(
                text=self.left_button_text,
                callback_data=pagination_button_data(
                    type=menu_type,
                    offset=self.offset - self.rows_per_page,
                ),
            )

            pagination_builder.button(
                text=self.page_button_text.format(self.current_page, total_pages),
                callback_data=self.stub_callback_data,
            )

            pagination_builder.button(
                text=self.right_button_text,
                callback_data=pagination_button_data(
                    type=menu_type,
                    offset=self.offset + self.rows_per_page,
                ),
            )

        objects_builder.adjust(row_size or self.default_row_size, repeat=True)
        pagination_builder.adjust(3)
        summarized_builder.attach(objects_builder)
        if attach_above_arrows is not None:
            summarized_builder.attach(attach_above_arrows)
        summarized_builder.attach(pagination_builder)
        if attach is not None:
            summarized_builder.attach(attach)
        return summarized_builder.as_markup()
