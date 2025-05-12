import asyncio
import logging
from typing import Any

from aiogram import Bot
from aiogram.client.session.middlewares.base import (
    BaseRequestMiddleware,
    NextRequestMiddlewareType,
)
from aiogram.dispatcher.dispatcher import DEFAULT_BACKOFF_CONFIG
from aiogram.exceptions import (
    RestartingTelegram,
    TelegramNetworkError,
    TelegramRetryAfter,
    TelegramServerError,
)
from aiogram.methods import AnswerCallbackQuery, Response, TelegramMethod
from aiogram.methods.base import TelegramType
from aiogram.utils.backoff import Backoff, BackoffConfig

from ..const import DEFAULT_MAX_RETRIES

logger: logging.Logger = logging.getLogger(__name__)


class RetryRequestMiddleware(BaseRequestMiddleware):
    backoff_config: BackoffConfig
    max_retries: int
    exclude_methods: tuple[type[TelegramMethod[Any]]]

    __slots__ = ("backoff_config", "max_retries", "exclude_methods")

    def __init__(
        self,
        backoff_config: BackoffConfig = DEFAULT_BACKOFF_CONFIG,
        max_retries: int = DEFAULT_MAX_RETRIES,
        exclude_methods: tuple[type[TelegramMethod[Any]]] = (AnswerCallbackQuery,),
    ) -> None:
        self.backoff_config = backoff_config
        self.max_retries = max_retries
        self.exclude_methods = exclude_methods

    async def __call__(
        self,
        make_request: NextRequestMiddlewareType[TelegramType],
        bot: Bot,
        method: TelegramMethod[TelegramType],
    ) -> Response[TelegramType]:
        backoff: Backoff = Backoff(config=self.backoff_config)
        retries: int = 0
        max_retries: int = (
            method.model_extra.get("_max_retries", self.max_retries)
            if method.model_extra is not None
            else self.max_retries
        )

        while True:
            retries += 1
            try:
                return await make_request(bot, method)
            except TelegramRetryAfter as e:
                if isinstance(method, self.exclude_methods):
                    raise
                if retries == max_retries:
                    raise
                logger.error(
                    "Request '%s' failed due to rate limit. Sleeping %s seconds.",
                    type(method).__name__,
                    e.retry_after,
                )
                backoff.reset()
                await asyncio.sleep(e.retry_after)

            except (TelegramServerError, RestartingTelegram, TelegramNetworkError) as error:
                if retries == max_retries:
                    raise
                logger.error(
                    "Request '%s' failed due to %s - %s. Sleeping %s seconds.",
                    type(method).__name__,
                    type(error).__name__,
                    error,
                    backoff.next_delay,
                )
                await backoff.asleep()
