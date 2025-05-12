from aiogram.filters.callback_data import CallbackData


class CDStub(CallbackData, prefix="aiogram-contrib-stub"):
    pass


class CDPagination(CallbackData, prefix="aiogram-contrib-pt"):
    type: str
    offset: int = 0
