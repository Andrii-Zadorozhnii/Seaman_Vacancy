from math import ceil


def count_pages(count: int, rows_per_page: int) -> int:
    result = ceil(count / rows_per_page)
    if result is None:
        return 0
    return result
