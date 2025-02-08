import os
from collections.abc import Callable, Generator, Iterable, Iterator
from typing import TypeVar

T = TypeVar('T')


def find(predicate: Callable[[T], bool], iterable: Iterable[T]) -> T | None:
    """
    Find the first element in an iterable that satisfies a predicate, or return `None` if no match
    is found.
    """

    for item in iterable:
        if predicate(item):
            return item

    return None


T = TypeVar('T')  # type: ignore
U = TypeVar('U')


def filter_map(function: Callable[[T], U | None], iterable: Iterable[T]) -> Iterator[U]:
    """
    Apply a function to each element of an iterator and yields the results that are not `None`.
    """

    for item in iterable:
        result = function(item)
        if result is not None:
            yield result


def try_parse_int(value: str) -> int | None:
    """
    Parse a string into an integer (base 10), or return `None` if the string does not correspond
    to an integer.
    """

    try:
        return int(value)
    except ValueError:
        return None


def iter_all_files(dir_path: str) -> Generator[str, None, None]:
    """
    Iterate through all the files in a directory recursively, and yield the path of each file
    relative to that directory.
    """

    for sub_dir_path, _, file_names in os.walk(dir_path):
        for file_name in file_names:
            file_path = os.path.join(sub_dir_path, file_name)
            yield os.path.relpath(file_path, start=dir_path)
