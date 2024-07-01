#!/usr/bin/env python

"""
Read the shared state time series as recorded by nanover.

This file can be used as a python module or as a script.

Example when used as a module:

.. code:: python

    import read_state

    # Print the timestamps and the state updates
    from timestamp, update in iter_state_file('my_file.state'):
        print(timestamp, update)

    # Print the timestamps and the full state
    from timestamp, state in iter_full_states(iter_state_file('my_file.state')):
        print(timestamp, state)

Example when used as a script:

.. code:: bash

    # show the help
    python read_state.py --help

    # show the timestamps and state updates one record per line
    python read_state.py my_file.state
    # show the timestamps and the state updates in human-readable form
    python read_state.py --pretty my_file.state

    # show the timestamps and full states one record per line
    python read_state.py --full my_file.state
    # show the timestamps and full state in human-readable form
    python read_state.py --full --pretty my_file.state

Recording files written using Narupa may contain the string "narupa" in some
state update keys. Using the `--narupa` keyword of the `read_state.py` command
line writes a new file with the occurence of "narupa" by "nanover".

"""

import argparse
from typing import BinaryIO, Iterable, Tuple
from pathlib import Path
from pprint import pprint
from nanover.recording.reading import (
    iter_state_file,
    read_u64,
    MAGIC_NUMBER,
    InvalidMagicNumber,
    UnsupportedFormatVersion,
    iter_state_recording,
)
from nanover.recording.writing import write_entry
from nanover.state.state_service import dictionary_change_to_state_update
from nanover.utilities.change_buffers import DictionaryChange


class Header:
    magic_number: int
    format_version: int

    def __init__(self, magic_number, format_version):
        self.magic_number = magic_number
        self.format_version = format_version

    def as_bytes(self) -> bytes:
        magic_number_bytes = self.magic_number.to_bytes(8, "little", signed=False)
        format_version_bytes = self.format_version.to_bytes(8, "little", signed=False)
        return magic_number_bytes + format_version_bytes


def read_header(input_stream: BinaryIO) -> Header:
    supported_format_versions = (2,)
    magic_number = read_u64(input_stream)
    if magic_number != MAGIC_NUMBER:
        raise InvalidMagicNumber
    format_version = read_u64(input_stream)
    if format_version not in supported_format_versions:
        raise UnsupportedFormatVersion(format_version, supported_format_versions)
    return Header(magic_number, format_version)


def iter_updates(changes: Iterable[Tuple[int, DictionaryChange]]):
    for timestamp, change in changes:
        yield (timestamp, change.updates)


def iter_full_states(changes: Iterable[Tuple[int, DictionaryChange]]):
    """
    Read a stream of timestamps and state updates and yield the timestamp
    and the aggregated state.
    """
    aggregate_state = {}
    for timestamp, change in changes:
        aggregate_state.update(change.updates)
        aggregate_state = {
            key: value for key, value in aggregate_state.items() if value is not None
        }
        yield (timestamp, aggregate_state)


def copy_header(input_stream: BinaryIO, output_stream: BinaryIO) -> Header:
    header = read_header(input_stream)
    output_stream.write(header.as_bytes())
    return header


def recursive_replace(value, to_replace, replace_by):
    if not isinstance(value, dict):
        return value

    return {
        key.replace(to_replace, replace_by): recursive_replace(
            value, to_replace, replace_by
        )
        for key, value in value.items()
    }


def replace_and_copy_records(
    input_stream: BinaryIO, output_stream: BinaryIO, to_replace, replace_by
):
    for timestamp, change in iter_state_recording(input_stream):
        change.updates = {
            key.replace(to_replace, replace_by): recursive_replace(
                value, to_replace, replace_by
            )
            for key, value in change.updates.items()
        }
        change.removals = [
            key.replace(to_replace, replace_by) for key, value in change.removes
        ]

        write_entry(output_stream, timestamp, dictionary_change_to_state_update(change))


def replace_narupa(input_stream: BinaryIO, output_stream: BinaryIO):
    """
    Replace every occurrence of `narupa` to `nanover` in key names.
    """
    header = copy_header(input_stream, output_stream)
    replace_and_copy_records(input_stream, output_stream, header, "narupa", "nanover")


def command_line():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--full",
        action="store_true",
        default=False,
        help="Display the aggregated state instead of the state updates.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        default=False,
        help="Display the state in a more human-readable way.",
    )
    parser.add_argument(
        "--narupa",
        type=Path,
        default=None,
        help='Write a new file where "narupa" is replaced by "nanover" in all keys.',
    )
    parser.add_argument("path", help="Path to the file to read.")
    args = parser.parse_args()

    if args.narupa is not None:
        with open(args.path, "rb") as input_file, open(args.narupa, "wb") as writer:
            replace_narupa(input_file, writer)
    else:
        stream = iter_state_file(args.path)
        if args.full:
            stream = iter_full_states(stream)
        else:
            stream = iter_updates(stream)

        for update in stream:
            if args.pretty:
                print(f"---- {update[0]} ---------")
                pprint(update[1])
            else:
                print(update)


if __name__ == "__main__":
    command_line()
