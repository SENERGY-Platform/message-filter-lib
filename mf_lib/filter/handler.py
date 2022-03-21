"""
   Copyright 2022 InfAI (CC SES)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

__all__ = ("FilterHandler", "FilterResult")

from .._util import hash_dict, hash_list, get_value, json_to_str, validate
from .. import builders
from .exceptions import *
from .model import *
import typing
import threading
import time

type_map = {
    "int": int,
    "float": float,
    "string": str,
    "bool": bool,
    "string_json": json_to_str
}


def hash_mappings(mappings: typing.Dict):
    try:
        return hash_dict(mappings)
    except Exception as ex:
        raise HashMappingError(ex, mappings)


def parse_mappings(mappings: typing.Dict) -> typing.Dict:
    try:
        parsed_mappings = {
            MappingType.data: list(),
            MappingType.extra: list()
        }
        for key, value in mappings.items():
            validate(value, str, "source path")
            dst_path, val_type, m_type = key.split(":")
            validate(dst_path, str, "destination path")
            validate(val_type, str, "value type")
            validate(m_type, str, "mapping type")
            assert m_type in MappingType.__dict__.values()
            parsed_mappings[m_type].append(
                {
                    Mapping.src_path: value,
                    Mapping.dst_path: dst_path,
                    Mapping.value_type: val_type
                }
            )
        return parsed_mappings
    except Exception as ex:
        raise ParseMappingError(ex, mappings)


def mapper(mappings: typing.List, msg: typing.Dict) -> typing.Generator:
    for mapping in mappings:
        try:
            src_path = mapping[Mapping.src_path].split(".")
            yield mapping[Mapping.dst_path], type_map[mapping[Mapping.value_type]](get_value(src_path, msg, len(src_path) - 1))
        except Exception as ex:
            raise MappingError(ex, mapping)


def validate_identifier(key: str, value: typing.Optional[typing.Union[str, int, float]] = None):
    validate(key, str, f"identifier {Identifier.key}")
    if value:
        validate(value, (str, int, float), f"identifier {Identifier.value}")
    return key, value


class FilterResult:
    def __init__(self, data=None, extra=None, exports=None, ex=None):
        self.data = data
        self.extra = extra
        self.exports = exports
        self.ex = ex

    def __iter__(self):
        for item in self.__dict__.items():
            if not item[0].startswith("_"):
                yield item

    def __str__(self):
        return str(dict(self))

    def __repr__(self):
        args = ", ".join(tuple(f"{key}={val}" for key, val in self))
        return f"{self.__class__.__name__}({args})"


class FilterHandler:
    """
    Provides functionality for adding and removing filters as well as applying filters to messages and extracting data.
    """
    __log_msg_prefix = "filter handler"
    __log_err_msg_prefix = f"{__log_msg_prefix} error"

    def __init__(self):
        self.__lock = threading.Lock()
        self.__msg_identifiers = dict()
        self.__msg_filters = dict()
        self.__mappings = dict()
        self.__sources = set()
        self.__exports = dict()
        self.__mappings_export_map = dict()
        self.__msg_identifiers_export_map = dict()
        self.__sources_export_map = dict()
        self.__sources_timestamp = None

    def __add_filter(self, i_str, m_hash, export_id):
        try:
            try:
                self.__msg_filters[i_str][m_hash].add(export_id)
            except KeyError:
                if i_str not in self.__msg_filters:
                    self.__msg_filters[i_str] = dict()
                if m_hash not in self.__msg_filters[i_str]:
                    self.__msg_filters[i_str][m_hash] = {export_id}
        except Exception as ex:
            raise AddFilterError(ex)

    def __del_filter(self, i_str, m_hash, export_id):
        try:
            self.__msg_filters[i_str][m_hash].discard(export_id)
            if not self.__msg_filters[i_str][m_hash]:
                del self.__msg_filters[i_str][m_hash]
                if not self.__msg_filters[i_str]:
                    del self.__msg_filters[i_str]
        except Exception as ex:
            raise DeleteFilterError(ex)

    def __add_mappings(self, mappings: typing.Dict, m_hash: str, export_id: str):
        try:
            if m_hash not in self.__mappings:
                self.__mappings[m_hash] = parse_mappings(mappings=mappings)
            if m_hash not in self.__mappings_export_map:
                self.__mappings_export_map[m_hash] = {export_id}
            else:
                self.__mappings_export_map[m_hash].add(export_id)
        except Exception as ex:
            raise AddMappingError(ex)

    def __del_mappings(self, m_hash: str, export_id: str):
        try:
            self.__mappings_export_map[m_hash].discard(export_id)
            if not self.__mappings_export_map[m_hash]:
                del self.__mappings[m_hash]
                del self.__mappings_export_map[m_hash]
        except Exception as ex:
            raise DeleteMappingError(ex)

    def __add_msg_identifier(self, identifiers: list, export_id: str):
        try:
            i_val_keys = list()
            i_no_val_keys = list()
            i_values = list()
            for identifier in identifiers:
                key, value = validate_identifier(**identifier)
                if value:
                    i_val_keys.append(key)
                    i_values.append(value)
                else:
                    i_no_val_keys.append(key)
            i_val_keys.sort()
            i_no_val_keys.sort()
            i_values.sort()
            i_keys = i_val_keys + i_no_val_keys
            i_hash = hash_list(i_keys)
            if i_hash not in self.__msg_identifiers:
                self.__msg_identifiers[i_hash] = (set(i_keys), i_val_keys, "".join(i_no_val_keys), len(i_keys))
            if i_hash not in self.__msg_identifiers_export_map:
                self.__msg_identifiers_export_map[i_hash] = {export_id}
            else:
                self.__msg_identifiers_export_map[i_hash].add(export_id)
            return i_hash, "".join(i_values) + self.__msg_identifiers[i_hash][2]
        except Exception as ex:
            raise AddMessageIdentifierError(ex)

    def __del_msg_identifier(self, i_hash: str, export_id: str):
        try:
            self.__msg_identifiers_export_map[i_hash].discard(export_id)
            if not self.__msg_identifiers_export_map[i_hash]:
                del self.__msg_identifiers[i_hash]
                del self.__msg_identifiers_export_map[i_hash]
        except Exception as ex:
            raise DeleteMessageIdentifierError(ex)

    def __add_source(self, source: str, export_id: str):
        try:
            self.__sources.add(source)
            if source not in self.__sources_export_map:
                self.__sources_export_map[source] = {export_id}
            else:
                self.__sources_export_map[source].add(export_id)
            self.__sources_timestamp = time.time_ns()
        except Exception as ex:
            raise AddSourceError(ex)

    def __del_source(self, source: str, export_id: str):
        try:
            self.__sources_export_map[source].discard(export_id)
            if not self.__sources_export_map[source]:
                self.__sources.discard(source)
                del self.__sources_export_map[source]
                self.__sources_timestamp = time.time_ns()
        except Exception as ex:
            raise DeleteSourceError(ex)

    def __add_export(self, export_id: str, source: str, m_hash: str, i_hash: str, i_str: str, export_args: typing.Optional[typing.Dict] = None):
        try:
            self.__exports[export_id] = {
                Export.source: source,
                Export.m_hash: m_hash,
                Export.i_hash: i_hash,
                Export.i_str: i_str,
                Export.args: export_args
            }
        except Exception as ex:
            raise AddExportError(ex)

    def __del_export(self, export_id: str):
        try:
            del self.__exports[export_id]
        except Exception as ex:
            raise DeleteExportError(ex)

    def __add(self, source: str, mappings: typing.Dict, export_id: str, identifiers: typing.Optional[list] = None, export_args: typing.Optional[typing.Dict] = None):
        validate(source, str, f"filter {Filter.source}")
        validate(mappings, dict, f"filter {Filter.mappings}")
        validate(export_id, str, f"filter {Filter.export_id}")
        if identifiers:
            validate(identifiers, list, f"filter {Filter.identifiers}")
        if export_args:
            validate(export_args, dict, f"filter {Filter.export_args}")
        with self.__lock:
            m_hash = hash_mappings(mappings=mappings)
            if identifiers:
                i_hash, i_str = self.__add_msg_identifier(identifiers=identifiers, export_id=export_id)
            else:
                i_hash = None
                i_str = source
            self.__add_export(
                export_id=export_id,
                source=source,
                m_hash=m_hash,
                i_hash=i_hash,
                i_str=i_str,
                export_args=export_args
            )
            self.__add_mappings(mappings=mappings, m_hash=m_hash, export_id=export_id)
            self.__add_source(source=source, export_id=export_id)
            self.__add_filter(
                i_str=i_str,
                m_hash=m_hash,
                export_id=export_id
            )
            self.__sources_timestamp = time.time_ns()

    def __identify_msg(self, msg: typing.Dict):
        try:
            msg_keys = set(msg.keys())
            identifier = None
            for i_hash in self.__msg_identifiers:
                if self.__msg_identifiers[i_hash][0].issubset(msg_keys):
                    if not identifier:
                        identifier = self.__msg_identifiers[i_hash]
                    else:
                        if self.__msg_identifiers[i_hash][3] > identifier[3]:
                            identifier = self.__msg_identifiers[i_hash]
            if identifier:
                return "".join([str(msg[key]) for key in identifier[1]]) + identifier[2]
        except Exception as ex:
            raise MessageIdentificationError(ex)

    def get_results(self, message: typing.Dict, source: typing.Optional[str] = None, builder: typing.Optional[typing.Callable[[typing.Generator], typing.Any]] = builders.dict_builder) -> typing.Generator[FilterResult, None, None]:
        """
        Generator that applies filters to a message and yields extracted data for exports.
        :param message: Dictionary containing message data.
        :param source: Message source.
        :param builder: Builder function for custom data structures. Default is ew_lib.builders.dict_builder.
        :returns: FilterResult objects.
        """
        with self.__lock:
            i_str = self.__identify_msg(msg=message) or source
            if i_str in self.__msg_filters:
                for m_hash in self.__msg_filters[i_str]:
                    try:
                        yield FilterResult(
                            data=builder(mapper(self.__mappings[m_hash][MappingType.data], message)),
                            extra=builder(mapper(self.__mappings[m_hash][MappingType.extra], message)),
                            exports=tuple(self.__msg_filters[i_str][m_hash])
                        )
                    except Exception as ex:
                        yield FilterResult(ex=ex)
            else:
                raise NoFilterError(message)

    def add_filter(self, filter: typing.Dict):
        """
        Add a filter.
        :param filter: Dictionary containing filter data.
        :return: None
        """
        self.__add(**filter)

    def delete_filter(self, export_id: str):
        """
        Delete a filter.
        :param export_id: ID of an export of which the filter is to be deleted.
        :return: None
        """
        validate(export_id, str, Filter.export_id)
        with self.__lock:
            if export_id in self.__exports:
                export = self.__exports[export_id]
                self.__del_export(export_id=export_id)
                if export[Export.i_hash]:
                    self.__del_msg_identifier(i_hash=export[Export.i_hash], export_id=export_id)
                self.__del_mappings(m_hash=export[Export.m_hash], export_id=export_id)
                self.__del_source(source=export[Export.source], export_id=export_id)
                self.__del_filter(
                    i_str=export[Export.i_str],
                    m_hash=export[Export.m_hash],
                    export_id=export_id
                )

    def get_export_args(self, export_id: str) -> typing.Dict:
        """
        Get export arguments.
        :param export_id: ID of an export.
        :return: Dictionary containing args of an export.
        """
        with self.__lock:
            return self.__exports[export_id][Export.args]

    def get_sources(self) -> typing.List:
        """
        Get all sources added by filters.
        :return: List containing sources.
        """
        with self.__lock:
            return list(self.__sources)

    def get_sources_timestamp(self) -> typing.Optional[int]:
        """
        Get timestamp that indicates the last time a filter was added or removed.
        :return: Timestamp or None.
        """
        with self.__lock:
            return self.__sources_timestamp
