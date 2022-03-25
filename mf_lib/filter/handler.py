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

from .._util import hash_dict, hash_list, get_value, validate
from ..exceptions import _exceptions
from . import model
import mf_lib.builders
import typing
import threading


def hash_mappings(mappings: typing.Dict):
    try:
        return hash_dict(mappings)
    except Exception as ex:
        raise _exceptions.HashMappingsError(ex, mappings)


def parse_mappings(mappings: typing.Dict) -> typing.Dict:
    try:
        parsed_mappings = {
            model.MappingType.data: list(),
            model.MappingType.extra: list()
        }
        for key, value in mappings.items():
            validate(value, str, "source path")
            dst_path, m_type = key.split(":")
            validate(dst_path, str, "destination path")
            validate(m_type, str, "mapping type")
            assert m_type in model.MappingType.__dict__.values()
            parsed_mappings[m_type].append(
                {
                    model.Mapping.src_path: value,
                    model.Mapping.dst_path: dst_path
                }
            )
        return parsed_mappings
    except Exception as ex:
        raise _exceptions.ParseMappingsError(ex, mappings)


def mapper(mappings: typing.List, msg: typing.Dict, filter_ids: typing.Tuple) -> typing.Generator:
    for mapping in mappings:
        try:
            src_path = mapping[model.Mapping.src_path].split(".")
            yield mapping[model.Mapping.dst_path], get_value(src_path, msg, len(src_path) - 1)
        except Exception as ex:
            raise _exceptions.MappingError(ex, mapping, filter_ids)


def validate_identifier(key: str, value: typing.Optional[typing.Union[str, int, float]] = None):
    validate(key, str, f"identifier {model.Identifier.key}")
    if value:
        validate(value, (str, int, float), f"identifier {model.Identifier.value}")
    return key, value


class FilterResult:
    def __init__(self, data=None, extra=None, filter_ids=None, ex=None):
        self.data = data
        self.extra = extra
        self.filter_ids = filter_ids
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
    def __init__(self):
        self.__lock = threading.Lock()
        self.__identifiers = dict()
        self.__filters = dict()
        self.__mappings = dict()
        self.__sources = set()
        self.__filter_metadata = dict()
        self.__mappings_filter_map = dict()
        self.__identifiers_filter_map = dict()
        self.__sources_filter_map = dict()

    def __add_filter(self, i_str, m_hash, filter_id):
        try:
            self.__filters[i_str][m_hash].add(filter_id)
        except KeyError:
            if i_str not in self.__filters:
                self.__filters[i_str] = dict()
            if m_hash not in self.__filters[i_str]:
                self.__filters[i_str][m_hash] = {filter_id}

    def __del_filter(self, i_str, m_hash, filter_id):
        self.__filters[i_str][m_hash].discard(filter_id)
        if not self.__filters[i_str][m_hash]:
            del self.__filters[i_str][m_hash]
            if not self.__filters[i_str]:
                del self.__filters[i_str]

    def __add_mappings(self, mappings: typing.Dict, m_hash: str, filter_id: str):
        if m_hash not in self.__mappings:
            self.__mappings[m_hash] = parse_mappings(mappings=mappings)
        if m_hash not in self.__mappings_filter_map:
            self.__mappings_filter_map[m_hash] = {filter_id}
        else:
            self.__mappings_filter_map[m_hash].add(filter_id)

    def __del_mappings(self, m_hash: str, filter_id: str):
        self.__mappings_filter_map[m_hash].discard(filter_id)
        if not self.__mappings_filter_map[m_hash]:
            del self.__mappings[m_hash]
            del self.__mappings_filter_map[m_hash]

    def __add_identifier(self, identifiers: list, filter_id: str):
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
        if i_hash not in self.__identifiers:
            self.__identifiers[i_hash] = (set(i_keys), i_val_keys, "".join(i_no_val_keys), len(i_keys))
        if i_hash not in self.__identifiers_filter_map:
            self.__identifiers_filter_map[i_hash] = {filter_id}
        else:
            self.__identifiers_filter_map[i_hash].add(filter_id)
        return i_hash, "".join(i_values) + self.__identifiers[i_hash][2]

    def __del_identifier(self, i_hash: str, filter_id: str):
        self.__identifiers_filter_map[i_hash].discard(filter_id)
        if not self.__identifiers_filter_map[i_hash]:
            del self.__identifiers[i_hash]
            del self.__identifiers_filter_map[i_hash]

    def __add_source(self, source: str, filter_id: str):
        self.__sources.add(source)
        if source not in self.__sources_filter_map:
            self.__sources_filter_map[source] = {filter_id}
        else:
            self.__sources_filter_map[source].add(filter_id)

    def __del_source(self, source: str, filter_id: str):
        self.__sources_filter_map[source].discard(filter_id)
        if not self.__sources_filter_map[source]:
            self.__sources.discard(source)
            del self.__sources_filter_map[source]

    def __add_filter_metadata(self, filter_id: str, source: str, m_hash: str, i_hash: str, i_str: str, args: typing.Optional[typing.Dict] = None):
        self.__filter_metadata[filter_id] = {
            model.FilterMetadata.source: source,
            model.FilterMetadata.m_hash: m_hash,
            model.FilterMetadata.i_hash: i_hash,
            model.FilterMetadata.i_str: i_str,
            model.FilterMetadata.args: args
        }

    def __del_filter_metadata(self, filter_id: str):
        del self.__filter_metadata[filter_id]

    def __add(self, source: str, mappings: typing.Dict, id: str, identifiers: typing.Optional[list] = None, args: typing.Optional[typing.Dict] = None):
        validate(source, str, f"filter {model.Filter.source}")
        validate(mappings, dict, f"filter {model.Filter.mappings}")
        validate(id, str, f"filter {model.Filter.id}")
        if identifiers:
            validate(identifiers, list, f"filter {model.Filter.identifiers}")
        if args:
            validate(args, dict, f"filter {model.Filter.args}")
        with self.__lock:
            m_hash = hash_mappings(mappings=mappings)
            if identifiers:
                i_hash, i_str = self.__add_identifier(identifiers=identifiers, filter_id=id)
            else:
                i_hash = None
                i_str = source
            self.__add_filter_metadata(
                filter_id=id,
                source=source,
                m_hash=m_hash,
                i_hash=i_hash,
                i_str=i_str,
                args=args
            )
            self.__add_mappings(mappings=mappings, m_hash=m_hash, filter_id=id)
            self.__add_source(source=source, filter_id=id)
            self.__add_filter(
                i_str=i_str,
                m_hash=m_hash,
                filter_id=id
            )

    def __identify_msg(self, msg: typing.Dict):
        try:
            msg_keys = set(msg.keys())
            identifier = None
            for i_hash in self.__identifiers:
                if self.__identifiers[i_hash][0].issubset(msg_keys):
                    if not identifier:
                        identifier = self.__identifiers[i_hash]
                    else:
                        if self.__identifiers[i_hash][3] > identifier[3]:
                            identifier = self.__identifiers[i_hash]
            if identifier:
                return "".join([str(msg[key]) for key in identifier[1]]) + identifier[2]
        except Exception as ex:
            raise _exceptions.MessageIdentificationError(ex)

    def get_results(self, message: typing.Dict, source: typing.Optional[str] = None, data_builder: typing.Optional[typing.Callable[[typing.Generator], typing.Any]] = mf_lib.builders.dict_builder, extra_builder: typing.Optional[typing.Callable[[typing.Generator], typing.Any]] = mf_lib.builders.dict_builder) -> typing.Generator[FilterResult, None, None]:
        """
        Generator that applies filters to a message and yields extracted data.
        :param message: Dictionary containing message data.
        :param source: Message source.
        :param data_builder: Builder function for custom data structures. Default is ew_lib.builders.dict_builder.
        :param extra_builder: Builder function for custom data structures. Default is ew_lib.builders.dict_builder.
        :returns: FilterResult objects.
        """
        with self.__lock:
            i_str = self.__identify_msg(msg=message) or source
            if i_str in self.__filters:
                for m_hash in self.__filters[i_str]:
                    filter_ids = tuple(self.__filters[i_str][m_hash])
                    try:
                        yield FilterResult(
                            data=data_builder(mapper(mappings=self.__mappings[m_hash][model.MappingType.data], msg=message, filter_ids=filter_ids)),
                            extra=extra_builder(mapper(mappings=self.__mappings[m_hash][model.MappingType.extra], msg=message, filter_ids=filter_ids)),
                            filter_ids=filter_ids
                        )
                    except Exception as ex:
                        yield FilterResult(filter_ids=filter_ids, ex=ex)
            else:
                raise _exceptions.NoFilterError()

    def add_filter(self, filter: typing.Dict):
        """
        Add a filter.
        :param filter: Dictionary containing filter data.
        :return: None
        """
        try:
            self.__add(**filter)
        except Exception as ex:
            raise _exceptions.AddFilterError(ex)

    def delete_filter(self, id: str):
        """
        Delete a filter.
        :param id: ID of a filter to be deleted.
        :return: None
        """
        try:
            validate(id, str, "id")
            with self.__lock:
                if id in self.__filter_metadata:
                    filter_md = self.__filter_metadata[id]
                    self.__del_filter_metadata(filter_id=id)
                    if filter_md[model.FilterMetadata.i_hash]:
                        self.__del_identifier(i_hash=filter_md[model.FilterMetadata.i_hash], filter_id=id)
                    self.__del_mappings(m_hash=filter_md[model.FilterMetadata.m_hash], filter_id=id)
                    self.__del_source(source=filter_md[model.FilterMetadata.source], filter_id=id)
                    self.__del_filter(
                        i_str=filter_md[model.FilterMetadata.i_str],
                        m_hash=filter_md[model.FilterMetadata.m_hash],
                        filter_id=id
                    )
                else:
                    raise _exceptions.UnknownFilterIDError(filter_id=id)
        except Exception as ex:
            raise _exceptions.DeleteFilterError(ex)

    def get_filter_args(self, id: str) -> typing.Dict:
        """
        Get filter arguments.
        :param id: ID of a filter.
        :return: Dictionary containing args of a filter.
        """
        validate(id, str, "id")
        with self.__lock:
            if id in self.__filter_metadata:
                return self.__filter_metadata[id][model.FilterMetadata.args]
            else:
                raise _exceptions.UnknownFilterIDError(filter_id=id)

    def get_sources(self) -> typing.List:
        """
        Get all sources added by filters.
        :return: List containing sources.
        """
        with self.__lock:
            return list(self.__sources)
