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

from ._model import *
import mf_lib.exceptions
import typing
import hashlib


class HashMappingsError(mf_lib.exceptions.FilterHandlerError):
    def __init__(self, ex, mappings):
        super().__init__(msg="hashing mappings failed: ", msg_args=f" mappings={mappings}", ex=ex)


class ParseMappingsError(mf_lib.exceptions.FilterHandlerError):
    def __init__(self, ex, mappings):
        super().__init__(msg="parsing mappings failed: ", msg_args=f" mappings={mappings}", ex=ex)


class IdentifierKeyError(mf_lib.exceptions.FilterHandlerError):
    def __init__(self, key, identifiers):
        super().__init__(msg=f"invalid key configuration: key='{key}' identifiers={identifiers}")


class DuplicateFilterIDError(mf_lib.exceptions.FilterHandlerError):
    def __init__(self, id):
        super().__init__(msg=f"filter ID already exists: id={id}")


def validate(obj, cls, name):
    assert obj, f"'{name}' can't be None"
    assert isinstance(obj, cls), f"'{name}' can't be of type '{type(obj).__name__}'"


def hash_mappings(mappings: typing.Dict):
    try:
        return hash_dict(mappings)
    except Exception as ex:
        raise HashMappingsError(ex, mappings)


def parse_mappings(mappings: typing.Dict) -> typing.Dict:
    try:
        parsed_mappings = {
            MappingType.data: list(),
            MappingType.extra: list()
        }
        for key, value in mappings.items():
            validate(value, str, "source path")
            dst_path, m_type = key.split(":")
            validate(dst_path, str, "destination path")
            validate(m_type, str, "mapping type")
            assert m_type in MappingType.__dict__.values()
            parsed_mappings[m_type].append(
                {
                    Mapping.src_path: value,
                    Mapping.dst_path: dst_path
                }
            )
        return parsed_mappings
    except Exception as ex:
        raise ParseMappingsError(ex, mappings)


def get_value(path: typing.List, obj: typing.Dict, size: int, pos: typing.Optional[int] = 0) -> typing.Any:
    if pos < size:
        return get_value(path, obj[path[pos]], size, pos + 1)
    return obj[path[pos]]


def mapper(mappings: typing.List, msg: typing.Dict, ignore_missing=False) -> typing.Generator:
    for mapping in mappings:
        try:
            src_path = mapping[Mapping.src_path].split(".")
            try:
                yield mapping[Mapping.dst_path], get_value(src_path, msg, len(src_path) - 1)
            except KeyError:
                if not ignore_missing:
                    raise
        except Exception as ex:
            raise mf_lib.exceptions.MappingError(ex, mapping)


def validate_identifier(key: str, value: typing.Optional[typing.Union[str, int, float]] = None):
    validate(key, str, f"identifier {Identifier.key}")
    if value:
        validate(value, (str, int, float), f"identifier {Identifier.value}")
    return key, value


def hash_str(obj: str) -> str:
    return hashlib.sha256(obj.encode()).hexdigest()


def hash_list(obj: typing.List) -> str:
    return hash_str("".join(obj))


def hash_dict(obj: typing.Dict) -> str:
    items = ["{}{}".format(key, value) for key, value in obj.items()]
    items.sort()
    return hash_list(items)
