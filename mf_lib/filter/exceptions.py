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

import traceback


class FilterHandlerError(Exception):
    def __init__(self, msg, msg_args=None, ex=None):
        msg += ": "
        if ex:
            ex_str = [item.strip().replace("\n", " ") for item in traceback.format_exception_only(type(ex), ex)]
            msg += f"reason={ex_str} "
        if msg_args:
            msg += msg_args
        super().__init__(msg)


class MessageIdentificationError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(msg="message identification failed", ex=ex)


class NoFilterError(FilterHandlerError):
    def __init__(self):
        super().__init__("no filters for message")


class MappingError(FilterHandlerError):
    def __init__(self, ex, mapping):
        super().__init__(msg="mapping error", msg_args=f"mapping={mapping}", ex=ex)


class HashMappingsError(FilterHandlerError):
    def __init__(self, ex, mappings):
        super().__init__(msg="hashing mappings failed", msg_args=f"mappings={mappings}", ex=ex)


class ParseMappingsError(FilterHandlerError):
    def __init__(self, ex, mappings):
        super().__init__(msg="parsing mappings failed", msg_args=f"mappings={mappings}", ex=ex)


class AddFilterError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(msg="adding filter failed", ex=ex)


class DeleteFilterError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(msg="deleting filter failed", ex=ex)


class AddMappingsError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(msg="adding mappings failed", ex=ex)


class DeleteMappingsError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(msg="deleting mappings failed", ex=ex)


class AddSourceError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(msg="adding source failed", ex=ex)


class DeleteSourceError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(msg="deleting source failed", ex=ex)


class AddMessageIdentifierError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(msg="adding message identifier failed", ex=ex)


class DeleteMessageIdentifierError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(msg="deleting message identifier failed", ex=ex)


class AddFilterMetadataError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(msg="adding filter metadata failed", ex=ex)


class DeleteFilterMetadataError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(msg="deleting metadata failed", ex=ex)
