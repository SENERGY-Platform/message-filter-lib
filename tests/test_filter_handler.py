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

import unittest
import mf_lib
import json

with open("tests/resources/sources.json") as file:
    sources: list = json.load(file)

with open("tests/resources/data_good.json") as file:
    data_good: list = json.load(file)

with open("tests/resources/data_bad.json") as file:
    data_bad: list = json.load(file)

with open("tests/resources/filters_good.json") as file:
    filters_good: list = json.load(file)

with open("tests/resources/filters_bad.json") as file:
    filters_bad: list = json.load(file)

with open("tests/resources/get_results_good_filters.json") as file:
    get_results_good_filters: list = json.load(file)

with open("tests/resources/get_results_bad_filters.json") as file:
    get_results_bad_filters: list = json.load(file)


class TestFilterHandler(unittest.TestCase):
    def _test_filter_ingestion(self, filters):
        filter_handler = mf_lib.FilterHandler()
        count = 0
        for item in filters:
            try:
                if item["action"] == "delete":
                    filter_handler.delete_filter(id=item["id"])
                if item["action"] == "add":
                    filter_handler.add_filter(filter=item["filter"])
                count += 1
            except Exception as ex:
                self.assertIsInstance(ex, mf_lib.exceptions.FilterHandlerError)
                count += 1
        self.assertEqual(count, len(filters))
        for source in filter_handler.get_sources():
            self.assertIn(source, sources)
        return filter_handler

    def _test_get_results(self, filters, data, data_builder=mf_lib.builders.dict_builder, extra_builder=mf_lib.builders.dict_builder, data_ignore_missing_keys=False, extra_ignore_missing_keys=False):
        filter_handler = self._test_filter_ingestion(filters=filters)
        for source in data:
            for message in data[source]:
                try:
                    for result in filter_handler.get_results(message=message, source=source, data_builder=data_builder, extra_builder=extra_builder, data_ignore_missing_keys=data_ignore_missing_keys, extra_ignore_missing_keys=extra_ignore_missing_keys):
                        yield result
                except (mf_lib.exceptions.NoFilterError, mf_lib.exceptions.MessageIdentificationError):
                    pass

    def test_get_results(self):
        count = 0
        for result in self._test_get_results(filters=filters_good, data=data_good):
            if result.ex:
                self.assertIsInstance(result.ex, mf_lib.exceptions.MappingError)
            self.assertIn(str(result), get_results_good_filters[count])
            count += 1

    def test_get_results_data_ignore_missing_keys(self):
        count = 0
        for result in self._test_get_results(filters=filters_good, data=data_good, data_ignore_missing_keys=True):
            self.assertIn(str(result), get_results_good_filters[count])
            count += 1

    def test_get_results_tuple_list_builder(self):
        count = 0
        for result in self._test_get_results(filters=filters_good, data=data_good, data_builder=mf_lib.builders.tuple_list_builder, extra_builder=mf_lib.builders.tuple_list_builder, data_ignore_missing_keys=True):
            self.assertIn(str(result), get_results_good_filters[count])
            count += 1

    def test_get_results_string_list_builder(self):
        count = 0
        for result in self._test_get_results(filters=filters_good, data=data_good, data_builder=mf_lib.builders.string_list_builder, extra_builder=mf_lib.builders.string_list_builder, data_ignore_missing_keys=True):
            self.assertIn(str(result), get_results_good_filters[count])
            count += 1

    def test_get_results_bad_filters(self):
        count = 0
        for result in self._test_get_results(filters=filters_bad, data=data_good):
            if result.ex:
                self.assertIsInstance(result.ex, mf_lib.exceptions.FilterHandlerError)
            self.assertIn(str(result), get_results_bad_filters[count])
            count += 1

    def test_get_results_bad_messages(self):
        for result in self._test_get_results(filters=filters_good, data=data_bad):
            self.assertIsNone(result)

    def test_get_filter_args(self):
        filter_handler = self._test_filter_ingestion(filters=filters_good)
        export_args = filter_handler.get_filter_args(id="filter-1")
        self.assertEqual(export_args["arg"], "test")
