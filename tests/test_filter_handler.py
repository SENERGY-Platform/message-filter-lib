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

    def test_get_results_good_filters(self):
        filter_handler = self._test_filter_ingestion(filters=filters_good)
        count = 0
        for source in data_good:
            for message in data_good[source]:
                try:
                    for result in filter_handler.get_results(message=message, source=source):
                        self.assertIn(str(result), get_results_good_filters)
                        count += 1
                except mf_lib.exceptions.NoFilterError:
                    pass
        self.assertEqual(count, len(get_results_good_filters) - 1)

    def test_get_results_bad_filters(self):
        filter_handler = self._test_filter_ingestion(filters=filters_bad)
        ex_count = 0
        r_count = 0
        for source in data_good:
            for message in data_good[source]:
                ex_count += 1
                try:
                    for result in filter_handler.get_results(message=message, source=source):
                        if result.ex:
                            self.assertIsInstance(result.ex, Exception)
                            ex_count -= 1
                        else:
                            self.assertIn(str(result), get_results_bad_filters)
                            r_count += 1
                except mf_lib.exceptions.NoFilterError:
                    ex_count -= 1
        self.assertEqual(ex_count, 0)
        self.assertEqual(r_count, len(get_results_bad_filters))

    def test_get_results_bad_messages(self):
        filter_handler = self._test_filter_ingestion(filters=filters_good)
        count = 0
        for source in data_bad:
            for message in data_bad[source]:
                try:
                    for _ in filter_handler.get_results(message=message, source=source):
                        count += 1
                except mf_lib.exceptions.MessageIdentificationError:
                    pass
        self.assertEqual(count, 0)

    def test_get_filter_args(self):
        filter_handler = self._test_filter_ingestion(filters=filters_good)
        export_args = filter_handler.get_filter_args(id="filter-1")
        self.assertEqual(export_args["arg"], "test")
