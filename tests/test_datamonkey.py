#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `datamonkey` package."""

import pytest
import pandas
import os
import json
import shutil

from datamonkey import FileProcessor


def load_json(file_path):
    with open(file_path) as file:
        # should probably use simplejson for better error handling
        return json.load(file)

@pytest.yield_fixture(autouse=True)
def run_around_tests():
    if not os.path.exists("tests/test_output/"):
        os.mkdir("tests/test_output/")

    # A test function will be run at this point
    yield

    # shutil.rmtree("tests/test_output")


def test_load_configuration():
    """ Test __load_configuration() """
    with pytest.raises(ValueError):
        # Test fake configuration invalid format
        FileProcessor("1234234")

    with pytest.raises(ValueError):
        # test no configuration supplied
        FileProcessor("")

    with pytest.raises(FileNotFoundError):
        # test configuration file doesn't exist
        FileProcessor("fc01da57-106a-4255-be48-3e634296ce3f", "fake")

    # with pytest.raises(ValueError):
    #     # test 404 is caught for invalid config
    #     FileProcessor("fc01da57-fake-fake-fake-3e634296ce3f")

    # test valid
    test_valid = FileProcessor("fc01da57-fake-fake-fake-3e634296ce3f",
                               template_file_path="tests/config_tests/configurations/type/csv/JSON_oto_header.json")
    assert test_valid.configuration is not None


def test_validate_configuration():
    """ Test __load_configuration() """

    with pytest.raises(ValueError):
        # test missing keys in config are caught
        FileProcessor("fc01da57-fake-fake-fake-3e634296ce3f",
                      template_file_path="tests/config_tests/configurations/invalid.json")


def test_csv_configurations():
    tests = load_json("tests/config_tests/csv_tests.json")
    __test_configuration_files(tests)


def test_io_configurations():
    tests = load_json("tests/config_tests/io_tests.json")
    __test_configuration_files(tests)


def test_validate_configurations():
    tests = load_json("tests/config_tests/validate_tests.json")
    __test_configuration_files(tests)


def test_transformation_options():
    tests = load_json("tests/config_tests/transform_tests.json")
    __test_configuration_files(tests)


def test_excel_configurations():
    tests = load_json("tests/config_tests/excel_tests.json")
    __test_configuration_files(tests)


def test_fwf_configurations():
    tests = load_json("tests/config_tests/fwf_tests.json")
    __test_configuration_files(tests)


def test_json_configurations():
    tests = load_json("tests/config_tests/json_tests.json")
    __test_configuration_files(tests)


def __test_configuration_files(tests):
    extensions = {"CSV": "csv", "JSON": "json", "EXCEL": "xls", "FWF": "txt"}

    for i, test in enumerate(tests):
        print("Now performing configuration test #%d, %s --- %s" % (i, test["name"], test["details"]))

        configuration_id = test["id"]
        configuration_file = test["configuration_file"]
        source_files = test["source_files"]
        first = test["first"]
        last = test["last"]
        number_items = test["number_items"]

        processor = FileProcessor(configuration_id, template_file_path=configuration_file)
        output_type = processor.output_file.type
        output_file_path = "tests/test_output/%s_output.%s" % (test["name"], extensions[output_type])
        error_file_path = "tests/test_output/"

        try:
            processor.process(source_files, output_file_path=output_file_path, error_file_path=error_file_path)
            assert len(processor.warnings) == test["warnings"]
            has_header = processor.output_file.has_header
            sheet_name = processor.output_file.sheet_name
            index_rows = processor.output_file.index_rows
            names = [field.name for field in processor.output_fields]

            if output_type == "CSV":
                objects = _test_csv_output(output_file_path, has_header, names)

            elif output_type == "JSON":
                objects = _test_json_output(output_file_path)

            elif output_type == "EXCEL":
                objects = _test_excel_output(output_file_path, has_header, sheet_name, names)

            elif output_type == "FWF":
                widths = [(field.col_specs[1] - field.col_specs[0] + 1)  for field in processor.output_fields]
                objects = _test_fwf_output(output_file_path, has_header, widths, names)

            assert len(objects) == number_items

            if number_items > 0:
                # test first object
                __test_output(first, objects[0])

            if number_items > 1:
                # test last object
                __test_output(last, objects[-1])

        except ValueError as error:
            # catch expected errors thrown by validation tests (validate_by... transformations)
            if test['errors']:
                assert len(processor.errors) == test["errors"]
            else:
                raise error


def _test_csv_output(file_path, has_header, names):
    objects = pandas.read_csv(file_path, header=0 if has_header else None, names=names)
    return objects.to_dict(orient="records")


def _test_json_output(file_path):
    return load_json(file_path)


def _test_excel_output(file_path, has_header, sheet_name, names):
    objects = pandas.read_excel(file_path, header=0 if has_header else None, sheet_name=sheet_name, names=names)
    return objects.to_dict(orient="records")


def _test_fwf_output(file_path, has_header, widths, names):
    objects = pandas.read_fwf(file_path, header=0 if has_header else None, names=names, widths=widths)
    return objects.to_dict(orient="records")


def __test_output(expected, output):
    # all of the keys in the output should match what was defined in the test configuration
    for key in expected.keys():
        try:
            assert key in output.keys()
        except AssertionError as e:
            e.args += (key,)
            raise

        try:
            assert expected[key] == output[key]
        except AssertionError as e:
            e.args += (key,)
            raise
