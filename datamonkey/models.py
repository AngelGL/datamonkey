import pandas
import numpy
import ujson as json
import requests
import os
import io

from datamonkey.settings import BASE_API_URL
from datamonkey.helpers import validate_file_exists, check_S3_path


class File:

    CSV, EXCEL, JSON, FWF, PYTHON = ("CSV", "EXCEL", "JSON", "FWF", "PYTHON")
    valid_extensions = {
        CSV: ("csv", "txt"),
        EXCEL: ("xlsx", "xls", "xlm", "xlb"),
        JSON: ("json", "txt"),
        FWF: ("txt", "flat")
    }

    @property
    def file_path(self):
        return self._file_path

    @file_path.setter
    def file_path(self, path):
        self.s3_path = check_S3_path(path)
        self._file_path = path

    def __init__(self, type, hasHeader, lineDelimitedJSON, sheetName, skipRows):
        self.s3_path = False  # whether the file path is a reference to AWS S3
        self.type = type
        self.has_header = hasHeader  # whether a CSV or EXCEL file has a header
        self.skip_rows = skipRows  # CSV/EXCEL: the number of lines to skip at the start of a file
        self.line_delimited_JSON = lineDelimitedJSON  # whether a JSON file follows the LDJSON standard (one object per line)
        self.sheet_name = 0 if sheetName == "" else sheetName  # EXCEL: the sheet name to pull data in an EXCEL files

    def _validate(self):
        self._validate_type()

    def _validate_type(self):
        if self.type not in [File.CSV, File.EXCEL, File.JSON, File.PYTHON, File.FWF]:
            raise ValueError("%s is not a valid file type." % self.type)

    def remove_existing_file(self):
        """ Remove file at path if it already exists. Some file types will append data, and existing files should be overwritten. """
        if self.s3_path:
            import s3fs
            fs = s3fs.S3FileSystem()
            if fs.exists(self.file_path):
                fs.rm(self.file_path)
        else:
            if os.path.isfile(self.file_path):
                os.remove(self.file_path)


class SourceFile(File):

    def __init__(self, type, file_index, hasHeader=False, lineDelimitedJSON=False, sheetName="", skipRows=0, **kwargs):
        super(SourceFile, self).__init__(type, hasHeader, lineDelimitedJSON, sheetName, skipRows)
        self.file_index = file_index
        self.generator = None
        self.start_index = 1 + skipRows + (1 if hasHeader else 0)

    def _validate(self):
        super(SourceFile, self)._validate()

    def process_file(self, source_fields, chunk_data=False):
        if self.type == File.CSV:
            return self._process_csv_file(source_fields, chunk_data)
        elif self.type == File.JSON:
            return self._process_json_file(source_fields)
        elif self.type == File.EXCEL:
            return self._process_excel_file(source_fields)
        elif self.type == File.FWF:
            return self._process_fixed_width_file(source_fields, chunk_data)

    def _process_csv_file(self, source_fields, chunk_data=False):
        """
        Processes a CSV file using supplied configuration. Parses the file into a pandas dataframe for additional
        processing.
        """
        columns = pandas.read_csv(self.file_path,
                                  nrows=1,
                                  skiprows=self.skip_rows).columns

        if source_fields is None:
            # Generate fields from data in first row.
            if self.has_header:
                source_fields = [SourceField(column, True, self.file_index) for column in columns]
            else:
                source_fields = [SourceField("Column %d" % (i + 1), True, self.file_index) for i, column in enumerate(columns)]

        if len(columns) != len(source_fields):
            raise ValueError(("%d fields were expected in the file, but only %d were found. \n"
                              "If the input file format has changed, please update your file template.") %
                             (len(source_fields), len(columns)))

        if self.has_header:
            header = 0
            names = None
            for field in source_fields:
                if field.name not in columns:
                    raise ValueError("Expected header field '%s' was not found in the file. "
                                     "If this field is no longer required, please update the file template." % field.name)

            use_cols = [field.name for field in source_fields if field.used]

        else:
            header = None
            use_cols = [i for i, field in enumerate(source_fields) if field.used]
            names = ["Column %d" % (i + 1) for i in use_cols]

        return pandas.read_csv(self.file_path,
                               header=header,
                               usecols=use_cols,
                               names=names,
                               skiprows=self.skip_rows,
                               chunksize=100000 if chunk_data else None), source_fields

    def _process_json_file(self, source_fields):
        """
        Processes a JSON file using supplied configuration. Parses the file into a pandas dataframe for additional
        processing.
        """

        try:
            data = pandas.read_json(self.file_path,
                                    self.line_delimited_JSON)

            if source_fields is None:
                # generate fields from column keys
                source_fields = [SourceField(column, True, self.file_index) for column in data.columns]

        except Exception as error:
            # get a more descriptive error
            import json
            with open(self.file_path) as file:
                json.load(file)
            raise ValueError("Invalid JSON encountered in file: %s" % self.file_path)  # fallback error if json loads correctly in SimpleJson but not Pandas

        used_fields = [field for field in source_fields if field.used]

        for field in used_fields:
            if field.name not in list(data.columns):
                # treat missing keys as a column of nulls
                data[field.name] = ""

        unused_field_names = [field.name for field in source_fields if not field.used]
        data.drop(columns=unused_field_names)

        return data, source_fields

    def _process_fixed_width_file(self, source_fields, chunk_data=False):
        """
        Processes a flat file using supplied configuration. Parses the file into a pandas dataframe for additional
        processing.
        """
        if source_fields:
            # pandas has some weird issues with the colspecs list based on testing... use field widths instead
            widths = [field.col_specs[1] - field.col_specs[0] for field in source_fields]
        else:
            widths = None

        columns = pandas.read_fwf(self.file_path,
                                  widths=widths,
                                  nrows=1,
                                  skiprows=self.skip_rows).columns

        if source_fields is None:
            # Generate fields from data in first row, used when running reports.
            if self.has_header:
                source_fields = [SourceField(column, True, self.file_index) for column in columns]
            else:
                source_fields = [SourceField("Column %d" % (i + 1), True, self.file_index) for i, column in enumerate(columns)]

        if len(columns) != len(source_fields):
            raise ValueError(("%d fields were expected in the file, but only %d were found. \n"
                              "If the input file format has changed, please update your file template.") %
                             (len(source_fields), len(columns)))

        if self.has_header:
            header = 0
            names = None
            for field in source_fields:
                if field.name not in columns:
                    raise ValueError("Expected header field '%s' was not found in the file. "
                                     "If this field is no longer required, please update the file template." % field.name)

            use_cols = [field.name for field in source_fields if field.used]

        else:
            header = None
            use_cols = [i for i, field in enumerate(source_fields) if field.used]
            names = ["Column %d" % (i + 1) for i in use_cols]

        return pandas.read_fwf(self.file_path,
                               widths=widths,
                               header=header,
                               names=names,
                               use_cols=use_cols,
                               chunksize=100000 if chunk_data else None,
                               skiprows=self.skip_rows), source_fields

    def _process_excel_file(self, source_fields):
        """
        Processes an excel using supplied configuration. Parses the file into a pandas dataframe for additional
        processing.
        """
        columns = pandas.read_excel(self.file_path,
                                    nrows=1,
                                    sheet_name=self.sheet_name,
                                    skiprows=self.skip_rows).columns

        if source_fields is None:
            # Generate fields from data in first row.
            if self.has_header:
                source_fields = [SourceField(column, True, self.file_index) for column in columns]
            else:
                source_fields = [SourceField("Column %d" % (i + 1), True, self.file_index) for i, column in enumerate(columns)]

                parse_dates = list(range(1, len(source_fields)))

        if len(columns) != len(source_fields):
            raise ValueError(("%d columns were expected in the file, but only %d were found. \n"
                              "If the input file format has changed, please update your file template.") %
                             (len(source_fields), len(columns)))
        if self.has_header:
            header = 0

            for field in source_fields:
                if field.name not in columns:
                    raise ValueError("A column with header '%s' is expected but not found in the file. "
                                     "If this field is no longer required, please update the file template." % field.name)

            use_cols = [columns.tolist().index(field.name) for field in source_fields if field.used]
            data = pandas.read_excel(self.file_path,
                                     header=header,
                                     usecols=use_cols,
                                     sheet_name=self.sheet_name,
                                     skiprows=self.skip_rows)

        else:
            header = None
            use_cols = [i for i, field in enumerate(source_fields) if field.used]
            names = ["Column %d" % (i + 1) for i in use_cols]

            data = pandas.read_excel(self.file_path,
                                     header=header,
                                     names=names,
                                     usecols=use_cols,
                                     sheet_name=self.sheet_name,
                                     parse_dates=False,
                                     skiprows=self.skip_rows)

        return data, source_fields


class OutputFile(File):

    @property
    def data(self):
        if self._data is None:
            return ValueError("You must successfully process a file before accessing the output data.")
        return self._data.to_dict("records")

    NONE, GZIP, BZ2, ZIP, XZ = ('none', 'gzip', 'bz2', 'zip', 'xz')

    def __init__(self, type, hasHeader=False, lineDelimitedJSON=False, sheetName="Sheet1", skipRows=0, compression="", name="", delimiter=",", indexRows=False, indent=2, **kwargs):

        super(OutputFile, self).__init__(type, hasHeader, lineDelimitedJSON, sheetName, skipRows)
        self.compression = compression  # the type of compression pandas will use on the output file
        self.delimiter = delimiter  # CSV: delimiter used in the file
        self.index_rows = indexRows  # EXCEL/CSV: whether or not to show the row index in the output
        self.indent = indent  # JSON: how many spaces will be used to indent the file. Defaults to 2, which is human-readable.
        self.fwf_format = None
        self.fwf_header_format = None

        self._data = None
        self._first_write = True

        if name:
            self.name = name
        else:
            if self.type in self.valid_extensions.keys():
                self.name = "output.%s" % (self.valid_extensions[self.type][0])
            else:
                self.name = None

    def _validate(self):
        super(OutputFile, self)._validate()
        self._validate_compression()

    def _validate_compression(self):
        if self.compression and self.compression not in [OutputFile.GZIP, OutputFile.BZ2, OutputFile.ZIP, OutputFile.XZ]:
            raise ValueError("%s is not a valid compression type." % self.type)

    def append_data(self, data):
        """ If a file type can be flushed, e.g. CSVs, data will be None.
            If a file must be written in one go, e.g. Excel, data must be appended.
        """
        if self._data is None:
            self._data = data
        else:
            self._data = pandas.concat([self._data, data])

    def generate_output(self, fields=None):
        # TODO: Figure out how compression should be implemented
        if self.type == File.CSV:
            pass
        elif self.type == File.JSON:
            self._close_json_file()
        elif self.type == File.FWF:
            pass
        elif self.type == File.EXCEL:
            self._generate_excel_file()
        elif self.type == File.PYTHON:
            # data when be returned to the user when the run is complete.
            pass

    def flush_output(self, fields=None):
        """
        Some output file types can be flushed as data is accrued to reduce memory load.
        Currently allowed: CSV & JSON
        Not allowed: EXCEL
        """
        if self.type == File.CSV:
            self._flush_csv_file()
            self.reset_data()
            self._first_write = False

        elif self.type == File.JSON:
            self._flush_json_file()
            self.reset_data()
            self._first_write = False

        elif self.type == File.FWF:
            self._flush_fwf_file()
            self.reset_data()
            self._first_write = False

    def reset_data(self):
        self._data = None

    def _write_data(self, data, mode="ab", format=None):
        if self.s3_path:
            import s3fs
            fs = s3fs.S3FileSystem()
            open_file = fs.open
        else:
            open_file = open

        with open_file(self.file_path, mode) as file:
            if self.type in [File.JSON, File.CSV, File.EXCEL]:
                file.write(data)

            elif self.type == File.FWF:
                numpy.savetxt(file, data, fmt=format)

    def _flush_json_file(self):
        data = json.dumps(json.loads(self._data.to_json(orient="records")), indent=self.indent)

        if not self._first_write:
            data = "," + data[1:]  # remove opening bracket

        data = data[:-2 if self.indent else -1]  # remove trailing bracket
        self._write_data(data.encode())

    def _close_json_file(self):
        data = "\n]" if self.indent else "]"
        self._write_data(data.encode())

    def _flush_csv_file(self):
        data = self._data.to_csv(sep=self.delimiter,
                                 header=(self.has_header and self._first_write),
                                 index=self.index_rows).encode()
        self._write_data(data)

    def _flush_fwf_file(self):
        if self._first_write and self.has_header:
            self._write_data([[column for column in self._data.columns]], format=self.fwf_header_format)

        self._write_data(self._data.values, format=self.fwf_format)

    def _generate_excel_file(self):
        """
         Generates an excel file from the dataframe using xlsxwriter.
         NOTE -- This is severely limited by the amount of available memory, Excel files are bundles of XML that can be
         very bloated before they're zipped.
        """

        if len(self._data) > 1048576:
            raise ValueError("Excel has a maximum row limit of 1,048,576 rows, but the output would contain %s rows. Please update your configutation to output in another data format (for example, a CSV) or reduce the number of rows to be processed." % len(self._data))

        import xlsxwriter

        output_stream = io.BytesIO()
        workbook = xlsxwriter.Workbook(output_stream, {'constant_memory': True, 'in_memory': True})  # in_memory option is used to prevent 1GB+ files from being written to disk. Writing to disk has constant memory, but is SLOW. We can specify the tmp directory if needed.
        worksheet = workbook.add_worksheet(self.sheet_name)

        output = self._data.to_dict('split')
        if self.has_header:
            for col in range(0, len(output['columns'])):
                worksheet.write(0, col + 1 if self.index_rows else col, output['columns'][col])

        for row in range(0, len(output['data'])):
            if self.index_rows:
                worksheet.write(row + 1 if self.has_header else row, 0, row + 1)

            for col in range(0, len(output['columns'])):
                worksheet.write(row + 1 if self.has_header else row, col + 1 if self.index_rows else col, output['data'][row][col])

        workbook.close()
        output_stream.seek(0)

        self._write_data(output_stream.getvalue(), "wb")


class Metrics:
    # Date Intervals
    IRREGULAR, YEAR, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND = ("IRREGULAR", "YEAR", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND")

    def __init__(self):
        # Universal
        self.number_values = 0
        self.number_null = 0
        self.common_values = []
        self.top_values = []
        self.num_duplicate_values = 0

        # Outliers
        self.duplicate_outliers = {}
        self.unique_outliers = {}
        self.non_sequential_values = []  # any values that are potential anomalies in the sequence of numbers
        self.null_outliers = []  # missing values that seem out of place
        self.date_regex_outliers = []  # values that don't match the regex mask
        self.statistical_outliers = []  # values above a Z-score of +/- 3

        # Sequence
        self.is_sequential = False  # true if all numbers are separated by the same common sequence value, e.g. +1
        self.common_sequence_value = None  # common differential value in the sequence

        # Strings
        self.min_length = None
        self.max_length = None
        self.mean_length = None
        self.mask_matches = []

        # Integers / Decimals
        self.min_value = None
        self.max_value = None
        self.min_excluding_outliers = None
        self.max_excluding_outliers = None
        self.mean = None
        self.lower_quartile = None
        self.upper_quartile = None
        self.median = None
        self.min_precision_length = 0
        self.max_precision_length = 0
        self.mean_precision_length = 0

        # Dates
        self.min_date = None
        self.max_date = None
        self.contains_future_dates = False
        self.date_interval = Metrics.IRREGULAR
        self.date_format = None


class Field:
    # Field Types
    STRING, INT, FLOAT, DATE, DATETIME, BOOLEAN = ("STRING", "INT", "FLOAT", "DATE", "DATETIME", "BOOLEAN")

    def __init__(self, name, type="", colSpecs=[]):
        self.name = name
        self.type = type
        self.col_specs = colSpecs

        self.metrics = Metrics()

    def _validate(self):
        if not self.name:
            raise ValueError("Each source field must have a name.")

        self._validate_type()

    def _validate_type(self):
        if self.type and self.type not in [OutputField.STRING, OutputField.INT, OutputField.FLOAT, OutputField.DATE, OutputField.BOOLEAN]:
            raise ValueError("%s is not a valid field type for output field '%s'." % (self.type, self.name))


class SourceField(Field):

    def __init__(self, name, used, fileIndex, type="", colSpecs=[], **kwargs):
        super(SourceField, self).__init__(name, type, colSpecs)
        self.used = used
        self.file_index = fileIndex
        self._validate()

    def _validate(self):
        super(SourceField, self)._validate()

        if self.file_index is None or self.file_index < 0:
            raise ValueError("Each source field must have a valid file index.")


class OutputField(Field):
    STRING, INT, FLOAT, DATE, BOOLEAN = ("STRING", "INT", "FLOAT", "DATE", "BOOLEAN")

    def __init__(self, name, type, sourceFields, transformations=[], allowNull=False, replaceNullWith=None, mergeDelimiters=[], truthyStrings=None, colSpecs=[], **kwargs):
        super(OutputField, self).__init__(name, type, colSpecs)

        self.source_fields = sourceFields
        self.merge_delimiters = mergeDelimiters
        self.truthy_strings = truthyStrings if truthyStrings else ["True", "1", "true", "Yes", "yes"]
        self.allow_null = allowNull  # whether the field can contain null values; if false, the entire row is excluded and a warning is set
        self.replace_null_with = replaceNullWith  # an optional value to replace nulls with

        self._validate()

        self.transformations = []
        for transformation in transformations:
            self.transformations.append(Transformation(**transformation))

    def _validate(self):
        super(OutputField, self)._validate()

        self._validate_source_fields()

    def _validate_source_fields(self):
        if len(self.source_fields) == 0:
            raise ValueError("Output field '%s' needs at least one source field" % self.name)

        if len(self.merge_delimiters) != len(self.source_fields) - 1:
            raise ValueError("Output field '%s' has the wrong number of merge delimiters; expected %d but got %d" %
                             (self.name, len(self.source_fields) - 1, len(self.merge_delimiters)))


class Transformation:
    MODIFY_DO_MATH, MODIFY_TRIM_STRING, MODIFY_REMOVE_WHITESPACE, MODIFY_CHANGE_CASE, \
    MODIFY_REMOVE_SUBSTRING, MODIFY_APPEND_STRING, MODIFY_REPLACE_VALUE, MODIFY_MASK_FIELD, MODIFY_CAST_TYPE, \
    MODIFY_CHANGE_DATE_FORMAT, MODIFY_ROUND_NUMBER = \
        (
            "MODIFY_DO_MATH",
            "MODIFY_TRIM_STRING",
            "MODIFY_REMOVE_WHITESPACE",
            "MODIFY_CHANGE_CASE",
            "MODIFY_REMOVE_SUBSTRING",
            "MODIFY_APPEND_STRING",
            "MODIFY_REPLACE_VALUE",
            "MODIFY_MASK_FIELD",
            "MODIFY_CAST_TYPE",
            "MODIFY_CHANGE_DATE_FORMAT",
            "MODIFY_ROUND_NUMBER",
        )

    VALIDATE_BY_RANGE, VALIDATE_BY_VALUE, VALIDATE_BY_DATE_RANGE, VALIDATE_BY_DATE_VALUE, VALIDATE_BY_LIST, VALIDATE_BY_REGEX, \
    VALIDATE_BY_LENGTH, VALIDATE_BY_SUBSTRING = \
        (
            "VALIDATE_BY_RANGE",
            "VALIDATE_BY_VALUE",
            "VALIDATE_BY_DATE_RANGE",
            "VALIDATE_BY_DATE_VALUE",
            "VALIDATE_BY_LIST",
            "VALIDATE_BY_REGEX",
            "VALIDATE_BY_LENGTH",
            "VALIDATE_BY_SUBSTRING",
        )

    FILTER_BY_RANGE, FILTER_BY_VALUE, FILTER_BY_DATE_RANGE, FILTER_BY_DATE_VALUE, FILTER_BY_LIST, FILTER_BY_REGEX, FILTER_BY_LENGTH, FILTER_BY_SUBSTRING = \
        (
            "FILTER_BY_RANGE",
            "FILTER_BY_VALUE",
            "FILTER_BY_DATE_RANGE",
            "FILTER_BY_DATE_VALUE",
            "FILTER_BY_LIST",
            "FILTER_BY_REGEX",
            "FILTER_BY_LENGTH",
            "FILTER_BY_SUBSTRING",
        )

    def __init__(self, operation, parameters, type="", **kwargs):
        self.operation = operation
        self.parameters = parameters
        self.type = type
        self._validate()

    def _validate(self):
        if self.operation not in [self.MODIFY_DO_MATH, self.MODIFY_TRIM_STRING, self.MODIFY_REMOVE_WHITESPACE, self.MODIFY_CHANGE_CASE,
                                  self.MODIFY_REMOVE_SUBSTRING, self.MODIFY_APPEND_STRING, self.MODIFY_REPLACE_VALUE,
                                  self.MODIFY_ROUND_NUMBER, self.MODIFY_CHANGE_DATE_FORMAT,
                                  self.MODIFY_MASK_FIELD, self.MODIFY_CAST_TYPE, self.VALIDATE_BY_RANGE, self.VALIDATE_BY_VALUE,
                                  self.VALIDATE_BY_LIST, self.VALIDATE_BY_REGEX, self.VALIDATE_BY_LENGTH, self.VALIDATE_BY_SUBSTRING,
                                  self.VALIDATE_BY_DATE_RANGE, self.VALIDATE_BY_DATE_VALUE, self.FILTER_BY_DATE_RANGE,
                                  self.FILTER_BY_DATE_VALUE, self.FILTER_BY_RANGE, self.FILTER_BY_VALUE, self.FILTER_BY_LIST,
                                  self.FILTER_BY_REGEX, self.FILTER_BY_LENGTH, self.FILTER_BY_SUBSTRING]:
            raise ValueError("Operation is not valid.")


class Configuration:

    def __init__(self, id, file_path):
        self.id = id
        self.file_path = os.path.expanduser(file_path)

        self._validate_id()

        config_data = self._load_configuration()
        self._populate(**config_data)
        self._validate()

    def _load_configuration(self):
        """ Retrieve configuration using either the API, a local cache file, or a supplied file path. """
        if self.file_path:
            # try to use user-supplied file, breaks if error
            validate_file_exists(self.file_path)
            return self._retrieve_from_file()
        else:
            return self._retrieve_from_api()

    def _populate(self, sourceFields=[], outputFields=[], sourceFiles=[], outputFile=None, label="", version="", createdAt="", owner="", description="", id="", **kwargs):
        """ populate the configuration and initialize all child objects. """
        self.label = label if label else "No Name"
        self.version = version if version else "No Version"
        self.created_at = createdAt if createdAt else "Not Set"
        self.owner = owner if owner else "Not Set"
        self.description = description
        self.source_fields = [SourceField(**field) for field in sourceFields]
        self.output_fields = [OutputField(**field) for field in outputFields]
        self.source_files = []

        for file_index, source_file in enumerate(sourceFiles):
            source_file["file_index"] = file_index
            self.source_files.append(SourceFile(**source_file))

        self.output_file = OutputFile(**outputFile)

        if self.output_file.type == File.FWF:
            # intialize fwf output format based on output fields
            format = ""
            for field in self.output_fields:
                if field.col_specs is None:
                    raise ValueError("Field '%s' does not have column markers set." % field.name)

                width = field.col_specs[1] - field.col_specs[0]
                if field.type in [Field.STRING, Field.DATETIME, Field.DATE, Field.BOOLEAN]:
                    format += "%-" + str(width) + '.' + str(width) + 's'
                elif field.type == Field.INT:
                    format += "%-" + str(width) + 'i'
                elif field.type == Field.FLOAT:
                    format += "%-" + str(width) + '.' + str(width) + 'f'

            self.output_file.fwf_format = format

            if self.output_file.has_header:
                format = ""
                for field in self.output_fields:
                    if field.col_specs is None:
                        raise ValueError("Field '%s' does not have column markers set." % field.name)

                    width = field.col_specs[1] - field.col_specs[0]
                    format += "%-" + str(width) + '.' + str(width) + 's'

            self.output_file.fwf_header_format = format


    def _retrieve_from_file(self):
        with open(self.file_path) as file:
            # should probably use simplejson for better error handling
            return json.load(file)

    def _retrieve_from_api(self):
        url = BASE_API_URL + 'configurations/{id}'.format(id=self.id)
        response = requests.get(url)
        if response.status_code == requests.codes.OK:
            return response.json()

        elif response.status_code == requests.codes.NOT_FOUND:
            raise ValueError("Unable to retrieve configuration for Configuration Id: %s. Please verify the configuration "
                             "Id on your profile at https://datamonkey.io." % self.id)

        else:
            response.raise_for_status()

    def _validate(self):
        if self.source_fields is None or len(self.source_fields) == 0:
            raise ValueError("There were no source fields defined for this configuration.")

        if self.output_fields is None or len(self.output_fields) == 0:
            raise ValueError("There were no output fields defined for this configuration.")

        if self.source_files is None or len(self.source_files) == 0:
            raise ValueError("There were no source files defined for this configuration.")

        if self.output_file is None:
            raise ValueError("There was no output file defined for this configuration.")

    def _validate_id(self):
        if self.id == "":
            raise ValueError("Configuration Id must be set.")
        elif len(self.id) != 36:
            raise ValueError("The configuration Id you supplied (%s) does not match the expected 36 character length."
                             % self.id)

    def print_details(self):
        print("\n*****  Configuration Details *****")
        print("----------------------------------")
        print("Name: %s" % self.label)
        print("Description: %s" % self.description)
        print("----------------------------------")
        print("Input: 1 %s file" % self.source_files[0].type)
        print("%d Expected Fields: %s" % (len(self.source_fields), ', '.join(field.name for field in self.source_fields)))
        print("----------------------------------")
        print("Output: 1 %s file" % self.output_file.type)
        print("%d Expected Fields: %s" % (len(self.output_fields), ', '.join(field.name for field in self.output_fields)))
        print("----------------------------------")



