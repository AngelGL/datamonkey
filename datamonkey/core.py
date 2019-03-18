# <editor-fold desc="Imports">
import numpy
import pandas

from datamonkey.transformations import *
from datamonkey.helpers import *
from datamonkey.models import *
from datamonkey.settings import PANDAS_TYPE_MAP, PYTHON_TYPE_MAP, DMK_TYPE_MAP

# </editor-fold>


class FileProcessor:

    ERROR_FILE_DEFAULT_NAME = "errors_and_warnings.txt"
    CHUNKSIZE = 1000000

    # Stages of processing
    INITIALIZING, RETRIEVE_DATA, VALIDATE, MAP, TRANSFORM, WRITING_DATA, OUTPUT_DATA, WRITE_ERRORS, ERROR = \
        ("Initializing.",
         "Retrieving input data from provided files.",
         "Validating data and preparing to process.",
         "Mapping input data to output fields.",
         "Applying transformations (filters, modifiers, and validators).",
         "Flushing data to output file.",
         "Finalizing output file.",
         "Writing errors and warnings to log file.",
         "Exiting prematurely due to errors encountered.")

    # de-normalize fields from configuration for convenience / brevity
    @property
    def source_files(self):
        return self.configuration.source_files

    @property
    def output_file(self):
        return self.configuration.output_file

    @property
    def source_fields(self):
        return self.configuration.source_fields

    @property
    def output_fields(self):
        return self.configuration.output_fields

    def __init__(self, template_id, template_file_path="", max_errors=None):
        self.configuration = Configuration(template_id, template_file_path)

        self.output_file_path = ""
        self.error_file_path = ""
        self.errors = []
        self.warnings = []
        self.max_errors = max_errors

        self.stage = self.INITIALIZING
        self.processing_index_start = 0
        self.processing_index_end = 0
        self.output_items = 0

        self.source_data = None
        self.chunk_source = self._can_chunk_source()  # splitting source data into chunks will reduce memory footprint

    def show_configuration_details(self):
        self.configuration.print_details()

    def process(self, source_file_paths, output_file_path="", error_file_path="", source_data=None):
        """ processes file(s) using a supplied configuration """

        if self.output_file.name is not None:
            # only PYHTON configurations will not have a file output
            self.output_file.file_path = parse_file_path(output_file_path, self.output_file.name)
            self.output_file.remove_existing_file()

        self.error_file_path = parse_file_path(error_file_path, FileProcessor.ERROR_FILE_DEFAULT_NAME)

        if source_data is None:
            if not source_file_paths:
                raise ValueError("Please supply at least one source file path for processing.")

            # Users can pass in one or many paths to source files as a string / list based on the configuration.
            if isinstance(source_file_paths, str):
                source_file_paths = [source_file_paths]

            if len(source_file_paths) != len(self.source_files):
                raise ValueError("You supplied %d file paths, but %d source files were defined in the configuration. You can call FileProcess.list_configuration_details() for more information on the expected source files for this configuration." % len(source_file_paths), len(self.source_files))

            # verify all files exist
            for i, path in enumerate(source_file_paths):
                path = os.path.expanduser(path)
                source_file = self.source_files[i]
                source_file.file_path = path

                # TODO: write this as model methods instead of helpers
                validate_file_exists(source_file.file_path, source_file.s3_path)
                check_file_size(source_file.file_path, source_file.s3_path)

        self._get_source_data(source_data=source_data)
        self._process_data()

        if self.output_file.type == File.PYTHON:
            return self.output_file.data

    def _get_source_data(self, source_data=None):
        """ pull source data from all files and combine if multiple sources.
        Users can also manually pass a list of dictionaries directly from Python """
        self.stage = self.RETRIEVE_DATA

        if source_data is None:
            if self.chunk_source:
                # returns a generator if file type can be chunked
                source_data, _ = self.source_files[0].process_file(self.source_fields, chunk_data=True)

            else:
                for source_file in self.source_files:
                    fields = [field for field in self.source_fields if field.file_index == source_file.file_index]
                    data, _ = source_file.process_file(fields)

                    if source_data is None:
                        source_data = data
                    else:
                        for column in data.columns:
                            source_data[column] = data[column]

            self.source_data = source_data

        else:
            self.source_data = pandas.DataFrame.from_records(source_data)
            for field in self.source_fields:
                if field.name not in self.source_data.columns:
                    raise ValueError("Expected field '%s' was not found in the provided data. "
                                     "If this field is no longer required, please update the file template." % field.name)

    def _get_next_chunk(self):
        if self.source_data is None:
            raise ValueError("Source data has not been set.")

        elif type(self.source_data) is pandas.io.parsers.TextFileReader:
            try:
                return self.source_data.get_chunk(FileProcessor.CHUNKSIZE)
            except StopIteration:
                return None

        elif type(self.source_data) is pandas.DataFrame:
            if len(self.source_data):
                # return a chunk of the data and erase it from the source data -- to balance out the size increase in output data
                data = self.source_data.head(FileProcessor.CHUNKSIZE)
                self.source_data = self.source_data.iloc[FileProcessor.CHUNKSIZE:]
                return data
            else:
                return None

    def _process_data(self):
        """ Reads in source files, transforms the data, outputs the results """
        self.output_file.reset_data()  # reset data in case processor instance is used multiple times

        while True:
            data = self._get_next_chunk()
            if data is None:
                break

            self.processing_index_start = data.index[0]
            self.processing_index_end = data.index[-1]

            data = self._apply_field_mapping(data)  # Map inputs to outputs
            data = self._validate_and_prepare_data(data)  # Validate data by handling any nulls + type-casting
            data = self._apply_field_transformations(data)  # Process data transformations for all columns
            self._flush_data(data)

        self._write_errors_and_warnings()

        self.stage = self.OUTPUT_DATA
        self.output_file.generate_output()  # append all data to output file or return processed data for python configs

    def _validate_and_prepare_data(self, output_data):
        """ Check for nulls and replace with supplied values or remove invalid lines.
         Also check for correct data types. """
        self.stage = self.VALIDATE

        # Replace all empty strings and strings with only whitespace with NaN for null validation
        output_data.replace("", numpy.nan, inplace=True)

        # find any nulls in all columns and check config for handling
        for field in self.output_fields:
            nulls = output_data[field.name].isnull()
            if len(output_data[nulls]):
                if field.allow_null:
                    if field.replace_null_with == "":
                        if field.type in [field.STRING]:
                            replace_with = ""
                        else:
                            replace_with = numpy.nan
                    else:
                        replace_with = field.replace_null_with
                    # TODO: re-implement, ix is slow
                    output_data.ix[nulls, field.name] = replace_with
                else:
                    indices = [str(x + 1) for x in output_data[nulls].index]
                    output_data.drop(output_data[nulls].index, inplace=True)

                    location = "%s(s): %s" % (self._decode_input_field_type(field), ", ".join(indices))
                    warning = "Missing values found in field '%s' for %s. These rows will be skipped in the output. " \
                              "If missing values should be allowed (or replaced) for this field, please alter your " \
                              "file template." % (field.name, location)
                    self._append_errors_and_warnings(warning=warning)

        # ensure column is the expected data type from config, cast to correct type if not (and log casting errors)
        for field in self.output_fields:
            type = PANDAS_TYPE_MAP[field.type]
            col = output_data[field.name]

            if col.dtype != type:
                if col.dtype == PANDAS_TYPE_MAP[Field.STRING] and field.type == Field.BOOLEAN:
                    # special scenario, compare string to user-provided list of truthy-strings
                    def test_truthy(target_data, truthy):
                        return target_data in truthy

                    truthy_strings = field.truthy_strings
                    output_data[field.name] = output_data[field.name].apply(test_truthy, args=(truthy_strings,))

                elif field.type in [Field.DATE, Field.DATETIME]:
                    try:
                        output_data[field.name] = pandas.to_datetime(output_data[field.name], errors='raise')
                    except ValueError as err:
                        # catch failed date conversion issues and report.
                        invalid_value = err.args[1]
                        idx = (col == invalid_value).idxmax() + 1
                        location = "%s %d" % (self._decode_input_field_type(field), idx)
                        error = "Could not coerce the value '%s' into a date or datetime for field '%s' (%s)." % \
                                (invalid_value, field.name, location)
                        self._append_errors_and_warnings(error)

                else:
                    # attempt a hard cast of the data to desired type
                    try:
                        output_data[field.name] = col.astype(PYTHON_TYPE_MAP[field.type])

                    except ValueError as err:
                        invalid_value = ""

                        for value in re.findall(r"'(.*?)'", err.args[0]):
                            invalid_value += value

                        idx = (col == invalid_value).idxmax() + 1
                        location = "%s %d" % (self._decode_input_field_type(field), idx)
                        error = "Could not coerce the value '%s' into a %s for field '%s' (%s)." % \
                                (invalid_value, DMK_TYPE_MAP[field.type], field.name, location)

                        self._append_errors_and_warnings(error)

        if len(self.errors):
            self._exit_with_errors()

        return output_data

    def _apply_field_mapping(self, source_data):
        """ create a field mapping of inputs to outputs based on the configuration. """
        self.stage = self.MAP

        columns = [field.name for field in self.output_fields]
        output_data = pandas.DataFrame(columns=columns)

        for field in self.output_fields:
            if len(field.source_fields) == 1:
                # one-to-one mapping
                field_index = field.source_fields[0]
                source_field = self.source_fields[field_index]
                output_data[field.name] = source_data[source_field.name]
            else:
                # merge multiple columns
                data = {"merge_col": ""}

                for j, delimiter in enumerate(field.merge_delimiters):
                    data["delimiter%d" % j] = delimiter

                delimiters = pandas.DataFrame(data=data, index=numpy.arange(len(source_data)))
                col = delimiters["merge_col"]

                field_names = []
                for field_index in field.source_fields:
                    source_field = self.source_fields[field_index]
                    field_names.append(source_field.name)

                for k, name in enumerate(field_names):
                    # need to hard-cast all merge columns to strings in this version, might want to consider merging integers/floats w/ math operations
                    if k == 0:
                        col = source_data[name].astype(str)
                    else:
                        col = col + delimiters["delimiter%d" % (k-1)] + source_data[name].astype(str)

                output_data[field.name] = col

        return output_data

    def _apply_field_transformations(self, output_data):
        self.stage = self.TRANSFORM

        """ Apply transformations (modifiers, validators, filters) to fields as configured. """

        def do_transformations(target_value, target_transformations):
            """ Loop through transformations and apply them to the supplied value """
            for i, transformation in enumerate(target_transformations):
                try:
                    target_value, target_action, target_message = transformation[0](target_value, transformation[1])
                    if target_action:
                        return target_value, target_action, "transformation #%d: %s" \
                               % (i + 1, target_message)
                except Exception as err:
                    return target_value, "ERROR", "transformation #%d: %s" % \
                           (i + 1, repr(err))

            return target_value, None, None

        # TODO: SORT FIELDS BY ORDER OF IMPORTANCE (FILTERS FIRST)
        sorted_fields = self.output_fields

        for field in sorted_fields:
            if len(field.transformations):

                name = field.name
                transformations = [(FUNCTION_MAP[transformation.operation], transformation.parameters) for
                                   transformation in field.transformations]  # turn into list of function handlers
                data_indices, data_values, filter_indices = [], [], []

                # run transformations as a lambda for each column
                results = output_data[name].dropna().apply(do_transformations, args=(transformations,))

                for row in results.iteritems():
                    index = row[0]
                    (value, action, message) = row[1]

                    if not action:
                        # value was modified without issue
                        data_indices.append(index)
                        data_values.append(value)

                    elif action == 'ERROR':
                        # error captured and the line is filtered, but isn't reported until after all columns process
                        filter_indices.append(index)
                        location = "%s %d" % (self._decode_input_field_type(field), index + 1)
                        error = "'%s', %s, %s." % (name, location, message)
                        self._append_errors_and_warnings(error=error)

                    elif action == "WARN":
                        # warning is generated but the line remains in the output
                        data_indices.append(index)
                        data_values.append(value)
                        location = "%s %d" % (self._decode_input_field_type(field), index + 1)
                        warning = "'%s', %s, %s." % (name, location, message)
                        self._append_errors_and_warnings(warning=warning)

                    elif action == "FILTER":
                        # row is filtered from the output
                        filter_indices.append(index)

                if len(filter_indices):
                    # remove all filtered data
                    output_data.drop(filter_indices, inplace=True)

                if len(data_indices):
                    # replace modified column values in the output
                    col = pandas.DataFrame(index=data_indices, data=data_values, columns=[name])
                    output_data[name] = col

        if len(self.errors):
            self._exit_with_errors()

        # after transforming the data, replace any NaN or NaT values with a blank string for output purposes
        output_data.replace(numpy.nan, "", inplace=True)
        return output_data

    def _flush_data(self, data):
        self.stage = self.WRITING_DATA
        self.output_items += len(data)
        self.output_file.append_data(data)
        self.output_file.flush_output()

    def _exit_with_errors(self):
        self.stage = self.ERROR
        """ Logs any errors/warnings to the job and local file and raises fatal exception.  """
        self._write_errors_and_warnings()
        raise ValueError("Encountered errors while processing; please see the errors file for more details.")

    def _append_errors_and_warnings(self, error="", warning=""):
        # Prevent millions of errors and warnings from generating to preserve memory/space if required
        if self.max_errors is None or ((len(self.errors) + len(self.warnings)) < self.max_errors):
            if error:
                self.errors.append(error)

            if warning:
                self.warnings.append(warning)


    def _write_errors_and_warnings(self):
        """ Pretty prints errors/warnings to a local file """
        self.stage = self.WRITE_ERRORS

        if len(self.errors) or len(self.warnings):
            try:
                if check_S3_path(self.error_file_path):
                    import s3fs
                    fs = s3fs.S3FileSystem()
                    file = fs.open(self.error_file_path, "wb")

                else:
                    file = open(self.error_file_path, "wb")

                file.write(('***** ERRORS *****\n').encode())

                for error in self.errors:
                    file.write((error + '\n').encode())

                file.write(('\n***** WARNINGS *****\n').encode())
                for warning in self.warnings:
                    file.write((warning + '\n').encode())
                file.close()

            except Exception as err:
                file.close()
                raise err

    def _decode_input_field_type(self, output_field):
        """ Figures out the correct terminology to use when referencing data. Should probably be deprecated/improved. """
        source_field = self.source_fields[output_field.source_fields[0]]
        source_file = self.source_files[source_field.file_index]
        return "Object" if source_file.type == File.JSON else 'Row'

    def _can_chunk_source(self):
        """Determines if data can be chunked to reduce memory usage."""
        if len(self.source_files) > 1:
            # multiple files will need to be joined in memory as a whole
            return False

        if self.source_files[0].type in [File.EXCEL, File.JSON]:
            # Excel files are awful and can not be read / written in chunks.
            return False

        return True

