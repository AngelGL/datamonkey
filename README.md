Data Monkey
==========

**Easy-to-use file transformation library** 

---      

## What is it?

**Data Monkey** is a Python file transformation library designed to take the stress
out of manipulating data. It provides powerful tools to both **validate
data integrity** and **modify your files into the exact format you need**.
This library works in conjunction with file templates created on through the [Data Monkey web application](https://app.data-monkey.com) -- once you've created a file template, it's simple to start processing files locally.

Data Monkey currently supports the following file formats:
1. CSV
2. Excel
3. Fixed-width text files (AKA flat files)
4. JSON (with a flat, non-nested data structure)

## Main Features

  - Spend less time writing routine code to ingest or transfrom data loaded from a file or external API. Data Monkey lets you reliably manipulate data into exactly what you need; whether you're converting a CSV file into JSON or generating an Excel report from an API response, Data Monkey has you covered.
  - A no code, configuration-based approach -- use our web application to quickly update your file templates as often as necessary. It's simple to change the file type that's generated, filter out unnecessary results, combine and transform data, and much more.
  - It's frustrating to find unexpected 'surprises' in your data that don't line up with your expectations -- which is why data validation is a core feature of Data Monkey. Configure any number of data validations that will alert you if something isn't quite right. 
  - Data Monkey provides play-by-play progress tracking when transforming large files to let you know exactly where you're at in the process.

## Where to get it

The source code for the community edition is currently hosted on GitHub at:
https://github.com/datamonkey-hq/datamonkey

Binary installers for the latest released version are available at the [Python
package index](https://pypi.org/project/datamonkey).

```python
# PyPI
pip install datamonkey
```

This will automatically install the required dependencies listed [below](#dependencies), if you don't already have them available.
Please see the list of optional dependencies below; you may need them if you're working with Excel files, or writing/reading files to S3.

## Usage & Examples

The first step to processing any file is creating a file template on the [Data Monkey web application](https://app.data-monkey.com).
File templates are a configuration-based approach to defining exactly how your files will be transformed -- such as the desired file types, outgoing data fields, and any custom validators or modifications to the data itself.
You can view a guide on creating file templates [here](https://documentation.data-monkey.com).

Once your file template is complete, you can use the template key to locally process files with the following commands:

```python
from datamonkey import FileProcessor
# intialize the file processor using your file template key (generated on data-monkey.com)
processor = FileProcessor(YOUR_TEMPLATE_KEY)
# process a local file (or point to a location on S3)
processor.process(PATH/TO/FILE)
```

If successful, the new file will be generated; otherwise, you'll see an errors and
warnings file that will contain details on any issues.


## Additional Configuration


### Caching Behavior
By default, Data Monkey will attempt to pull the most recent version of your file template
on each run using the template key you provide. If you'd prefer to use a local cached
version of the template (downloadable on the summary screen of your file template),
you can initialize the `FileProcessor` with the location of the template file:

```python
from datamonkey import FileProcessor
processor = FileProcessor(YOUR_KEY, template_file_path="PATH/TO/FILE")
```

### File Locations
You can also override the location/name of:
1. The generated output file
2. The errors and warnings file 

```python
from datamonkey import FileProcessor
processor = FileProcessor(YOUR_KEY)
processor.process("PATH/TO/INPUT", output_file_path="PATH/TO/OUTPUT", error_file_path="PATH/TO/ERROR")
```

### Files on AWS S3
Data Monkey uses ``s3fs`` to read and write files on Amazon Web Service's S3. To access S3, replace the file path with the S3 bucket location:

```python
from datamonkey import FileProcessor
processor = FileProcessor(YOUR_KEY)
processor.process("s3://BUCKET_NAME/INPUT_FILE_NAME", output_file_path=""s3://BUCKET_NAME/OUTPUT_FILE_NAME", error_file_path="s3://BUCKET_NAME/ERROR_FILE_NAME")
```

Note: You must have locally configured AWS credentials with read/write access to the buckets you want to use. See the [AWS documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) for more help.

## <a name="dependencies"></a> Dependencies

The following libraries are required for Data Monkey to function:

- [numpy](https://www.numpy.org): 1.14.5 or higher
- [pandas](https://labix.org/python-dateutil): 0.23.1 or higher
- [requests](https://pythonhosted.org/pytz): 2.19.1 or higher
- [ujson](https://pythonhosted.org/pytz): 1.35.0 or higher

These dependencies are required if reading or writing files stored on Amazon Web Services S3:
- [s3fs](https://www.numpy.org): 0.1.5 or higher

These dependencies are required if working with Excel files:
- [xlrd](https://labix.org/python-dateutil): 1.1.0 or higher
- [XlsxWriter](https://labix.org/python-dateutil): 1.1.1 or higher


## License
[BSD 3](LICENSE)

## Additional Documentation
The official documentation is hosted here: https://documentation.data-monkey.com/. It has several helpful guides, such as how to create a file template, and an appendix with information on the various operations you can perform on your data.

## Background
`datamonkey` was born out of the frustration of having to reinvent the wheel every
time any new file needed to be processed with specific requirements. Work on ``datamonkey`` started in early 2017 and development is ongoing; we'd love your feedback on features you'd like to see or file formats that should be supported -- please let us know at feedback@data-monkey.com!

## Getting Help

For usage questions, inquiries, or general feedback, please feel free to contact us via email at help@data-monkey.com.
