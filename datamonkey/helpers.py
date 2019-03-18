import os
import re
import numpy

# ************** FILE PROCESSOR HELPERS **************

def change_ext_to_zip(file_path):
    base = os.path.splitext(file_path)
    new_name = file_path.replace(base[1], '.zip')
    os.rename(file_path, new_name)


def make_directory_tree(file_path):
    file_path = os.path.expanduser(file_path)
    file_path = os.path.normpath(file_path)

    if len(file_path.split(os.sep)) > 1:
        try:
            os.makedirs(file_path)
            validate_directory_exists(file_path)
        except FileExistsError:
            pass

    return file_path


def make_S3_directory_tree(file_path):
    dir_path = file_path if file_path[-1] == "/" else file_path + "/"
    import s3fs
    fs = s3fs.S3FileSystem()
    if not fs.exists(file_path):
        fs.mkdir(dir_path)


def check_file_size(file_path, s3=False):

    if s3:
        import s3fs
        fs = s3fs.S3FileSystem()
        size = fs.info(file_path).get('Size', 0)
    else:
        size = os.path.getsize(file_path)

    if size > 1024 ** 3:
        print("WARNING: We've detected that your file size is greater than 1GB. DataMonkey has not yet been optimized "
              "for large files; it may fail to process the file if your system's available memory is too low.")


def check_S3_path(file_path):
    return len(file_path) >= 5 and file_path[0:5] == "s3://"


def parse_file_path(file_path, default_file_name):
    """
    Detects if supplied path is a directory or filename and makes sure the directory structure is present.
    """

    if check_S3_path(file_path):
        # S3 path
        if os.path.splitext(file_path)[1] == "":
            make_S3_directory_tree(file_path)
            file_path = os.path.join(file_path, default_file_name)
        else:
            dir_path = os.path.split(file_path)[0]
            make_S3_directory_tree(dir_path)
    else:
        # local file path
        base = os.path.basename(file_path)
        if os.path.splitext(base)[1] == "":
            # assume directory since no extension was set
            make_directory_tree(file_path)
            file_path = os.path.join(file_path, default_file_name)
        else:
            dir_path = os.path.split(file_path)[0]
            make_directory_tree(dir_path)

    return file_path


def validate_file_exists(file_path, s3=False):
    # Used to validate in a file exists at the specified location
    if s3:
        import s3fs
        fs = s3fs.S3FileSystem()
        if not fs.exists(file_path):
            raise FileNotFoundError('File does not exist at specified S3 location: %s' % file_path)

    else:
        if not os.path.exists(file_path) or not os.path.isfile(file_path):
            raise FileNotFoundError('File does not exist: %s' % file_path)


def validate_directory_exists(file_path, s3=False):
    # Used to validate in a directory exists at the specified location
    if not os.path.exists(file_path) or not os.path.isdir(file_path):
        raise NotADirectoryError('Directory does not exist: %s' % file_path)
