"""
This module implements helper functions for the core.

@author: Lior Regev
"""
from glob import glob
import gzip
import hashlib
import logging
import os
import random
import select
import struct
from datetime import datetime, timedelta
from uuid import uuid4

from source.query.ml_phase.constants import MAX_64_INT, MAX_32_INT


SYSLOG_LEVEL = logging.WARNING

LOGSTASH_LEVEL = logging.WARNING
LOGSTASH_PORT = 3333
LOGSTASH_ADDRESS = 'elk.endor.com'

UPLOAD_TIMEOUT = 300

STDOUT_SELECT_TIMEOUT = 5


def generate_hash_for_multiple_strings(*args):
    strings_for_hash = []
    for string in args:
        try:
            string_for_hash = string.decode('utf-8', 'ignore').encode('utf-8')
        except UnicodeDecodeError:
            try:
                string_for_hash = string.encode('utf-8')
            except UnicodeDecodeError:
                string_for_hash = string
        strings_for_hash.append(string_for_hash)
    key = long(hashlib.sha224(''.join(strings_for_hash)).hexdigest()[:8], 16)
    if key > MAX_32_INT:
        key -= (MAX_32_INT + 1)
    return key


def unix_to_matlab_time(unix_time, units_exponent=0):
    """
    Returns the given UNIX time as Matlab time.
    @param unix_time: The UNIX time to convert
    @type unix_time: C{float}
    @param units_exponent: The unix time unit's exponent in relation to seconds e.g.: for ms use -3 for ns use -6
    @type units_exponent: C{int}
    @rtype: C{float}
    """
    return 719529 + unix_time / (86400.0 * (10 ** units_exponent))


def rounded_matlab_time(matlab_time, round_to_min):
    """
    @param matlab_time: Given matlab time 
    @param round_to_min: Minutes to round to... 
    @return:
    """
    return ((round_to_min * (round(matlab_time * 24 * 60 / round_to_min))) / 60) / 24


def bucket_value(value, bucket_by):
    return bucket_by * round(value / bucket_by)


def generate_int_64():
    """
    Generates a random 64-bit integer
    @return: The generated integer.
    @rtype: C{int}
    """
    key = (uuid4().int & (1 << 64) - 1)
    if key > MAX_64_INT:
        key -= (MAX_64_INT + 1)
    return key


def partition_randomly(iterable, num_subsets):
    """
    Partitions iterable into subsets, each containing a roughly equal amount of items.
    @param iterable: An iterable, e.g. a list.
    @type iterable: iterable
    @param num_subsets: number of subsets into which to partition the iterable.
    @type num_subsets: int
    @return: Resulting subsets.
    @rtype: list of list.
    """
    items = list(iterable)
    random.shuffle(items)
    set_size, leftover = divmod(len(items), num_subsets)
    parts = []
    for i in range(num_subsets):
        slice_begin = i * set_size
        slice_end = (i + 1) * set_size
        parts.append(items[slice_begin:slice_end])
    # Give any leftover items to the last part.
    for i in range(leftover):
        parts[i].append(items[-1 - i])
    return parts

def row_to_dict(row):
    """
    Converts a single DF row to a dict. This should be only used by dataframe_to_dict.
    @param row: The input row
    @rtype: C{dict}
    """
    cols = row.index.values.tolist()
    return {row.iloc[0]: {col: row[col] for col in cols[1:]}}


def dataframe_to_dict(df):
    """
    Converts a *2* column DataFrame to a dict where the left column values are keys
    and right column are values
    @param df: The DataFrame to convert
    @type df: C{pd.DataFrame}

    @rtype: C{dict}
    """
    row_func = lambda x, y: y.update(row_to_dict(x))
    result = {}
    df.apply(row_func, args=(result, ), axis=1)
    return result


def gunzip(logger, compressed_file_pathname, delete_compressed_file=True, buf_size_bytes=10 * 1024 ** 2):
    """
    Decompresses a gzipp'ed file. The decompressed result is saved in the same folder as the compressed file.

    @param logger: A logger, for reporting the decompression.
    @type logger: C{logging.logger}
    @param compressed_file_pathname: An absolute path to the gzipp'ed file, which will be decompressed.
    @type compressed_file_pathname: C{str}
    @param delete_compressed_file: If True, the original .gz file is deleted, otherwise the .gz file isn't deleted.
    @type delete_compressed_file: C{bool}
    @param buf_size_bytes: Size in bytes of memory buffer to use for reading the compressed file. Default = 10MB.
    @type buf_size_bytes: int.

    @return: Absolute path to the decompressed file.
    @rtype: C{str}

    @raise IOError: if the compressed file cannot be read or deleted, or the decompressed file cannot be written to.
    """
    logger.info("Starting to decompress " + compressed_file_pathname)
    # Decompress the file in buf_size_bytes pieces into a similarly named file, without the ".gz" extension.
    with gzip.open(compressed_file_pathname) as decompressed_contents:
        decompressed_pathname = os.path.splitext(compressed_file_pathname)[0]  # Get rid of ".gz" extension.
        with open(decompressed_pathname, 'wb') as decompressed_file:
            while True:
                decompressed_piece = decompressed_contents.read(buf_size_bytes)
                if not decompressed_piece:  # Finished reading all decompressed contents.
                    break
                decompressed_file.write(decompressed_piece)
    logger.info("Finished decompressing " + compressed_file_pathname)
    if delete_compressed_file:
        os.remove(compressed_file_pathname)
        logger.debug("Deleted " + compressed_file_pathname)
    logger.info("Finished decompressing, results are in " + decompressed_pathname)
    return decompressed_pathname


def num_line_from_line_index(line_index_path):
    """
    Returns the amount of lines in the given line index file
    @param line_index_path: Path to the line index file
    @type line_index_path: C{str}
    @rtype: C{int}
    """
    return os.path.getsize(line_index_path) / 8


def line_numbers_to_seek_positions(line_numbers, line_index_path):
    """
    Calculates a seek position for every given line number
    @param line_numbers: Iterable line numbers to calculate (zero-based)
    @type line_numbers: C{Iterable}
    @param line_index_path: Path to the line index file
    @type line_index_path: C{str}
    @return: A list of the seek positions.
    @rtype: C{list}
    """
    with open(line_index_path, 'rb') as line_index_file:
        for line_number in line_numbers:
            index_seek_position = line_number * 8
            line_index_file.seek(index_seek_position)
            line_seek_position = struct.unpack('>Q', line_index_file.read(8))[0]
            yield line_seek_position


def matlab_time_to_datetime(matlab_datenum, logger=None):
    try:
        # The logic is as following:
        # 1. matlab_datenum is a float >= 0, the value of which is the number of days since 00-Jan-0000
        #    (namely *1 day before* January 1st of *year 0*.)
        # 2. So on one hand, matlab_datenum can be zero (as indeed is the default elsewhere in the code).
        #    On the other hand, datetime.fromordinal takes int >= 1, where 1 means January 1st of *year 1*.
        #    Also, year 0 had 366 days.
        # 3. Therefore, we add 1 to int(matlab_datenum) before passing as argument to datetime.fromordinal,
        #    and eventually compensate for the days discrepancy by subtracting 366 + 1 days.
        # 4. Since datetime doesn't support dates in year 0, any such Matlab date is converted to January 1st of year 1.
        datetime_days_difference_from_matlab = 367
        if matlab_datenum <= datetime_days_difference_from_matlab - 1:
            return datetime(year=1, month=1, day=1)
        return datetime.fromordinal(int(matlab_datenum) + 1) \
            + timedelta(days=matlab_datenum % 1) \
            - timedelta(days=datetime_days_difference_from_matlab)
    except Exception as exc:
        if logger:
            logger.debug("Got exception ; matlab_datenum = %s" % matlab_datenum)
        raise exc


def simple_logger_proxy(stdout, logger, log_file_handle, stop_event):
    while True:
        rlist = select.select([stdout.fileno()], [], [], STDOUT_SELECT_TIMEOUT)[0]
        if rlist:
            line = stdout.readline().strip()
            if line:
                logger.debug(line)
                log_file_handle.write(line + os.linesep)
            elif stop_event.is_set():
                    break
        else:
            if stop_event.is_set():
                break
    log_file_handle.flush()


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
            yield l[i:i+n]


def timestamp():
    """
    Returns the current time as a string, per time zone UTC (Coordinated Universal Time).
    The string format is yyyy-mm-dd_HH-MM-SS, e.g. "2015-07-23_17-04-38". This format is suitable for
    lexicographical sorting and for file names in common Windows and Linux file systems (e.g. FAT, NTFS, ext.)

    @return: The formatted time stamp.
    @rtype: C{str}
    """
    string_format = "%Y-%m-%d_%H-%M-%S"
    current_utc_time = datetime.utcnow()
    current_utc_time_formatted = current_utc_time.strftime(string_format)
    return current_utc_time_formatted


def get_first_mandatory_file(glob_mask):
    """
    Returns the first file from a list of files found by glob(glob_mask).
    If the list is empty, raises an informative exception.

    :param glob_mask: The mask of files to list.
    :type glob_mask: C{str}

    :return: The name of the first file in the list.
    :rtype: C{str}

    :raises RuntimeError: if no such files.
    """
    all_files = glob(glob_mask)
    if not all_files:
        raise RuntimeError("No files found for glob mask " + glob_mask)
    else:
        return all_files[0]