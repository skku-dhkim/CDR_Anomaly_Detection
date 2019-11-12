"""
@ File name: file_handler.py
@ Version: 1.1.1
@ Last update: 2019.Nov.12
@ Author: DH.KIM
@ Company: Ntels Co., Ltd
"""

import pandas as pd
import csv
import glob
import time
import config.file_path as fp
import os
import traceback

from datetime import datetime, timedelta
from utils.logger import FileLogger


def file_handler(in_file, loggers):
    """
    Read an original file and convert to trainable file. If finish converting, remove the original file.
    :param in_file: A String. Input file path.
    :param loggers: A Tuple. Loggers as format --> (logger, elogger)
    :return: None
    """
    df = pd.read_csv(in_file, delimiter='|', header=None, names=['PGW_IP', 'DTmm', 'SVC_TYPE', 'UP', 'DN'])
    ip_addr = df['PGW_IP'].unique().tolist()
    svc_type = df['SVC_TYPE'].unique().tolist()

    lg_info = loggers[0]
    lg_error = loggers[1]

    # NOTE: For every single services.
    for svc in svc_type:
        output_path = fp.input_dir(ip_addr[0], svc)

        # NOTE: If output path doesn't exist, create one.
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        selected = df.loc[df['SVC_TYPE'] == svc].values.tolist()

        # [!]Exceptions
        if len(selected) > 1:
            lg_error.warning("Two or more data is detected: {}".format(selected))
        else:
            selected = selected[0]

        # [*]Output file path
        output_path = output_path + '{}.DAT'.format(datetime.now())

        with open(output_path, 'w') as out:
            writer = csv.writer(out, delimiter='|')
            writer.writerow(selected)

        # [*]Log
        lg_info.info("Successfully write the file: {}".format(svc))

    # [*]Log
    lg_info.info("Job is finished: {}".format(in_file))

    # [*]If clearly finished, remove original file.
    os.remove(in_file)


if __name__ == '__main__':
    # [*]If file doesn't exist, make one.
    if not os.path.exists(fp.log_dir()):
        os.makedirs(fp.log_dir())

    # [*]Every day logging in different fie.
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)

    log_path = fp.log_dir() + 'file_handler_{}.log'.format(today)
    elog_path = fp.log_dir() + 'file_handler_error_{}.log'.format(today)

    '''
        - logger; Informative logger.
        - elogger; error logger.
    '''
    logger = FileLogger('file_handler_info', log_path=log_path, level='INFO').get_instance()
    elogger = FileLogger('file_handler_error', log_path=elog_path, level='WARNING').get_instance()

    try:
        while True:
            # [*]If Day pass by create a new log file.
            if today >= tomorrow:
                today = tomorrow
                tomorrow = today + timedelta(days=1)

                # [*]Log handler updates
                log_path = fp.log_dir() + 'file_handler_{}.log'.format(today)
                elog_path = fp.log_dir() + 'file_handler_error_{}.log'.format(today)

                logger = FileLogger('file_handler_info', log_path=log_path, level='INFO').get_instance()
                elogger = FileLogger('file_handler_error', log_path=elog_path, level='WARNING').get_instance()

            # [*]File read & check
            file = glob.glob(fp.original_input_path() + '*.DAT')

            # [*]If file exists.
            if file:
                for f in file:
                    file_handler(f, (logger, elogger))
            time.sleep(1)
    except KeyboardInterrupt as ke:
        logger.info("Program exit with Keyboard interruption.")
    except Exception as e:
        # [*]Log the errors.
        elogger.error(traceback.format_exc())