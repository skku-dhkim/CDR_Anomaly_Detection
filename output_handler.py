"""
@ File name: output_handler.py
@ Version: 0.9.0
@ Last update: 2019.Nov.14
@ Author: DH.KIM
@ Company: Ntels Co., Ltd
"""

import os
import csv
import pandas as pd
import config.file_path as fp
import time
import traceback

from multiprocessing import Process
from utils.logger import StreamLogger, FileLogger
from datetime import datetime, timedelta


def data_integration(list_of_files):
    """
    Integrate all output data.
    :param list_of_files: A list. All file lists in management directory.
    :return:
        - dt: A list. unique date time in the data.
        - df_all_data: A Dataframe. All integrated output data.
    """
    # [*] Create empty data frame.
    df_all_data = pd.DataFrame(columns=["PGW_IP", "DTmm", "SVC_TYPE", "REAL_UP", "REAL_DN",
                                        "ANOMALY_SCORE", "ESTIMATION", "PERCENTAGE"])
    dt = None
    # [*] Integrate all data in files.
    for f in list_of_files:
        split_name = f.split("/")
        if "output" not in split_name:
            continue
        data = pd.read_csv(f, delimiter='|', header=None, names=["PGW_IP", "DTmm", "SVC_TYPE", "REAL_UP", "REAL_DN",
                                                                 "ANOMALY_SCORE", "ESTIMATION", "PERCENTAGE"])
        df_all_data = df_all_data.append(data, ignore_index=True)

        # [*] Extract date and time.
        dt = sorted(df_all_data['DTmm'].unique())

        # [*] Remove already read file.
        os.remove(f)
    return dt, df_all_data


def file_list():
    """
    Find all files in management directory.
    :return:
        - files; all files in management directory.
    """
    # [*]Find all files in management directory.
    files = []
    for (dirpath, dirnames, filenames) in os.walk(fp.management_dir()):
        files += [os.path.join(dirpath, file) for file in filenames]
    return files


def to_file(pid, dateTime, original_data):
    """
    Multi-processing function. Write a final output file.
    :param pid: A String. Process id.
    :param dateTime: A List. Unique date time from integrated data.
    :param original_data: A Dataframe. Integrated data.
    :return: None.
    """
    for dtmm in dateTime:
        data = original_data.loc[original_data['DTmm'] == dtmm].reset_index(drop=True)
        output_path = fp.final_output_path() + "{}.{}.DAT.RESULT".format(pid, dtmm)
        with open(output_path, "w") as file:
            csv_writer = csv.writer(file, delimiter='|')
            np_data = data.to_numpy()
            for d in np_data:
                csv_writer.writerow(d)


def ip_filter(total_data):
    """
    Filtering unique ip address.
    :param total_data: A Dataframe. Integrated data.
    :return:
        - filter_result: A dataframe. Filtered integrated data py ip address.
    """
    ip_list = total_data['PGW_IP'].unique()
    filter_result = {}
    for ip in ip_list:
        r = total_data.loc[total_data['PGW_IP'] == ip]
        filter_result[ip] = r
    return filter_result


if __name__ == "__main__":
    """
    Work flow:
        1) Log directory create, if doesn't exist.
        2) Logger define.
        3) While roof
            3-1) Walk all files in management directory
            3-2) Integrate all output data from it's ip and service type.
            3-3) Filtering the data by ip address.
            3-4) Write the final output file filter by date time.
    """
    # [*]Make Final output directory, if doesn't exist.
    if not os.path.exists(fp.final_output_path()):
        os.makedirs(fp.final_output_path())

    # [*]Every day logging in different fie.
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)

    log_path = fp.log_dir() + 'output_handler_{}.log'.format(today)
    elogger = FileLogger("output_handler", log_path, level="WARNING").get_instance()
    slogger = StreamLogger("stream_output_handler", level="INFO").get_instance()

    while True:
        # [*]If Day pass by create a new log file.
        if today >= tomorrow:
            today = tomorrow
            tomorrow = today + timedelta(days=1)

            # [*]Log handler updates
            log_path = fp.log_dir() + 'output_handler_{}.log'.format(today)
            elogger = FileLogger('file_handler_error', log_path=log_path, level='WARNING').get_instance()

        # [*]Multi-process init.
        multi_process = []

        try:
            # [*]Get all files from management path.
            all_files = file_list()

            # [*]Gather all output files into data frame.
            date_time, all_data = data_integration(all_files)

            # [*]Filters the data using ip address.
            filtered = ip_filter(all_data)

            for ip_addr in filtered.keys():
                # [*] Write the final output.
                process = Process(target=to_file, args=(ip_addr, date_time, filtered[ip_addr]))
                process.start()
                multi_process.append(process)
        except KeyboardInterrupt:
            slogger.info("Output handler finished by Keyboard Interrupt.")
            raise SystemExit
        except Exception:
            elogger.error(traceback.format_exc())
            slogger.error("Output handler didn't work properly. Check your error log: {}".format(log_path))
            raise SystemExit
        finally:
            if multi_process:
                for p in multi_process:
                    # [*] Process end, if work is finished.
                    p.join()

        time.sleep(1)
