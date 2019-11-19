"""
@ File name: output_handler.py
@ Version: 1.0.0
@ Last update: 2019.Nov.19
@ Author: DH.KIM
@ Company: Ntels Co., Ltd
"""

import os
import csv
import pandas as pd
import config.file_path as fp
import time
import traceback
import glob
import timeit

from multiprocessing import Process
from utils.logger import StreamLogger, FileLogger
from datetime import datetime, timedelta
from queue import Queue
from threading import Thread
from utils.graceful_killer import GracefulKiller

num_of_thread = 10
STREAM_LOG_LEVEL = "WARNING"
LOG_LEVEL = "INFO"


class Clean(GracefulKiller):
    def exit_gracefully(self, signum, frame):
        # [*]Process killed by command or Keyboard Interrupt.
        if signum in [2, 15]:
            os.remove(fp.run_dir() + "output_handler.run")
            self.kill_now = True


def get_running_process():
    """
    Get running anomaly detection process.
    :return:
        - A dictionary. Dictionary of p-gateway ip and its service ids.
    """
    running_file = glob.glob(fp.run_dir() + "*.detector.run")
    running_process = {}
    for rf in running_file:
        temp = rf.split("/")[-1].split("_")
        pgw_ip = temp[0]
        service = temp[-1].split(".")[0]
        if pgw_ip not in running_process.keys():
            running_process[pgw_ip] = [service]
        else:
            running_process[pgw_ip].append(service)
    return running_process


def multi_process_by_ip(pid, svc_list):
    """
    This method is worked by multi-processing. It gather the output data from each service directory using thread.
    And then write a file.

    :param pid: A String. P-gateway IP.
    :param svc_list: A List. List of working anomaly detector.
    :return: None.
    """
    # [*] Initializing variables.
    files = []
    queue = Queue()

    for svc in svc_list:
        f = glob.glob(fp.management_dir() + "/{}/{}/output/*.DAT".format(pid, svc))
        files += f

    # [*] Multi-tasking.
    threads = []
    unit = int(len(files)/num_of_thread)

    for i in range(num_of_thread):
        if i+1 == num_of_thread:
            t = Thread(target=_data_integration, args=(queue, files[i*unit:]))
            t.start()
            threads.append(t)
        else:
            t = Thread(target=_data_integration, args=(queue, files[i*unit:i*unit+unit]))
            t.start()
            threads.append(t)

    for t in threads:
        t.join()

    # [*] Integrate date frames from queue.
    df_all_data = pd.DataFrame(columns=["PGW_IP", "DTmm", "SVC_TYPE", "REAL_UP", "REAL_DN",
                                        "ANOMALY_SCORE", "ESTIMATION", "PERCENTAGE"])

    while not queue.empty():
        df_all_data = df_all_data.append(queue.get(), ignore_index=True)

    # [*] Sort by time and re-indexing.
    if not df_all_data.empty:
        df_all_data.sort_values(by=["DTmm"], inplace=True)
        df_all_data = df_all_data.reset_index(drop=True)

    # [*] Extract date and time.
    date_time = sorted(df_all_data['DTmm'].unique())

    # [*] Write into OUTPUT file.
    for dt in date_time:
        output_path = fp.final_output_path() + "{}.{}.DAT.RESULT".format(pid, dt)
        data = df_all_data.loc[df_all_data['DTmm'] == dt]

        with open(output_path, "w") as file:
            csv_writer = csv.writer(file, delimiter='|')
            np_data = data.to_numpy()
            for d in np_data:
                csv_writer.writerow(d)

    # [*] Remove finished files.
    for f in files:
        os.remove(f)


def _data_integration(q, files):
    """
    The thread of 'multi_process_by_ip' function. It gathers all the data from each service output.
    All the result will write in shared queue.
    :param q: A Queue object. Shared Queue of thread.
    :param files: A List object. List of file path for read.
    :return: None.
    """
    # [*] Create empty data frame.
    df_all_data = pd.DataFrame(columns=["PGW_IP", "DTmm", "SVC_TYPE", "REAL_UP", "REAL_DN",
                                        "ANOMALY_SCORE", "ESTIMATION", "PERCENTAGE"])

    # [*] Integrate all data in files.
    for f in files:
        split_name = f.split("/")
        if "output" not in split_name:
            continue
        data = pd.read_csv(f, delimiter='|', header=None, names=["PGW_IP", "DTmm", "SVC_TYPE", "REAL_UP", "REAL_DN",
                                                                 "ANOMALY_SCORE", "ESTIMATION", "PERCENTAGE"])
        df_all_data = df_all_data.append(data, ignore_index=True)

    # [*] Shared Queue.
    q.put(df_all_data)


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

    if not os.path.exists(fp.run_dir()):
        os.makedirs(fp.run_dir())

    with open(fp.run_dir() + "output_handler.run", "w") as file:
        file.write(str(os.getpid()))

    # [*]Every day logging in different fie.
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)

    elog_path = fp.log_dir() + 'output_handler_error_{}.log'.format(today)
    log_path = fp.log_dir() + 'output_handler_{}.log'.format(today)

    elogger = FileLogger("output_handler_error", elog_path, level="WARNING").get_instance()
    slogger = StreamLogger("stream_output_handler", level=STREAM_LOG_LEVEL).get_instance()
    logger = FileLogger("output_handler", log_path, level=LOG_LEVEL).get_instance()

    '''
        Graceful killer
    '''
    killer = Clean()

    while not killer.kill_now:
        today = datetime.now().date()
        # [*]If Day pass by create a new log file.
        if today >= tomorrow:
            today = tomorrow
            tomorrow = today + timedelta(days=1)

            # [*]Log handler updates
            elog_path = fp.log_dir() + 'output_handler_error_{}.log'.format(today)
            log_path = fp.log_dir() + 'output_handler_{}.log'.format(today)

            elogger = FileLogger("output_handler_error", elog_path, level="WARNING").get_instance()
            logger = FileLogger("output_handler", elog_path, level=LOG_LEVEL).get_instance()

        # [*]Multi-process init.
        multi_process = []

        try:
            # --------------------------------------
            slogger.debug("Running process list up starts.")
            stime = timeit.default_timer()

            # [*]Get all files from management path.
            process_list = get_running_process()
            logger.info("Got running process: {}".format(process_list))

            slogger.debug("Running process ends.")
            etime = timeit.default_timer()

            logger.info("'get_running_process' function required time: {}".format(etime-stime))
            # --------------------------------------

            # --------------------------------------
            slogger.debug("Multiprocessing starts.")
            stime = timeit.default_timer()

            # if not process_list:

            # [*]Multi-process by ip address.
            for p in process_list.keys():
                process = Process(target=multi_process_by_ip, args=(p, process_list[p]), name=p)
                process.start()
                multi_process.append(process)

            logger.info("Multiprocess starts: {}".format(multi_process))

            for mp in multi_process:
                mp.join()

            slogger.debug("Multiprocessing end.")
            etime = timeit.default_timer()

            logger.info("Multiprocessing require time: {}".format(etime - stime))

            if multi_process:
                for p in multi_process:
                    # [*] Process end, if work is finished.
                    p.terminate()
            # --------------------------------------

        except KeyboardInterrupt:
            slogger.info("Output handler finished by Keyboard Interrupt.")
            raise SystemExit
        except Exception:
            elogger.error(traceback.format_exc())
            slogger.error("Output handler didn't work properly. Check your error log: {}".format(log_path))

        time.sleep(1)
