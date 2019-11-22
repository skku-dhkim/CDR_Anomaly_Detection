"""
@ File name: output_handler.py
@ Version: 1.2.0
@ Last update: 2019.Nov.22
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
import utils.marker as mk
import numpy as np

from multiprocessing import Process
from utils.logger import StreamLogger, FileLogger
from datetime import datetime, timedelta
from queue import Queue
from threading import Thread
from utils.graceful_killer import GracefulKiller

num_of_thread = 10
sleep_time = 1
STREAM_LOG_LEVEL = "WARNING"
LOG_LEVEL = "INFO"


class Clean(GracefulKiller):
    def exit_gracefully(self, signum, frame):
        # [*]Process killed by command or Keyboard Interrupt.
        os.remove(fp.run_dir() + "output_handler.run")
        mk.debug_info("output_handler running end..")
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
        stime = timeit.default_timer()
        info_file = glob.glob(fp.management_dir() + "/{}/{}/output/*.DAT.INFO".format(pid, svc))
        logger.debug("INFO files: {}/{}::{}".format(pid, svc, info_file))
        etime = timeit.default_timer()
        logger.debug(".INFO searched time: {}".format(etime-stime))

        # [*] When .INFO files doesn't exist.
        if not info_file:
            return

        # [*] Remove .INFO extension.
        stime = timeit.default_timer()
        for info in info_file:
            temp = info[:-5]
            files.append(temp)

            # [*] Remove finished files.
            os.remove(info)
            logger.info("Info file deleted: {}".format(info))

        etime = timeit.default_timer()
        logger.debug("Extension removal time: {}".format(etime-stime))

    # [*] Multi-tasking.
    threads = []
    unit = int(len(files)/num_of_thread)
    files = sorted(files)
    logger.info("Thread input files list: {}".format(files))

    stime = timeit.default_timer()
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
    etime = timeit.default_timer()
    logger.info("Thread required time : {}".format(etime-stime))

    all_data = []
    while not queue.empty():
        all_data += queue.get()

    # [*] Integrate date frames from queue.
    df_all_data = pd.DataFrame(all_data, columns=["PGW_IP", "DTmm", "SVC_TYPE", "REAL_UP", "REAL_DN",
                                                  "ANOMALY_SCORE", "ESTIMATION", "PERCENTAGE"])
    # [*] Sort by time and re-indexing.
    if not df_all_data.empty:
        df_all_data.sort_values(by=["DTmm"], inplace=True)
        df_all_data = df_all_data.reset_index(drop=True)

    logger.debug(df_all_data)

    # [*] Extract date and time.
    date_time = sorted(df_all_data['DTmm'].unique())
    logger.info("Extracted date and time: {}".format(date_time))

    # [*] Write into OUTPUT file.
    for dt in date_time:
        output_path = fp.final_output_path() + "{}.{}.DAT.RESULT".format(pid, dt)
        data = df_all_data.loc[df_all_data['DTmm'] == dt]

        with open(output_path, "w") as file:
            csv_writer = csv.writer(file, delimiter='|')
            np_data = data.to_numpy()
            for d in np_data:
                csv_writer.writerow(d)

        with open(output_path + ".INFO", "w") as file_pointer:
            file_pointer.write("")

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
    all_data = []
    # [*] Integrate all data in files.
    for f in files:
        with open(f, "r") as file:
            csv_reader = csv.reader(file, delimiter="|")
            for line in csv_reader:
                if len(line) < 8:
                    line.append(np.nan)
                all_data.append(line)

    # [*] Shared Queue.
    q.put(all_data)


def directory_check():
    # [*]Make Final output directory, if doesn't exist.
    if not os.path.exists(fp.final_output_path()):
        os.makedirs(fp.final_output_path())

    if not os.path.exists(fp.run_dir()):
        os.makedirs(fp.run_dir())

    if not os.path.exists(fp.log_dir()):
        os.makedirs(fp.log_dir())


def main():
    global today, tomorrow
    global elogger, logger, slogger

    while not killer.kill_now:
        directory_check()
        today = datetime.now().date()
        # [*]If Day pass by create a new log file.
        if today >= tomorrow:
            today = tomorrow
            tomorrow = today + timedelta(days=1)

            # [*]Log handler updates
            update_elog_path = fp.log_dir() + 'output_handler_error_{}.log'.format(today)
            update_log_path = fp.log_dir() + 'output_handler_{}.log'.format(today)

            elogger = FileLogger("output_handler_error", update_elog_path, level="WARNING").get_instance()
            logger = FileLogger("output_handler", update_log_path, level=LOG_LEVEL).get_instance()

        # [*]Multi-process init.
        multi_process = []

        try:
            # --------------------------------------
            slogger.debug("Running process list up starts.")
            stime = timeit.default_timer()

            # [*]Get all files from management path.
            process_list = get_running_process()

            if not process_list:
                time.sleep(sleep_time)
                continue

            slogger.debug("Got running process: {}".format(process_list))
            logger.info("Got running process: {}".format(process_list))

            etime = timeit.default_timer()
            logger.info("'get_running_process' function required time: {}".format(etime-stime))
            # --------------------------------------

            # --------------------------------------
            slogger.debug("Multiprocessing starts.")
            stime = timeit.default_timer()

            # [*]Multi-process by ip address.
            for p in process_list.keys():
                process = Process(target=multi_process_by_ip, args=(p, process_list[p]), name=p)
                process.start()
                multi_process.append(process)

            logger.info("Multiprocess starts: {}".format(multi_process))

            for mp in multi_process:
                mp.join()

            etime = timeit.default_timer()
            logger.info("Multiprocessing require time: {}".format(etime - stime))
            slogger.debug("Multiprocessing ends.")

            # --------------------------------------
        except Exception:
            elogger.error(traceback.format_exc())
            slogger.error("Output handler didn't work properly. Check your error log: {}".format(log_path))
            raise SystemExit

        time.sleep(sleep_time)


if __name__ == "__main__":
    """
    Work flow:
        1) Log directory create, if doesn't exist.
        2) Logger define.
        3) While roof
            3-1) Get running process of anomaly detection module.
            3-2) Each IP address got it's process to integrate all output data from it's service type.
    """
    # [*]Make Final output directory, if doesn't exist.
    directory_check()

    '''
        Graceful killer
    '''
    killer = Clean()

    # [*]Every day logging in different file.
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)

    elog_path = fp.log_dir() + 'output_handler_error_{}.log'.format(today)
    log_path = fp.log_dir() + 'output_handler_{}.log'.format(today)

    elogger = FileLogger("output_handler_error", elog_path, level="WARNING").get_instance()
    slogger = StreamLogger("stream_output_handler", level=STREAM_LOG_LEVEL).get_instance()
    logger = FileLogger("output_handler", log_path, level=LOG_LEVEL).get_instance()

    if os.path.exists(fp.run_dir() + "output_handler.run"):
        elogger.error("output handler is already running. Program exit.")
        mk.debug_info("output handler is already running. Program exit.", m_type="ERROR")
    else:
        with open(fp.run_dir() + "output_handler.run", "w") as run_file:
            run_file.write(str(os.getpid()))

    mk.debug_info("output_handler start running.")
    main()
