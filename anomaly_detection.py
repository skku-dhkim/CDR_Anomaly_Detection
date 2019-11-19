"""
@ File name: anomaly_detection.py
@ Version: 1.1.0
@ Last update: 2019.Nov.19
@ Author: DH.KIM
@ Company: Ntels Co., Ltd
"""

import pandas as pd
import os
import config.file_path as file_path
import argparse
import time
import glob
import numpy as np
import traceback

from models.anomaly_detector import AnomalyDetector
from datetime import datetime, timedelta
from utils.queue import Queue
from utils.logger import FileLogger, StreamLogger
from utils.graceful_killer import GracefulKiller

SLOG_LEVEL = "WARNING"


class Clean(GracefulKiller):
    def __init__(self, ip, svc):
        super().__init__()
        self.ip = ip
        self.svc = svc

    def exit_gracefully(self, signum, frame):
        # [*]Process killed by command or Keyboard Interrupt.
        if signum in [2, 15]:
            os.remove(file_path.run_dir() + "{}_{}.detector.run".format(self.ip, self.svc))
            self.kill_now = True


def data_loader(queue, input_dir):
    """
    Load the train data from input directory.
    :param queue: A Queue object. Represent Anomaly Queue.
    :param input_dir: A String. Train data path.
    :return:
        - queue: A Queue object.
        - boolean: if file exist returns True, else returns False.
    """
    input_file = input_dir + "*.DAT"
    file_list = glob.glob(input_file)
    file_list = sorted(file_list)

    if file_list:
        # [*] Work with first file
        df = pd.read_csv(file_list[0], delimiter='|', names=['PGW_IP', 'DTmm', 'SVC_TYPE', 'UP', 'DN']).to_numpy()
        if queue.full():
            queue.get()
        queue.put([df[0][1], df[0][3], df[0][4]])
        os.remove(file_list[0])

        return queue, True
    return queue, False


def detection(detector, data, output_dir):
    """
    Compute the anomaly scores and write a output into a file.
    :param detector: An Anomaly Detector object. Anomaly Detector that contains its ip address and service type.
    :param data: A Queue object. Input training data.
    :param output_dir: A String. Output directory path.
    :return: None
    """
    td = np.array(data.indexList)
    td_date = td[:, 0]
    td_data = td[:, 1:]
    output_path = output_dir + '{}_{}_{}.DAT'.format(detector.ip, detector.svc_type, td_date[-1])
    detector.compute_anomaly_score(td_date, td_data, output_path)


def main(ip, svc, t, l, seq, q):
    """
    Work flow:
        1) Directory creation, if doesn't exist.
        2) Logger define.
        3) Anomaly Detector define.
        4) While roof
            4-1) Check logger's date.
            4-2) Loading the data, if input file exists.
            4-3) Anomaly detection, if data queue is full and file is read.

    :param ip: A String. P-gateway address.
    :param svc: A String. Service Type.
    :param t: An Integer. Number of trees.
    :param l: An Integer. Leaf size.
    :param seq: An Integer. Sequences.
    :param q: A Float. Quantile.
    :return: None.
    """
    LOG_DIR = file_path.svc_log_dir(ip, svc)
    INPUT_DIR = file_path.input_dir(ip, svc)
    OUTPUT_DIR = file_path.output_dir(ip, svc)
    FINAL_OUTPUT_DIR = file_path.final_output_path()
    RUN_DIR = file_path.run_dir()

    '''
        - slogger: Stream logger.
    '''
    slogger = StreamLogger('anomaly_detection_stream_logger', level=SLOG_LEVEL).get_instance()

    # [*]Create directory if doesn't exist.
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
        slogger.debug("LOG directory doesn't exist. Create one; ({})".format(LOG_DIR))

    if not os.path.exists(INPUT_DIR):
        os.makedirs(INPUT_DIR)
        slogger.debug("INPUT directory doesn't exist. Create one; ({})".format(INPUT_DIR))

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        slogger.debug("OUTPUT directory doesn't exist. Create one; ({})".format(OUTPUT_DIR))

    if not os.path.exists(FINAL_OUTPUT_DIR):
        os.makedirs(FINAL_OUTPUT_DIR)
        slogger.debug("FINAL_OUTPUT directory doesn't exist. Create one; ({})".format(FINAL_OUTPUT_DIR))

    if not os.path.exists(RUN_DIR):
        os.makedirs(RUN_DIR)
        slogger.debug("RUNNING directory doesn't exist. Create one; ({})".format(RUN_DIR))

    # [*]Every day logging in different fie.
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)

    log_path = file_path.svc_log_dir(ip, svc) + 'anomaly_detection_{}.log'.format(today)
    elog_path = file_path.svc_log_dir(ip, svc) + 'anomaly_detection_error_{}.log'.format(today)

    '''
        - logger; Informative logger.
        - elogger; error logger.
    '''
    logger = FileLogger('anomaly_detection_info', log_path=log_path, level='INFO').get_instance()
    elogger = FileLogger('anomaly_detection_error', log_path=elog_path, level='WARNING').get_instance()

    slogger.debug("\n\t\t@Hyper parameters: \n"
                  "\t\t\t+IP addr: {}\n"
                  "\t\t\t+SVC type: {}\n"
                  "\t\t\t+Trees: {}\n"
                  "\t\t\t+Leaves: {}\n"
                  "\t\t\t+Sequences: {}\n"
                  "\t\t\t+Quantile: {}".format(ip, svc, t, l, seq, q))

    '''
        Initialize Graceful Killer
    '''
    killer = Clean(ip, svc)

    try:
        anomaly_detector = AnomalyDetector(t, l, sequences=seq, quantile=q, ip=ip, svc_type=svc)
        slogger.debug("Anomaly Detector successfully created.")
    except Exception:
        elogger.error(traceback.format_exc())
        slogger.error("Anomaly Detector couldn't be created. Check your error log: {}".format(elog_path))
        raise SystemExit

    dstore = Queue(seq)

    with open(RUN_DIR + '{}_{}.detector.run'.format(ip, svc), "w") as out:
        out.write(str(os.getpid()))

    while not killer.kill_now:
        # [*]If Day pass by create a new log file.
        today = datetime.now().date()
        if today >= tomorrow:
            tomorrow = today + timedelta(days=1)

            # [*]Log handler updates
            log_path = file_path.svc_log_dir(ip, svc) + 'anomaly_detection_{}.log'.format(today)
            elog_path = file_path.svc_log_dir(ip, svc) + 'anomaly_detection_error_{}.log'.format(today)

            slogger = StreamLogger('anomaly_detection_stream_logger', level=SLOG_LEVEL).get_instance()
            logger = FileLogger('anomaly_detection_info', log_path=log_path, level='INFO').get_instance()
            elogger = FileLogger('anomaly_detection_error', log_path=elog_path, level='WARNING').get_instance()

        try:
            # [*]Loading the data and save it into queue.
            dstore, rstaus = data_loader(dstore, INPUT_DIR)
            if rstaus is True:
                slogger.debug("Data loader worked successfully.")
                logger.info("Data loader worked successfully.")
        except Exception:
            elogger.error(traceback.format_exc())
            slogger.error("Data loader can't work properly. Check your error log: {}".format(elog_path))
            os.remove(file_path.run_dir() + "{}_{}.run".format(ip, svc))
            raise SystemExit

        try:
            if rstaus is True and dstore.full() is True:
                # [*]Anomaly Detection.
                detection(anomaly_detector, dstore, OUTPUT_DIR)
                slogger.debug("Detection is normally worked.")
                logger.info("Detection is worked.")
            time.sleep(1)
        except Exception:
            elogger.error(traceback.format_exc())
            slogger.error("Detection method didn't work properly. Check your error log: {}".format(elog_path))
            os.remove(file_path.run_dir() + "{}_{}.run".format(ip, svc))
            raise SystemExit


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='CDR anomaly detection module.')
    # [*]Mandatory parameters.
    parser.add_argument('--ip', type=str, help='IP address of services.', required=True)
    parser.add_argument('--svc', type=str, help='Service Type', required=True)

    # [*]Hyper parameters.
    parser.add_argument('--trees', type=int, help='Number of trees.(Default:80)', default=80)
    parser.add_argument('--seq', type=int, help='Sequences to observe.(Default: 10)', default=10)
    parser.add_argument('--leaves', type=int, help='Leaf size to memorize.(Default: 4320)', default=4320)
    parser.add_argument('--q', type=float, help='Quantile value.(Default: 0.99)', default=0.99)

    args = parser.parse_args()

    main(args.ip, args.svc, args.trees, args.leaves, args.seq, args.q)
