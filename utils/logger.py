"""
    @ Version: 2.0.0
    @ Author: DH KIM
    @ Copyrights: Ntels Co., LTD
    @ Last update: 2019.Nov.08
"""
import logging
import utils.marker as mark


class StreamLogger(object):
    def __init__(self, name, level="INFO"):
        # [*]Formatter
        fomatter = logging.Formatter('[!]%(levelname)s: \n'
                                     '\t- File: %(filename)s:%(lineno)s\n'
                                     '\t- Time: %(asctime)s\n'
                                     '\t- Message: %(message)s')

        # [*]Logger instance
        self.logger = logging.getLogger(name)

        log_level = level.upper()

        # [!]Log level check
        if log_level not in ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']:
            mark.debug_info("Input param \'Log level\' is not proper: {}".format(level), m_type='ERROR')

        # [*]Logger mode
        if level == "CRITICAL":
            self.logger.setLevel(logging.CRITICAL)
        elif level == "ERROR":
            self.logger.setLevel(logging.ERROR)
        elif level == "WARNING":
            self.logger.setLevel(logging.WARNING)
        elif level == "INFO":
            self.logger.setLevel(logging.INFO)
        elif level == "DEBUG":
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.NOTSET)

        # [*]Handler
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(fomatter)
        self.logger.addHandler(streamHandler)

    def get_instance(self):
        return self.logger


class FileLogger(object):
    def __init__(self, name, log_path, level="INFO"):
        # [*]Formatter
        fomatter = logging.Formatter('[!]%(levelname)s: \n'
                                     '\t- File: %(filename)s:%(lineno)s\n'
                                     '\t- Time: %(asctime)s\n'
                                     '\t- Message: %(message)s')

        # [*]Logger instance
        self.logger = logging.getLogger(name)

        log_level = level.upper()

        # [!]Log level check
        if log_level not in ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']:
            mark.debug_info("Input param \'Log level\' is not proper: {}".format(level), m_type='ERROR')

        # [*]Logger mode
        if level == "CRITICAL":
            self.logger.setLevel(logging.CRITICAL)
        elif level == "ERROR":
            self.logger.setLevel(logging.ERROR)
        elif level == "WARNING":
            self.logger.setLevel(logging.WARNING)
        elif level == "INFO":
            self.logger.setLevel(logging.INFO)
        elif level == "DEBUG":
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.NOTSET)

        # [*]Handler
        fileHandler = logging.FileHandler('{}'.format(log_path))
        fileHandler.setFormatter(fomatter)
        self.logger.addHandler(fileHandler)

    def get_instance(self):
        return self.logger
