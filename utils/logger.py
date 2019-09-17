"""
    @ Version: 1.0.0
    @ Author: DH KIM
    @ Copyrights: Ntels Co., LTD
    @ Last update: 2019.08.05
"""
import logging


class Logger(object):
    stream_logger = None
    file_logger = None

    def __init__(self, type="stream", file_path="", level="INFO"):
        # Formatter
        fomatter = logging.Formatter('[%(levelname)s|%(filename)s:%(lineno)s] %(asctime)s >> %(message)s')

        # Logger instance
        if type is "stream":
            self.stream_logger = logging.getLogger('logger_stream')

            # Logger mode
            if level == "CRITICAL":
                self.stream_logger.setLevel(logging.CRITICAL)
            elif level == "ERROR":
                self.stream_logger.setLevel(logging.ERROR)
            elif level == "WARNING":
                self.stream_logger.setLevel(logging.WARNING)
            elif level == "INFO":
                self.stream_logger.setLevel(logging.INFO)
            elif level == "DEBUG":
                self.stream_logger.setLevel(logging.DEBUG)
            else:
                self.stream_logger.setLevel(logging.NOTSET)

            # Handler
            streamHandler = logging.StreamHandler()
            streamHandler.setFormatter(fomatter)
            self.stream_logger.addHandler(streamHandler)

        elif type == "file":
            self.file_logger = logging.getLogger('logger_file')

            # Logger mode
            if level == "CRITICAL":
                self.file_logger.setLevel(logging.CRITICAL)
            elif level == "ERROR":
                self.file_logger.setLevel(logging.ERROR)
            elif level == "WARNING":
                self.file_logger.setLevel(logging.WARNING)
            elif level == "INFO":
                self.file_logger.setLevel(logging.INFO)
            elif level == "DEBUG":
                self.file_logger.setLevel(logging.DEBUG)
            else:
                self.file_logger.setLevel(logging.NOTSET)

            # Handler
            fileHandler = logging.FileHandler('{}'.format(file_path))
            fileHandler.setFormatter(fomatter)
            self.file_logger.addHandler(fileHandler)

        else:
            raise ValueError("Invalid logger type: \'{}\'".format(type))

    def logger_inst(self, type):
        """
        Returns the logger instance.
        If type is 'stream'
            then returns stream logger.
        If type is 'file'
            then returns file logger.
        Else
            returns None.

        :param type: String-type
        :return: logger instance
        """
        if type is 'stream':
            return self.stream_logger
        elif type is 'file':
            return self.file_logger
        else:
            return None
