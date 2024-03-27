import sys
import os
import inspect
import logging
import datetime
import logging.handlers
from SetUp import SetUp

class Logger:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance.log_file = os.path.join(SetUp.ClearingAndSettlement().AMEXFileLog, f"AMEX_{datetime.date.today().strftime('%d%m%Y')}.log")
            cls._instance.logger = cls._instance.setup_logger()
            sys.excepthook = cls._instance.log_exception
        return cls._instance

    def setup_logger(self):
        logger = logging.getLogger(__name__)

        if logger.getEffectiveLevel() >= logging.ERROR:
            logger.setLevel(logging.ERROR)

        # File Handler
        file_handler = logging.handlers.TimedRotatingFileHandler(self.log_file, when="midnight", interval=1, backupCount=20)
        file_handler.setFormatter(logging.Formatter("%(asctime)s — %(thread)s — %(levelname)s — %(message)s", datefmt='%m/%d/%Y %H:%M:%S'))
        logger.addHandler(file_handler)

        return logger

    def log_exception(self, exc_type, exc_value, exc_traceback):
        error_msg = "EXCEPTION CAUGHT: "
        self.logger.error(error_msg, exc_info=(exc_type, exc_value, exc_traceback))
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        sys.exit(1)

    def log_with_level(self, level, message, console_enable=False):
        if level.upper() not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
            raise ValueError(f"Invalid log level: {level}")

        if level.upper() in ['WARNING', 'ERROR', 'CRITICAL']:
            console_enable = True
        
        caller_info = self.get_caller_info()
        formatted_message = f"{caller_info} — {message}"

        self.logger.setLevel(getattr(logging, level))

        # Console Handler
        if console_enable:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(logging.Formatter("%(asctime)s — %(thread)s — %(levelname)s — %(message)s", datefmt='%m/%d/%Y %H:%M:%S'))
            self.logger.addHandler(console_handler)

        getattr(self.logger, level.lower())(formatted_message)

        # Remove console handler if added
        if console_enable:
            self.logger.removeHandler(console_handler)

    def debug(self, message, console_enable=False):
        self.log_with_level('DEBUG', message, console_enable)

    def info(self, message, console_enable=False):
        self.log_with_level('INFO', message, console_enable)

    def warning(self, message, console_enable=True):
        self.log_with_level('WARNING', message, console_enable)
        
    def error(self, message, console_enable=True):
        self.log_with_level('ERROR', message, console_enable)
        sys.exit(1)

    def get_caller_info(self):
        frame = inspect.currentframe()

        while frame and (os.path.basename(frame.f_code.co_filename) in {'Logger.py', '__init__.py'}):
            frame = frame.f_back

        if frame:
            module_name = os.path.splitext(os.path.basename(frame.f_code.co_filename))[0]
            func_name = frame.f_code.co_name
            line_number = frame.f_lineno
            return f"{module_name} — {func_name} — Line {line_number}"

        return ""