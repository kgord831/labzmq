import logging
from logging.handlers import RotatingFileHandler

def make_logger(log_filename):
    """return a logging object"""
    log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')
    logFile = log_filename
    my_handler = RotatingFileHandler(logFile, mode='a', maxBytes=5*1024*1024, backupCount=2, encoding=None, delay=0)
    my_handler.setFormatter(log_formatter)
    my_handler.setLevel(logging.DEBUG)
    
    app_log = logging.getLogger('root')
    app_log.setLevel(logging.DEBUG)
    
    app_log.addHandler(my_handler)
    return app_log
