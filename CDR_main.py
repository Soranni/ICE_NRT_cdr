import config
import psycopg2
import time
import os
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler
from file_iterator import *
from CDR_base import *
import multiprocessing
import signal
import argparse
import lmdb


import statistics

def setup_db():
    """
    Setup of DB connetion 
    """
    dsn = "dbname=NRT_test user=postgres password=admin"
    
    p_list = psycopg2.connect(dsn)
    
    return p_list

def setup_logging():
    # statdard logger
    logger = logging.getLogger("run")
    handler = TimedRotatingFileHandler("./logs/file_iterator_log.txt", when="d", interval=1, backupCount=30)
    formatter = logging.Formatter("%(asctime)s %(process)d [%(levelname)s] " "%(name)s %(lineno)d: %(message)s")
    logger.addHandler(handler)
    handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.info("Starting file-handling")

    # error logger
    logger = logging.getLogger("error")
    handler = TimedRotatingFileHandler("./logs/file_error_log.txt", when="d", interval=1, backupCount=30)
    formatter = logging.Formatter("%(asctime)s %(process)d [%(levelname)s] " "%(name)s %(lineno)d: %(message)s")
    logger.addHandler(handler)
    handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.info("Starting error handling")

def processing(files):
    """
    Calling CDRiterator.to_treatment for each file
    Args:
        files : CDR file
    """
    [CDRiterator.to_treatment(file) for file in files]

def Args_guide():
    parser = argparse.ArgumentParser(description='Set number of processors.')
    parser.add_argument('Processors', metavar='P', type=int,
                        help='Number og processors which will be used   ')

    args = vars(parser.parse_args())
    return args["Processors"]

def main():
    """
    Loop each file in incoming folder and run CDRiterator.to_treatment on each file
    """
    
    setup_logging()
    log_run = logging.getLogger("run")
    tim_c = []
    i = 0 
    killer = GracefulKiller()
   
    
    while not killer.kill_now:
        config.conn = setup_db() # reset connetion strings
        
        log_run.info("Run: {}".format(i))
        incoming_files = [ os.path.basename(src_file) for src_file in  Path(config.incoming).glob("*.*")]

        t0 = time.time()
        
        processing(incoming_files)
        
        t1 = time.time()
        tim_c.append(t1-t0)
        print("RUNTIME: {}".format(t1-t0))
        log_run.info("RUNTIME: {}".format(t1-t0))
        time.sleep(config.iteration_time)
        i += 1
        #break
    
    print("MEAN RUN TIME: " , statistics.mean(tim_c))
    
    log_run.info("Stopping db pool")
    log_run.info("Stopping skript")

class GracefulKiller:
  kill_now = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)
    #signal.signal(signal.SIGTSTP, self.exit_gracefully)

  def exit_gracefully(self, *args):
    self.kill_now = True
    
if __name__ == "__main__":
    main()
