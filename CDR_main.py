import config
import pyodbc
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

def setup_db(P):
    """
    Setup of DB connetion 
    """
    server = config.server
    database = config.database
    username = config.username
    password = config.password
    dsn = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=" + server + ";DATABASE=" + database + ";UID=" + username + ";PWD=" + password
    p_list = [pyodbc.connect(dsn) for prc in range(P)]
    return p_list

def setup_db_lookup(P):
    """
    Setup of DB connetion 
    """
    server = 'BSS-P-SQL03'
    database = 'ICE'
    username ='svcfictst'
    password='A138sinA@ab3'
    dsn = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=" + server + ";DATABASE=" + database + ";UID=" + username + ";PWD=" + password
    p_list = [pyodbc.connect(dsn) for prc in range(P)]
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

def split(a, n):
    """
    Reforming a array into equal sized nested array
    Args:
        a : Array
        n : n nested arrays of a

    Returns:
        Nested array 
    """
    
    k, m = divmod(len(a), n)
    return list((a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n)))

def processing(files, conn, conn_lookup, p):
    """
    Calling CDRiterator.to_treatment for each file
    Args:
        files : CDR file
        conn : Dsn connections string
        p : Prossecor number
    """
    [CDRiterator.to_treatment(i,conn, conn_lookup, p) for i in files]

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
    
    nr_processors = Args_guide() # number of processors
    setup_logging()
    log_run = logging.getLogger("run")
    tim_c = []
    i = 0 
    print("multiprocessing.cpu_count(): " + str(multiprocessing.cpu_count()))
    killer = GracefulKiller()
   
    config.env = lmdb.open('Temp_LMDB', map_size = 100000000) # max storage capicity = 100GB
    
    while not killer.kill_now:
        #config.connlist = setup_db(nr_processors) # reset connetion strings
        #config.connlist_lookup = setup_db_lookup(nr_processors) # reset connetion strings
        
        log_run.info("Run: {}".format(i))
        incoming_files = [ os.path.basename(src_file) for src_file in  Path(config.incoming).glob("*.*")]

        t0 = time.time()
        splited_fields = split(incoming_files, nr_processors)
        
        #proc_list  = [multiprocessing.Process(target=processing, args=(splited_fields[prc],config.connlist[prc],config.connlist_lookup[prc],"P"+str(prc), )) for prc in range(nr_processors)]
        # TEST
        proc_list  = [multiprocessing.Process(target=processing, args=(splited_fields[prc],"","","P"+str(prc), )) for prc in range(nr_processors)]
        
        for process in proc_list:
            process.start()
            
        for process in proc_list:
            process.join()
        
        t1 = time.time()
        tim_c.append(t1-t0)
        print("RUNTIME: {}".format(t1-t0))
        log_run.info("RUNTIME: {}".format(t1-t0))
        time.sleep(config.iteration_time)
        
        i += 1
        break
    """
    # INSIDE OR OUTSIDE WHILE LOOP ?
    for connstr in config.connlist:
        connstr.close()
    for connstr in config.connlist_lookup:
        connstr.close()
    """
    
    #print(statistics.mean(tim_c))
    config.env.close() 
    
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
