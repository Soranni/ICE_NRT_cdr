import os

# Environment variables for CDR
incoming = "incoming"  # os.getenv('CDR_INCOMING_FILES')
generic = "generic"  # os.getenv('CDR_GENERIC_FILES')
debug = True # True
retry = "retry"  # os.getenv('CDR_RETRY_FILES')
done = "done"  # os.getenv('CDR_DONE_FILES')
run_as_daemon = "True"  # os.getenv("CDR_RUN_AS_DAEMON") == 'True'
move_files = "FTrue" == "True"  # os.getenv("CDR_MOVE_FILES") == 'True'
iteration_time = int("1")  # int(os.getenv("ITERATION_TIME"))

# Other
cdr_bulk_size_limit = 500

env = None
conn = None  # global db pool
conn_lookup = None  # global db pool
connlist = None  # global db pool
connlist_lookup = None  # global db pool

server = os.getenv('DB_SERVER')
database = os.getenv('DB_DATABASE')
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
