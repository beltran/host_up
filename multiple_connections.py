
import logging
import traceback
from functools import partial

from cassandra.cluster import Cluster, NoHostAvailable
from tests.integration.long.utils import wait_for_down, wait_for_up, force_stop, start
import time
from ccmlib.cluster import Cluster as CCMCluster


from cassandra.io.libevreactor import LibevConnection
from cassandra.io.asyncorereactor import AsyncoreConnection

from tests.integration import execute_with_long_wait_retry, drop_keyspace_shutdown_cluster, MockLoggingHandler
from tests.integration.standard.utils import create_table_with_all_types
import sys
from cassandra.policies import HostDistance
from cassandra.query import tuple_factory, SimpleStatement
from itertools import cycle
from cassandra import InvalidRequest, ConsistencyLevel, ReadTimeout, WriteTimeout, OperationTimedOut, \
    ReadFailure, WriteFailure
from cassandra.concurrent import execute_concurrent, execute_concurrent_with_args, ExecutionResult
from threading import Event
from cassandra.protocol import QueryMessage

connection_class = AsyncoreConnection


keyspace_name = "testspace"


log = logging.getLogger()
log.setLevel('WARNING')

PROTOCOL_VERSION = 4


import logging

log = logging.getLogger()
log.setLevel('INFO')


def get_connection(timeout=5):
    """
    Helper method to solve automated testing issues within Jenkins.
    Officially patched under the 2.0 branch through
    17998ef72a2fe2e67d27dd602b6ced33a58ad8ef, but left as is for the
    1.0 branch due to possible regressions for fixing an
    automated testing edge-case.
    """
    e = None

    conn = connection_class.factory(host='127.0.0.1', timeout=timeout, protocol_version=PROTOCOL_VERSION)

    if conn:
        return conn
    else:
        raise e

def make_request():
    N = 1
    log.debug("\n\nMake request called\n\n")

    query = "SELECT keyspace_name FROM system.schema_keyspaces LIMIT 1"

    def cb(*args, **kargs):
        pass

    conn = connection_class.factory(host='127.0.0.1', timeout=5, protocol_version=PROTOCOL_VERSION)
    conn.send_msg( QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE), request_id=0, cb=cb)
    conn.close()

    log.debug("\n\nRequest ended\n\n")


CCM_KWARGS = {}
CCM_KWARGS['version'] = "3.7"

path_to_ccm = "/home/jaume/.ccm/"

setup_cluster = False

if setup_cluster:

    CCM_CLUSTER = CCMCluster(path_to_ccm, "test_for_is_conn", **CCM_KWARGS)
    CCM_CLUSTER.populate(3)
    CCM_CLUSTER.clear()
    CCM_CLUSTER.start()
    time.sleep(5)

connection_class.initialize_reactor()

log.setLevel('DEBUG')

mock_handler = MockLoggingHandler()
logger = logging.getLogger()
logger.addHandler(mock_handler)


try:
    for _ in range(4000):
        #connection_class.initialize_reactor()
        for _ in range(10):
            log.setLevel("WARNING")
            try:
                #get_connection(5)
                conn = get_connection(sys.float_info.min)
                #log.info("Went through the tiny timeout!")
                #make_request_with_conn(conn)
                conn.close()
            except Exception as e:
                log.info(e)
                log.info("Timeout generated")
                pass
            log.debug("\n\n\n\n\n")
            log.setLevel("DEBUG")
            make_request()
            #mock_handler.reset()
        #connection_class.initialize_reactor()

except Exception as e:
    import traceback;traceback.print_exc()
    raise e
finally:
    if setup_cluster:
        CCM_CLUSTER.remove()