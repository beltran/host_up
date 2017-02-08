import time
from ccmlib.cluster import Cluster as CCMCluster
from cassandra.io.asyncorereactor import AsyncoreConnection
from cassandra import  ConsistencyLevel
from cassandra.protocol import QueryMessage
import logging

connection_class = AsyncoreConnection


PROTOCOL_VERSION = 4



log = logging.getLogger()
log.setLevel('DEBUG')

def make_request():
    log.debug("\n\nMake request called\n\n")

    query = "SELECT keyspace_name FROM system.schema_keyspaces LIMIT 1"

    def cb(*args, **kargs):
        pass

    conn = connection_class.factory(host='127.0.0.1', timeout=5, protocol_version=PROTOCOL_VERSION)
    log.debug("Got connection, everything should be good for query")
    conn.send_msg( QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE), request_id=0, cb=cb)
    conn.close()

    log.debug("\n\nRequest ended\n\n")


setup_cluster = False

if setup_cluster:
    CCM_KWARGS = {}
    CCM_KWARGS['version'] = "3.7"

    path_to_ccm = "/home/jaume/.ccm/"

    CCM_CLUSTER = CCMCluster(path_to_ccm, "test_for_is_conn", **CCM_KWARGS)
    CCM_CLUSTER.populate(3)
    CCM_CLUSTER.clear()
    CCM_CLUSTER.start()
    time.sleep(5)

connection_class.initialize_reactor()

try:
    for _ in range(40000):
            make_request()

except Exception as e:
    import traceback;traceback.print_exc()
    raise e
finally:
    if setup_cluster:
        CCM_CLUSTER.remove()