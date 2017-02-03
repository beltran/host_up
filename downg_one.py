
import logging
import traceback

from cassandra.cluster import Cluster
from tests.integration.long.utils import wait_for_down, wait_for_up, force_stop, start
import time
from ccmlib.cluster import Cluster as CCMCluster


from cassandra.io.libevreactor import LibevConnection
from cassandra.io.asyncorereactor import AsyncoreConnection


connection_class = AsyncoreConnection


KEYSPACE = "testkeyspace"


log = logging.getLogger()
log.setLevel('DEBUG')

PROTOCOL_VERSION = 2

def force_stop(node):
    log.debug("Forcing stop of node %s", node)
    get_node(node).stop(wait=False, gently=False)
    log.debug("Node %s was stopped", node)

def get_node(node_id):
    return CCM_CLUSTER.nodes['node%s' % node_id]

CCM_KWARGS = {}
CCM_KWARGS['version'] = "2.0.17"


path_to_ccm = "/home/jaume/.ccm/"

CCM_CLUSTER = CCMCluster(path_to_ccm, "test_one", **CCM_KWARGS)
CCM_CLUSTER.populate(1)
CCM_CLUSTER.start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=[])


def main():
    log.info("\n\n\n\n Starting iteration\n")
    cluster = Cluster(protocol_version=PROTOCOL_VERSION, connection_class=connection_class)
    cluster.connect(wait_for_all_pools=True)

    node_to_stop = 1
    cluster.shutdown()

    log.info("node_to_stop: {0}".format(node_to_stop))

    cluster = Cluster(protocol_version=PROTOCOL_VERSION, connection_class=connection_class)
    cluster.connect(wait_for_all_pools=True)

    force_stop(node_to_stop)
    #Node is never signaled as down
    wait_for_down(cluster, node_to_stop)

try:
    main()
finally:
    CCM_CLUSTER.remove()
