
import logging

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


def get_node_not_x(node_to_stop):
    nodes = [1, 2, 3]
    for num in nodes:
        if num is not node_to_stop:
            return num

def start(node):
    get_node(node).start()

def force_stop(node):
    log.debug("Forcing stop of node %s", node)
    get_node(node).stop(wait=False, gently=False)
    log.debug("Node %s was stopped", node)

def get_node(node_id):
    return CCM_CLUSTER.nodes['node%s' % node_id]

CCM_KWARGS = {}
CCM_KWARGS['version'] = "2.0.17"


path_to_ccm = "/home/jaume/.ccm/"

CCM_CLUSTER = CCMCluster(path_to_ccm, "test_for_is_up", **CCM_KWARGS)
CCM_CLUSTER.populate(3)
CCM_CLUSTER.start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=[])


def main():

    log.info("\n\n\n\n Starting iteration\n")
    node_to_stop = 3
    log.info("node_to_stop: {0}".format(node_to_stop))

    cluster = Cluster(protocol_version=PROTOCOL_VERSION, connection_class=connection_class)
    cluster.connect(wait_for_all_pools=True)

    force_stop(node_to_stop)
    wait_for_down(cluster, node_to_stop)

    log.info("\n\nNode is down, starting node {0}\n\n".format(node_to_stop))
    start(node_to_stop)
    #Host marked as up despite not completely up because of callback to connection._handle_status_change.
    #When communication starts again, it is called, but port 9042 is not opened until a bit later
    wait_for_up(cluster, node_to_stop)
    cluster.shutdown()
    log.info("\n\nCluster is shutdown and node {0} should be up\n\n".format(node_to_stop))

    cluster = Cluster(
    protocol_version=PROTOCOL_VERSION,
    connection_class=connection_class)

    cluster.connect(wait_for_all_pools=True)

    for _ in range(20):
        hosts = cluster.metadata.all_hosts()
        log.info("cluster.metadata.all_hosts(): {0}".format(hosts))
        if len(hosts) == 3 and list(map(lambda x : x.is_up, hosts)) == [True] * 3:
            log.info("Found all the required nodes")
            break
        else:
            log.error("Not all hosts are up despite having waited for them")
        time.sleep(1)
    else:
        raise Exception("Some node never came back")

    cluster.shutdown()

try:
    main()
finally:
    CCM_CLUSTER.remove()
