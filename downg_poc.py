
import logging
import traceback

from cassandra.cluster import Cluster
from tests.integration.long.utils import wait_for_down, wait_for_up, force_stop, start
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy, DowngradingConsistencyRetryPolicy
import time
from ccmlib.cluster import Cluster as CCMCluster

#from cassandra.io.geventreactor import GeventConnection
from cassandra.io.eventletreactor import EventletConnection
from cassandra.io.libevreactor import LibevConnection
from cassandra.io.asyncorereactor import AsyncoreConnection


connection_class = AsyncoreConnection


KEYSPACE = "testkeyspace"


log = logging.getLogger()
log.setLevel('DEBUG')


all_contact_points = ["127.0.0.1", "127.0.0.2", "127.0.0.3"]

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

CCM_CLUSTER = CCMCluster("/home/jaume/.ccm/", "poc", **CCM_KWARGS)
CCM_CLUSTER.populate(1)
CCM_CLUSTER.start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=[])

def main():
    while 1:
        log.info("\n\n\n\n Starting iteration\n")
        cluster = Cluster(protocol_version=PROTOCOL_VERSION, connection_class=connection_class)
        cluster.connect(wait_for_all_pools=True)
        hosts = cluster.metadata.all_hosts()
        address = hosts[0].address
        node_to_stop = int(address.split('.')[-1:][0])
        cluster.shutdown()

        log.info("node_to_stop: {0}".format(node_to_stop))

        contact_point = '127.0.0.{0}'.format(get_node_not_x(node_to_stop))
        cluster = Cluster(contact_points=[contact_point], protocol_version=PROTOCOL_VERSION, connection_class=connection_class)
        cluster.connect(wait_for_all_pools=True)
        try:
            force_stop(node_to_stop)
            wait_for_down(cluster, node_to_stop)
            # Attempt a query against that node. It should complete
            cluster2 = Cluster(contact_points=all_contact_points, protocol_version=PROTOCOL_VERSION, connection_class=connection_class)
            session2 = cluster2.connect()
            session2.execute("SELECT * FROM system.local")
        except Exception as e:
            traceback.print_exc()
        finally:
            cluster2.shutdown()
            log.info("\n\nDone, with cluster2, starting node {0}\n\n".format(node_to_stop))
            start(node_to_stop)
            wait_for_up(cluster, node_to_stop)
            cluster.shutdown()
            log.info("\n\nCluster is shutdown and node {0} should be up\n\n".format(node_to_stop))


        cluster3 = Cluster(
            load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
            default_retry_policy=DowngradingConsistencyRetryPolicy(),
            protocol_version=PROTOCOL_VERSION,
            connection_class=connection_class)
        session = cluster3.connect(wait_for_all_pools=True)

        for _ in range(20):
            hosts = cluster3.metadata.all_hosts()
            log.info("cluster.metadata.all_hosts(): {0}".format(hosts))
            if len(hosts) == 3 and (map(lambda x : x.is_up, hosts) == [True] * 3):
                log.info("Found all the required nodes")
                break
            time.sleep(1)
        else:
            raise Exception("Some node never came back")
        cluster3.shutdown()
        time.sleep(4)

if __name__ == "__main__":
    main()
