from kubernetes import client, config
import sys
import sched, time
from collector import query_all_nodes
from multiprocessing import Pool
from db import create_tables

def main():
    if len(sys.argv) != 2:
        print("Usage: python collector.py <namespace>")
        sys.exit(1)

    config.load_kube_config()

    v1 = client.CoreV1Api()
    print("Listing pods with their IPs:")
    ret = v1.list_namespaced_pod(sys.argv[1])
    for i in ret.items:
        print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))

    node_names = list(map(lambda x: x.metadata.name, v1.list_node().items))

    # Create table if not exists
    create_tables()

    pool = Pool(processes=len(node_names))
    my_scheduler = sched.scheduler(time.time, time.sleep)
    my_scheduler.enter(0, 1, query_all_nodes, (my_scheduler, pool, node_names))

    print("Starting scheduler")
    my_scheduler.run()
    pool.close()
    pool.join()


if __name__ == "__main__":
    main()