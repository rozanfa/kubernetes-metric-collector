from kubernetes import client, config
import sched, time
from collector import query_all_nodes
from LoggingPool import LoggingPool
from db import create_tables
import argparse

def main():
    config.load_kube_config()

    parser = argparse.ArgumentParser(
                        prog='DataCollector',
    )
    parser.add_argument(
        '-p', '--periode',
        help='Periode of data collection',
        default=10,
        type=int
    )

    args = parser.parse_args()

    v1 = client.CoreV1Api()
    node_names = list(map(lambda x: x.metadata.name, v1.list_node().items))

    # Create table if not exists
    create_tables()
    pool = LoggingPool(processes=len(node_names))
    my_scheduler = sched.scheduler(time.time, time.sleep)
    try :
        my_scheduler.enter(0, 1, query_all_nodes, (my_scheduler, pool, node_names, args.periode))
        print("Starting scheduler")
        my_scheduler.run()
    except KeyboardInterrupt:
        print("Stopping scheduler")
    finally:
        pool.close()
        pool.join()


if __name__ == "__main__":
    main()