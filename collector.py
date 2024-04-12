from processor import process_pod_or_container, process_node
import time, os
import pandas as pd
from multiprocessing.pool import Pool
from sched import scheduler
from kubernetes import client
from db import connect, insert_batch_container_data, insert_batch_node_data, insert_batch_pod_data, insert_error_count_data


def query_a_node(node_name: str): 
    print("Doing stuff...")

    # Create a dictionary with the data
    pod_data = {}
    container_data = {}
    node_data = {}
    error_count = 0

    api_client = client.ApiClient()
    response = api_client.call_api(f"/api/v1/nodes/{node_name}/proxy/metrics/resource", "GET", response_type="str")
    data = str(response[0])
    rows = data.split("\n")

    # Remove comments and empty lines
    rows = [row for row in rows if not row.startswith("#") and row.strip() != ""]

    # Split lines from rows
    rows = [row.split() for row in rows]

    print("Processing data...")
    for row in rows:
        try:
            if row[0] == "scrape_error":
                error_count = row[1]
            elif row[0][:4] == "node":
                process_node(row, node_data, node_name)
            else:
                process_pod_or_container(row, pod_data, container_data)
                
        except IndexError:
            print("Error:",row)

    # Insert data into the database
    print("Inserting data into the database...")
    conn = connect()
    insert_timestamp = time.time()

    insert_batch_pod_data(conn, pod_data, insert_timestamp)
    print("Pod data inserted")

    insert_batch_container_data(conn, container_data, insert_timestamp)
    print("Container data inserted")

    insert_batch_node_data(conn, node_data, insert_timestamp)
    print("Node data inserted")
    
    insert_error_count_data(conn, error_count, insert_timestamp)
    print("Error count inserted")

    conn.close()

    print("Done")


def save_to_csv(pod_data: dict, container_data: dict, node_data: dict, error_count: int):
    pods_df = pd.DataFrame(pod_data.values())
    pods_df.to_csv("pods.csv", mode='a', header=not os.path.exists("pods.csv"), index=False)

    containers_df = pd.DataFrame(container_data.values())
    containers_df.to_csv("containers.csv", mode='a', header=not os.path.exists("containers.csv"), index=False)

    nodes_df = pd.DataFrame(node_data.values())
    nodes_df.to_csv("nodes.csv", mode='a', header=not os.path.exists("nodes.csv"), index=False)

    error_count_df = pd.DataFrame({"error_count": [error_count], "timestamp": [time.time()]})
    error_count_df.to_csv("error_count.csv", mode='a', header=not os.path.exists("error_count.csv"), index=False)


def query_all_nodes(scheduler: scheduler, pool: Pool, node_names: list[str], periode: int = 10):
    # schedule the next call first
    scheduler.enter(periode, 1, query_all_nodes, (scheduler, pool, node_names))
    
    for node_name in node_names:
        print("Querying node:", node_name)
        pool.apply_async(query_a_node, ([node_name]))
    