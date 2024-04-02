import psycopg2
from dotenv import load_dotenv
import os
from typing import Dict
from model import PodMetric, ContainerMetric, NodeMetric
from psycopg2.extensions import connection

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def connect():
    return psycopg2.connect(f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host=localhost")

def create_tables():
    conn = connect()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pods (
            id SERIAL PRIMARY KEY,
            pod_name TEXT,
            namespace TEXT,
            cpu_usage DOUBLE PRECISION,
            memory_usage DOUBLE PRECISION,
            cpu_timestamp BIGINT,
            memory_timestamp BIGINT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS containers (
            id SERIAL PRIMARY KEY,
            pod_name TEXT,
            container_name TEXT,
            namespace TEXT,
            cpu_usage DOUBLE PRECISION,
            memory_usage DOUBLE PRECISION,
            cpu_timestamp BIGINT,
            memory_timestamp BIGINT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS nodes (
            id SERIAL PRIMARY KEY,
            name TEXT,
            cpu_usage DOUBLE PRECISION,
            memory_usage DOUBLE PRECISION,
            cpu_timestamp BIGINT,
            memory_timestamp BIGINT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS error_count (
            id SERIAL PRIMARY KEY,
            error_count INT,
            timestamp TIMESTAMP(6)
        )
    """)

    conn.commit()
    cur.close()
    conn.close()

def insert_batch_pod_data(conn: connection, pod_data: Dict[str, PodMetric]):
    cur = conn.cursor()
    for pod in pod_data.values():
        cur.execute("INSERT INTO pods (pod_name, namespace, cpu_usage, memory_usage, cpu_timestamp, memory_timestamp) VALUES (%s, %s, %s, %s, %s, %s)", (pod.pod, pod.namespace, pod.cpu_usage, pod.memory_usage, pod.cpu_timestamp, pod.memory_timestamp))
    conn.commit()
    cur.close()


def insert_batch_container_data(conn: connection, container_data: Dict[str, ContainerMetric]):
    cur = conn.cursor()
    for container in container_data.values():
        cur.execute("INSERT INTO containers (pod_name, container_name, namespace, cpu_usage, memory_usage, cpu_timestamp, memory_timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s)", (container.pod, container.container, container.namespace, container.cpu_usage, container.memory_usage, container.cpu_timestamp, container.memory_timestamp))

    conn.commit()
    cur.close()

def insert_batch_node_data(conn: connection, node_data: Dict[str, NodeMetric]):
    cur = conn.cursor()

    for node in node_data.values():
        cur.execute("INSERT INTO nodes (name, cpu_usage, memory_usage, cpu_timestamp, memory_timestamp) VALUES (%s, %s, %s, %s, %s)", (node.name, node.cpu_usage, node.memory_usage, node.cpu_timestamp, node.memory_timestamp))

    conn.commit()
    cur.close()


def insert_error_count_data(conn: connection, error_count: int, timestamp: float):
    cur = conn.cursor()

    cur.execute("INSERT INTO error_count (error_count, timestamp) VALUES (%s, %s)", (error_count, timestamp))

    conn.commit()
    cur.close()


if __name__ == "__main__":
    create_tables()
    conn = connect()
    
    insert_batch_pod_data(conn, {"pod1": PodMetric("pod1", "namespace1", 1.0, 1.0, 1.0, 1.0)})
    insert_batch_container_data(conn, {"container1": ContainerMetric("container1", "namespace1", 1.0, 1.0, 1.0, 1.0)})
    insert_batch_node_data(conn, {"node1": NodeMetric("node1", 1.0, 1.0, 1.0, 1.0)})
    insert_error_count_data(1, 1.0)

    conn.close()
