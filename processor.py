from typing import Dict
from dataclass import PodMetric, ContainerMetric, NodeMetric

def process_pod_or_container(row: list[str], pods: Dict[str, PodMetric], containers: Dict[str, ContainerMetric]):
    raw_name = row[0]
    value = row[1]
    timestamp = row[2]
    raw_type = raw_name.split("{")[0]
    pod = raw_name.split("\"")[-2].split("\"")[-1]
    namespace = raw_name.split("\"")[-4].split("\"")[-1]
    container = raw_name.split("\"")[1].split("\"")[0]
    match raw_type:
        case "container_cpu_usage_seconds_total":
            try:
                containers[pod].cpu_usage = value
                containers[pod].cpu_timestamp = timestamp
            except KeyError:
                containers[pod] = ContainerMetric(namespace=namespace, pod=pod, container=container)
                containers[pod].cpu_usage = value
                containers[pod].cpu_timestamp = timestamp
        case "container_memory_working_set_bytes":
            try:
                containers[pod].memory_usage = value
                containers[pod].memory_timestamp = timestamp
            except KeyError:
                containers[pod] = ContainerMetric(namespace=namespace, pod=pod, container=container)
                containers[pod].memory_usage = value
                containers[pod].memory_timestamp = timestamp
        case "pod_memory_working_set_bytes":
            try:
                pods[pod].memory_usage = value
                pods[pod].memory_timestamp = timestamp
            except KeyError:
                pods[pod] = PodMetric(namespace=namespace, pod=pod)
                pods[pod].memory_usage = value
                pods[pod].memory_timestamp = timestamp
        case "pod_cpu_usage_seconds_total":
            try:
                pods[pod].cpu_usage = value
                pods[pod].cpu_timestamp = timestamp
            except KeyError:
                pods[pod] = PodMetric(namespace=namespace, pod=pod)
                pods[pod].cpu_usage = value
                pods[pod].cpu_timestamp = timestamp


def process_node(row: list[str], nodes: Dict[str, NodeMetric], node_name: str):
    raw_type = row[0]
    value = row[1]
    timestamp = row[2]
    match raw_type:
        case "node_memory_working_set_bytes":
            try:
                nodes[node_name].memory_usage = value 
                nodes[node_name].memory_timestamp = timestamp 

            except KeyError:
                nodes[node_name] = NodeMetric(name=node_name)
                nodes[node_name].memory_usage = value
                nodes[node_name].memory_timestamp = timestamp
        case "node_cpu_usage_seconds_total":
            try:
                nodes[node_name].cpu_usage = value 
                nodes[node_name].cpu_timestamp = timestamp 
            except KeyError:
                nodes[node_name] = NodeMetric(name=node_name)
                nodes[node_name].cpu_usage = value
                nodes[node_name].cpu_timestamp = timestamp