from dataclasses import dataclass, field

@dataclass
class PodMetric:
    pod: str
    namespace: str
    cpu_usage: float = field(init=False)
    memory_usage: float = field(init=False)
    cpu_timestamp: float = field(init=False)
    memory_timestamp: float = field(init=False)

@dataclass
class ContainerMetric:
    pod: str
    container: str
    namespace: str
    cpu_usage: float = field(init=False)
    memory_usage: float = field(init=False)
    cpu_timestamp: float = field(init=False)
    memory_timestamp: float = field(init=False)

@dataclass
class NodeMetric:
    name: str
    cpu_usage: float = field(init=False)
    memory_usage: float = field(init=False)
    cpu_timestamp: float = field(init=False)
    memory_timestamp: float = field(init=False)

@dataclass
class ErrorCount:
    error_count: int
    timestamp: float
