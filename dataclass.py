from dataclasses import dataclass, field

@dataclass
class PodMetric:
    pod: str
    namespace: str
    cpu: float = field(init=False)
    memory: float = field(init=False)
    cpu_timestamp: float = field(init=False)
    memory_timestamp: float = field(init=False)

@dataclass
class ContainerMetric:
    pod: str
    container: str
    namespace: str
    cpu: float = field(init=False)
    memory: float = field(init=False)
    cpu_timestamp: float = field(init=False)
    memory_timestamp: float = field(init=False)

@dataclass
class NodeMetric:
    name: str
    cpu: float = field(init=False)
    memory: float = field(init=False)
    cpu_timestamp: float = field(init=False)
    memory_timestamp: float = field(init=False)

@dataclass
class ErrorCount:
    error_count: int
    timestamp: float
