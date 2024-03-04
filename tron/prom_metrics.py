from prometheus_client import Gauge


tron_cpu_gauge = Gauge("tron_k8s_cpus", "Measuring CPU for tron jobs on K8s")
tron_memory_gauge = Gauge("tron_k8s_mem", "Measuring memory for tron jobs on K8s")
tron_disk_gauge = Gauge("tron_k8s_disk", "Measuring disk for tron jobs on K8s")
