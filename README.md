# redis-traffic-monitor

- It tracks all Redis commands being executed in a Kubernetes cluster, without any change in Redis clients. It tracks metrics like execution time, response size in bytes, etc.
- This Node.js repository contains a tool for monitoring Redis traffic using the tcpdump utility and pcap library. It provides functionality to track commands executed and the time taken to execute particular commands in a Redis instance, without being dependent on a specific Redis client or server implementation.

### Features

- Captures Redis traffic on a specified port using tcpdump.
- Parses captured packets to extract Redis commands and responses.
- Tracks the time taken to execute each Redis command.
- Provides visualization and analysis of Redis traffic metrics.

### Requirements
- kubernetes
- grafana
- influxdb

### Setting up grafana:
```
kubectl apply -f ./grafana/pod.yaml
```
```
kubectl port-forward -n grafana service/grafana 3000:3000
```
- This creates an grafana Namespace, Service, and Deployment. A PersistentVolumeClaim is also created to store data written to grafana.
- Ensure the Pod is running: `kubectl get pods -n grafana`
- Ensure the Service is available: `kubectl describe service -n grafana grafana`
- Forward port 3000 from inside the cluster to localhost:
`kubectl port-forward -n grafana service/grafana 3000:80`

### Setting up influx db:
```
kubectl apply -f ./influxdb/pod.yaml
```
```
kubectl port-forward -n influxdb service/influxdb 8086:8086
```

- This creates an influxdb Namespace, Service, and StatefulSet. A PersistentVolumeClaim is also created to store data written to InfluxDB.
- Ensure the Pod is running: `kubectl get pods -n influxdb`
- Ensure the Service is available: `kubectl describe service -n influxdb influxdb`
- Forward port 8086 from inside the cluster to localhost:
`kubectl port-forward -n influxdb service/influxdb 8086:8086`

### Dependencies
- libpcap-dev

### Installation
- Clone the repository: ```git clone https://github.com/emasdigi/redis-query-analyzer.git```
- Install dependencies: ```npm install```
- Update Environment variables in Dockerfile
- Start container to monitor redis traffic: ```docker compose up```
- To start application on host machine itself
```sudo node index.js```

### Contributing

Contributions are welcome! Please fork the repository and submit a pull request with your changes.
