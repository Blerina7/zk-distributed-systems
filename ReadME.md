#  Apache ZooKeeper — Distributed Coordination System

A comprehensive distributed systems project using **Apache ZooKeeper**, structured as a 4-node cluster featuring 3 independent Java coordination modules.

This project demonstrates core concepts of distributed coordination, including topology monitoring, fault tolerance, mutual exclusion, and distributed messaging pipelines.

---

##  Project Structure

zookeeper-distributed-coordination/
├── ClusterNodeMonitoring/ # Cluster node availability monitoring
├── DistributedLocking/    # Distributed lock mechanism using Apache Curator
├── DistributedQueues/     # FIFO distributed queue implementation
├── docker-compose.yml     # 4-node ZooKeeper cluster setup
└── pom.xml                # Parent Maven POM

---

##  Getting Started

### Prerequisites
- Docker & Docker Compose
- Java 17+
- Maven 3.8+

### Running the Cluster
To build and start the entire distributed environment, run the following command in your terminal:

docker-compose up --build

This command provisions:
- 4 ZooKeeper Nodes: Node1, Node2, Node3, and Node4 configured as an ensemble.
- 3 Java Applications: Monitoring, Locking, and Queue services running concurrently.

---

##  Modules

###  ClusterNodeMonitoring
Monitors and logs the real-time status (ONLINE / OFFLINE) of each node within the cluster every 10 seconds. It utilizes native ZooKeeper APIs, connection strings, and Watchers to automatically detect cluster state changes and trigger transparent failovers if a node drops.

###  DistributedLocking
Implements a distributed lock system utilizing Apache Curator's InterProcessMutex over a shared coordination path (lock). It leverages ephemeral sequential znodes to guarantee strict mutual exclusion and global ordering across multiple competing processes while effectively preventing deadlocks.

###  DistributedQueues
A strict FIFO (First In, First Out) distributed message queue architecture designed with a Producer-Consumer pattern. Messages are stored as sequential znodes under /distributed-queue, ensuring data consistency and guaranteeing that each message is processed exactly once by consumer threads without data duplication or race conditions.

---



.