# Job Scheduler Architecture Document

This document outlines the architecture, components, and data flow of the Job Scheduler application using UML diagrams.

## 1. System Component Diagram

The component diagram illustrates the high-level architecture of the system.

```mermaid
flowchart TD
    %% Nodes
    Client((Client API Consumer))
    
    subgraph FastAPI Application
        API[FastAPI Service\nPOST/GET Jobs]
    end
    
    subgraph Data Layer
        Cassandra[Apache Cassandra\nJobs & Executions DB]
        Redis[Redis\nJob Queue & Deduplication]
    end
    
    subgraph Processing Layer
        Scheduler[Scheduler Service\nAPScheduler/croniter]
        Workers[Worker Nodes\nThreadPool Executors]
    end
    
    %% Relationships
    Client -->|REST API calls| API
    API -->|Read/Write Jobs| Cassandra
    API -->|Write Immediate Jobs| Redis
    
    Scheduler -->|Polls DB every 5m| Cassandra
    Scheduler -->|Enqueues Jobs| Redis
    
    Workers -->|Consumes Jobs| Redis
    Workers -->|Updates Execution Status| Cassandra
```

## 2. Sequence Diagram: Job Submission & Execution Flow

The sequence diagram explains the two-layer scheduling flow showing interaction among the API, DB, Queue, Scheduler, and Workers.

```mermaid
sequenceDiagram
    autonumber
    
    actor Client
    participant API as FastAPI App
    participant DB as Cassandra (DB)
    participant Sched as Scheduler Service
    participant Q as Redis Queue
    participant Worker as Worker Service

    %% Job Submission
    Client->>API: POST /jobs (task, schedule)
    API->>DB: Insert into `jobs` and `executions`
    alt IMMEDIATE execution
        API->>Q: ZADD `queue:pending` (score=now)
    end
    API-->>Client: 201 Created (job_id)

    %% Scheduling Layer
    loop Every 5 minutes
        Sched->>DB: Poll for jobs due in next 6 min
        DB-->>Sched: List of due jobs
        Sched->>Q: ZADD `queue:pending` (score=exec_ts)
    end

    %% Execution Layer
    loop Background Thread
        Worker->>Q: ZRANGEBYSCORE `queue:pending` <= now
        Q-->>Worker: Ready Jobs
        Worker->>Q: Atomic Pop & move to `queue:processing` (now+30s)
        Worker->>Worker: Execute Task (log, http, email)
        
        alt Task Succeeds
            Worker->>Q: ZREM from `queue:processing` (ACK)
            Worker->>DB: Update execution status = COMPLETED
        else Task Fails
            Worker->>Q: Delay processing (Backoff retry logic)
            Worker->>DB: Update execution status = FAILED (if max retries met)
        end
    end
```

## 3. Data Model (Entity Relationship)

```mermaid
erDiagram
    JOBS {
        uuid job_id PK
        string user_id
        string task_id
        string schedule_type
        timestamp created_at
    }

    EXECUTIONS {
        uuid execution_id PK
        timestamp time_bucket PK
        uuid job_id
        string status
        timestamp scheduled_for
        timestamp executed_at
    }

    USER_EXECUTIONS {
        string user_id PK
        uuid execution_id PK
        string status
    }

    JOBS ||--o{ EXECUTIONS : "has many"
    JOBS ||--o{ USER_EXECUTIONS : "denormalized view"
```

## 4. Deployment Context

The deployment diagram structure representing the Docker/Kubernetes container orchestrators for horizontal scalability.

```mermaid
flowchart LR
    LB[Load Balancer / Ingress]
    
    subgraph API Cluster
        API1[API Pod 1]
        API2[API Pod 2]
    end
    
    subgraph Scheduler Cluster
        Sched1[Scheduler Pod\nSingle Instance]
    end
    
    subgraph Worker Cluster
        Work1[Worker Pod 1]
        Work2[Worker Pod 2]
        Work3[Worker Pod N...]
    end
    
    %% Connections
    LB --> API1
    LB --> API2
    
    API1 -.-> Cassandra[(Cassandra Cluster)]
    API1 -.-> Redis[(Redis Cluster)]
    
    Sched1 -.-> Cassandra
    Sched1 -.-> Redis
    
    Work1 -.-> Redis
    Work1 -.-> Cassandra
```

## Architecture Design Principles

1. **Two-Layer Scheduling**: Enhances system durability and precision. Cassandra handles persistence and robustness against restarts, while Redis handles sub-second queuing and deduplication.
2. **Horizontal Scalability**: Both API and Worker components are stateless and horizontally scalable. 
3. **Atomic Queue Processing**: Uses Redis sorted sets `queue:pending` and `queue:processing` along with visibility timeouts for safe concurrent consumption by multiple workers.
4. **Retry Logic**: Failed tasks implement an exponential backoff directly within the worker processes before permanently failing the execution.
