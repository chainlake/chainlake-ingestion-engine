# High-level flow

```mermaid
flowchart LR
    A[Engine] --> B[Fetcher]
    B --> C[Scheduler]
    C --> D[RPC Client]
    D --> E[EVM RPC Request Builder]

    C -->|response - value, meta| B
    B --> A

    A --> F[Processor]
    F --> G1[parse_blocks]
    F --> G2[parse_transactions]

    F --> H[Kafka Sink]
    H --> I[Kafka Producer]
```


# Detailed flow
```mermaid
flowchart TD

    subgraph ingestion
        Engine[engine.py]
        Fetcher[fetcher.py]
        Processor[processor.py]
    end

    subgraph scheduler
        Scheduler[adaptive.py]
    end

    subgraph client
        Client[jsonrpc.py]
    end

    subgraph adapters
        RpcReq[rpc_requests.py]
        ParseBlocks[parse_blocks.py]
        ParseTx[parse_transactions.py]
    end

    subgraph sinks
        KafkaSink[producer.py]
        Kafka[(Kafka Broker)]
    end

    %% Flow
    Engine --> Fetcher
    Fetcher --> Scheduler
    Scheduler --> Client
    Client --> RpcReq

    Scheduler -->|value, meta| Fetcher
    Fetcher --> Engine

    Engine --> Processor
    Processor --> ParseBlocks
    Processor --> ParseTx

    Processor --> KafkaSink
    KafkaSink --> Kafka
```

# Block lifecycle
```mermaid
sequenceDiagram
    participant Engine
    participant Fetcher
    participant Scheduler
    participant RPC
    participant Processor
    participant Kafka

    Engine->>Fetcher: get_block(90000100)
    Fetcher->>Scheduler: submit_request()
    Scheduler->>RPC: eth_getBlockByNumber
    RPC-->>Scheduler: (value, meta)
    Scheduler-->>Fetcher: (value, meta)
    Fetcher-->>Engine: (value, meta)

    Engine->>Processor: process(value)
    Processor->>Processor: parse_blocks + parse_tx

    Processor-->>Engine: structured rows

    Engine->>Kafka: send(blocks)
    Engine->>Kafka: send(transactions)
```
## Layer Responsibility
| Layer     | Responsibility |
| --------- | -------------- |
| Engine    | orchestration  |
| Fetcher   | get raw data   |
| Processor | transform      |
| Sink      | deliver        |
