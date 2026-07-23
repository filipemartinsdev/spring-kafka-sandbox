# Spring Kafka Sandbox


This is a study project implementing Apache Kafka using Spring Boot 4.


## Technologies

- Java 21
- Spring Boot 4.1
- PostgreSQL 17
- Apache Kafka 3.8 (KRaft)
- KafkaUI
- Docker

## Domain

The purpose is to create a simples IoT simulation with constant events and analytics.

![Architecture](images/arch.png)

## How to execute

1. Start all services

```bash
docker compose up -d --build
```