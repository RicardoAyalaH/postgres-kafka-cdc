# postgres-kafka-cdc

> **Work in progress.** This is a learning project, not a production system.

A local change-data-capture (CDC) pipeline that streams row-level changes from one
PostgreSQL database to another in near real time:

```
Postgres (WAL) → Debezium (Kafka Connect) → Apache Kafka → TypeScript consumer → Postgres
                                                                  └─ SSE → React dashboard
```

Everything runs locally with Docker Compose. Setup instructions, architecture notes,
measured results and known limitations will be added as the project is built.

## License

[MIT](LICENSE)
