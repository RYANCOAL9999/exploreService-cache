# Explore Service (High-Concurrency Caching Infrastructure)

A high-performance Go backend optimized with Redis to manage user matchmaking, focusing on preventing cache stampedes and protecting the database.

## Core Features & Design
* **Separation of Concerns**: Clean architecture isolating DB (GORM) and cache logic.
* **Cache Optimization**: Collapses concurrent, identical requests to prevent database overload during cache misses.
* **Performance**: Uses stack allocation to reduce GC pressure.
* **Resilience**: Implements graceful shutdown for safe state management.

## Setup
```bash
docker-compose up --build
```
Or locally: `go run main.go` (requires `.env` setup).
