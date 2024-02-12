# Multiprocess Map-Reduce Engine

A parallel Map-Reduce engine implemented in C using pthreads. Uses thread-safe
bounded queues for inter-process communication via shared memory (mutex,
condition variables).

## Build & Run

gcc -o combiner combiner.c && ./combiner 10 3 < testcases/input0

## Architecture

- **mapper** — reads input and pushes elements into a shared queue
- **reducer** — pops elements from the shared queue and produces output
- **shared_queue_push** — thread-safe enqueue (mutex + condition variable)
- **shared_queue_pop** — thread-safe dequeue (mutex + condition variable)
