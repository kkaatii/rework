# rework
[![version](https://img.shields.io/crates/v/rework)](https://crates.io/crates/rework)
[![documentation](https://docs.rs/rework/badge.svg)](https://docs.rs/rework)  

Multi-threaded async task processing in the Master-Worker pattern.

A `rework` system consists of a dispatcher and multiple workers. The dispatcher receives work requests via its `Handle` and schedules work with the help of a `Scheduler`. The way workers process incoming requests is defined by a `WorkFn` which takes a request and asynchronously generates a response.

The dispatcher, as well as every worker, runs on its dedicated thread. Although the concrete `Request` type must be `Send`, the response futures do not have to, allowing workers to leverage `!Send` mechanisms, e.g., thread locals.

## Features

* Multi-threaded workers, each with a single-thread async runtime, that can work on `!Send` futures.
* Async as an option--you don't necessarily need an async runtime at all.
* Load balanced, as long as `Workload` is properly defined.