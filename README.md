# CSE 585 Cache Eviction

This project replays real LLM serving traces against Mooncake Store to study cache-eviction policies. The pipeline has 3 main steps.

Note: This code should be compiled and run on a server-class machine, not a laptop. A CUDA-capable GPU may be required (the build script will attempt to install the CUDA toolkit if `nvcc` is missing). Linux (Ubuntu/Debian) is assumed.

Step 1: Build Mooncake and set up the Python environment

Run `./build.sh`. This installs system dependencies, builds Mooncake with CMake, installs the binaries, and creates a `.venv/` with the required Python packages.

Output: `mooncake_master` is installed and `.venv/` is ready.

Step 2: Start the Mooncake master service

In a separate terminal, run:

```
mooncake_master --enable_http_metadata_server=true --http_metadata_server_port=8080
```

Output: The master listens on `localhost:50051` and serves metadata at `http://localhost:8080/metadata`.

Step 3: Run the trace replay suite

Activate the environment and run the replay driver:

```
source .venv/bin/activate
python 585temp/trace_replay_suite.py --profile toolagent_replay
```

Available profiles: `toolagent_replay`, `conversation_replay`, `trace_phase_shift`. Use `--help` for all options.

Output: Hit rate, prefix hit rate, cold miss rate, and put-failure metrics are printed to the terminal.
