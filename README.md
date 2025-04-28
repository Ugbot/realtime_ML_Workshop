# Real-time Crypto Data Analysis with Python Streaming Tools

Welcome to the Prague ML Fintech Workshop! This project provides a local development environment using Docker Compose to explore real-time data analysis of cryptocurrency data. While this setup specifically uses PyFlink (Apache Flink's Python API) and Redpanda (a Kafka-compatible streaming data platform), the concepts and techniques explored are applicable across the broader Python streaming ecosystem.

Python offers a growing number of powerful tools and platforms for stream processing, including managed services like [Ververica Cloud](https://www.ververica.com/cloud-trial) (from the original creators of Flink) and Quix Platform, as well as libraries like Faust, Bytewax, and standard Python concurrency models. This workshop uses PyFlink as a robust example for building stateful streaming applications.

**Workshop Focus:** Analyzing real-time cryptocurrency market data (e.g., trades, order books) to build interesting models and derive insights using Python and PyFlink.

## Table of Contents

*   [Prerequisites](#prerequisites)
*   [Setup](#setup)
*   [Environment Overview](#environment-overview)
*   [Included Tools & Data Sources](#included-tools--data-sources)
*   [Using the Flink CLI](#using-the-flink-cli)
*   [Running PyFlink Jobs](#running-pyflink-jobs)
*   [Workshop Goals & Potential Explorations](#workshop-goals--potential-explorations)
*   [Next Steps / Going Further](#next-steps--going-further)
*   [Troubleshooting](#troubleshooting)
*   [Project Structure](#project-structure)
*   [License](#license)

## Prerequisites

*   **Docker & Docker Compose:** Ensure you have Docker Desktop or Docker Engine with Compose installed and running. [Install Docker](https://docs.docker.com/get-docker/)
*   **Python:** Python 3.8+ is recommended but should be no later than 3.11.x for Flink compatibility. [Install Python](https://www.python.org/downloads/)
*   **Git:** To clone this repository (if applicable).
*   **Terminal/Command Prompt:** Basic familiarity with using a terminal.


## Setup

1.  **Clone the Repository (if you haven't already):**
    ```bash
    git clone <repository-url> # Replace with the actual URL
    cd <repository-directory>
    ```

2.  **Install Python Dependencies:**
    These dependencies are primarily for the Flink CLI tool (`flink_cli.py`). It's highly recommended to use a Python virtual environment:
    ```bash
    # Create a virtual environment (optional but recommended)
    python -m venv venv
    # Activate it (Linux/macOS)
    source venv/bin/activate
    # Activate it (Windows - Git Bash/WSL)
    source venv/Scripts/activate
    # Activate it (Windows - Command Prompt)
    .\\venv\\Scripts\\activate.bat
    # Activate it (Windows - PowerShell)
    .\\venv\\Scripts\\Activate.ps1

    # Install required packages
    pip install -r requirements.txt
    ```

3.  **Start the Docker Environment:**
    We will be starting 2 Docker Compose environments and these commands need to be run from the project root directory. It will look for the compose file in 2 project subdirectories.
    
    - Run Docker Compose in the `crypto_data` subdirectory.
    ```bash
    docker compose -f crypto_data/docker-compose.yml up -d
    ```
    
    - Run Docker Compose in the `pyflink` subdirectory.
    ```bash
    docker compose -f pyflink/docker-compose.yml up -d
    ```
    *   Wait a minute or two for the services to initialize fully. You can monitor the logs:
        ```bash
        docker compose -f pyflink/docker-compose.yml logs -f
        ```
        Press `Ctrl+C` to stop viewing logs.

4.  **Verify the Setup:**
    *   **Flink UI:** Open your web browser and navigate to [http://localhost:8088](http://localhost:8088). You should see the Apache Flink Dashboard.
    *   **Redpanda UI:** Open your web browser and navigate to [http://localhost:8080](http://localhost:8080). You should see the Redpanda Console.
    *   **Redpanda (Optional):** Redpanda's Kafka API is at `localhost:9092` and the Schema Registry at `localhost:8081`.
    *   **Flink CLI:** Test the command-line interface (run from the project root):
        ```bash
        python flink_cli.py info
        ```
        You should see output detailing the Flink cluster status.

## Environment Overview

The `crypto_data/docker-compose.yml` file sets up the following service:
*   `redpanda`: A single-node Redpanda cluster.
    *   Kafka API: `localhost:9092` (from host), `redpanda:19092` (within Docker network)
    *   Schema Registry: `localhost:8081` (from host), `redpanda:28081` (within Docker)
*   `console`: A web console for the Redpanda cluster.
    *   Web UI: `localhost:8080`

The `pyflink/docker-compose.yml` file sets up the following services:

*   `jobmanager`: The Flink JobManager.
    *   Flink UI / REST API: `http://localhost:8088` (from host), `jobmanager:8081` (within Docker)
*   `taskmanager`: Two Flink TaskManagers.
    *   **Volume Mount:** The project root directory (`./`) on your host machine is mounted to `/app` inside the TaskManager containers. This allows Flink jobs to access Python scripts and dependencies located anywhere in the project structure.

## Included Tools & Data Sources

This project includes helper tools and scripts:

*   **`crypto_data/coinbase2parquet.py`:** A versatile Python script to fetch real-time ticker data from Coinbase Advanced Trade API via WebSocket.
    *   Requires additional dependencies: `pip install websockets certifi confluent-kafka pandas pyarrow`
    *   **Modes:**
        *   `-k` (default): Stream data directly to the `coinbase-ticker` Kafka topic in the local Redpanda instance.
        *   `-F`: Stream data and append it to a local Parquet file (`./coinbase_ticker_data.parquet` by default).
        *   `-FK`: Read data *from* the local Parquet file and send it to the `coinbase-ticker` Kafka topic.
        *   `-PF`: Print the full path to the Parquet file and its first 100 rows (if it exists), then exit.
    *   Can be run concurrently with Flink jobs that consume from the `coinbase-ticker` topic.
    *   Press `q` then Enter to gracefully stop streaming modes (`-k`, `-F`).

*   **`flink_cli.py`:** A simple CLI for basic Flink cluster interactions (see next section).

## Using the Flink CLI

The `flink_cli.py` script (located at the project root) provides basic interaction with the Flink cluster.

```bash
# Show available commands
python flink_cli.py --help

# Get cluster overview
python flink_cli.py info

# List running/finished jobs
python flink_cli.py list-jobs

# Submit a job JAR (less common for pure PyFlink)
# python flink_cli.py submit-job /path/to/flink-*.jar --args "-py /app/path/to/your_script.py"

# Stop a running job (get JOB_ID from 'list-jobs')
python flink_cli.py stop-job <JOB_ID>
```
**Note:** The `submit-job` command is primarily for JARs. See the next section for typical PyFlink submission.

## Running PyFlink Jobs

1.  **Place your Python script(s)** (e.g., `my_flink_job.py`) anywhere within the project structure (e.g., in a new `jobs/` directory or directly in `pyflink/`).
2.  **Use Flink's submission mechanisms:** Submit jobs using `docker compose exec` to run the `flink` command inside the `jobmanager` container. Reference your scripts using the `/app` mount point.

    ```bash
    # Example: Run a script located at ./jobs/crypto_analyzer.py
    # Assume crypto_analyzer.py has a main guard (`if __name__ == "__main__":`)
    docker compose -f pyflink/docker-compose.yml exec jobmanager ./bin/flink run \
      --python /app/jobs/crypto_analyzer.py

    # Example: Run a script with additional Python file dependencies
    # Assume utils.py is in the same directory as the main script
    docker compose -f pyflink/docker-compose.yml exec jobmanager ./bin/flink run \
      --python /app/jobs/crypto_analyzer.py \
      --pyFiles /app/jobs/utils.py

    # Example: Running a script packaged as a Python module
    # docker compose -f pyflink/docker-compose.yml exec jobmanager ./bin/flink run \
    #  -pyfs /app/dependencies/ \
    #  -pym jobs.crypto_analyzer
    ```
    *   Adjust paths (`/app/...`) based on where you place your Python scripts within the project root.

## Workshop Goals & Potential Explorations

This environment serves as a foundation for:

*   Connecting PyFlink jobs to Redpanda topics.
*   Stateless transformations (filtering, mapping).
*   Stateful operations (windowing, aggregations) for metrics like moving averages, volatility, etc.
*   Anomaly detection.
*   Simple rule-based signals.

## Next Steps / Going Further

*   **Explore Ververica Cloud:** Dive into a comprehensive, enterprise-grade stream processing platform based on Apache Flink, built by the original creators of Flink. [Get Started with Ververica Cloud](https://www.ververica.com/cloud-trial)
*   **Explore Advanced Flink Features:** Investigate Table API/SQL, complex event processing (CEP), or asynchronous I/O.
*   **Integrate More Data Sources:** Connect to other exchanges or financial data APIs.
*   **Build More Sophisticated Models:** Implement more complex algorithms or ML models within your Flink jobs.

## Troubleshooting

*   **Port Conflicts:** If `docker compose -f pyflink/docker-compose.yml up` fails due to port conflicts (8081, 8088, 9092), stop the conflicting application or change port mappings in `pyflink/docker-compose.yml`. Update `FLINK_REST_URL` for the CLI if needed.
*   **Insufficient Resources:** Allocate more resources to Docker if needed.
*   **Container Issues:**
    *   Check logs: `docker compose -f pyflink/docker-compose.yml logs <service_name>`
    *   Restart: `docker compose -f pyflink/docker-compose.yml restart <service_name>` or `docker compose -f pyflink/docker-compose.yml down && docker compose -f pyflink/docker-compose.yml up -d`
*   **CLI Connection Error:** Ensure Flink is running (`docker compose -f pyflink/docker-compose.yml ps`) and accessible at `http://localhost:8088`.

## Project Structure

```
.
├── pyflink/
│   ├── docker-compose.yml  # Docker setup for Flink & Redpanda
│   └── README.md           # Info about the Docker environment
├── jobs/                   # Suggested location for your PyFlink job scripts
│   └── (your scripts).py
├── flink_cli.py            # Python CLI tool for basic Flink interaction
├── crypto_data/            # Scripts related to crypto data fetching/handling
│   └── coinbase2parquet.py # Coinbase ticker data fetcher (Kafka/Parquet)
├── requirements.txt        # Python dependencies for flink_cli.py
├── venv/                   # Python virtual environment (optional)
└── README.md               # This file - Main project documentation
```

## License

This project is licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for details (if one exists) or visit [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0).

---
Happy Streaming!