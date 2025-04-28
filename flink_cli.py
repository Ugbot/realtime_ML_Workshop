import argparse
import requests
import json
import os
from typing import Any, Dict, List, Optional

# Import Rich components
from rich import print
from rich.table import Table
from rich.panel import Panel
from rich.console import Console

console = Console()
error_console = Console(stderr=True, style="bold red")

FLINK_REST_URL: str = os.environ.get("FLINK_REST_URL", "http://localhost:8088")

def _make_request(endpoint: str, method: str = "GET", data: Optional[Dict[str, Any]] = None, files: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Helper function to make requests to the Flink REST API."""
    url: str = f"{FLINK_REST_URL}{endpoint}"
    try:
        if method == "GET":
            response = requests.get(url)
        elif method == "POST":
            response = requests.post(url, json=data, files=files)
        elif method == "PATCH":
            response = requests.patch(url, json=data)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        if response.content:
            return response.json()
        else:
            return {}
    except requests.exceptions.RequestException as e:
        error_console.print(f"Error connecting to Flink REST API at {url}: {e}")
        exit(1)
    except json.JSONDecodeError:
        error_console.print(f"Error decoding JSON response from {url}. Response text: {response.text}")
        exit(1)

def get_cluster_info() -> None:
    """Prints basic Flink cluster information using Rich Panel."""
    try:
        overview: Dict[str, Any] = _make_request("/overview")

        # Create a Rich Panel for the overview
        info_text = (
            f"[bold cyan]Flink Version:[/bold cyan] {overview.get('flink-version', 'N/A')}\n"
            f"[bold]Task Managers:[/bold] {overview.get('taskmanagers', 'N/A')}\n"
            f"[bold]Slots Total:[/bold] {overview.get('slots-total', 'N/A')}\n"
            f"[bold]Slots Available:[/bold] {overview.get('slots-available', 'N/A')}\n\n"
            f"[green]Jobs Running:[/green] {overview.get('jobs-running', 'N/A')}\n"
            f"[blue]Jobs Finished:[/blue] {overview.get('jobs-finished', 'N/A')}\n"
            f"[yellow]Jobs Cancelled:[/yellow] {overview.get('jobs-cancelled', 'N/A')}\n"
            f"[red]Jobs Failed:[/red] {overview.get('jobs-failed', 'N/A')}"
        )

        panel = Panel(info_text, title="Flink Cluster Overview", border_style="blue", expand=False)
        console.print(panel)

    except Exception as e:
        error_console.print(f"Failed to get cluster info: {e}")

def list_jobs() -> None:
    """Lists all jobs on the Flink cluster using Rich Table."""
    try:
        jobs_data: Dict[str, List[Dict[str, Any]]] = _make_request("/jobs/overview")
        jobs: List[Dict[str, Any]] = jobs_data.get('jobs', [])

        if not jobs:
            console.print("[yellow]No jobs found on the cluster.[/yellow]")
            return

        table = Table(title="Flink Jobs", show_header=True, header_style="bold magenta")
        table.add_column("JOB ID", style="dim", width=35)
        table.add_column("NAME", width=30)
        table.add_column("STATE", width=15)
        table.add_column("START TIME", width=25)
        table.add_column("DURATION (ms)", width=15, justify="right")

        pd = None
        try:
            import pandas as pd # Import locally for optional dependency
        except ImportError:
            pass # Will display timestamp as epoch ms

        for job in jobs:
            job_id = job.get('jid', 'N/A')
            name = job.get('name', 'N/A')
            state = job.get('state', 'N/A')
            duration = str(job.get('duration', 'N/A'))
            start_time_ms = job.get('start-time', -1)

            start_time_str = f"{start_time_ms}"
            if start_time_ms > 0:
                if pd:
                    try:
                        start_time_str = str(pd.to_datetime(start_time_ms, unit='ms'))
                    except Exception:
                        start_time_str = f"{start_time_ms} (epoch ms)"
                else:
                     start_time_str = f"{start_time_ms} (epoch ms)"
            else:
                start_time_str = "N/A"

            # Apply styling based on state
            state_style = ""
            if state == "RUNNING":
                state_style = "green"
            elif state == "FAILED":
                state_style = "red"
            elif state == "CANCELED" or state == "CANCELING":
                state_style = "yellow"
            elif state == "FINISHED":
                state_style = "blue"

            table.add_row(
                job_id,
                name,
                f"[{state_style}]{state}[/{state_style}]",
                start_time_str,
                duration
            )

        console.print(table)

    except ImportError:
         error_console.print("Error: The 'list-jobs' command requires pandas for formatted timestamps.")
         error_console.print("Please install it: pip install pandas")
         exit(1)
    except Exception as e:
        error_console.print(f"Failed to list jobs: {e}")

def submit_job(jar_path: str, entry_class: Optional[str] = None, parallelism: Optional[int] = None, args: Optional[List[str]] = None) -> None:
    """Submits a Flink job JAR to the cluster (less common for PyFlink)."""
    if not os.path.exists(jar_path):
        error_console.print(f"Error: JAR file not found at {jar_path}")
        return

    jar_name: str = os.path.basename(jar_path)
    upload_endpoint: str = "/jars/upload"
    submit_endpoint: str = ""
    jar_id: str = ""

    try:
        # 1. Upload JAR
        console.print(f"Uploading JAR: [cyan]{jar_name}[/cyan]...")
        with open(jar_path, 'rb') as f:
            files: Dict[str, Any] = {'jarfile': (jar_name, f, 'application/x-java-archive')}
            upload_response: Dict[str, Any] = _make_request(upload_endpoint, method="POST", files=files)

        uploaded_jar_name: str = upload_response.get('filename', '')
        if not uploaded_jar_name:
            error_console.print("Failed to upload JAR. Response:", upload_response)
            return
        console.print(f":heavy_check_mark: JAR uploaded successfully: [green]{uploaded_jar_name}[/green]")

        jar_id = uploaded_jar_name.split('/')[-1]
        submit_endpoint = f"/jars/{jar_id}/run"

        # 2. Submit Job
        console.print(f"Submitting job from JAR ID: [cyan]{jar_id}[/cyan]...")
        submit_data: Dict[str, Any] = {}
        if entry_class:
            submit_data['entryClass'] = entry_class
        if parallelism is not None:
            submit_data['parallelism'] = parallelism
        if args:
            submit_data['programArgs'] = ' '.join(args)
            console.print(f"Using programArgs: {submit_data['programArgs']}")

        submit_response: Dict[str, Any] = _make_request(submit_endpoint, method="POST", data=submit_data)
        job_id: Optional[str] = submit_response.get('jobid')
        if job_id:
            console.print(f":rocket: Job submitted successfully! JOB ID: [bold green]{job_id}[/bold green]")
        else:
            error_console.print("Failed to submit job. Response:", submit_response)

    except Exception as e:
        error_console.print(f"Failed to submit job: {e}")
        if jar_id:
            try:
                console.print(f"Attempting to clean up uploaded JAR: {jar_id}")
                _make_request(f"/jars/{jar_id}", method="DELETE")
                console.print(f":wastebasket: Cleaned up JAR {jar_id}.")
            except Exception as delete_e:
                error_console.print(f"Warning: Failed to clean up JAR {jar_id} after submission error: {delete_e}")

def stop_job(job_id: str) -> None:
    """Stops (cancels) a running Flink job."""
    endpoint: str = f"/jobs/{job_id}"
    console.print(f"Attempting to stop job: [cyan]{job_id}[/cyan]...")
    try:
        _make_request(endpoint, method="PATCH", data={"mode": "cancel"})
        console.print(f":stop_button: Stop request sent for job {job_id}. Check job status or Flink UI to confirm cancellation.")
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            error_console.print(f"Error: Job with ID '{job_id}' not found.")
        else:
            error_console.print(f"Failed to stop job {job_id}: {e}")
    except Exception as e:
        error_console.print(f"Failed to stop job {job_id}: {e}")

def generate_pyflink_run_command(
    py_script: Optional[str] = None,
    py_module: Optional[str] = None,
    py_files: Optional[str] = None,
    parallelism: Optional[int] = None,
) -> None:
    """Generates and prints the docker compose command to run a PyFlink job."""
    if not py_script and not py_module:
        error_console.print("Error: Either --python (-py) or --pyModule (-pym) must be specified.")
        return
    if py_script and py_module:
        error_console.print("Error: Specify either --python (-py) or --pyModule (-pym), not both.")
        return

    base_cmd = "docker compose -f pyflink/docker-compose.yml exec jobmanager ./bin/flink run"

    args = []
    if parallelism is not None:
        args.append(f"-p {parallelism}")
    if py_script:
        # Assume script path is relative to project root, map to /app
        app_path = os.path.join("/app", py_script.lstrip('./'))
        args.append(f"--python {app_path}")
    if py_module:
        args.append(f"--pyModule {py_module}")
    if py_files:
        # Assume files are relative to project root, map to /app
        app_file_paths = [
            os.path.join("/app", f.strip().lstrip('./')) for f in py_files.split(',')
        ]
        args.append(f"--pyFiles {','.join(app_file_paths)}")

    full_command = f"{base_cmd} {' '.join(args)}"

    console.print(Panel(
        f"Run the following command in your terminal (from the project root):\n\n[bold cyan]{full_command}[/bold cyan]",
        title="PyFlink Run Command",
        border_style="green"
    ))

    

def main() -> None:
    # Declare FLINK_REST_URL as global at the start of the function
    global FLINK_REST_URL

    parser = argparse.ArgumentParser(
        description="Flink Cluster CLI - Interact with a Flink cluster via its REST API.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--flink-url", default=FLINK_REST_URL, help=f"Flink REST API URL (default: {FLINK_REST_URL})")

    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")

    # Info command
    parser_info = subparsers.add_parser("info", help="Get cluster overview")
    parser_info.set_defaults(func=lambda args: get_cluster_info())

    # List jobs command
    parser_list = subparsers.add_parser("list-jobs", help="List jobs and their status")
    parser_list.set_defaults(func=lambda args: list_jobs())

    # Submit job command
    parser_submit = subparsers.add_parser("submit-job", help="Upload a JAR and submit it as a job (less common for PyFlink)")
    parser_submit.add_argument("jar_path", help="Path to the Flink job JAR file")
    parser_submit.add_argument("-e", "--entry-class", help="Fully qualified name of the job's entry point class (often needed for Java/Scala)")
    parser_submit.add_argument("-p", "--parallelism", type=int, help="Job parallelism (optional)")
    parser_submit.add_argument("-a", "--args", nargs='*', help="Arguments for the Flink job (passed as a single space-separated string)")
    parser_submit.set_defaults(func=lambda args: submit_job(args.jar_path, args.entry_class, args.parallelism, args.args))

    # Stop job command
    parser_stop = subparsers.add_parser("stop-job", help="Stop (cancel) a running job by ID")
    parser_stop.add_argument("job_id", help="ID of the job to stop")
    parser_stop.set_defaults(func=lambda args: stop_job(args.job_id))

    # Run PyFlink job command (generates docker command)
    parser_pyflink = subparsers.add_parser("run-pyflink", help="Generate the command to run a PyFlink job via docker exec")
    parser_pyflink.add_argument("-py", "--python", help="Path to the main Python script (relative to project root)")
    parser_pyflink.add_argument("-pym", "--pyModule", help="Name of the Python module to run")
    parser_pyflink.add_argument("-pyfs", "--pyFiles", help="Comma-separated list of additional Python files (relative to project root)")
    parser_pyflink.add_argument("-p", "--parallelism", type=int, help="Job parallelism (optional)")
    parser_pyflink.set_defaults(func=lambda args: generate_pyflink_run_command(args.python, args.pyModule, args.pyFiles, args.parallelism))

    try:
        args = parser.parse_args()

        # Update Flink URL if provided via argument (global declared above)
        FLINK_REST_URL = args.flink_url

        # Execute the function associated with the chosen subcommand
        args.func(args)

    except ImportError as e:
        if 'pandas' in str(e):
            error_console.print("Error: The 'list-jobs' command requires pandas for formatted timestamps.")
            error_console.print("Please install it: pip install pandas")
            exit(1)
        else:
            error_console.print(f"An unexpected import error occurred: {e}")
            exit(1)
    except Exception as e:
        error_console.print(f"An unexpected error occurred in main execution: {e}")
        exit(1)

if __name__ == "__main__":
    main() 