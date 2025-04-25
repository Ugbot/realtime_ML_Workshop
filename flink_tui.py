#!/usr/bin/env python
import os
import json
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

import requests

# Textual Imports
from rich.panel import Panel
from rich.syntax import Syntax
from rich.text import Text
from textual import work
from textual.app import App, ComposeResult
from textual.containers import Container, VerticalScroll
from textual.reactive import var
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    Log,
    Static,
    TabbedContent,
    TabPane,
)

# --- API Interaction Logic (Adapted from flink_cli.py) --- 

FLINK_REST_URL: str = os.environ.get("FLINK_REST_URL", "http://localhost:8088")

# Global flag to check pandas availability
PANDAS_AVAILABLE = False
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    pass

def _make_request(endpoint: str, method: str = "GET", data: Optional[Dict[str, Any]] = None, files: Optional[Dict[str, Any]] = None) -> Union[Dict[str, Any], str]:
    """Helper function to make requests. Returns dict or error string."""
    url: str = f"{FLINK_REST_URL}{endpoint}"
    try:
        if method == "GET":
            response = requests.get(url, timeout=10)
        elif method == "POST":
            response = requests.post(url, json=data, files=files, timeout=10)
        elif method == "PATCH":
            response = requests.patch(url, json=data, timeout=10)
        elif method == "DELETE":
             response = requests.delete(url, timeout=10)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        response.raise_for_status() 
        if response.content:
            return response.json()
        else:
            return {} # Return empty dict for successful calls with no body (e.g., stop)
    except requests.exceptions.Timeout:
        return f"Error: Request timed out contacting {url}"
    except requests.exceptions.ConnectionError:
        return f"Error: Could not connect to Flink REST API at {url}. Is it running?"
    except requests.exceptions.RequestException as e:
        error_detail = str(e)
        if e.response is not None:
            try:
                # Try to get more specific Flink error message
                error_json = e.response.json()
                if 'errors' in error_json and error_json['errors']:
                   error_detail = ", ".join(error_json['errors'])
            except (json.JSONDecodeError, AttributeError):
                pass # Stick with the default exception string
            return f"Error: API request failed ({e.response.status_code}): {error_detail}"
        return f"Error: API request failed: {error_detail}"
    except json.JSONDecodeError as e:
        return f"Error: Could not decode JSON response from {url}. {e}"
    except Exception as e:
        return f"An unexpected error occurred: {e}"

def get_cluster_info() -> Union[Dict[str, Any], str]:
    """Gets cluster overview."""
    return _make_request("/overview")

def get_jobs_overview() -> Union[Dict[str, Any], str]:
    """Gets job overview."""
    return _make_request("/jobs/overview")

def stop_flink_job(job_id: str) -> Union[Dict[str, Any], str]:
    """Sends request to stop a job."""
    return _make_request(f"/jobs/{job_id}", method="PATCH", data={"mode": "cancel"})

def submit_flink_jar(jar_path: str, entry_class: Optional[str] = None, parallelism: Optional[int] = None, args_list: Optional[List[str]] = None) -> Union[Dict[str, Any], str]:
    """Uploads and runs a JAR job."""
    if not os.path.exists(jar_path):
        return f"Error: JAR file not found at {jar_path}"

    jar_name = os.path.basename(jar_path)
    upload_endpoint = "/jars/upload"
    jar_id = ""

    try:
        # 1. Upload JAR
        with open(jar_path, 'rb') as f:
            files = {'jarfile': (jar_name, f, 'application/x-java-archive')}
            upload_response = _make_request(upload_endpoint, method="POST", files=files)
            if isinstance(upload_response, str): # Check if upload failed
                 return f"JAR Upload failed: {upload_response}"

        uploaded_jar_name = upload_response.get('filename', '')
        if not uploaded_jar_name:
            return f"Failed to upload JAR. Response: {upload_response}"

        jar_id = uploaded_jar_name.split('/')[-1]
        submit_endpoint = f"/jars/{jar_id}/run"

        # 2. Submit Job
        submit_data: Dict[str, Any] = {}
        if entry_class:
            submit_data['entryClass'] = entry_class
        if parallelism is not None:
            submit_data['parallelism'] = parallelism
        if args_list:
            submit_data['programArgs'] = ' '.join(args_list)

        submit_response = _make_request(submit_endpoint, method="POST", data=submit_data)
        return submit_response # Return success dict or error string

    except Exception as e:
        # Attempt to clean up if upload succeeded but submission failed
        if jar_id:
            _make_request(f"/jars/{jar_id}", method="DELETE") # Best effort cleanup
        return f"Failed during JAR submission process: {e}"

# --- Textual UI Components --- 

class ClusterInfo(Static):
    """Displays Flink cluster information."""
    info = var[Union[Dict[str, Any], str]]({}, init=False)

    def render(self) -> Panel:
        content = "Loading..."
        if isinstance(self.info, str):
             content = f"[bold red]Error:[/bold red]\n{self.info}"
        elif isinstance(self.info, dict) and self.info:
            content = (
                f"[bold cyan]Flink Version:[/bold cyan] {self.info.get('flink-version', 'N/A')}\n"
                f"[bold]Task Managers:[/bold] {self.info.get('taskmanagers', 'N/A')}\n"
                f"[bold]Slots Total:[/bold] {self.info.get('slots-total', 'N/A')}\n"
                f"[bold]Slots Available:[/bold] {self.info.get('slots-available', 'N/A')}\n\n"
                f"[green]Jobs Running:[/green] {self.info.get('jobs-running', 'N/A')}\n"
                f"[blue]Jobs Finished:[/blue] {self.info.get('jobs-finished', 'N/A')}\n"
                f"[yellow]Jobs Cancelled:[/yellow] {self.info.get('jobs-cancelled', 'N/A')}\n"
                f"[red]Jobs Failed:[/red] {self.info.get('jobs-failed', 'N/A')}"
            )
        elif not self.info: # Initial state
            content = "Press 'r' to refresh."
        return Panel(content, title="Cluster Overview", border_style="blue", expand=False)

    @work(exclusive=True, thread=True)
    def update_info(self) -> None:
        self.info = get_cluster_info()

class JobList(Container):
    """Displays Flink jobs in a table."""
    jobs = var[Union[List[Dict[str, Any]], str]]([], init=False)

    def compose(self) -> ComposeResult:
        yield DataTable(id="job_table")

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.cursor_type = "row"
        table.add_column("JOB ID", key="jid", width=33)
        table.add_column("NAME", key="name", width=30)
        table.add_column("STATE", key="state", width=12)
        table.add_column("START TIME", key="start", width=25)
        table.add_column("DURATION (s)", key="duration", width=15, ) # numeric=True not ideal here
        self.update_jobs()

    def _format_timestamp(self, ts_ms: int) -> str:
        if ts_ms <= 0:
            return "N/A"
        if PANDAS_AVAILABLE:
            try:
                # Use Pandas for robust parsing if available
                return str(pd.to_datetime(ts_ms, unit='ms'))
            except Exception:
                 pass # Fallback if pandas fails
        # Basic datetime formatting as fallback
        try:
            dt_object = datetime.fromtimestamp(ts_ms / 1000)
            return dt_object.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return f"{ts_ms} (epoch ms)"

    def _format_duration(self, duration_ms: int) -> str:
        if duration_ms < 0:
            return "N/A"
        return f"{duration_ms / 1000:.1f}" # Duration in seconds

    def watch_jobs(self, old_jobs, new_jobs: Union[List[Dict[str, Any]], str]) -> None:
        table = self.query_one(DataTable)
        table.clear()
        if isinstance(new_jobs, str):
            self.app.log.error(f"Failed to load jobs: {new_jobs}")
            table.add_row(Text(f"Error loading jobs: {new_jobs}", style="bold red"))
        elif isinstance(new_jobs, list):
            if not new_jobs:
                 table.add_row(Text("No jobs found on cluster.", style="italic yellow"))
            else:
                for job in new_jobs:
                    state = job.get('state', 'N/A')
                    state_style = ""
                    if state == "RUNNING": state_style = "green"
                    elif state == "FAILED": state_style = "red"
                    elif state in ("CANCELED", "CANCELING"): state_style = "yellow"
                    elif state == "FINISHED": state_style = "blue"
                    elif state == "RESTARTING": state_style = "cyan"
                    elif state == "SUSPENDED": state_style = "magenta"

                    table.add_row(
                        job.get('jid', 'N/A'),
                        job.get('name', 'N/A'),
                        Text(state, style=state_style),
                        self._format_timestamp(job.get('start-time', -1)),
                        self._format_duration(job.get('duration', -1)),
                        key=job.get('jid')
                    )

    @work(exclusive=True, thread=True)
    def update_jobs(self) -> None:
        jobs_response = get_jobs_overview()
        if isinstance(jobs_response, dict):
            self.jobs = jobs_response.get('jobs', [])
        else:
            self.jobs = jobs_response # Store the error string

class SubmitPane(Container):
    """Pane for submitting JAR jobs."""
    def compose(self) -> ComposeResult:
        yield Label("Submit Flink JAR Job (Experimental)")
        yield Input(placeholder="/path/to/your/job.jar", id="jar_path")
        yield Input(placeholder="Entry Class (optional for PyFlink, required for Java/Scala)", id="entry_class")
        yield Input(placeholder="Parallelism (optional, e.g., 2)", id="parallelism")
        yield Input(placeholder="Job Arguments (optional, space-separated)", id="job_args")
        yield Button("Submit Job", id="submit_button", variant="primary")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "submit_button":
            jar_path = self.query_one("#jar_path", Input).value
            entry_class = self.query_one("#entry_class", Input).value or None
            parallelism_str = self.query_one("#parallelism", Input).value
            job_args = self.query_one("#job_args", Input).value

            parallelism = None
            if parallelism_str:
                try:
                    parallelism = int(parallelism_str)
                except ValueError:
                    self.app.log.error("Invalid parallelism value. Must be an integer.")
                    return

            args_list = job_args.split() if job_args else None

            if not jar_path:
                 self.app.log.error("JAR Path is required.")
                 return

            self.app.log.info(f"Submitting job: {jar_path}...")
            self.app.trigger_jar_submission(jar_path, entry_class, parallelism, args_list)

# --- Main App --- 

class FlinkTUI(App):
    """A Textual app to manage Flink clusters."""

    CSS_PATH = "flink_tui.css"
    BINDINGS = [
        ("q", "quit", "Quit"),
        ("r", "refresh", "Refresh Info/Jobs"),
        ("s", "stop_job", "Stop Selected Job"),
    ]

    def compose(self) -> ComposeResult:
        yield Header()
        with TabbedContent(initial="jobs_tab"):
            with TabPane("Cluster Info", id="info_tab"):
                yield ClusterInfo(id="cluster_info")
            with TabPane("Jobs", id="jobs_tab"):
                 yield JobList(id="job_list")
            with TabPane("Submit JAR", id="submit_tab"):
                 yield SubmitPane(id="submit_pane")
        yield VerticalScroll(Log(id="log_area", highlight=True, max_lines=200))
        yield Footer()

    def on_mount(self) -> None:
        self.query_one(ClusterInfo).update_info()
        self.query_one(JobList).update_jobs()
        self.log.info(f"Connected to Flink: {FLINK_REST_URL}")
        if not PANDAS_AVAILABLE:
            self.log.warning("Pandas not found. Job timestamps will be shown as epoch milliseconds.")

    def add_log(self, message: str, level: str = "info") -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_widget = self.query_one(Log)
        if level == "info":
            log_widget.write_line(f"{timestamp} [INFO] {message}")
        elif level == "warning":
            log_widget.write_line(f"{timestamp} [WARNING] {message}")
        elif level == "error":
             log_widget.write_line(f"{timestamp} [ERROR] {message}")
        else:
             log_widget.write_line(f"{timestamp} {message}")

    # Override default log handler to use our widget
    def log(self, *objects: object, **kwargs: object) -> None:
        self.add_log(str(objects[0]))

    # --- Actions --- 

    def action_refresh(self) -> None:
        """Called when the user presses R."""
        self.log.info("Refreshing data...")
        self.query_one(ClusterInfo).update_info()
        self.query_one(JobList).update_jobs()

    def action_stop_job(self) -> None:
        """Called when the user presses S."""
        job_table = self.query_one("#job_table", DataTable)
        if not job_table.row_count:
             self.log.warning("No jobs to stop.")
             return
        try:
            cursor_row = job_table.cursor_row
            job_id_to_stop = job_table.get_row_at(cursor_row)[0]
            if job_id_to_stop:
                self.log.warning(f"Attempting to stop job {job_id_to_stop}...")
                self.trigger_job_stop(job_id_to_stop)
            else:
                 self.log.error("Could not determine Job ID for selected row.")
        except Exception as e:
            self.log.error(f"Error getting selected job ID: {e}")

    @work(exclusive=True, thread=True)
    def trigger_job_stop(self, job_id: str) -> None:
        result = stop_flink_job(job_id)
        if isinstance(result, str): # Error occurred
             self.log.error(f"Failed to stop job {job_id}: {result}")
        else:
            self.log.info(f"Stop request sent for job {job_id}. Refreshing job list...")
            self.query_one(JobList).update_jobs() # Refresh job list after stopping

    @work(exclusive=True, thread=True)
    def trigger_jar_submission(self, jar_path: str, entry_class: Optional[str], parallelism: Optional[int], args_list: Optional[List[str]]) -> None:
         result = submit_flink_jar(jar_path, entry_class, parallelism, args_list)
         if isinstance(result, str): # Error occurred
              self.log.error(f"JAR Submission failed: {result}")
         elif isinstance(result, dict) and result.get('jobid'):
              job_id = result.get('jobid')
              self.log.info(f"Job submitted successfully! JOB ID: {job_id}")
              # Switch to job tab and refresh
              self.query_one(TabbedContent).active = "jobs_tab"
              self.query_one(JobList).update_jobs()
         else:
              self.log.error(f"JAR Submission failed with unexpected response: {result}")


if __name__ == "__main__":
    # Basic CSS (can be moved to flink_tui.css)
    FlinkTUI.CSS = """
    Screen {
        layout: vertical;
    }
    Header {
        dock: top;
    }
    Footer {
        dock: bottom;
    }
    TabbedContent {
        height: 70%; 
    }
    VerticalScroll#log_area_container { 
        height: 30%; 
    }
    Log {
         border: round white;
         height: 100%;
    }
    DataTable {
        height: 100%;
        border: round white;
    }
    ClusterInfo {
         height: 100%;
    }
    SubmitPane {
        padding: 1;
        border: round white;
        height: 100%;
        grid-size: 2;
        grid-gutter: 1 2;
    }
    SubmitPane > Label {
        column-span: 2;
        width: 100%;
        text-align: center;
        margin-bottom: 1;
    }
    SubmitPane > Input {
        margin-bottom: 1;
        column-span: 2;
    }
    SubmitPane > Button {
         column-span: 2;
         width: 100%;
    }
    """
    app = FlinkTUI()
    app.run() 