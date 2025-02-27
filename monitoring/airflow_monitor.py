from prefect import task, flow
import subprocess
from datetime import datetime, timedelta
import os
from typing import Dict, Any, List
import json
from prefect import get_run_logger

class AirflowMonitorError(Exception):
    """Custom exception for Airflow monitoring errors.
    Used to differentiate Airflow-specific errors from other exceptions."""
    pass

@task
def check_airflow_setup() -> bool:
    """Verify Airflow is properly configured and running.
    
    Returns:
        bool: True if Airflow is running and accessible, False otherwise.
    """
    logger = get_run_logger()
    airflow_home = os.environ.get("AIRFLOW_HOME")
    logger.info(f"Current AIRFLOW_HOME: {airflow_home}")
    
    try:
        # Run Airflow health check command
        health_check = subprocess.run(
            ["airflow", "jobs", "check"], 
            capture_output=True, 
            text=True
        )
        return health_check.returncode == 0
    except Exception as e:
        logger.error(f"Airflow health check failed: {str(e)}")
        return False

@task
def get_dag_status(dag_id: str) -> Dict[str, Any]:
    """Get the detailed status of a specific DAG and its recent runs.
    
    Args:
        dag_id (str): The ID of the DAG to check.
        
    Returns:
        Dict[str, Any]: Dictionary containing:
            - current_state: Latest state of the DAG
            - recent_runs: List of up to 5 most recent runs
            - last_run_time: Timestamp of the last run
            - run_id: ID of the most recent run
            
    Raises:
        AirflowMonitorError: If there's any issue getting or parsing DAG status.
    """
    logger = get_run_logger()
    
    try:
        # Construct and execute Airflow command to get DAG runs
        cmd = ["airflow", "dags", "list-runs", "--dag-id", dag_id, "--output", "json"]
        logger.info(f"Executing command: {' '.join(cmd)}")
        
        # Run command and check for errors
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        # Handle empty result case
        if not result.stdout.strip():
            logger.warning("No DAG runs found")
            return {
                "current_state": "No runs",
                "recent_runs": [],
                "last_run_time": None,
                "run_id": None
            }
            
        # Parse JSON output from Airflow
        runs_data = json.loads(result.stdout)
        # Get only the 5 most recent runs for analysis
        recent_runs = runs_data[:5]
        logger.info(f"Found {len(recent_runs)} recent runs")
        
        if recent_runs:
            latest_run = recent_runs[0]
            return {
                "current_state": latest_run["state"],
                "recent_runs": recent_runs,
                "last_run_time": latest_run["end_date"],
                "run_id": latest_run["run_id"]
            }
        else:
            return {
                "current_state": "No runs",
                "recent_runs": [],
                "last_run_time": None,
                "run_id": None
            }
            
    except subprocess.CalledProcessError as e:
        error_msg = f"Failed to execute Airflow command: {e.stderr}"
        logger.error(error_msg)
        raise AirflowMonitorError(error_msg)
    except json.JSONDecodeError as e:
        error_msg = f"Failed to parse Airflow output: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Raw output: {result.stdout[:200]}...")
        raise AirflowMonitorError(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg)
        raise AirflowMonitorError(error_msg)

@task
def analyze_dag_health(status_info: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze DAG health by checking recent run history.
    
    Raises an error if 3 or more of the last 5 runs failed.
    
    Args:
        status_info (Dict[str, Any]): Status information from get_dag_status
        
    Returns:
        Dict[str, Any]: Analysis results containing:
            - issues: List of identified problems
            - severity: Current severity level (info/warning/critical)
            - recent_failures: Count of recent failures
            
    Raises:
        AirflowMonitorError: If failure threshold is exceeded (3+ failures in 5 runs)
    """
    logger = get_run_logger()
    
    # Check if we have any runs to analyze
    if not status_info.get("recent_runs"):
        return {
            "issues": ["No DAG runs found to analyze"],
            "severity": "warning",
            "recent_failures": 0
        }
    
    # Extract and count failures
    recent_runs = status_info["recent_runs"]
    failed_runs = [run for run in recent_runs if run["state"] == "failed"]
    failed_count = len(failed_runs)
    total_runs = len(recent_runs)
    
    # Log status of each run with visual indicators
    logger.info("\nAnalyzing recent DAG runs:")
    for run in recent_runs:
        state_emoji = "❌" if run["state"] == "failed" else "✅"
        logger.info(f"{state_emoji} Run {run['run_id']}: {run['state']}")
    
    # Check if we've exceeded failure threshold (3+ failures)
    if failed_count >= 3:
        # Create visual alert with details
        logger.error("\n" + "="*50)
        logger.error("🚨 CRITICAL ALERT: DAG Failure Threshold Exceeded 🚨")
        logger.error("="*50)
        logger.error(f"\n{failed_count} out of the last {total_runs} runs have failed!")
        logger.error("\nFailed Runs:")
        
        # Log details for each failed run
        for run in failed_runs:
            logger.error(f"\n❌ Run ID: {run['run_id']}")
            logger.error(f"   Start: {run['start_date']}")
            logger.error(f"   End: {run['end_date']}")
        
        error_msg = f"{failed_count} out of the last {total_runs} DAG runs have failed!"
        raise AirflowMonitorError(error_msg)
    
    return {
        "issues": [],
        "severity": "info",
        "recent_failures": failed_count
    }

@flow(name="Airflow Monitor")
def monitor_airflow_dag(dag_id: str = "test_workflow") -> Dict[str, Any]:
    """Monitor an Airflow DAG and check for failure patterns.
    
    This flow will:
    1. Verify Airflow is running
    2. Get the status of recent DAG runs
    3. Analyze run history for failure patterns
    4. Alert if too many failures are detected
    
    Args:
        dag_id (str): The ID of the DAG to monitor
        
    Returns:
        Dict[str, Any]: Monitoring results containing status and health analysis
        
    Raises:
        AirflowMonitorError: If Airflow is down or too many failures are detected
    """
    logger = get_run_logger()
    
    # First, verify Airflow is running
    if not check_airflow_setup():
        raise AirflowMonitorError("Airflow setup check failed - service may be down!")
    
    # Get current DAG status and recent runs
    status = get_dag_status(dag_id)
    
    # Analyze health and check failure threshold
    health_analysis = analyze_dag_health(status)
    
    return {
        "status": status,
        "health_analysis": health_analysis
    }

if __name__ == "__main__":
    monitor_airflow_dag("test_workflow")