# Airflow DAG Monitor

A robust monitoring solution for Airflow DAGs that detects failure patterns and provides alerting capabilities using Prefect.

## Overview

This repository contains a monitoring framework that helps teams running Apache Airflow quickly identify problematic DAGs by tracking run history and failure patterns. The system is built with Prefect, providing a modern workflow management approach to monitor your existing Airflow infrastructure.

## Features

- **Health Checks**: Verify Airflow is running properly
- **DAG Status Tracking**: Monitor the state of specific DAGs
- **Failure Pattern Detection**: Automatically identify DAGs with consistent failures
- **Threshold-Based Alerting**: Get notified when DAGs exceed specified failure thresholds
- **Detailed Reporting**: View comprehensive logs of DAG run history

## Requirements

- Python 3.7+
- Apache Airflow
- Prefect 2.0+

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/airflow-dag-monitor.git
cd airflow-dag-monitor

# Install dependencies
pip install -r requirements.txt
```

### Make sure your AIRFLOW_HOME environment variable is properly set:
```bash
export AIRFLOW_HOME=/path/to/your/airflow
```

## Example DAGs
The repository includes an example Airflow DAG for testing:

**test_workflow.py**: A simple DAG with tasks that randomly fail, useful for testing the monitoring system

## How It Works

- **Check Airflow Setup**: First verifies that Airflow is properly configured and running
- **Get DAG Status:** Retrieves status information for a specific DAG including recent runs
- **Analyze DAG Health:** Checks for failure patterns and compares against thresholds
- **Alerting:** Raises exceptions when critical thresholds are exceeded

## Customization
You can customize the failure thresholds by modifying the analyze_dag_health function. By default, it will alert when 3 or more of the last 5 runs have failed.

## Contributing
Contributions are welcome! Please feel free to submit a Pull Request.
