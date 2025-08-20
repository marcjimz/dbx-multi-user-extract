import requests
import json
import time
from typing import Dict, Any, List
from datetime import datetime
import pandas as pd
from pyspark.sql.connect.session import SparkSession

class ServerlessJobManager:
    """Manages Databricks Jobs with Serverless Compute via REST API"""
    
    def __init__(self, workspace_url: str, token: str):
        self.workspace_url = workspace_url
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        self.default_token = token
    
    def set_auth_token(self, token: str):
        """Update the authorization token"""
        self.headers['Authorization'] = f'Bearer {token}'
    
    def reset_auth_token(self):
        """Reset to the original token"""
        self.headers['Authorization'] = f'Bearer {self.default_token}'
    
    def get_job_by_name(self, job_name: str) -> Dict[str, Any]:
        """Get a job by its name"""
        try:
            response = requests.get(
                f"{self.workspace_url}/api/2.1/jobs/list",
                headers=self.headers,
                params={"limit": 100, "expand_tasks": True}
            )
            response.raise_for_status()
            
            jobs = response.json().get('jobs', [])
            for job in jobs:
                if job.get('settings', {}).get('name') == job_name:
                    return {'job_id': job['job_id'], 'settings': job['settings']}
            return None
        except Exception as e:
            print(f"Error searching for job: {e}")
            return None
    
    def create_or_get_serverless_job(self, job_name: str, notebook_path: str, 
                                    catalog: str, schema: str,
                                    run_as_sp_id: str = None, description: str = None) -> Dict[str, Any]:
        """Create a serverless job or return existing job"""
        
        # Check if job already exists
        existing_job = self.get_job_by_name(job_name)
        if existing_job:
            print(f"   ℹ️ Job already exists: {job_name}")
            print(f"   Using existing job ID: {existing_job['job_id']}")
            return existing_job
        
        # Create new job
        job_config = {
            "name": job_name,
            "description": description or f"Serverless job: {job_name}",
            "max_concurrent_runs": 1,
            "timeout_seconds": 3600,
            "tasks": [
                {
                    "task_key": "process_data",
                    "description": "Process regional data based on permissions",
                    "notebook_task": {
                        "notebook_path": notebook_path,
                        "base_parameters": {
                            "catalog": catalog,
                            "schema": schema
                        },
                        "source": "WORKSPACE"
                    },
                    "timeout_seconds": 1800,
                }
            ],
            "format": "MULTI_TASK",
            "queue": {
                "enabled": True
            }
        }
        
        if run_as_sp_id:
            job_config["run_as"] = {
                "service_principal_name": run_as_sp_id
            }
        
        try:
            response = requests.post(
                f"{self.workspace_url}/api/2.1/jobs/create",
                headers=self.headers,
                json=job_config
            )
            
            if response.status_code not in [200, 201]:
                error_text = response.text.lower()
                if 'already exists' in error_text or 'duplicate' in error_text:
                    print(f"   ℹ️ Job name conflict, fetching existing...")
                    existing_job = self.get_job_by_name(job_name)
                    if existing_job:
                        return existing_job
                
                print(f"❌ HTTP Error {response.status_code}")
                print(f"Response: {response.text}")
                raise requests.exceptions.HTTPError(f"Job creation failed")
            
            print(f"   ✅ Created new job: {job_name}")
            return response.json()
            
        except Exception as e:
            print(f"❌ Error creating job: {e}")
            raise
    
    def run_job_now(self, job_id: int) -> Dict[str, Any]:
        """Trigger a job to run immediately"""
        
        payload = {
            "job_id": job_id
        }
        
        try:
            response = requests.post(
                f"{self.workspace_url}/api/2.1/jobs/run-now",
                headers=self.headers,
                json=payload
            )
            
            if response.status_code not in [200, 201]:
                print(f"❌ Failed to trigger job {job_id}: HTTP {response.status_code}")
                print(f"   Response: {response.text}")
                return None
            
            run_data = response.json()
            return run_data
            
        except Exception as e:
            print(f"❌ Error triggering job {job_id}: {e}")
            return None
    
    def get_run_status(self, run_id: int) -> Dict[str, Any]:
        """Get the status of a job run"""
        
        try:
            response = requests.get(
                f"{self.workspace_url}/api/2.1/jobs/runs/get?run_id={run_id}",
                headers=self.headers
            )
            
            if response.status_code != 200:
                return None
            
            return response.json()
            
        except Exception as e:
            print(f"Error getting run status: {e}")
            return None
    
    def get_task_run_output(self, run_id: int, task_key: str = "process_data") -> Dict[str, Any]:
        """Get the output of a specific task within a job run"""
        
        try:
            # For multi-task jobs, we need to get the task run output, not the job run output
            response = requests.get(
                f"{self.workspace_url}/api/2.1/jobs/runs/get",
                headers=self.headers,
                params={"run_id": run_id}
            )
            
            if response.status_code != 200:
                print(f"   Failed to get run details - HTTP {response.status_code}")
                return None
            
            run_data = response.json()
            
            # Find the task run
            tasks = run_data.get('tasks', [])
            for task in tasks:
                if task.get('task_key') == task_key:
                    # Get the task run ID
                    task_run_id = task.get('run_id')
                    if task_run_id:
                        # Now get the output for this specific task
                        output_response = requests.get(
                            f"{self.workspace_url}/api/2.1/jobs/runs/get-output",
                            headers=self.headers,
                            params={"run_id": task_run_id}
                        )
                        
                        if output_response.status_code == 200:
                            return output_response.json()
                        else:
                            print(f"   Failed to get task output - HTTP {output_response.status_code}")
                            if output_response.status_code == 400:
                                print(f"   Response: {output_response.text[:200]}")
            
            return None
            
        except Exception as e:
            print(f"   Error getting task output: {e}")
            return None
    
    def get_run_output(self, run_id: int) -> Dict[str, Any]:
        """Get the output of a completed job run (handles both single and multi-task jobs)"""
        
        try:
            # First try to get output directly (works for single-task jobs)
            response = requests.get(
                f"{self.workspace_url}/api/2.1/jobs/runs/get-output?run_id={run_id}",
                headers=self.headers
            )
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 400:
                # Check if it's a multi-task error
                error_response = response.json()
                if "multiple tasks" in error_response.get('message', '').lower():
                    # Try to get the task output instead
                    print(f"   Multi-task job detected, getting task output...")
                    return self.get_task_run_output(run_id, "process_data")
                else:
                    print(f"   Failed to get output - HTTP {response.status_code}")
                    print(f"   Response: {response.text[:200]}")
            
            return None
            
        except Exception as e:
            print(f"   Error getting run output: {e}")
            return None
    
    def wait_for_run_completion(self, run_id: int, max_wait_seconds: int = 300, poll_interval: int = 10) -> str:
        """Wait for a job run to complete and return final status"""
        
        import time
        start_time = time.time()
        
        while time.time() - start_time < max_wait_seconds:
            status_response = self.get_run_status(run_id)
            
            if status_response:
                life_cycle_state = status_response.get('state', {}).get('life_cycle_state', 'UNKNOWN')
                result_state = status_response.get('state', {}).get('result_state', '')
                
                # Terminal states
                if life_cycle_state in ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']:
                    return result_state if result_state else life_cycle_state
                
                # Still running
                print(f"   Status: {life_cycle_state}", end='\r')
            
            time.sleep(poll_interval)
        
        return 'TIMEOUT'

