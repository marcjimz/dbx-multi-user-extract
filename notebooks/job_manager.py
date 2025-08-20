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
    
    def grant_permission(self, job_id: int, sp_application_id: str, permission: str) -> Dict[str, Any]:
        """Grant permissions on a job to a service principal"""
        
        payload = {
            "access_control_list": [
                {
                    "service_principal_name": sp_application_id,
                    "permission_level": permission
                }
            ]
        }
        
        try:
            response = requests.patch(
                f"{self.workspace_url}/api/2.0/permissions/jobs/{job_id}",
                headers=self.headers,
                json=payload
            )
            return response.json()
        except Exception as e:
            print(f"Error granting permission: {e}")
            return None
