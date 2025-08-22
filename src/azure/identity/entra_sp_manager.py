import requests
import json
import time
from typing import Dict, Any, List
from datetime import datetime
import pandas as pd
from pyspark.sql.connect.session import SparkSession

class EntraIDServicePrincipalManager:
    """Manages Entra ID Service Principal authentication and registration in Databricks"""
    
    def __init__(self, workspace_url: str, admin_token: str):
        self.workspace_url = workspace_url
        self.admin_headers = {
            'Authorization': f'Bearer {admin_token}',
            'Content-Type': 'application/json'
        }
    
    def get_oauth_token(self, tenant_id: str, client_id: str, client_secret: str) -> str:
        """Get OAuth token for Entra ID Service Principal to authenticate with Databricks"""
        
        # Azure OAuth endpoint
        oauth_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        
        # Databricks resource ID for Azure
        databricks_resource_id = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
        
        payload = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": f"{databricks_resource_id}/.default"
        }
        
        try:
            response = requests.post(oauth_url, data=payload)
            
            if response.status_code != 200:
                print(f"❌ OAuth token request failed with status {response.status_code}")
                print(f"   Response: {response.text}")
                return None
            
            token_data = response.json()
            print(f"✅ Successfully obtained OAuth token for Entra ID SP")
            return token_data['access_token']
            
        except Exception as e:
            print(f"❌ Failed to get OAuth token: {e}")
            return None
    
    def register_sp_in_workspace(self, client_id: str, display_name: str = None) -> Dict[str, Any]:
        """Register an existing Entra ID Service Principal in Databricks workspace"""
        
        if not display_name:
            display_name = "marcin-sp-app-orchestrator"
        
        # Check if SP already exists in workspace
        existing_sp = self.get_sp_by_application_id(client_id)
        if existing_sp:
            print(f"✅ Service Principal already registered in workspace: {existing_sp.get('displayName')}")
            return existing_sp
        
        # Register the Entra ID SP in Databricks workspace
        payload = {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"],
            "applicationId": client_id,
            "displayName": display_name,
            "active": True,
            "entitlements": [
                {
                    "value": "allow-cluster-create"
                },
                {
                    "value": "databricks-sql-access"
                },
                {
                    "value": "workspace-access"
                }
            ]
        }
        
        try:
            response = requests.post(
                f"{self.workspace_url}/api/2.0/preview/scim/v2/ServicePrincipals",
                headers=self.admin_headers,
                json=payload
            )
            
            if response.status_code == 409:
                # Already exists, try to get it
                existing_sp = self.get_sp_by_application_id(client_id)
                if existing_sp:
                    print(f"✅ Service Principal found in workspace: {existing_sp.get('displayName')}")
                    return existing_sp
                else:
                    print(f"⚠️  SP exists but cannot retrieve it")
                    return None
            
            if response.status_code not in [200, 201]:
                print(f"❌ Failed to register SP: HTTP {response.status_code}")
                print(f"   Response: {response.text}")
                return None
            
            sp_data = response.json()
            print(f"✅ Registered Entra ID SP in Databricks workspace: {display_name}")
            print(f"   SP ID in Databricks: {sp_data.get('id')}")
            return sp_data
            
        except Exception as e:
            print(f"❌ Error registering SP in workspace: {e}")
            return None
    
    def get_sp_by_application_id(self, app_id: str) -> Dict[str, Any]:
        """Get service principal by application ID from Databricks"""
        try:
            # Try with filter parameter
            response = requests.get(
                f"{self.workspace_url}/api/2.0/preview/scim/v2/ServicePrincipals",
                headers=self.admin_headers,
                params={"filter": f'applicationId eq "{app_id}"'}
            )
            
            if response.status_code == 200:
                resources = response.json().get('Resources', [])
                if resources:
                    return resources[0]
            
            # If filter doesn't work, try getting all and filtering locally
            response = requests.get(
                f"{self.workspace_url}/api/2.0/preview/scim/v2/ServicePrincipals",
                headers=self.admin_headers
            )
            
            if response.status_code == 200:
                all_sps = response.json().get('Resources', [])
                for sp in all_sps:
                    if sp.get('applicationId') == app_id:
                        return sp
            
            return None
        except Exception as e:
            print(f"Error getting SP: {e}")
            return None
