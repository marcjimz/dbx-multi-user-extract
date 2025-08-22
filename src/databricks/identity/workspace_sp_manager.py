import requests
import json
import time
from typing import Dict, Any, List
from datetime import datetime
import pandas as pd
from pyspark.sql.connect.session import SparkSession

class WorkspaceServicePrincipalManager:
    """Manages workspace-scoped service principals and Unity Catalog groups"""
    
    def __init__(self, workspace_url: str, token: str, spark_session):
        self.workspace_url = workspace_url
        self.spark = spark_session  # Store spark session
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/scim+json'
        }
    
    def create_service_principal(self, display_name: str, app_id: str = None) -> Dict[str, Any]:
        """Create a workspace-scoped service principal"""
        
        # Generate a unique application ID if not provided
        if not app_id:
            import uuid
            app_id = str(uuid.uuid4())
        
        payload = {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"],
            "applicationId": app_id,
            "displayName": display_name,
            "active": True,
            "entitlements": [
                {
                    "value": "allow-cluster-create"
                },
                {
                    "value": "databricks-sql-access"
                }
            ]
        }
        
        try:
            response = requests.post(
                f"{self.workspace_url}/api/2.0/preview/scim/v2/ServicePrincipals",
                headers=self.headers,
                json=payload
            )
            
            if response.status_code == 409:
                # Service principal already exists, fetch it
                return self.get_service_principal_by_name(display_name)
            
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error creating service principal: {e}")
            # Try to get existing if creation failed
            return self.get_service_principal_by_name(display_name)
    
    def get_service_principal_by_name(self, display_name: str) -> Dict[str, Any]:
        """Get service principal by display name"""
        response = requests.get(
            f"{self.workspace_url}/api/2.0/preview/scim/v2/ServicePrincipals?filter=displayName eq '{display_name}'",
            headers=self.headers
        )
        response.raise_for_status()
        resources = response.json().get('Resources', [])
        if resources:
            return resources[0]
        return None
    
    def get_or_create_service_principal(self, display_name: str, role_description: str = None) -> Dict[str, Any]:
        """Get existing service principal or create if it doesn't exist"""
        import uuid
        
        # Check if SP exists
        sp = self.get_service_principal_by_name(display_name)
        
        if not sp:
            # Create new SP
            sp = self.create_service_principal(display_name, str(uuid.uuid4()))
            if sp:
                print(f"✅ Created Service Principal: {sp.get('displayName')} - ID: {sp.get('id')}")
            else:
                print(f"❌ Failed to create Service Principal: {display_name}")
                return None
        else:
            print(f"✅ Found existing Service Principal: {sp.get('displayName')} - ID: {sp.get('id')}")
        
        if sp and role_description:
            print(f"   Role: {role_description}")
        
        return sp
    
    def create_uc_group(self, group_name: str) -> bool:
        """Create a Unity Catalog group using SQL"""
        try:
            # Unity Catalog CREATE GROUP syntax - no backticks, use identifier directly
            # Replace hyphens with underscores for SQL identifier compatibility
            self.spark.sql(f"CREATE GROUP {group_name}")
            return True
        except Exception as e:
            error_msg = str(e).lower()
            if "already exists" in error_msg:
                return True
            print(f"Error creating UC group {group_name}: {e}")
            return False
    
    def get_uc_group(self, group_name: str) -> bool:
        """Check if Unity Catalog group exists"""
        try:
            # Try to list all groups and check if our group is in the list
            all_groups_df = self.spark.sql("SHOW GROUPS")
            groups_list = [row['principal'] for row in all_groups_df.collect()]
            # Check both the original name and the safe name
            return group_name in groups_list
        except Exception as e:
            # If we can't list groups, assume it doesn't exist
            print(f"Warning: Could not list groups to check existence: {e}")
            return False
    
    def get_or_create_uc_group(self, group_name: str) -> Dict[str, Any]:
        """Get existing Unity Catalog group or create if it doesn't exist"""
        
        # Check if group exists in Unity Catalog
        group_exists = self.get_uc_group(group_name)
        
        if not group_exists:
            # Create new UC group
            success = self.create_uc_group(group_name)
            if success:
                print(f"✅ Created Unity Catalog Group: {group_name}")
                return {"displayName": group_name, "id": group_name}
            else:
                print(f"❌ Failed to create Unity Catalog Group: {group_name}")
                return None
        else:
            print(f"✅ Found existing Unity Catalog Group: {group_name}")
            return {"displayName": group_name, "id": group_name}
    
    def add_member_to_uc_group(self, group_name: str, member_sp: Dict[str, Any]) -> bool:
        """Add a service principal to a Unity Catalog group"""
        try:
             
            # First ensure the group actually exists
            if not self.get_uc_group(group_name):
                print(f"⚠️  Group {group_name} does not exist, creating it first...")
                if not self.create_uc_group(group_name):
                    print(f"❌ Failed to create group {group_name}")
                    return False
            
            # IMPORTANT: Use the display name, not the application ID for Unity Catalog
            sp_identifier = member_sp.get('displayName')
            
            if not sp_identifier:
                print(f"❌ No display name found for service principal")
                return False
            
            # First, ensure the service principal exists in Unity Catalog
            # Try to create/register the SP in Unity Catalog
            try:
                # Check if SP exists in Unity Catalog
                check_sp_sql = f"DESCRIBE USER `{sp_identifier}`"
                self.spark.sql(check_sp_sql)
            except:
                # SP doesn't exist in UC, try alternative approaches
                print(f"Service principal {sp_identifier} not found in Unity Catalog")
                
                # Alternative 1: Try using the application ID with specific format
                app_id = member_sp.get('applicationId')
                if app_id:
                    try:
                        # Some environments need the SP in this format
                        sp_identifier = f"{app_id}@databricks.com"
                        check_sp_sql = f"DESCRIBE USER `{sp_identifier}`"
                        self.spark.sql(check_sp_sql)
                    except:
                        # Still not found, use display name
                        sp_identifier = member_sp.get('displayName')
            
            # Add SP to UC group using the correct identifier
            alter_group_sql = f"ALTER GROUP {group_name} ADD USER `{sp_identifier}`"
            self.spark.sql(alter_group_sql)
            return True
            
        except Exception as e:
            error_msg = str(e).lower()
            if "already a member" in error_msg or "already exists" in error_msg:
                print(f"ℹ️  {sp_identifier} is already a member of {group_name}")
                return True
            else:
                # Real error - print it and return False
                print(f"Error adding {sp_identifier} to UC group {group_name}: {e}")
                return False
    