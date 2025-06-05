## Prerequisites

Before calling Azure Data Factory (ADF) pipelines from Databricks, ensure you have the following:

1. **Create a Service Principal** in Azure Active Directory.
    Under App Registrations, create a new registration for your application. This will give you a Client ID and Tenant ID.
    - Navigate to Azure Portal > Azure Active Directory > App registrations > New registration. 
    - Fill in the name and redirect URI (optional).
2. **Create a Client Secret** for the Service Principal.
    Under the created App registration, go to "Certificates & secrets" and create a new client secret. Note down the value as it will not be shown again.
3. **Assign the Service Principal** the "Contributor" role on your Azure Data Factory instance.
# Calling Azure Data Factory Pipelines from Databricks
## Prerequisites
Before calling Azure Data Factory (ADF) pipelines from Databricks, ensure you have the following:
- **Azure Data Factory Name** and **Resource Group**
- **Subscription ID**   
- **Azure Active Directory Service Principal** with permissions to trigger pipelines (Contributor role on ADF)
    - **Tenant ID**
    - **Client ID** (Application ID)
    - **Client Secret**

- **Note down the Tenant ID, Client ID, and Client Secret** for the Service Principal.
# Calling Azure Data Factory Pipelines from Databricks
You can call Azure Data Factory pipelines from Databricks using the Azure REST API. Below is a step-by-step guide to authenticate and trigger a pipeline run.

## Step 1: Get Azure AD OAuth 2.0 Token

You first authenticate against Azure AD to get an access token for the REST API.

```python
import requests

tenant_id = "your-tenant-id-here"  # Replace with your actual tenant ID
client_id = "your-client-id-here"  # Replace with your actual client ID
client_secret = "your-client-secret-here"  # Replace with your actual client secret
resource = "https://management.azure.com/"

url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
payload = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "resource": resource
}

response = requests.post(url, data=payload)
access_token = response.json()["access_token"]
print("Token acquired:", access_token is not None)
```

## Step 2: Trigger the ADF Pipeline

Use the Azure REST API to trigger the pipeline run. The example below demonstrates how to send a POST request to start the pipeline:

```python
subscription_id = "your-subscription-id-here"  # Replace with your actual subscription ID
resource_group = "your-resource-group-name-here"  # Replace with your actual resource group name
data_factory_name = "your-data-factory-name-here"  # Replace with your actual Data Factory name
pipeline_name = "your-pipeline-name-here"  # Replace with your actual pipeline name

url = (
    f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/"
    f"{resource_group}/providers/Microsoft.DataFactory/factories/"
    f"{data_factory_name}/pipelines/{pipeline_name}/createRun?api-version=2018-06-01"
)

headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}

# Optional: pass parameters to pipeline
payload = {
    # 'param1': 'value1',
    # 'param2': 'value2'
}

response = requests.post(url, headers=headers, json=payload)
print(response.status_code)
print(response.json())
```

This will initiate the pipeline run in Azure Data Factory. You can use the returned `runId` to monitor the pipeline execution status.