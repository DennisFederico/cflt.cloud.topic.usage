# TODO: Confluent Cloud Topic Usage Console - Future Features

## Grouping Clusters by Environment

Since environment IDs and names are not exposed in Confluent Cloud Telemetry/Metrics API responses, we will implement environment grouping as a user-configured metadata layer in the UI and database.

### Proposed Solution
1. **Database Schema Update (`clusters_config.json`)**:
   Add `environment_id` and `environment_name` fields to each cluster record:
   ```json
   {
     "clusters": {
       "lkc-xxxxxx": {
         "name": "Production Cluster",
         "environment_id": "env-production",
         "environment_name": "Production",
         "kafka_api_endpoint": "https://...",
         "configured": true
       }
     }
   }
   ```
2. **Dashboard Configuration Modal**:
   Extend the edit credentials modal in the Web UI to include input fields for **Environment ID** and **Environment Name**.
3. **Collapsible Accordions in the Sidebar**:
   - Parse and group clusters by their `environment_id`/`environment_name` in the UI javascript.
   - Render them in collapsible sections (e.g. using HTML `<details>` and `<summary>` tags with glassmorphic styling).
   - Display a fallback "Default/Unassigned" collapsible category for newly discovered clusters that do not have an environment assigned yet.
