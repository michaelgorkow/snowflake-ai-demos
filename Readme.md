# Snowflake AI Demos

A collection of AI and ML demonstrations showcasing Snowflake's native AI/ML capabilities.

## Prerequisites

Before running any demo, set up your environment using the provided setup script:

```
Run as ACCOUNTADMIN:
environment-setup.sql
```

This creates:
- **Database**: `AI_DEMOS` with logging enabled
- **Warehouse**: `AI_WH` (X-Small, auto-suspend)
- **Role**: `AI_DEVELOPER` with ML feature access
- **Integrations**: External access, email notifications, GitHub API
- **Infrastructure**: Event table, image repository for SPCS

## Demos

| Demo | Description | Key Features |
|------|-------------|--------------|
| [Predictive Maintenance MLOps](./predictive-maintenance-mlops-demo/) | End-to-end MLOps workflow for IoT equipment failure prediction | Feature Store, Model Registry, Experiment Tracking, Model Monitoring, SPCS Deployment |
| [Master Data Mapping](./master-data-mapping-demo/) | AI-powered master data mapping using semantic search to match and harmonize records across disparate data sources | Cortex Search, Semantic Matching, Data Harmonization, Master Data Management |

## Getting Started

1. Run `environment-setup.sql` as `ACCOUNTADMIN`
2. Switch to the `AI_DEVELOPER` role or log into the newly created user
3. Navigate to a demo folder and follow its README

## Requirements

- `ACCOUNTADMIN` access for initial setup
- Snowpark Container Services (SPCS) for containerized deployments

## License

Apache License 2.0 - see [LICENSE](./LICENSE) for details.
