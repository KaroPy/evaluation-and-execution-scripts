Repository to calculate / list all costs based on the following categories
- storage
- model development
- targeting


Sources by category
Targeting
- stackit: used nodes per run

#TODOS
- [ ] add to log_active_prefect_runs
- [ ] add node to etlFLow monitoring

- [ ] Implement cost calculation for storage
- [ ] Implement cost calculation for model development
- [ ] Document cost calculation methodology
- [ ] Add more sources for each category
- [ ] Review and update README with usage examples

#TODO: add to log_active_prefect_runs
#TODO: add node to etlFLow monitoring


#################
# Project Structure Suggestion for Extracting and Mapping Cloud Costs to Prefect Flow Runs

cloud_cost_extractor/
|
├── README.md
├── Pipfile
├── .env                         # For storing API keys and secrets (not checked into version control)
|
├── config/
│   ├── settings.py              # Environment and config management
|
├── data/
│   └── costs/                   # Raw and processed cost reports
├── src/
│   ├── extractors/
│   │   ├── aws_extractor.py        # Fetch AWS cloud costs
│   │   ├── azure_extractor.py      # Fetch Azure cloud costs
│   │   └── stackit_extractor.py    # Fetch Stackit cloud costs
│   |
│   ├── mappers/
│   │   └── prefect_mapper.py       # Map cloud resources to Prefect flow runs
│   |
│   ├── models/
│   │   └── cost_model.py           # Unified data model for cloud costs
│   |
│   ├── flows/
│   │   └── cost_flow.py            # Prefect flow to orchestrate extraction and mapping
│   |
│   ├── utils/
│   │   └── helpers.py              # Common utility functions
│   │   └── constants.py              # Common utility functions
|
└── main.py                     # Entry point to run the whole process

# Short Explanation of Components:

## config/settings.py
Handles loading environment variables and configurations using something like `pydantic` or `dotenv`.

## extractors/
Each extractor uses its respective SDK or API to fetch usage and cost data. Normalize the output to a common format.

## mappers/prefect_mapper.py
Fetches Prefect flow runs metadata via Prefect API and tries to correlate resources (e.g., instance IDs, tags) to specific flow runs.

## models/cost_model.py
Defines data classes (e.g., using `pydantic`) for cost records, resource usage, and flow mapping.

## flows/cost_flow.py
Defines a Prefect flow to run each extractor and then the mapping logic. Optionally schedules the job.

## main.py
Script to run the Prefect flow locally or deploy it to a Prefect agent.

---
Would you like me to generate boilerplate code for this structure or for specific components?


# Troubleshooting
## Problem acessing databricks:
Error: FileNotFoundError

> export SSL_CERT_FILE=$(python -m certifi)


# Generate OAuth Databricks
https://docs.databricks.com/aws/en/dev-tools/auth/oauth-u2m?language=Environment#call-api 
