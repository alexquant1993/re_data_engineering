import asyncio
import os
import sys

from flows.idealista_flow import idealista_to_gcp_pipeline

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)


if __name__ == "__main__":
    zone = "madrid"
    province = "madrid"
    type_search = "sale"
    time_period = "24"
    bucket_name = "idealista_data_lake_idealista-scraper-384619"
    dataset_id = "idealista_listings"
    credentials_path = "~/.gcp/prefect-agent.json"
    asyncio.run(
        idealista_to_gcp_pipeline(
            province,
            type_search,
            time_period,
            bucket_name,
            dataset_id,
            credentials_path,
            zone,
            batch_size=3,
            testing=True,
        )
    )
