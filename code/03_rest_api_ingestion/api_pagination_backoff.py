"""
api_pagination_backoff.py

Demonstrates production-ready REST API ingestion.
When fetching data from SaaS platforms (Salesforce, Zendesk), you must handle:
1. Pagination (iterating through 'Next Page' tokens).
2. Rate Limits (HTTP 429 'Too Many Requests'), utilizing Exponential Backoff.
"""
import requests
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("REST_API_Ingestion")

def fetch_api_with_backoff(api_url, headers):
    all_records = []
    next_page_token = None
    max_retries = 5

    # 1. Cursor Pagination Loop
    while True:
        params = {"pageToken": next_page_token} if next_page_token else {}
        
        # 2. Exponential Backoff Loop for Rate Limits
        for attempt in range(max_retries):
            response = requests.get(api_url, headers=headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                all_records.extend(data.get("records", []))
                
                # Retrieve the token for the next page
                next_page_token = data.get("nextPageToken")
                break # Success, exit retry loop and go to next page
                
            elif response.status_code == 429:
                # HTTP 429: Too Many Requests. Wait exponentially: 1s, 2s, 4s, 8s...
                wait_time = 2 ** attempt
                logger.warning(f"Rate limited (429). Backing off for {wait_time}s...")
                time.sleep(wait_time)
            else:
                response.raise_for_status()
        
        # If the API did not return a next page token, we have downloaded everything
        if not next_page_token:
            logger.info(f"Ingestion complete. Downloaded {len(all_records)} records.")
            break 

    return all_records

if __name__ == "__main__":
    # Example usage:
    # records = fetch_api_with_backoff("https://api.saas-platform.com/v1/customers", {"Authorization": "Bearer TOKEN"})
    pass
