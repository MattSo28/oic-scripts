import requests
import csv
import os
import json
import time
from typing import List, Dict, Any


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from a JSON file."""
    with open(config_path) as f:
        return json.load(f)


def fetch_data(session: requests.Session, url: str) -> Dict[str, Any]:
    """Fetch data from the API."""
    response = session.get(url, allow_redirects=False, timeout=10)

    if response.status_code == 307:
        redirect_url = response.headers.get("Location")
        response = session.get(redirect_url)

    response.raise_for_status()
    return response.json()


def process_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Process and extract necessary fields from items."""
    processed_items = []
    for item in items:
        processed_item = {
            "code": item.get("id", ""),
            "adapter_type": item.get("adapterType", {}).get("displayName", ""),
            "policy": item.get("securityPolicy", ""),
            "status": item.get("status", ""),
            "usage": item.get("usage", ""),
            "usage_active": item.get("usageActive", ""),
            "host_url": next(
                (
                    prop.get("propertyValue")
                    for prop in item.get("connectionProperties", [])
                    if prop.get("displayName")
                    in [
                        "WSDL URL",
                        "Connection URL",
                        "Host",
                        "ERP Cloud Host",
                        "FTP Server Host Address",
                    ]
                ),
                "",
            ),
        }
        processed_items.append(processed_item)
    return processed_items


def fetch_service_account(
    session: requests.Session, base_url: str, code: str, instance: str
) -> str:
    """Fetch the service account for a given code."""
    url = f"{base_url}/ic/api/integration/v1/connections/{code}?integrationInstance={instance}"
    response_data = fetch_data(session, url)
    return next(
        (
            prop.get("propertyValue", "")
            for prop in response_data.get("securityProperties", [])
            if prop.get("propertyName") == "username"
        ),
        "",
    )


def write_to_csv(
    file_path: str, rows: List[Dict[str, Any]], fieldnames: List[str]
) -> None:
    """Write processed rows to a CSV file."""
    with open(file_path, mode="w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main(config_path: str) -> None:
    """Main function to fetch API data and write to CSV."""
    config = load_config(config_path)

    access_token = config["environments"]["dev"]["authorization"]["bearer_token"]
    instance = config["environments"]["dev"]["instance"]
    api_path = config["api_uris"]["retrieve_connections"]
    region = config['region']
    base_url = config['base_url']

    output_directory = os.path.join(os.getcwd(), "conn_inventory_dev")
    os.makedirs(output_directory, exist_ok=True)
    output_file = os.path.join(output_directory, "oic_dev_conn_extract.csv")

    headers = {"Authorization": f"Bearer {access_token}"}

    session = requests.Session()
    session.headers.update(headers)

    limit = 100
    offset = 0
    fieldnames = [
        "code",
        "adapter_type",
        "policy",
        "status",
        "usage",
        "usage_active",
        "host_url",
        "service_account",
    ]

    all_processed_items = []
    errors = []

    try:
        while True:
            print(f"Fetching data with offset: {offset}")
            old_url = f"https://{instance}.integration.{region}.ocp.oraclecloud.com{api_path}?limit={limit}&offset={offset}"
            try:
                response_data = fetch_data(session, old_url)
            except requests.exceptions.RequestException as err:
                errors.append(f"Error fetching data from old URL: {err}")
                break

            items = response_data.get("items", [])
            if items:
                all_processed_items.extend(process_items(items))
            else:
                print("No more items to process. Stopping execution.")
                break

            if not response_data.get("hasMore", False):
                break
            offset += limit

            new_url = f"https://design.integration.{region}.ocp.oraclecloud.com{api_path}?limit={limit}&offset={offset}&integrationInstance={instance}"
            try:
                response_data = fetch_data(session, new_url)
            except requests.exceptions.RequestException as err:
                errors.append(f"Error fetching data from new URL: {err}")
                break

            items = response_data.get("items", [])
            if items:
                all_processed_items.extend(process_items(items))
            else:
                print("No more items to process. Stopping execution.")
                break

            if not response_data.get("hasMore", False):
                break
            offset += limit

    except requests.exceptions.RequestException as err:
        errors.append(f"General request error occurred: {err}")

    if errors:
        print("Errors encountered during execution:")
        for error in errors:
            print(f"- {error}")
    else:
        write_to_csv(output_file, all_processed_items, fieldnames)
        print(f"Data successfully written to {output_file}")

        # Fetch service accounts
        for item in all_processed_items:
            code = item["code"]
            try:
                item["service_account"] = fetch_service_account(
                    session,
                    f"https://{instance}.integration.{region}.ocp.oraclecloud.com",
                    code,
                    instance,
                )
            except requests.exceptions.RequestException as err:
                print(f"Error fetching service account for {code}: {err}")

        # Write updated data back to CSV
        write_to_csv(output_file, all_processed_items, fieldnames)
        print(f"Service accounts successfully added to {output_file}")


if __name__ == "__main__":
    start_time = time.time()
    main("config.json")
    end_time = time.time()
    execution_time = end_time - start_time
    print(
        f"Execution time: {execution_time:.2f} seconds ({execution_time/60:.2f} minutes)"
    )
