import requests
import csv
import os
import json
from typing import List, Dict, Any


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from a JSON file."""
    with open(config_path) as f:
        return json.load(f)


def create_csv_writer(file_path: str, fieldnames: List[str]) -> csv.DictWriter:
    """Create a CSV writer."""
    csv_file = open(file_path, mode="w", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    return writer, csv_file


def fetch_data(session: requests.Session, url: str) -> Dict[str, Any]:
    """Fetch data from the API."""
    response = session.get(url, allow_redirects=False, timeout=10)

    if response.status_code == 307:
        redirect_url = response.headers.get("Location")
        response = session.get(redirect_url)

    response.raise_for_status()
    return response.json()


def process_items(items: List[Dict[str, Any]], writer: csv.DictWriter) -> None:
    """Process and write items to the CSV."""
    for item in items:
        writer.writerow(
            {
                "code": item.get("code", ""),
                "name": item.get("name", ""),
                "id": item.get("id", ""),
                "status": item.get("status", ""),
                "endpoint": item.get("endPointURI", ""),
                "description": item.get("description", ""),
            }
        )


def construct_url(
    base_url: str, api_path: str, limit: int, offset: int, instance: str = None
) -> str:
    """Construct the API URL for fetching data."""
    url = f"{base_url}{api_path}?limit={limit}&offset={offset}"
    if instance:
        url += f"&integrationInstance={instance}"
    return url


def main(config_path: str) -> None:
    """Main function to fetch API data and write to CSV."""
    config = load_config(config_path)

    access_token = config["environments"]["dev"]["authorization"]["bearer_token"]
    instance = config["environments"]["dev"]["instance"]
    api_path = config["api_uris"]["retrieve_integrations"]
    base_url = config["base_url"]
    region = config["region"]

    output_directory = os.path.join(os.getcwd(), "int_inventory_dev")
    os.makedirs(output_directory, exist_ok=True)
    output_file = os.path.join(output_directory, "oic_dev_int_extract.csv")

    headers = {"Authorization": f"Bearer {access_token}"}

    session = requests.Session()
    session.headers.update(headers)

    limit = 100
    offset = 0
    fieldnames = ["code", "name", "id", "status", "endpoint", "description"]
    writer, csv_file = create_csv_writer(output_file, fieldnames)

    try:
        while True:
            print(f"Fetching data with offset: {offset}")

            old_url = construct_url(
                base_url=f"https://{instance}.integration.{region}.ocp.oraclecloud.com",
                api_path=api_path,
                limit=limit,
                offset=offset,
            )
            response_data = fetch_data(session, old_url)

            items = response_data.get("items", [])
            if items:
                process_items(items, writer)
            else:
                print("No more items to process. Stopping execution.")
                break

            if not response_data.get("hasMore", False):
                print("No more data to fetch. Stopping execution.")
                break

            offset += limit

            new_url = construct_url(
                base_url=base_url,
                api_path=api_path,
                limit=limit,
                offset=offset,
                instance=instance,
            )
            response_data = fetch_data(session, new_url)

            items = response_data.get("items", [])
            if items:
                process_items(items, writer)
            else:
                print("No more items to process. Stopping execution.")
                break

            if not response_data.get("hasMore", False):
                print("No more data to fetch. Stopping execution.")
                break

            offset += limit

    except requests.exceptions.RequestException as err:
        print(f"Request error occurred: {err}")

    finally:
        csv_file.close()  # Ensure CSV file is closed

    print(f"Data successfully written to {output_file}")


if __name__ == "__main__":
    main("config.json")
