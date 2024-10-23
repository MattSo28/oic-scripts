import csv
import os
import json
import requests
import time
from typing import List, Dict, Any

def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from a JSON file."""
    with open(config_path) as f:
        return json.load(f)

def ensure_https(base_url: str) -> str:
    """Ensure the base URL starts with https://."""
    if not base_url.startswith('https://'):
        base_url = 'https://' + base_url.lstrip('http://')
    return base_url

def upload_file(filepath: str, base_url: str, access_token: str, instance: str, api_path: str, method: str) -> requests.Response:
    """Upload a file using the specified HTTP method (POST or PUT)."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json',
    }
    
    with open(filepath, 'rb') as file:
        files = {
            'file': (os.path.basename(filepath), file, 'application/octet-stream'),
            'type': (None, 'application/octet-stream'),
        }
        url = f'{base_url}{api_path}?integrationInstance={instance}'
        return requests.request(method, url, headers=headers, files=files, timeout=30)

def import_integration(filepath: str, base_url: str, access_token: str, instance: str, api_path: str) -> List[str]:
    """Import or replace an integration and return the status and message."""
    response = upload_file(filepath, base_url, access_token, instance, api_path, 'POST')

    if response.status_code in [200, 204]:
        status = [os.path.basename(filepath), 'SUCCESS', 'Imported']
    elif response.status_code == 409:
        # Attempt to replace the integration
        response = upload_file(filepath, base_url, access_token, instance, api_path, 'PUT')
        if response.status_code in [200, 204]:
            status = [os.path.basename(filepath), 'SUCCESS', 'Replaced']
        else:
            status = [os.path.basename(filepath), 'ERROR', str(response.status_code)]
    else:
        status = [os.path.basename(filepath), 'ERROR', str(response.status_code)]

    print(f"Importing {os.path.basename(filepath)}: {status[1]} ({status[2]})")
    return status

def import_integrations_from_directory(directory: str, base_url: str, access_token: str, instance: str, api_path: str) -> List[List[str]]:
    """Import integrations from a specified directory."""
    results = [['INTEGRATION', 'STATUS', 'MESSAGE']]
    for filename in os.listdir(directory):
        if filename.endswith('.iar'):
            filepath = os.path.join(directory, filename)  # Construct full file path
            result = import_integration(filepath, base_url, access_token, instance, api_path)
            results.append(result)
    return results

def write_results_to_csv(results: List[List[str]], instance: str) -> None:
    """Write the results to a CSV file."""
    with open(f'import_summary_{instance}.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(results)

def main() -> None:
    """Main function to execute the integration import process."""
    start_time = time.time()  # Start time measurement

    # Load configuration
    config = load_config('config.json')
    
    import_directory_target = config['export_directory']
    base_url = ensure_https(config['base_url'])
    access_token = config['environments']['dev']['authorization']['bearer_token']
    instance = config['environments']['dev']['instance']
    api_path = config['api_uris']['import_integration']

    # Import integrations and write results
    results = import_integrations_from_directory(import_directory_target, base_url, access_token, instance, api_path)
    write_results_to_csv(results, instance)

    elapsed_time = time.time() - start_time  # End time measurement
    print(f"Total execution time: {elapsed_time:.2f} seconds")

if __name__ == '__main__':
    main()