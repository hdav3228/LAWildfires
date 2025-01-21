import requests
from urllib.parse import urlparse, parse_qs
import csv

# Configuration
api_key = "bd2e74ce0b1f1334dbee28882d1fb4ff4b631655347f76ca590b93b9c86849e0" # change it to user's
url = "https://api.openaq.org/v3/locations?coordinates=34.0549%2C-118.2426&radius=12000&limit=100&page=1&order_by=id&sort_order=asc" # get locations by LA coordinates 

headers = {"X-API-Key": api_key}

parsed_url = urlparse(url)
base_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
query_params = parse_qs(parsed_url.query)
params = {k: v[0] for k, v in query_params.items()}

all_results = []
try:
    # Initial request
    response = requests.get(base_url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    
    # Get pagination information
    total_found = data['meta']['found']
    limit = int(params['limit'])
    total_pages = (total_found + limit - 1) // limit  # Calculate total pages
    
    all_results.extend(data['results'])
    
    # Fetch remaining pages if needed
    for page in range(2, total_pages + 1):
        params['page'] = str(page)
        response = requests.get(base_url, headers=headers, params=params)
        response.raise_for_status()
        page_data = response.json()
        all_results.extend(page_data['results'])

except requests.exceptions.RequestException as e:
    print(f"Error fetching data: {e}")
    exit(1)

# csv headers 
csv_columns = [
    'id', 'name', 'locality', 'timezone',
    'country_code', 'country_name',
    'owner_id', 'owner_name',
    'provider_id', 'provider_name',
    'isMobile', 'isMonitor', 'instruments'
]

csv_rows = []
for result in all_results:
    # Handle nested objects and arrays
    country = result.get('country', {})
    owner = result.get('owner', {})
    provider = result.get('provider', {})
    
    instruments = [
        str(instrument.get('name', '')) 
        for instrument in result.get('instruments', [])
    ]
    
    csv_rows.append({
        'id': result.get('id', ''),
        'name': result.get('name', ''),
        'locality': result.get('locality', ''),
        'timezone': result.get('timezone', ''),
        'country_code': country.get('code', ''),
        'country_name': country.get('name', ''),
        'owner_id': owner.get('id', ''),
        'owner_name': owner.get('name', ''),
        'provider_id': provider.get('id', ''),
        'provider_name': provider.get('name', ''),
        'isMobile': result.get('isMobile', ''),
        'isMonitor': result.get('isMonitor', ''),
        'instruments': '; '.join(instruments)
    })

# Write to CSV
try:
    with open('openaq_data.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        writer.writerows(csv_rows)
    print("Data successfully exported to openaq_data.csv")
except IOError as e:
    print(f"Error writing to CSV: {e}")