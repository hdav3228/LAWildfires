import requests
import csv

# Configuration
api_key = ""  # Replace with your API key
headers = {"X-API-Key": api_key}

# Read sensor IDs from CSV
sensor_ids = []
with open('openaq_data.csv', 'r', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        sensor_ids.append(row['id'])

# API parameters
base_params = {
    'datetime_to': '2025-01-22T00:00:00Z',
    'datetime_from': '2025-01-01T00:00:00Z',
    'limit': 100,
    'page': 1
}

# Prepare CSV
measurements_file = 'measurements_data.csv'
csv_columns_written = False

with open(measurements_file, 'w', newline='', encoding='utf-8') as csvfile:
    writer = None

    for sensor_id in sensor_ids:
        print(f"Fetching data for sensor ID: {sensor_id}")
        url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements"
        params = base_params.copy()

        try:
            all_measurements = []
            page = 1
            while True:
                params['page'] = page
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                results = data.get('results', [])

                if not results:
                    break

                all_measurements.extend(results)
                page += 1

            print(f"Retrieved {len(all_measurements)} measurements for sensor {sensor_id}")

            # Process measurements
            for measurement in all_measurements:
                date_info = measurement.get('date', {})
                row = {
                    'sensor_id': sensor_id,
                    'parameter': (measurement.get('parameter') or {}).get('name', ''),
                    'value': measurement.get('value', ''),
                    'unit': (measurement.get('parameter') or {}).get('units', ''),
                    'date_utc': date_info.get('utc', ''),
                    'date_local': date_info.get('local', ''),
                    'location_id': measurement.get('locationId', ''),
                    'coordinates_latitude': (measurement.get('coordinates') or {}).get('latitude', ''),
                    'coordinates_longitude': (measurement.get('coordinates') or {}).get('longitude', '')
                }

                if not csv_columns_written:
                    csv_columns = row.keys()
                    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                    writer.writeheader()
                    csv_columns_written = True

                writer.writerow(row)

        except requests.exceptions.RequestException as e:
            print(f"Error for sensor {sensor_id}: {str(e)}")
            continue

print(f"Data saved to {measurements_file}")