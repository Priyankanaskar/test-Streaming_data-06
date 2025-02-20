import requests

latitude, longitude = 40.7128, -74.0060  # New York (Change this if needed)

url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"

response = requests.get(url)

if response.status_code == 200:
    print("API Response:", response.json())
else:
    print(f"Error: {response.status_code}")
