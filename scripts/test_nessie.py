import requests

response = requests.get(
    "http://localhost:19120/api/v2/config"
)

print(response.status_code)
print(response.json())