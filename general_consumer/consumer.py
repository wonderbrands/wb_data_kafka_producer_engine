import requests
import os


try:
    with open('.env') as f:
        for line in f:
            key, value = line.strip().split('=')
            os.environ[key] = value
except FileNotFoundError:
    print("Error: Could not find .env file")
    sys.exit(1)


# Call the Odoo URL defined in the env variable ODOO_URL
odoo_url = os.environ.get('ODOO_URL')
model = os.environ.get('MODEL')

response = requests.get(f"{odoo_url}/api/model_details", params={'model': model})

if response.status_code == 200:
    data = response.json()
    
    
else:
    print(f"Error: {response.status_code} - {response.text}")


