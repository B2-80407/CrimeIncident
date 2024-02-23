import requests
import os
from datetime import datetime, timedelta

# Create a folder to store the data
folder_name = "/home/pratik/DBDA/MyProjects/api_data"
os.makedirs(folder_name, exist_ok=True)

# Define the API endpoint
api_url = "https://data.sfgov.org/resource/wg3w-h783.json"

# Define the date range from January 1, 2018, to today's date
start_date = datetime(2021 , 1 , 1)
end_date = datetime.now()

# Iterate over each date in the range
current_date = start_date
while current_date <= end_date:
    # Format the date in the required format for the API request
    date_str = current_date.strftime("%Y-%m-%dT%H:%M:%S.000")

    # Make the API request
    response = requests.get(api_url, params={"incident_datetime": date_str})
    if response.status_code == 200:
        # Save the data to a file in the folder
        file_name = os.path.join(folder_name, f"{current_date.strftime('%Y-%m-%d')}.json")
        with open(file_name, "w") as file:
            file.write(response.text)
        print(f"Data saved for {current_date.strftime('%Y-%m-%d')}")
    else:
        print(f"Failed to fetch data for {current_date.strftime('%Y-%m-%d')}")

    # Move to the next date
    current_date += timedelta(days=1)

print("All data fetched and saved successfully!")
k