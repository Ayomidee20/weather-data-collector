import os
import boto3
import requests
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Constants
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# AWS S3 Client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def fetch_weather_data(city):
    # WeatherAPI endpoint
    url = f"http://api.weatherapi.com/v1/current.json?key={WEATHER_API_KEY}&q={city}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None

def save_to_s3(data, city):
    # Generate a timestamped file name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"{city}_weather_{timestamp}.json"
    try:
        # Save the data to S3
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=file_name,
            Body=str(data)
        )
        print(f"Weather data for {city} saved to S3: {file_name}")
    except Exception as e:
        print(f"Error saving to S3: {e}")

def main():
    # List of cities to fetch weather data for
    cities = ["Lagos", "Abuja", "Port Harcourt"]
    for city in cities:
        data = fetch_weather_data(city)
        if data:
            # Parse relevant details from the response
            parsed_data = {
                "city": data["location"]["name"],
                "region": data["location"]["region"],
                "country": data["location"]["country"],
                "temperature_f": data["current"]["temp_f"],
                "humidity": data["current"]["humidity"],
                "condition": data["current"]["condition"]["text"],
                "timestamp": data["location"]["localtime"]
            }
            save_to_s3(parsed_data, city)

if __name__ == "__main__":
    main()
