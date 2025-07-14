import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")


BASE_URL = "https://api.twelvedata.com"

# L O A D  D A T A

def get_data(symbol,interval="1min"):
    url = f"{BASE_URL}/time_series?symbol={symbol}&interval={interval}&apikey={API_KEY}"
    response = requests.get(url)
    data = response.json()

    if "values" in data:
        latest_record = data["values"][0]
        print(latest_record)
        return data["values"]
    elif "message" in data:
        print("Error:",data["message"])
    else:
        print("no response",data)
    return None

def save_raw_data(data,filename="data/raw_data.json"):
    if data is not None:
        os.makedirs("data",exist_ok=True)
        with open(filename,"w") as f:
            json.dump(data,f,indent=4)
        print("DATA STORES SUCCESSFULLY")
    else:
        print('NO DATA TO PRINT')

if __name__ == "__main__":
    symbol = "AAPL"
    stock_data = get_data(symbol)
    save_raw_data(stock_data)
    print("SUCCESSFUL")