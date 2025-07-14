import pandas as pd
from datetime import datetime as dt
import json

def transform_data(raw_data):
    if not raw_data:
        print("NOTHING TO TRANSFORM")
        return []
    
    df = pd.DataFrame(raw_data)
    for col in ['open','high','low','close','volume']:
        #parsing values to numeric float
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # datetime -> pandas datetime
    df['datetime'] = pd.to_datetime(df['datetime'])

    #sort around dt
    df = df.sort_values(by='datetime')

    #percentage changes calculation
    df['pct_changes'] = df['close'].pct_change()*100

    #volatility zone logic
    def get_zone(pct):
        if pd.isna(pct):
            return "N/A"
        if abs(pct)>0.5:
            return "High Volatility"
        elif abs(pct)>0.2:
            return "Watch"
        else:
            return "Stable"
        
    df['volatility_zone'] = df['pct_changes'].apply(get_zone)

    # Derived features
    df['price_range'] = df['high'] - df['low']
    df['close_open_diff'] = df['close'] - df['open']
    df['volume_change'] = df['volume'].diff()

     # Trend classification
    def trend_tag(pct):
        if pd.isna(pct):
            return "N/A"
        elif pct > 0:
            return "Uptrend"
        elif pct < 0:
            return "Downtrend"
        return "Flat"

    df['trend'] = df['pct_changes'].apply(trend_tag)

    df['transformed_at'] = dt.now()

    return df.to_dict(orient='records')


def save_transformed_data(data,filename="data/transformed_data.json"):
    if data:
        with open(filename,"w") as f:
            json.dump(data , f , indent=4, default=str)
        print(f"transformed data saved to {filename}")

        df = pd.DataFrame(data)
        df.to_csv("stock_data.csv",index=False)
    else:
        print("No data to be transformed")


if __name__ == "__main__":
    with open("data/raw_data.json", "r") as f:
        raw_data = json.load(f)

    # Now transform the data
    transformed = transform_data(raw_data)

    # Save the transformed output
    save_transformed_data(transformed)