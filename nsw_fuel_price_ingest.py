# ==============================================================================
# COMP90024 Team 39 collaborative project
# Contributor attribution: see CONTRIBUTIONS.md
#
# Description: Script for ingesting daily NSW fuel price data (daily_fuel_price.py).
# ==============================================================================
import os
import uuid
import requests
import pandas as pd
import json
import urllib3
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, helpers

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TOKEN_URL = "https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken"
PRICE_URL = "https://api.onegov.nsw.gov.au/FuelPriceCheck/v1/fuel/prices"

def read_fission_secret(key, secret_name="fuel-price-secret"):
    """
    Read secret file in K8s Secret
    """
    paths = [
        f"/secrets/default/{secret_name}/{key}",
        f"/secrets/{secret_name}/{key}"
    ]
    
    for path in paths:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return f.read().strip()
                
    raise FileNotFoundError(f"Error found Secret file: {secret_name}/{key}")

def get_access_token(api_key, api_secret):
    params = {
        "grant_type": "client_credentials"
    }
    response = requests.get(
        TOKEN_URL,
        params=params,
        auth=(api_key, api_secret)
    )
    response.raise_for_status()
    return response.json()["access_token"]

def get_latest_fuel_prices(api_key, api_secret):
    token = get_access_token(api_key, api_secret)
    headers = {
        "Authorization": f"Bearer {token}",
        "apikey": api_key,
        "transactionid": str(uuid.uuid4()),
        "requesttimestamp": datetime.utcnow().strftime(
            "%d/%m/%Y %I:%M:%S %p"
        )
    }
    response = requests.get(
        PRICE_URL,
        headers=headers
    )
    response.raise_for_status()
    return response.json()

def main():
    try:
        # Load secrets
        api_key = read_fission_secret("NSW_FUEL_API_KEY")
        api_secret = read_fission_secret("NSW_FUEL_API_SECRET")
        es_user = read_fission_secret("ES_USER")
        es_pass = read_fission_secret("ES_PASS")

        # Connect to Elasticsearch
        es = Elasticsearch(
            ["https://elasticsearch-es-http.elastic:9200"],
            basic_auth=(es_user, es_pass),
            verify_certs=False
        )

        # Get API data
        data = get_latest_fuel_prices(api_key, api_secret)

        # Convert to dataframe
        prices = pd.json_normalize(data.get("prices", []))
        if prices.empty:
            return {"status": "success", "message": "No price data found.", "records_processed": 0}

        # Convert datetime
        prices["datetime"] = pd.to_datetime(prices["lastupdated"], dayfirst=True)

        # Keep only yesterday's updated records
        yesterday = (datetime.now() - timedelta(days=1)).date()
        prices = prices[prices["datetime"].dt.date == yesterday]

        if prices.empty:
            return {"status": "success", "message": f"No price data found for {yesterday}.", "records_processed": 0}

        # Extract date only
        prices["Date"] = prices["datetime"].dt.date

        # Only keep the specified fuel types (U91, E10, DL)
        target_fuels = ["U91", "E10", "DL"]
        prices = prices[prices["fueltype"].isin(target_fuels)]

        if prices.empty:
            return {"status": "success", "message": f"No target fuel data (U91, E10, DL) found for {yesterday}.", "records_processed": 0}
        # ----------------------------------------------------------

        # Calculate average daily fuel price
        daily_prices = (
            prices
            .groupby(["Date", "fueltype"])["price"]
            .mean()
            .reset_index()
        )

        # Rename columns
        daily_prices = daily_prices.rename(columns={
            "fueltype": "FuelCode",
            "price": "AvgPrice"
        })

        # Prepare actions for Elasticsearch bulk ingest
        actions = []
        for _, row in daily_prices.iterrows():
            # Add Location info
            doc = {
                "Date": row["Date"].strftime("%Y-%m-%d"),
                "FuelCode": row["FuelCode"],
                "AvgPrice": row["AvgPrice"],
                "Location": "NSW"
            }
            # Use deterministic ID based on date and fuel code for idempotency if needed
            doc_id = f"NSW_{doc['Date']}_{doc['FuelCode']}"
            actions.append({
                "_index": "fuel_price",
                "_id": doc_id,
                "_source": doc
            })

        # Bulk insert
        success, _ = helpers.bulk(es, actions)

        return {
            "status": "success",
            "message": f"Data saved to fuel_price index for {yesterday}.",
            "records_processed": success
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

if __name__ == "__main__":
    print(json.dumps(main(), indent=2))
