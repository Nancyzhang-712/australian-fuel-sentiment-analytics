# ==============================================================================
# COMP90024 Team 39 collaborative project
# Contributor attribution: see CONTRIBUTIONS.md
#
# Description: Script for setting up and optimizing Elasticsearch indices.
# ==============================================================================

import os
import json
import urllib3
from elasticsearch import Elasticsearch

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_es_client():
    es_user = os.environ.get("ES_USER")
    es_pass = os.environ.get("ES_PASS")
    es_host = os.environ.get("ES_HOST")
    
    es = Elasticsearch(
        [es_host],
        basic_auth=(es_user, es_pass),
        verify_certs=False
    )
    return es

def setup_indices():
    es = get_es_client()
    
    # Load mappings
    mappings_file = os.path.join(os.path.dirname(__file__), "mappings.json")
    with open(mappings_file, 'r', encoding='utf-8') as f:
        indices_config = json.load(f)
        
    for index_name, config in indices_config.items():
        print(f"Setting up index: {index_name}...")
        
        if es.indices.exists(index=index_name):
            print(f"Index {index_name} already exists. Applying mapping updates...")
            try:
                es.indices.put_mapping(index=index_name, body=config["mappings"])
                print(f"Successfully updated mappings for {index_name}.")
            except Exception as e:
                print(f"Failed to update mapping for {index_name}: {e}")
        else:
            print(f"Creating index {index_name}...")
            try:
                es.indices.create(index=index_name, body=config)
                print(f"Successfully created index {index_name}.")
            except Exception as e:
                print(f"Failed to create index {index_name}: {e}")

if __name__ == "__main__":
    print("Starting database setup...")
    setup_indices()
    print("Database setup complete.")
