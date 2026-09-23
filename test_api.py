# ==============================================================================
# COMP90024 Team 39 collaborative project
# Contributor attribution: see CONTRIBUTIONS.md
#
# Description: Test script for verifying backend API endpoints.
# ==============================================================================

import unittest
import requests

BASE_URL = "http://127.0.0.1:9090/api"

class TestAPIEndpoints(unittest.TestCase):
    """
    Test suite to verify that the Fission API endpoints are up and returning valid JSON.
    Assumes that the Fission router is accessible at 127.0.0.1:9090.
    """

    def check_endpoint(self, endpoint):
        url = f"{BASE_URL}/{endpoint}"
        print(f"Testing endpoint: {url}")
        try:
            response = requests.get(url, timeout=15)
            # Check if the request was successful
            self.assertEqual(response.status_code, 200, f"Endpoint {url} returned status code {response.status_code}")
            
            # Check if the response contains valid JSON
            try:
                data = response.json()
                self.assertIsInstance(data, (dict, list), f"Endpoint {url} did not return a JSON dictionary or list")
                print(f"[OK] {url} returned valid JSON.")
            except ValueError:
                self.fail(f"Endpoint {url} did not return valid JSON. Response text: {response.text[:200]}")
                
        except requests.exceptions.ConnectionError:
            self.fail(f"Failed to connect to {url}. Please ensure the Fission router is running and port-forwarded to 9090.")
        except requests.exceptions.Timeout:
            self.fail(f"Request to {url} timed out.")

    def test_get_social_media(self):
        self.check_endpoint("get-social-media")

    def test_get_word_frequency(self):
        self.check_endpoint("get-word-frequency")

    def test_get_fuel_price(self):
        self.check_endpoint("get-fuel-price")

    def test_get_news_daily(self):
        self.check_endpoint("get-news-daily")

    def test_get_news_word_frequency(self):
        self.check_endpoint("get-news-word-frequency")

if __name__ == '__main__':
    unittest.main(verbosity=2)
