# bots/custom_http_operator.py
import requests
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CustomHttpOperator(BaseOperator):
    @apply_defaults
    def __init__(self, endpoint, method='GET', headers=None, do_xcom_push=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.method = method
        self.headers = headers or {}
        self.do_xcom_push = do_xcom_push

    def execute(self, context):
        url = f"http://your_api_base_url/{self.endpoint}"
        
        # Make HTTP request based on method
        if self.method.upper() == 'GET':
            response = requests.get(url, headers=self.headers)
        else:
            self.log.error(f"HTTP method {self.method} not supported.")
            return None

        # Check response and handle XCom push if requested
        if response.status_code == 200:
            self.log.info(f"Request successful: {response.json()}")
            if self.do_xcom_push:
                return response.json()
        else:
            self.log.error(f"Request failed: {response.status_code} - {response.text}")
            return None
