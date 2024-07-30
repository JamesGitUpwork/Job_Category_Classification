import winthrop_client_python
from dotenv import load_dotenv
import os

class WinthropConfig:

    @staticmethod
    def create_instance(host_in):
        configuration = winthrop_client_python.Configuration(
            host = host_in
        )

        load_dotenv()
        api_key = os.getenv('API_KEY')

        configuration.api_key_prefix['ApiKey'] = 'Token'
        configuration.api_key['ApiKey'] = api_key
        api_client = winthrop_client_python.ApiClient(configuration)
        api_instance = winthrop_client_python.DefaultApi(api_client)
        return api_instance