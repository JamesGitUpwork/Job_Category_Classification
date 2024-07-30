import winthrop_client_python
from winthrop_client_python.rest import ApiException
from winthrop_client_python.models.school_collection import SchoolCollection
from winthrop_client_python.models.school import School
from winthrop_client_python.models.job_post_collection import JobPostCollection
from winthrop_client_python.models.category_collection import CategoryCollection
from winthrop_client_python.paginated_iterator import PaginatedIterator

class WinthropConfig:

    @staticmethod
    def create_instance(host_in,api_key_in):
        configuration = winthrop_client_python.Configuration(
            host = host_in
        )

        configuration.api_key_prefix['ApiKey'] = 'Token'
        api_key = api_key_in
        configuration.api_key['ApiKey'] = api_key
        api_client = winthrop_client_python.ApiClient(configuration)
        api_instance = winthrop_client_python.DefaultApi(api_client)
        return api_instance