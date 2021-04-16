"""
SOAP Retrieve
"""
import asyncio
from datetime import datetime
from typing import Callable, Awaitable, List, Optional
from aiohttp.client_exceptions import ClientResponseError, ClientConnectionError, ServerTimeoutError, ClientError
import pytz
from tenacity import retry, retry_if_exception_type, wait_random_exponential, stop_after_attempt
from zeep import Client as ZeepClient
from zeep import helpers
from src.lib.bb_serverless_lib.logging.slog import StructLog as log
from src.lib.crm.sfmc.sfmc_config import SfmcConfig
from src.lib.crm.sfmc.oauth2_request import Oauth2Request
from src.lib.ingest_exceptions import IngestDataRetrieveException, IngestServiceException
from src.lib.utils.lambda_launcher import is_it_time_to_pause


class SoapRetrieve:
    """
    Base class for all SFMC SOAP Data Retrieves
    """

    def __init__(self, pause_callback: Callable[[dict], Awaitable[None]] = None,
                 response_callback: Optional[Callable[[dict], Awaitable[None]]] = None):
        self._zeep_client: Optional[ZeepClient] = None
        self.pause_callback = pause_callback
        self.response_callback = response_callback

    @property
    def zeep_client(self) -> ZeepClient:
        """
        Provides the ZEEP soap client, from Oauth2Request
        :return:
        """
        if self._zeep_client is None:
            self._zeep_client = Oauth2Request.get_soap_client()
        return self._zeep_client

    async def _get_retrieve_request_base(self):
        """
        Retrieves base request
        :return: provides the RetrieveRequest
        """
        return self.zeep_client.get_type('ns0:RetrieveRequest')

    @retry(wait=wait_random_exponential(multiplier=SfmcConfig.wait_exponential_multiplier,
                                        max=SfmcConfig.max_read_backoff),
           stop=stop_after_attempt(SfmcConfig.max_retries),
           retry=retry_if_exception_type(IngestServiceException))
    async def submit_request(self, retrieve_request):
        """
        Submits the request
        :param retrieve_request: the request soap object
        """
        try:
            response = await self.zeep_client.service.Retrieve(RetrieveRequest=retrieve_request,
                                                               _soapheaders=(await Oauth2Request.get_soap_headers()))
        except (ClientResponseError, ClientConnectionError, ServerTimeoutError, ClientError, asyncio.TimeoutError)\
                as err:
            log.warn('SOAP Error', err)
            raise IngestServiceException('SOAP Service Fail', 0, err)

        if 'OverallStatus' not in response:
            raise IngestDataRetrieveException(f'Invalid Response {response}')

        if response['OverallStatus'].startswith('Error'):
            raise IngestDataRetrieveException(f'Status Error in Response {response["OverallStatus"]}')
        response = helpers.serialize_object(response)
        tasks = [self.response_callback(response), self._submit_next_request(retrieve_request, response)]
        await asyncio.gather(*tasks)

    async def _submit_next_request(self, request, response):
        """
        submits the next paginated request, if required, called starting from submit_request
        :param request: the previous request
        :param response: the previous response
        """

        if 'OverallStatus' in response and response['OverallStatus'] == 'MoreDataAvailable':
            if is_it_time_to_pause():
                await self.pause_callback({'oauth': await Oauth2Request.pause_extract(),
                                           'object_type': request.ObjectType,
                                           'continue_request': response['RequestID']})
            else:
                req_base = await self._get_retrieve_request_base()
                request2 = req_base(ObjectType=request.ObjectType, ContinueRequest=response['RequestID'])
                await self.submit_request(request2)

    async def resume_request(self, paused: dict):
        """
        Resumes the request from the spot paused
        :param paused: json
        :return:
        """
        if 'oauth' in paused:
            await Oauth2Request.resume_extract(paused['oauth'])
        req_base = await self._get_retrieve_request_base()
        request2 = req_base(ObjectType=paused['object_type'], ContinueRequest=paused['continue_request'])
        await self.submit_request(request2)

    async def build_common_request(self, object_type: str, properties: List[str],
                                   start_datetime: datetime = None,
                                   end_datetime: datetime = None,
                                   filter_date_field: Optional[str] = 'ModifiedDate') -> object:
        """
        Builds a common retrieve request for a given object type with a specified start and end time
        :param object_type: the object type to query, based on SFMC object types
        :param properties: Properties of that object to retrieve, as a list
        :param start_datetime: the start datetime to filter data by
        :param end_datetime: the end datetime to filter data
        :param filter_date_field: the name of the date field to filter, defaults to ModifiedDate
        :return: the soap request
        """
        # pylint: disable=too-many-arguments
        if not isinstance(start_datetime, datetime):
            start_datetime = datetime(1970, 1, 1, 0, 0, 0, 0).replace(tzinfo=pytz.utc)
        if not isinstance(end_datetime, datetime):
            end_datetime = datetime.utcnow().replace(tzinfo=pytz.utc)
        req_base = await self._get_retrieve_request_base()
        filter_base = self.zeep_client.get_type('ns0:SimpleFilterPart')
        simplefilter = None
        if filter_date_field:
            simplefilter = filter_base(Property=filter_date_field, SimpleOperator='between',
                                       DateValue=[start_datetime, end_datetime])
        request = req_base(ObjectType=object_type, Properties=properties, Filter=simplefilter)
        return request
