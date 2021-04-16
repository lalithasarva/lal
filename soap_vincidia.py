"""
Retrieve Sends
"""
from datetime import datetime
from src.lib.crm.sfmc.soap.soap_retrieve import SoapRetrieve


class RetrieveSend(SoapRetrieve):
    """
    SOAP Service to grab Sends from SFMC
    """

    async def get(self,
                  start_datetime: datetime = datetime(1970, 1, 1, 0, 0, 0, 0),
                  end_datetime: datetime = None):
        """
        Grabs the Sends from SFMC, if the data is paginated it will automatically get the paginated data,
        for each page of data it will call the callback
        :param start_datetime: data is pulled starting from this datetime, defaults to 01-01-1970 0:0:0
        :param end_datetime: data pulled ends at this datetime, optional
        """
        request = await self.build_common_request("Send",
                                                  ['Status', 'Additional', 'BccEmail', 'CreatedDate', 'ModifiedDate',
                                                   'FromAddress', 'FromName', 'IsMultipart', 'ID', 'SendLimit',
                                                   'SendWindowClose', 'SendWindowOpen', 'EmailName', 'Duplicates',
                                                   'ExistingUndeliverables', 'ExistingUnsubscribes', 'ForwardedEmails',
                                                   'NumberErrored', 'NumberExcluded', 'NumberSent', 'NumberTargeted',
                                                   'MissingAddresses', 'OtherBounces', 'PreviewURL',
                                                   'SendDate', 'SoftBounces', 'HardBounces', 'Subject', 'UniqueClicks',
                                                   'UniqueOpens', 'Unsubscribes', 'Email.ID'],
                                                  start_datetime, end_datetime)
        await self.submit_request(request)
