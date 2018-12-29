from asyncio import sleep

import aiohttp

from easydb.domain import Space, ElementField, MultipleElementFields, Element, TransactionOperation, PaginatedElements, \
    SPACE_DOES_NOT_EXIST, SpaceDoesNotExistException, BUCKET_DOES_NOT_EXIST, BucketDoesNotExistException, \
    ELEMENT_DOES_NOT_EXIST, ElementDoesNotExistException, TRANSACTION_DOES_NOT_EXIST, TransactionDoesNotExistException, \
    UnknownError, OPERATION_TYPES, UnknownOperationException, Transaction, OperationResult, FilterQuery, \
    TRANSACTION_ABORTED, TransactionAbortedException


class Request:
    def __init__(self, url: str, method: str, data: dict = None):
        self.url = url
        self.method = method
        self.data = data

    def __str__(self):
        return "Request(url=%s, method=%s, data=%s)" % (self.url, self.method, self.data)

    def __repr__(self):
        return self.__str__()


class ResponseData:
    def __init__(self, status: int, data: dict):
        self.status = status
        self.data = data

    def __str__(self):
        return "ResponseData(status=%s, data=%s)" % (self.status, self.data)


class EasydbClient:
    def __init__(self, server_url: str, retry_backoff_millis=300, retries_number=3):
        self.server_url = server_url + "/api/v1"
        self.retry_backoff_millis = retry_backoff_millis
        self.retries_number = retries_number

    async def create_space(self):
        response = await self.perform_request(Request("%s/spaces" % self.server_url, 'POST'))
        self.ensure_status_2xx(response)

        return response.data['spaceName']

    async def delete_space(self, space_name):
        response = await self.perform_request(Request("%s/spaces/%s" % (self.server_url, space_name), 'DELETE'))

        self.ensure_space_found(response, space_name)
        self.ensure_status_2xx(response)

    async def get_space(self, space_name):
        response = await self.perform_request(Request("%s/spaces/%s" % (self.server_url, space_name), 'GET'))

        self.ensure_space_found(response, space_name)
        self.ensure_status_2xx(response)
        return Space(response.data['spaceName'])

    async def add_element(self, space_name, bucket_name, element_fields: MultipleElementFields):
        response = await self.perform_request(
            Request("%s/%s/%s" % (self.server_url, space_name, bucket_name), 'POST', data=element_fields.as_json()))

        self.ensure_space_found(response, space_name)
        self.ensure_bucket_found(response, space_name, bucket_name)
        self.ensure_status_2xx(response)
        data = response.data
        return Element(data['id'], [ElementField(f['name'], f['value']) for f in data['fields']])

    async def delete_bucket(self, space_name, bucket_name):
        response = await self.perform_request(
            Request('%s/%s/%s' % (self.server_url, space_name, bucket_name), 'DELETE'))

        self.ensure_space_found(response, space_name)
        self.ensure_bucket_found(response, space_name, bucket_name)
        self.ensure_status_2xx(response)

    async def delete_element(self, space_name, bucket_name, element_id):
        response = await self.perform_request(
            Request('%s/%s/%s/%s' % (self.server_url, space_name, bucket_name, element_id), 'DELETE'))

        self.ensure_space_found(response, space_name)
        self.ensure_bucket_found(response, space_name, bucket_name)
        self.ensure_element_found(response, space_name, bucket_name, element_id)
        self.ensure_status_2xx(response)

    async def update_element(self, space_name, bucket_name, element_id, element_fields: MultipleElementFields):
        response = await self.perform_request(
            Request('%s/%s/%s/%s' % (self.server_url, space_name, bucket_name, element_id), 'PUT',
                    data=element_fields.as_json()))

        self.ensure_space_found(response, space_name)
        self.ensure_bucket_found(response, space_name, bucket_name)
        self.ensure_element_found(response, space_name, bucket_name, element_id)
        self.ensure_status_2xx(response)

    async def get_element(self, space_name, bucket_name, element_id):
        response = await self.perform_request(
            Request('%s/%s/%s/%s' % (self.server_url, space_name, bucket_name, element_id), 'GET'))

        self.ensure_space_found(response, space_name)
        self.ensure_bucket_found(response, space_name, bucket_name)
        self.ensure_element_found(response, space_name, bucket_name, element_id)
        self.ensure_status_2xx(response)
        element_id = response.data['id']
        fields = self.parse_element_fields(response.data['fields'])
        return Element(element_id, fields)

    async def filter_elements_by_query(self, query: FilterQuery):
        response = await self.perform_request(
            Request('%s/%s/%s?limit=%d&offset=%d' %
                    (self.server_url, query.space_name, query.bucket_name, query.limit, query.offset), 'GET'))

        self.ensure_space_found(response, query.space_name)
        self.ensure_bucket_found(response, query.space_name, query.bucket_name)
        return self._parse_filter_response(response)

    async def filter_elements_by_link(self, link: str):
        response = await self.perform_request(Request(link, 'GET'))
        return self._parse_filter_response(response)

    async def begin_transaction(self, space_name: str):
        response = await self.perform_request(
            Request('%s/transactions/%s' % (self.server_url, space_name), 'POST'))

        self.ensure_space_found(response, space_name)
        self.ensure_status_2xx(response)
        return self.parse_transaction(response.data)

    async def add_operation(self, transaction_id: str, operation: TransactionOperation):
        self.ensure_operation_constraints(operation)

        response = await self.add_operation_request_with_retry(
            Request('%s/transactions/%s/add-operation' % (self.server_url, transaction_id), 'POST',
                    operation.as_json()))

        self.ensure_transaction_found(response, transaction_id)
        self.ensure_bucket_found(response, space_name=None, bucket_name=operation.bucket_name,
                                 transaction_id=transaction_id)
        self.ensure_element_found(response, space_name=None, bucket_name=operation.bucket_name,
                                  element_id=operation.element_id, transaction_id=transaction_id)
        self.ensure_transaction_not_aborted(response, transaction_id)
        self.ensure_status_2xx(response)
        return self.parse_operation_result(response.data)

    async def commit_transaction(self, transaction_id):
        response = await self.perform_request(
            Request('%s/transactions/%s/commit' % (self.server_url, transaction_id), 'POST'))

        self.ensure_transaction_found(response, transaction_id)
        self.ensure_transaction_not_aborted(response, transaction_id)
        self.ensure_status_2xx(response)

    def _parse_filter_response(self, response):
        self.ensure_status_2xx(response)
        next_link = response.data['nextPageLink']
        elements = self.parse_multiple_elements(response.data['results'])
        return PaginatedElements(elements, next_link)

    async def add_operation_request_with_retry(self, request: Request):
        counter = 0
        response = await EasydbClient.perform_request(request)
        while counter < self.retries_number:
            if response.status == 200 and response.data['errorCode'] == TRANSACTION_ABORTED:
                await sleep(self.retry_backoff_millis / 1000)
                response = await EasydbClient.perform_request(request)
                counter += 1
            else:
                break
        return response

    @staticmethod
    async def perform_request(request: Request):
        async with aiohttp.ClientSession() as session:
            if request.method in ['GET', 'POST', 'DELETE', 'PUT']:
                async with session.request(request.method, request.url, json=request.data) as response:
                    return ResponseData(response.status, await response.json())
            else:
                raise Exception("Incorrect request type")

    @staticmethod
    def ensure_space_found(response, space_name):
        if response.status == 404 and response.data and response.data['errorCode'] == SPACE_DOES_NOT_EXIST:
            raise SpaceDoesNotExistException(space_name)

    @staticmethod
    def ensure_bucket_found(response, space_name, bucket_name, transaction_id=None):
        if response.status == 404 and response.data and response.data['errorCode'] == BUCKET_DOES_NOT_EXIST:
            raise BucketDoesNotExistException(space_name, bucket_name, transaction_id)

    @staticmethod
    def ensure_element_found(response, space_name, bucket_name, element_id, transaction_id=None):
        if response.status == 404 and response.data and response.data['errorCode'] == ELEMENT_DOES_NOT_EXIST:
            raise ElementDoesNotExistException(space_name, bucket_name, element_id, transaction_id)

    @staticmethod
    def ensure_transaction_found(response, transaction_id):
        if response.status == 404 and response.data and response.data['errorCode'] == TRANSACTION_DOES_NOT_EXIST:
            raise TransactionDoesNotExistException(transaction_id)

    @staticmethod
    def ensure_transaction_not_aborted(response, transaction_id):
        if response.status == 200 and response.data['errorCode'] == TRANSACTION_ABORTED:
            raise TransactionAbortedException(transaction_id)

    @staticmethod
    def ensure_status_2xx(response):
        if response.status >= 300 or response.status < 200:
            raise UnknownError("Unexpected status code: %s" % response.status)

    @staticmethod
    def ensure_operation_constraints(operation):
        if operation.type not in OPERATION_TYPES:
            raise UnknownOperationException()

    @staticmethod
    def parse_multiple_elements(data: dict):
        return [Element(f['id'], EasydbClient.parse_element_fields(f['fields'])) for f in data]

    @staticmethod
    def parse_single_element(data: dict):
        return Element(data['id'], EasydbClient.parse_element_fields(data['fields']))

    @staticmethod
    def parse_element_fields(data: dict):
        return [ElementField(f['name'], f['value']) for f in data]

    @staticmethod
    def parse_transaction(data: dict):
        return Transaction(data['transactionId'])

    @staticmethod
    def parse_operation_result(data: dict):
        return OperationResult(EasydbClient.parse_single_element(data['element']) if data['element'] else None)
