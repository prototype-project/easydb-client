import aiohttp
from typing import List


class SpaceDoesNotExistException(Exception):
    def __init__(self, space_name):
        super().__init__()
        self.space_name = space_name

    def __str__(self):
        return 'SpaceDoesNotExistException(space_name=%s)' % self.space_name

    def __repr__(self):
        return self.__str__()


class BucketDoesNotExistException(Exception):
    def __init__(self, space_name, bucket_name):
        super().__init__()
        self.space_name = space_name
        self.bucket_name = bucket_name

    def __str__(self):
        return 'BucketDoesNotExistException(space_name=%s, bucket_name=%s)' % (self.space_name, self.bucket_name)

    def __repr__(self):
        return self.__str__()


class ElementDoesNotExistException(Exception):
    def __init__(self, space_name, bucket_name, element_id):
        super().__init__()
        self.space_name = space_name
        self.bucket_name = bucket_name
        self.element_id = element_id

    def __str__(self):
        return 'ElementDoesNotExistException(space_name=%s, bucket_name=%s, element_id=%s)' % \
               (self.space_name, self.bucket_name, self.element_id)

    def __repr__(self):
        return self.__str__()


class UnknownError(Exception):
    def __init__(self, msg):
        super().__init__(msg)


SPACE_DOES_NOT_EXIST = 'SPACE_DOES_NOT_EXIST'
BUCKET_DOES_NOT_EXIST = 'BUCKET_DOES_NOT_EXIST'
ELEMENT_DOES_NOT_EXIST = 'ELEMENT_DOES_NOT_EXIST'


class Request:
    def __init__(self, url: str, method: str, data: dict = None):
        self.path = url
        self.method = method
        self.data = data


class ResponseData:
    def __init__(self, status: int, data: dict):
        self.status = status
        self.data = data


class Space:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return 'Space(name=%s)' % self.name

    def __str__(self):
        return self.__repr__()

class ElementField:
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def __eq__(self, other):
        return self.name == other.name and \
               self.value == other.value

    def __hash__(self):
        return hash((self.name, self.value))

    def __str__(self):
        return 'ElementField(name=%s, value=%s)' % (self.name, self.value)

    def __repr__(self):
        return self.__str__()


class MultipleElementFields:
    def __init__(self, fields: List[ElementField] = None):
        if not fields:
            self.fields = []
        else:
            self.fields = fields

    def add_field(self, name, value):
        self.fields.append(ElementField(name, value))
        return self

    def __str__(self):
        fields_str = ", ".join(['{%s = %s}' % (f.name, f.value) for f in self.fields])
        return 'ElementFields(fields=[ %s ])' % fields_str

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.fields == other.fields

    def __hash__(self):
        return hash(self.fields)

    def as_json(self):
        return {'fields': [dict((('name', f.name), ('value', f.value))) for f in self.fields]}


class Element:
    def __init__(self, identifier: str, fields: List[ElementField] = None):
        self.identifier = identifier
        self.element_fields = MultipleElementFields(fields)

    def __eq__(self, other):
        return self.identifier == other.identifier and \
               self.element_fields == other.element_fields

    def __hash__(self):
        return hash((self.identifier, self.element_fields))

    def __repr__(self):
        fields_str = ", ".join(['{%s = %s}' % (f.name, f.value) for f in self.fields])
        return 'Element(identifier=%s, fields=[ %s ])' % (
            self.identifier, fields_str)

    def __str__(self):
        return self.__repr__()

    @property
    def fields(self):
        return self.element_fields.fields

    def add_field(self, name, value):
        self.element_fields.add_field(name, value)
        return self


class FilterQuery:
    def __init__(self, space_name, bucket_name, limit=20, offset=0):
        self.space_name = space_name
        self.bucket_name = bucket_name
        self.limit = limit
        self.offset = offset


class PaginatedElements:
    def __init__(self, elements:List[Element]=None, next_link: str=None):
        self.elements = elements if elements else []
        self.next_link = next_link

    def __eq__(self, other):
        return self.elements == other.elements and self.next_link == other.next_link

    def __hash__(self):
        return hash((self.elements, self.next_link))


class EasydbClient:
    def __init__(self, server_url: str):
        self.server_url = server_url + "/api/v1"

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

    def _parse_filter_response(self, response):
        self.ensure_status_2xx(response)
        next_link = response.data['nextPageLink']
        elements = self.parse_elements(response.data['results'])
        return PaginatedElements(elements, next_link)

    @staticmethod
    async def perform_request(request: Request):
        async with aiohttp.ClientSession() as session:
            if request.method in ['GET', 'POST', 'DELETE', 'PUT']:
                async with session.request(request.method, request.path, json=request.data) as response:
                    return ResponseData(response.status, await response.json())
            else:
                raise Exception("Incorrect request type")

    @staticmethod
    def ensure_space_found(response, space_name):
        if response.status == 404 and response.data and response.data['errorCode'] == SPACE_DOES_NOT_EXIST:
            raise SpaceDoesNotExistException(space_name)

    @staticmethod
    def ensure_bucket_found(response, space_name, bucket_name):
        if response.status == 404 and response.data and response.data['errorCode'] == BUCKET_DOES_NOT_EXIST:
            raise BucketDoesNotExistException(space_name, bucket_name)

    @staticmethod
    def ensure_element_found(response, space_name, bucket_name, element_id):
        if response.status == 404 and response.data and response.data['errorCode'] == ELEMENT_DOES_NOT_EXIST:
            raise ElementDoesNotExistException(space_name, bucket_name, element_id)

    @staticmethod
    def ensure_status_2xx(response):
        if response.status >= 300 or response.status < 200:
            raise UnknownError("Unexpected status code: %s" % response.status)

    @staticmethod
    def parse_elements(data: dict):
        return [Element(f['id'], EasydbClient.parse_element_fields(f['fields'])) for f in data]

    @staticmethod
    def parse_element_fields(data: dict):
        return [ElementField(f['name'], f['value']) for f in data]