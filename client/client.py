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
    def __init__(self, path: str, method: str, data: dict = None):
        self.path = path
        self.method = method
        self.data = data


class ResponseData:
    def __init__(self, status: int, data: dict):
        self.status = status
        self.data = data


class Space:
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return 'Space(name=%s)' % self.name

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


class ElementFields:
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
        return 'ElementToCreate(fields=[ %s ])' % fields_str

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
        self.element_fields = ElementFields(fields)
        self.identifier = identifier

    def __eq__(self, other):
        return self.identifier == other.identifier and \
               self.fields == other.fields

    def __hash__(self):
        return hash((self.identifier, self.fields))

    def __str__(self):
        fields_str = ", ".join(['{%s = %s}' % (f.name, f.value) for f in self.fields])
        return 'CreatedElement(identifier=%s, fields=[ %s ])' % (
            self.identifier, fields_str)

    @property
    def fields(self):
        return self.element_fields.fields

    def add_field(self, name, value):
        self.element_fields.add_field(name, value)
        return self


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

    async def add_element(self, space_name, bucket_name, element_fields: ElementFields):
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

    async def update_element(self, space_name, bucket_name, element_id, element_fields: ElementFields):
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
        fields = [ElementField(f['name'], f['value']) for f in response.data['fields']]
        return Element(element_id, fields)

    @staticmethod
    async def perform_request(request: Request):
        async with aiohttp.ClientSession() as session:
            if request.method in ['GET', 'POST', 'DELETE', 'PUT']:
                async with session.request(request.method, request.path) as response:
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
