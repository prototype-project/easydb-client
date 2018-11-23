import aiohttp
from typing import List


class SpaceDoesNotExistException(Exception):
    def __init__(self, space_name):
        super().__init__()
        self.space_name = space_name

class BucketDoesNotExistException(Exception):
    def __init__(self, spaceName, bucket_name):
        super().__init__()
        self.space_name = spaceName
        self.bucket_name = bucket_name

class UnknownError(Exception):
    def __init__(self, msg):
        super().__init__(msg)


SPACE_DOES_NOT_EXIST = 'SPACE_DOES_NOT_EXIST'
BUCKET_DOES_NOT_EXIST = 'BUCKET_DOES_NOT_EXIST'

class Request:
    def __init__(self, path: str, method: str, data: dict=None):
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


class ElementToCreate:
    def __init__(self, fields: List[ElementField]=None):
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

    def as_json(self):
        return {'fields': [dict((('name', f.name), ('value', f.value))) for f in self.fields]}

class CreatedElement(ElementToCreate):
    def __init__(self, identifier:str, bucket_name: str, fields: List[ElementField]=None):
        super().__init__(fields)
        self.identifier = identifier
        self.bucket_name = bucket_name

    def __eq__(self, other):
        return self.identifier == other.identifier and \
               self.bucket_name == other.bucket_name and \
               self.fields == other.fields

    def __hash__(self):
        return hash((self.identifier, self.bucket_name, self.fields))

    def __str__(self):
        fields_str = ", ".join(['{%s = %s}' % (f.name, f.value) for f in self.fields])
        return 'CreatedElement(identifier=%s, bucket_name=%s, fields=[ %s ])' % (self.identifier, self.bucket_name, fields_str)


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

    async def add_element(self, space_name, bucket_name, element: ElementToCreate):
        response = await self.perform_request(Request("%s/%s/%s" % (self.server_url, space_name, bucket_name), 'POST', data=element.as_json()))

        self.ensure_space_found(response, space_name)
        self.ensure_bucket_found(response, space_name, bucket_name)
        self.ensure_status_2xx(response)
        data = response.data
        return CreatedElement(data['id'], data['bucketName'], [ElementField(f['name'], f['value']) for f in data['fields']])

    async def delete_bucket(self, space_name, bucket_name):
        response = await self.perform_request(Request("%s/%s/%s" % (self.server_url, space_name, bucket_name), 'DELETE'))

        self.ensure_space_found(response, space_name)
        self.ensure_bucket_found(response, space_name, bucket_name)
        self.ensure_status_2xx(response)

    async def perform_request(self, request: Request):
        async with aiohttp.ClientSession() as session:
            if request.method == 'GET':
                async with session.get(request.path) as response:
                    return ResponseData(response.status, await response.json())
            elif request.method == 'POST':
                async with session.post(request.path, json=request.data) as response:
                    return ResponseData(response.status, await response.json())
            elif request.method == 'DELETE':
                async with session.delete(request.path) as response:
                    return ResponseData(response.status, await response.json())
            else:
                raise Exception("Incorrect request type")

    def ensure_space_found(self, response, space_name):
        if response.status == 404 and response.data and response.data['errorCode'] == SPACE_DOES_NOT_EXIST:
            raise SpaceDoesNotExistException(space_name)

    def ensure_bucket_found(self, response, space_name, bucket_name):
        if response.status == 404 and response.data and response.data['errorCode'] == BUCKET_DOES_NOT_EXIST:
            raise BucketDoesNotExistException(space_name, bucket_name)

    def ensure_status_2xx(self, response):
        if response.status >= 300 or response.status < 200:
            raise UnknownError("Unexpected status code: %s" % response.status)