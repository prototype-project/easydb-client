import aiohttp


class SpaceNotFoundException(Exception):
    def __init__(self, spaceName):
        self.spaceName = spaceName


class Request:
    def __init__(self, path: str, method: str, data=None):
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


class EasydbClient:
    def __init__(self, server_url: str):
        self.server_url = server_url + "/api/v1"

    async def create_space(self):
        response = await self.perform_request(Request("%s/spaces" % self.server_url, 'POST'))
        return response.data['spaceName']

    async def delete_space(self, space_name):
        response = await self.perform_request(Request("%s/spaces/%s" % (self.server_url, space_name), 'DELETE'))

        if response.status == 404:
            raise SpaceNotFoundException(space_name)

    async def get_space(self, space_name):
        response = await self.perform_request(Request("%s/spaces/%s" % (self.server_url, space_name), 'GET'))

        if response.status == 404:
            raise SpaceNotFoundException(space_name)
        return Space(response.data['spaceName'])

    async def perform_request(self, request: Request):
        async with aiohttp.ClientSession() as session:
            if request.method == 'GET':
                async with session.get(request.path) as response:
                    return ResponseData(response.status, await response.json())
            elif request.method == 'POST':
                async with session.post(request.path) as response:
                    return ResponseData(response.status, await response.json())
            elif request.method == 'DELETE':
                async with session.delete(request.path) as response:
                    return ResponseData(response.status, await response.json())
            else:
                raise Exception("Incorrect request type")
