import json
import os

from aiohttp.web_request import Request
from aiohttp.web_routedef import route
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, TestServer


class RequestMappingKey:
    def __init__(self, path: str, method: str = None, request: dict = None):
        self.path = path
        self.method = method
        self.request = request

    def __hash__(self):
        return hash((self.path, self.method, self.request))

    def __eq__(self, other):
        return self.path == other.path and \
               self.method == other.method and \
               self.request == other.request

    def __str__(self):
        return "RequestMappingKey(path = %s, method = %s, request = %s)" % (self.path, self.method, self.request)

    def __repr__(self):
        return self.__str__()


class RequestNotMatchException(Exception):
    def __init__(self, actual, required):
        self.actual = actual
        self.required = required

    def __str__(self):
        return 'RequestNotMatchException(actual = %s, required = %s)' % (self.actual, self.required)

    def __repr__(self):
        return self.__str__()


class RequestMappingValue:
    def __init__(self):
        self.count = 0


class HttpTest(AioHTTPTestCase):
    rules_path = os.path.join(os.path.dirname(__file__), 'rules')
    server_port = 8080
    server_url = 'http://localhost:' + str(server_port)

    def __init__(self, methodName):
        super().__init__(methodName)
        self.request_mapping = {}
        self.web_app = web.Application()

    def setUp(self):
        super().setUp()
        self.reset_mapping()

    def reset_mapping(self):
        self.request_mapping = {k : RequestMappingValue() for (k, _) in self.request_mapping.items()}

    async def get_application(self):
        return self.web_app

    async def get_server(self, app):
        return TestServer(app=app, loop=self.loop, port=self.server_port)

    def register_route(self, path: str, method: str, status: int=200, request_file: str=None, response_file: str=None):
        request_body = self.read_file(request_file)
        response_body = self.read_file(response_file)

        self.web_app.router.add_routes(
            [route(method, path, handler=self.handler_factory(request_body, response_body, status))])

        self.request_mapping[RequestMappingKey(path, method, request_body)] = RequestMappingValue()

    def handler_factory(self, request_body: dict, response_body: dict, status=200):
        async def handle(request: Request):
            if request.body_exists and await request.json() != request_body:
                raise RequestNotMatchException(await request.json(), request_body)

            self.request_mapping[RequestMappingKey(request.path, request.method, request_body)].count += 1
            return web.Response(body=response_body, headers={'Content-Type': 'application/json'}, status=status)

        return handle

    def verify(self, path: str, method: str, request_file: str=None):
        request_body = self.read_file(request_file)
        return self.request_mapping.get(RequestMappingKey(path, method, request_body), RequestMappingValue()).count

    def read_file(self, file: str=None):
        if file:
            with open(HttpTest.rules_path + '/' + file) as file:
                return json.dumps(json.load(file))