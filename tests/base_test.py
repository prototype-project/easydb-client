import json
import os

from aiohttp.web_request import Request
from aiohttp.web_routedef import route
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, TestServer


class RequestMappingKey:
    def __init__(self, path: str, method: str = None, request: dict = None, query_params: dict = None):
        self.path = path
        self.method = method
        self.request = request
        self.query_params = query_params
        if not self.query_params:
            self.query_params = {}

    def __hash__(self):
        return hash((self.path, self.method, json.dumps(self.request), tuple(self.query_params)))

    def __eq__(self, other):
        return self.path == other.path and \
               self.method == other.method and \
               self.request == other.request and \
               tuple(self.query_params) == tuple(other.query_params)

    def __str__(self):
        return "RequestMappingKey(path = %s, method = %s, request = %s, query_params = %s)" % \
               (self.path, self.method, self.request, self.query_params)

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
    def __init__(self, response_body: dict=None):
        self.count = 0
        self.response_body = response_body if response_body else {}


class HttpTest(AioHTTPTestCase):
    rules_path = os.path.join(os.path.dirname(__file__), 'rules')
    server_port = 8000
    server_url = 'http://localhost:' + str(server_port)

    def setUp(self):
        self.request_mapping = {}
        self.routes = set()

        self.web_app = web.Application()
        setup_method = getattr(self, 'before_' + self._testMethodName, None)
        if setup_method:
            setup_method()
        super().setUp()

    async def get_application(self):
        return self.web_app

    async def get_server(self, app):
        return TestServer(app=app, loop=self.loop, port=self.server_port)

    def register_route(self, path: str, method: str, status: int = 200, request_file: str = None,
                       response_file: str = None, query_params: dict = None):
        request_body = self.read_file(request_file)
        response_body = self.read_file(response_file)

        self.request_mapping[RequestMappingKey(path, method, request_body, query_params)] = RequestMappingValue(response_body)

        if path not in self.routes:
            self.web_app.router.add_routes(
                [route(method, path, handler=self.handler_factory(request_body, status))])
        self.routes.add(path)

    def handler_factory(self, request_body: dict, status=200):
        async def handle(request: Request):
            if request.can_read_body and await request.json() != request_body:
                raise RequestNotMatchException(await request.json(), request_body)

            request_mapping = self.request_mapping[RequestMappingKey(request.path, request.method, request_body, dict(request.query))]
            request_mapping.count += 1
            return web.Response(body=json.dumps(request_mapping.response_body), headers={'Content-Type': 'application/json'},
                                status=status)

        return handle

    def verify(self, path: str, method: str, request_file: str = None, query_params: dict = None):
        request_body = self.read_file(request_file)
        return self.request_mapping.get(RequestMappingKey(path, method, request_body, query_params), RequestMappingValue()).count

    def read_file(self, file: str = None):
        if file:
            with open(HttpTest.rules_path + '/' + file) as file:
                return json.load(file)
