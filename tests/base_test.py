import json
import os

from aiohttp.web_request import Request
from aiohttp.web_routedef import route
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, TestServer


class RequestRecord:
    def __init__(self, path: str, method: str, request_body: dict = None,
                 query_params: dict = None):
        self.path = path
        self.method = method
        self.request_body = request_body if request_body else {}
        self.query_params = query_params if query_params else {}

    def __repr__(self):
        return 'RequestRecord(path = %s, method = %s, request_body = %s, query_params = %s)' %\
               (self.path, self.method, self.request_body, self.query_params)

    def __str__(self):
        return self.__repr__()

class ResponseRecord:
    def __init__(self, status: int = 200, response_body: dict = None):
        self.invokaction_count = 0
        self.status = status
        self.response_body = response_body if response_body else {}

    def mark_invocation(self):
        self.invokaction_count += 1


class RequestNotMatchException(Exception):
    def __init__(self, actual: RequestRecord, closest_match: RequestRecord):
        self.actual = actual
        self.closest_match = closest_match

    def __repr__(self):
        return 'RequestNotMatchException(given = %s, closest_match = %s)' % (self.actual, self.closest_match)

    def __str__(self):
        return self.__repr__()

# /users/ POST req = {} query_params = {} -> status  resp = {}

class RequestMappingRecorder:
    NUMBER_OF_RECORD_ATTRIBUTES = 4
    def __init__(self):
        self.records = []

    def add_record(self, request: RequestRecord, response: ResponseRecord):
        self.records.append((request, response))

    def find_closest(self, given: RequestRecord):
        max_match_fields = 0
        closest = None

        for req, res in self.records_for_route(given.path, given.method):
            match_fields = 2 # path and method match for sure
            if not req.request_body or req.request_body == given.request_body:
                match_fields += 1
            if not req.query_params or req.query_params == given.query_params:
                match_fields += 1
            if match_fields > max_match_fields:
                closest = req, res
                max_match_fields = match_fields

        if max_match_fields != self.NUMBER_OF_RECORD_ATTRIBUTES:
            raise RequestNotMatchException(given, closest[0])

        return closest[1]

    def contains_route(self, path:str, method:str):
        return len(self.records_for_route(path, method))

    def records_for_route(self, path, method):
        return list(filter(lambda r: r[0].path == path and r[0].method == method, self.records))


class HttpTest(AioHTTPTestCase):
    rules_path = os.path.join(os.path.dirname(__file__), 'rules')
    server_port = 8000
    server_url = 'http://localhost:' + str(server_port)

    def setUp(self):
        self.request_recorder = RequestMappingRecorder()
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

        if not self.request_recorder.contains_route(path, method):
            self.web_app.router.add_routes(
                [route(method, path, handler=self.handler_factory())])

        self.request_recorder.add_record(RequestRecord(path, method, request_body, query_params),
                                         ResponseRecord(status, response_body))

    def handler_factory(self):
        async def handle(request: Request):
            body = await request.json() if request.can_read_body else None
            expected_response = self.request_recorder.find_closest(
                RequestRecord(request.path, request.method, body, dict(request.query)))
            expected_response.mark_invocation()

            return web.Response(body=json.dumps(expected_response.response_body),
                                headers={'Content-Type': 'application/json'},
                                status=expected_response.status)

        return handle

    def verify(self, path: str, method: str, request_file: str = None, query_params: dict = None):
        request_body = self.read_file(request_file)
        return self.request_recorder.find_closest(
            RequestRecord(path, method, request_body, query_params)).invokaction_count

    @staticmethod
    def read_file(file: str = None):
        if file:
            with open(HttpTest.rules_path + '/' + file) as file:
                return json.load(file)
