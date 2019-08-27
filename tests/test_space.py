from aioresponses import aioresponses

from easydb import EasydbClient, SpaceDoesNotExistException
from tests.base_test import BaseTest


class SpaceTests(BaseTest):
    def setUp(self):
        super().setUp()
        self.easydb_client = EasydbClient(self.server_url)
        self.spaces_url = self.server_url + '/api/v1/spaces'

    @aioresponses()
    def test_should_create_space(self, mocked: aioresponses):
        # given
        mocked.post(self.spaces_url, status=201, payload=dict(spaceName="exampleSpace"))

        # when
        space_name = self.loop.run_until_complete(self.easydb_client.create_space())

        # then
        self.assertEqual(space_name, 'exampleSpace')

    @aioresponses()
    def test_should_delete_space(self, mocked: aioresponses):
        # given
        mocked.delete(self.spaces_url + "/exampleSpace")

        # when
        self.loop.run_until_complete(self.easydb_client.delete_space('exampleSpace'))

    @aioresponses()
    def test_should_throw_error_when_deleting_not_existing_space(self, mocked: aioresponses):
        # given
        mocked.delete(self.spaces_url + "/notExistingSpace", status=404, payload={"errorCode": "SPACE_DOES_NOT_EXIST",
                                                                                  "status": "NOT_FOUND",
                                                                                  "message": "Space notExistingSpace doues not exist"})

        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.delete_space('notExistingSpace'))

    @aioresponses()
    def test_should_get_space(self, mocked: aioresponses):
        # given
        mocked.get(self.spaces_url + "/exampleSpace", payload=dict(spaceName="exampleSpace"))

        # when
        space = self.loop.run_until_complete(self.easydb_client.get_space('exampleSpace'))

        # then
        self.assertEqual(space.name, 'exampleSpace')

    @aioresponses()
    def test_should_throw_error_when_getting_not_existing_space(self, mocked: aioresponses):
        # given
        mocked.get(self.spaces_url + "/notExistingSpace", status=404, payload={"errorCode": "SPACE_DOES_NOT_EXIST",
                                                                   "status": "NOT_FOUND",
                                                                   "message": "Space notExistingSpace doues not exist"})

        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.get_space('notExistingSpace'))