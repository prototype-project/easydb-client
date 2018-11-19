import unittest

from client.client import SpaceNotFoundException
from .base_test import HttpTest

from client import EasydbClient


class SpaceTests(HttpTest):

    def setUp(self):
        super().setUp()
        self.easydb_client = EasydbClient(self.server_url)

    def before_test_should_create_space(self):
        self.register_route('/api/v1/spaces', 'POST', 200, 'space.json', "space.json")

    def before_test_should_delete_space(self):
        self.register_route('/api/v1/spaces/exampleSpace', 'DELETE', 200)

    def before_test_should_throw_error_when_deleting_not_existing_space(self):
        self.register_route('/api/v1/spaces/exampleSpace', 'DELETE', 404)

    def before_test_should_get_space(self):
        self.register_route('/api/v1/spaces/exampleSpace', 'GET', 200, response_file="space.json")

    def before_test_should_throw_error_when_getting_not_existing_space(self):
        self.register_route('/api/v1/spaces/exampleSpace', 'GET', 404)


    def test_should_create_space(self):
        # when
        space_name = self.loop.run_until_complete(self.easydb_client.create_space())

        # then
        self.assertEqual(self.verify('/api/v1/spaces', 'POST', 'space.json'), 1)
        self.assertEqual(space_name, 'exampleSpace')

    def test_should_delete_space(self):
        # when
        self.loop.run_until_complete(self.easydb_client.delete_space('exampleSpace'))

        # then
        self.assertEqual(self.verify('/api/v1/spaces/exampleSpace', 'DELETE'), 1)

    def test_should_throw_error_when_deleting_not_existing_space(self):
        # expect
        with self.assertRaises(SpaceNotFoundException):
            self.loop.run_until_complete(self.easydb_client.delete_space('exampleSpace'))

        # and
        self.assertEqual(self.verify('/api/v1/spaces/exampleSpace', 'DELETE'), 1)

    def test_should_get_space(self):
        # when
        space = self.loop.run_until_complete(self.easydb_client.get_space('exampleSpace'))

        # then
        self.assertEqual(self.verify('/api/v1/spaces/exampleSpace', 'GET'), 1)
        self.assertEqual(space.name, 'exampleSpace')

    def test_should_throw_error_when_getting_not_existing_space(self):
        # expect
        with self.assertRaises(SpaceNotFoundException):
            self.loop.run_until_complete(self.easydb_client.get_space('exampleSpace'))

        # and
        self.assertEqual(self.verify('/api/v1/spaces/exampleSpace', 'GET'), 1)


def suite():
    test_suite = unittest.TestSuite()
    test_suite.addTest(SpaceTests("SpaceTests"))
    return suite

def run_tests():
    unittest.main()

if __name__ == '__main__':
    run_tests()