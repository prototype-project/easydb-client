import unittest

from .base_test import HttpTest

from client import EasydbClient


class SpaceTests(HttpTest):
    def __init__(self, methodName):
        super().__init__(methodName)
        self.register_route('/api/v1/spaces', 'POST', 200, 'create_space.json', "create_space.json")
        self.register_route('/api/v1/spaces', 'DELETE', 200)

    def setUp(self):
        super().setUp()
        self.easydb_client = EasydbClient(self.server_url)

    def test_should_create_space(self):
        # when
        space_name = self.loop.run_until_complete(self.easydb_client.create_space())

        # then
        self.assertEqual(self.verify('/api/v1/spaces', 'POST', 'create_space.json'), 1)
        self.assertEqual(space_name, 'exampleSpace')


    def test_should_delete_space(self):
        # when
        self.loop.run_until_complete(self.easydb_client.delete_space('exampleSpace'))

        # then
        self.assertEqual(self.verify('/api/v1/spaces', 'DELETE'), 1)


def suite():
    test_suite = unittest.TestSuite()
    test_suite.addTest(SpaceTests("SpaceTests"))
    return suite

def run_tests():
    unittest.main()

if __name__ == '__main__':
    run_tests()