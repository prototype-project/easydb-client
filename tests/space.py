import unittest

from .base_test import HttpTest

from client import EasydbClient


class SpaceTests(HttpTest):
    def __init__(self, methodName):
        super().__init__(methodName)
        self.register_route('/api/v1/spaces', 'POST', 'create_space.json', "create_space.json", 200)

    def setUp(self):
        super().setUp()
        self.easydb_client = EasydbClient(self.server_url)

    def test_should_create_space(self):
        space_name = self.loop.run_until_complete(self.easydb_client.create_space())
        self.assertEqual(space_name, "exampleSpace")


def suite():
    suite = unittest.TestSuite()
    suite.addTest(SpaceTests("SpaceTests"))
    return suite

def run_tests():
    unittest.main()

if __name__ == '__main__':
    run_tests()