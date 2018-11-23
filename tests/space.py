import unittest

from .base_test import HttpTest

from client import EasydbClient, ElementToCreate, CreatedElement, SpaceDoesNotExistException, BucketDoesNotExistException


class SpaceTests(HttpTest):
    def setUp(self):
        super().setUp()
        self.easydb_client = EasydbClient(self.server_url)

    def before_test_should_create_space(self):
        self.register_route('/api/v1/spaces', 'POST', 200, 'space.json', "space.json")

    def before_test_should_delete_space(self):
        self.register_route('/api/v1/spaces/exampleSpace', 'DELETE', 200)

    def before_test_should_throw_error_when_deleting_not_existing_space(self):
        self.register_route('/api/v1/spaces/notExistingSpace', 'DELETE', 404, response_file='not_existing_space_response.json')

    def before_test_should_get_space(self):
        self.register_route('/api/v1/spaces/exampleSpace', 'GET', 200, response_file="space.json")

    def before_test_should_throw_error_when_getting_not_existing_space(self):
        self.register_route('/api/v1/spaces/notExistingSpace', 'GET', 404, response_file='not_existing_space_response.json')

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
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.delete_space('notExistingSpace'))

        # and
        self.assertEqual(self.verify('/api/v1/spaces/notExistingSpace', 'DELETE'), 1)

    def test_should_get_space(self):
        # when
        space = self.loop.run_until_complete(self.easydb_client.get_space('exampleSpace'))

        # then
        self.assertEqual(self.verify('/api/v1/spaces/exampleSpace', 'GET'), 1)
        self.assertEqual(space.name, 'exampleSpace')

    def test_should_throw_error_when_getting_not_existing_space(self):
        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.get_space('notExistingSpace'))

        # and
        self.assertEqual(self.verify('/api/v1/spaces/notExistingSpace', 'GET'), 1)


class BucketTests(HttpTest):
    def setUp(self):
        super().setUp()
        self.easydb_client = EasydbClient(self.server_url)

    def before_test_should_add_element_to_bucket(self):
        self.register_route('/api/v1/exampleSpace/users', 'POST', 201, 'add_element_request.json',
                            'add_element_response.json')

    def before_test_should_throw_error_when_adding_element_in_not_existing_space(self):
        self.register_route('/api/v1/notExistingSpace/users', 'POST', 404, 'add_element_request.json', 'not_existing_space_response.json')

    def before_test_should_delete_bucket(self):
        self.register_route('/api/v1/exampleSpace/users', 'DELETE', 200)

    def before_test_should_throw_error_when_deleting_bucket_in_not_existing_space(self):
        self.register_route('/api/v1/notExistingSpace/users', 'DELETE', 404, response_file='not_existing_space_response.json')

    def before_test_should_throw_error_when_deleting_not_existing_bucket(self):
        self.register_route('/api/v1/exampleSpace/notExistingBucket', 'DELETE', 404, response_file='not_existing_bucket_response.json')

    def test_should_add_element_to_bucket(self):
        # when
        created_element = self.loop.run_until_complete(self.easydb_client. \
                                                       add_element('exampleSpace', 'users',
                                                                   ElementToCreate()
                                                                   .add_field('firstName', 'John')
                                                                   .add_field('lastName', 'Smith')))

        # then
        self.assertEqual(self.verify('/api/v1/exampleSpace/users', 'POST', 'add_element_request.json'), 1)
        self.assertEqual(created_element,
                         CreatedElement('elementId', 'users')
                         .add_field('firstName', 'John')
                         .add_field('lastName', 'Smith'))

    def test_should_throw_error_when_adding_element_in_not_existing_space(self):
        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client. \
                                         add_element('notExistingSpace', 'users',
                                                     ElementToCreate()
                                                     .add_field('firstName', 'John')
                                                     .add_field('lastName', 'Smith')))

        # and
        self.assertEqual(self.verify('/api/v1/notExistingSpace/users', 'POST', 'add_element_request.json'), 1)

    def test_should_delete_bucket(self):
        # when
        self.loop.run_until_complete(self.easydb_client.delete_bucket('exampleSpace', 'users'))

        # then
        self.assertEqual(self.verify('/api/v1/exampleSpace/users', 'DELETE'), 1)

    def test_should_throw_error_when_deleting_bucket_in_not_existing_space(self):
        # except
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.delete_bucket('notExistingSpace', 'users'))

        # and
        self.assertEqual(self.verify('/api/v1/notExistingSpace/users', 'DELETE'), 1)

    def test_should_throw_error_when_deleting_not_existing_bucket(self):
        # except
        with self.assertRaises(BucketDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.delete_bucket('exampleSpace', 'notExistingBucket'))

        self.assertEqual(self.verify('/api/v1/exampleSpace/notExistingBucket', 'DELETE'), 1)

def suite():
    test_suite = unittest.TestSuite()
    test_suite.addTest(SpaceTests("SpaceTests"))
    test_suite.addTest(BucketTests("BucketTests"))
    return suite


def run_tests():
    unittest.main()


if __name__ == '__main__':
    run_tests()
