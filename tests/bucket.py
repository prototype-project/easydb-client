from client import EasydbClient, ElementFields, ElementField, Element, SpaceDoesNotExistException, \
    BucketDoesNotExistException, ElementDoesNotExistException
from tests.base_test import HttpTest


class BucketTests(HttpTest):
    def setUp(self):
        super().setUp()
        self.easydb_client = EasydbClient(self.server_url)

    def before_test_should_add_element_to_bucket(self):
        self.register_route('/api/v1/exampleSpace/users', 'POST', 201, 'element_request.json',
                            'element_response.json')

    def before_test_should_throw_error_when_adding_element_in_not_existing_space(self):
        self.register_route('/api/v1/notExistingSpace/users', 'POST', 404, 'element_request.json',
                            'not_existing_space_response.json')

    def before_test_should_delete_bucket(self):
        self.register_route('/api/v1/exampleSpace/users', 'DELETE', 200)

    def before_test_should_throw_error_when_deleting_bucket_in_not_existing_space(self):
        self.register_route('/api/v1/notExistingSpace/users', 'DELETE', 404,
                            response_file='not_existing_space_response.json')

    def before_test_should_throw_error_when_deleting_not_existing_bucket(self):
        self.register_route('/api/v1/exampleSpace/notExistingBucket', 'DELETE', 404,
                            response_file='not_existing_bucket_response.json')

    def before_test_should_delete_element(self):
        self.register_route('/api/v1/exampleSpace/users/elementId', 'DELETE', 200)

    def before_test_should_throw_error_when_deleting_element_from_not_existing_space(self):
        self.register_route('/api/v1/notExistingSpace/users/elementId', 'DELETE', 404,
                            response_file='not_existing_space_response.json')

    def before_test_should_throw_error_when_deleting_element_from_not_existing_bucket(self):
        self.register_route('/api/v1/exampleSpace/notExistingBucket/elementId', 'DELETE', 404,
                            response_file='not_existing_bucket_response.json')

    def before_test_should_throw_error_when_deleting_not_existing_element(self):
        self.register_route('/api/v1/exampleSpace/users/notExistingElement', 'DELETE', 404,
                            response_file='not_existing_element_response.json')

    def before_test_should_update_element(self):
        self.register_route('/api/v1/exampleSpace/users/elementId', 'PUT', 200, request_file='element_request.json')

    def before_test_should_throw_error_when_updating_element_from_not_existing_space(self):
        self.register_route('/api/v1/notExistingSpace/users/elementId', 'PUT', 404, request_file='element_request.json',
                            response_file='not_existing_space_response.json')

    def before_test_should_throw_error_when_updating_element_from_not_existing_bucket(self):
        self.register_route('/api/v1/exampleSpace/notExistingBucket/elementId', 'PUT', 404,
                            request_file='element_request.json', response_file='not_existing_bucket_response.json')

    def before_test_should_throw_error_when_updating_not_existing_element(self):
        self.register_route('/api/v1/exampleSpace/users/notExistingElement', 'PUT', 404,
                            request_file='element_request.json', response_file='not_existing_element_response.json')

    def before_test_should_get_element(self):
        self.register_route('/api/v1/exampleSpace/users/elementId', 'GET', 200, response_file='element_response.json')

    def before_test_should_throw_error_when_getting_element_from_not_existing_space(self):
        self.register_route('/api/v1/notExistingSpace/users/elementId', 'GET', 404,
                            response_file='not_existing_space_response.json')

    def before_test_should_throw_error_when_getting_element_from_not_existing_bucket(self):
        self.register_route('/api/v1/exampleSpace/notExistingBucket/elementId', 'GET', 404,
                            response_file='not_existing_bucket_response.json')

    def before_test_should_throw_error_when_getting_not_existing_element(self):
        self.register_route('/api/v1/exampleSpace/users/notExistingElement', 'GET', 404,
                            response_file='not_existing_element_response.json')

    def test_should_add_element_to_bucket(self):
        # when
        created_element = self.loop.run_until_complete(self.easydb_client. \
                                                       add_element('exampleSpace', 'users',
                                                                   ElementFields()
                                                                   .add_field('firstName', 'John')
                                                                   .add_field('lastName', 'Smith')))

        # then
        self.assertEqual(self.verify('/api/v1/exampleSpace/users', 'POST', 'element_request.json'), 1)
        self.assertEqual(created_element,
                         Element('elementId')
                         .add_field('firstName', 'John')
                         .add_field('lastName', 'Smith'))

    def test_should_throw_error_when_adding_element_in_not_existing_space(self):
        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client. \
                                         add_element('notExistingSpace', 'users',
                                                     ElementFields()
                                                     .add_field('firstName', 'John')
                                                     .add_field('lastName', 'Smith')))

        # and
        self.assertEqual(self.verify('/api/v1/notExistingSpace/users', 'POST', 'element_request.json'), 1)

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

        # and
        self.assertEqual(self.verify('/api/v1/exampleSpace/notExistingBucket', 'DELETE'), 1)

    def test_should_delete_element(self):
        # when
        self.loop.run_until_complete(self.easydb_client.delete_element('exampleSpace', 'users', 'elementId'))

        # then
        self.assertEqual(self.verify('/api/v1/exampleSpace/users/elementId', 'DELETE'), 1)

    def test_should_throw_error_when_deleting_element_from_not_existing_space(self):
        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.delete_element('notExistingSpace', 'users', 'elementId'))

        # and
        self.assertEqual(self.verify('/api/v1/notExistingSpace/users/elementId', 'DELETE'), 1)

    def test_should_throw_error_when_deleting_element_from_not_existing_bucket(self):
        # expect
        with self.assertRaises(BucketDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.delete_element('exampleSpace', 'notExistingBucket', 'elementId'))

        # and
        self.assertEqual(self.verify('/api/v1/exampleSpace/notExistingBucket/elementId', 'DELETE'), 1)

    def test_should_throw_error_when_deleting_not_existing_element(self):
        # expect
        with self.assertRaises(ElementDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.delete_element('exampleSpace', 'users', 'notExistingElement'))

        # and
        self.assertEqual(self.verify('/api/v1/exampleSpace/users/notExistingElement', 'DELETE'), 1)

    def test_should_update_element(self):
        # when
        self.loop.run_until_complete(self.easydb_client.update_element('exampleSpace', 'users', 'elementId',
                                                                       ElementFields()
                                                                       .add_field('firstName', 'John')
                                                                       .add_field('lastName', 'Smith')))
        # then
        self.assertEqual(self.verify('/api/v1/exampleSpace/users/elementId', 'PUT', 'element_request.json'), 1)

    def test_should_throw_error_when_updating_element_from_not_existing_space(self):
        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.update_element('notExistingSpace', 'users', 'elementId',
                                                  ElementFields()
                                                  .add_field('firstName', 'John')
                                                  .add_field('lastName', 'Smith')))

        # and
        self.assertEquals(self.verify('/api/v1/notExistingSpace/users/elementId', 'PUT', 'element_request.json'), 1)

    def test_should_throw_error_when_updating_element_from_not_existing_bucket(self):
        # expect
        with self.assertRaises(BucketDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.update_element('exampleSpace', 'notExistingBucket', 'elementId',
                                                  ElementFields()
                                                  .add_field('firstName', 'John')
                                                  .add_field('lastName', 'Smith')))

        # and
        self.assertEqual(
            self.verify('/api/v1/exampleSpace/notExistingBucket/elementId', 'PUT', 'element_request.json'), 1)

    def test_should_throw_error_when_updating_not_existing_element(self):
        # expect
        with self.assertRaises(ElementDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.update_element('exampleSpace', 'users', 'notExistingElement',
                                                  ElementFields()
                                                  .add_field('firstName', 'John')
                                                  .add_field('lastName', 'Smith')))

        # and
        self.assertEquals(self.verify('/api/v1/exampleSpace/users/notExistingElement', 'PUT', 'element_request.json'), 1)

    def test_should_get_element(self):
        # when
        element = self.loop.run_until_complete(self.easydb_client.get_element('exampleSpace', 'users', 'elementId'))

        # then
        self.assertEqual(element, Element('elementId').add_field('firstName', 'John').add_field('lastName', 'Smith'))
        self.assertEqual(self.verify('/api/v1/exampleSpace/users/elementId', 'GET'), 1)

    def test_should_throw_error_when_getting_element_from_not_existing_space(self):
        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.get_element('notExistingSpace', 'users', 'elementId'))

        # and
        self.assertEqual(self.verify('/api/v1/notExistingSpace/users/elementId', 'GET'), 1)


    def test_should_throw_error_when_getting_element_from_not_existing_bucket(self):
        # expect
        with self.assertRaises(BucketDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.get_element('exampleSpace', 'notExistingBucket', 'elementId'))

        # and
        self.assertEqual(self.verify('/api/v1/exampleSpace/notExistingBucket/elementId', 'GET'), 1)

    def test_should_throw_error_when_getting_not_existing_element(self):
        # expect
        with self.assertRaises(ElementDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.get_element('exampleSpace', 'users', 'notExistingElement'))

        # and
        self.assertEqual(self.verify('/api/v1/exampleSpace/users/notExistingElement', 'GET'), 1)
