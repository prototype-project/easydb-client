from easydb import EasydbClient, MultipleElementFields, Element, SpaceDoesNotExistException, \
    BucketDoesNotExistException, ElementDoesNotExistException, FilterQuery, BucketAlreadyExistsException
from tests.base_test import HttpTest


class BucketTests(HttpTest):
    def setUp(self):
        super().setUp()
        self.easydb_client = EasydbClient(self.server_url)

    def before_test_should_add_element_to_bucket(self):
        self.register_route('/api/v1/spaces/exampleSpace/buckets/users/elements', 'POST', 201, 'element_request.json',
                            'element_response.json')

    def before_test_should_throw_error_when_adding_element_in_not_existing_space(self):
        self.register_route('/api/v1/spaces/notExistingSpace/buckets/users/elements', 'POST', 404, 'element_request.json',
                            'not_existing_space_response.json')

    def before_test_should_throw_error_when_adding_element_in_not_existing_bucket(self):
        self.register_route(self.build_element_url('exampleSpace', 'notExistingBucket'), 'POST', 404, 'element_request.json',
                            'not_existing_bucket_response.json')

    def before_test_should_create_bucket(self):
        self.register_route(self.build_bucket_url("exampleSpace"), "POST", 201, 'bucket.json')

    def before_test_should_throw_error_when_creating_bucket_in_not_existing_space(self):
        self.register_route(self.build_bucket_url('notExistingSpace'), 'POST', 404, 'bucket.json', 'not_existing_space_response.json')

    def before_test_should_throw_error_when_creating_already_existing_bucket(self):
        self.register_route(self.build_bucket_url('exampleSpace'), 'POST', 400, 'bucket.json', 'bucket_already_exists.json')

    def before_test_should_delete_bucket(self):
        self.register_route('/api/v1/spaces/exampleSpace/buckets/users', 'DELETE', 200)

    def before_test_should_throw_error_when_deleting_bucket_in_not_existing_space(self):
        self.register_route('/api/v1/spaces/notExistingSpace/buckets/users', 'DELETE', 404,
                            response_file='not_existing_space_response.json')

    def before_test_should_throw_error_when_deleting_not_existing_bucket(self):
        self.register_route('/api/v1/spaces/exampleSpace/buckets/notExistingBucket', 'DELETE', 404,
                            response_file='not_existing_bucket_response.json')

    def before_test_should_delete_element(self):
        self.register_route('/api/v1/spaces/exampleSpace/buckets/users/elements/elementId', 'DELETE', 200)

    def before_test_should_throw_error_when_deleting_element_from_not_existing_space(self):
        self.register_route('/api/v1/spaces/notExistingSpace/buckets/users/elements/elementId', 'DELETE', 404,
                            response_file='not_existing_space_response.json')

    def before_test_should_throw_error_when_deleting_element_from_not_existing_bucket(self):
        self.register_route('/api/v1/spaces/exampleSpace/buckets/notExistingBucket/elements/elementId', 'DELETE', 404,
                            response_file='not_existing_bucket_response.json')

    def before_test_should_throw_error_when_deleting_not_existing_element(self):
        self.register_route('/api/v1/spaces/exampleSpace/buckets/users/elements/notExistingElement', 'DELETE', 404,
                            response_file='not_existing_element_response.json')

    def before_test_should_update_element(self):
        self.register_route('/api/v1/spaces/exampleSpace/buckets/users/elements/elementId', 'PUT', 200, request_file='element_request.json')

    def before_test_should_throw_error_when_updating_element_from_not_existing_space(self):
        self.register_route('/api/v1/spaces/notExistingSpace/buckets/users/elements/elementId', 'PUT', 404, request_file='element_request.json',
                            response_file='not_existing_space_response.json')

    def before_test_should_throw_error_when_updating_element_from_not_existing_bucket(self):
        self.register_route('/api/v1/spaces/exampleSpace/buckets/notExistingBucket/elements/elementId', 'PUT', 404,
                            request_file='element_request.json', response_file='not_existing_bucket_response.json')

    def before_test_should_throw_error_when_updating_not_existing_element(self):
        self.register_route('/api/v1/spaces/exampleSpace/buckets/users/elements/notExistingElement', 'PUT', 404,
                            request_file='element_request.json', response_file='not_existing_element_response.json')

    def before_test_should_get_element(self):
        self.register_route('/api/v1/spaces/exampleSpace/buckets/users/elements/elementId', 'GET', 200, response_file='element_response.json')

    def before_test_should_throw_error_when_getting_element_from_not_existing_space(self):
        self.register_route('/api/v1/spaces/notExistingSpace/buckets/users/elements/elementId', 'GET', 404,
                            response_file='not_existing_space_response.json')

    def before_test_should_throw_error_when_getting_element_from_not_existing_bucket(self):
        self.register_route('/api/v1/spaces/exampleSpace/buckets/notExistingBucket/elements/elementId', 'GET', 404,
                            response_file='not_existing_bucket_response.json')

    def before_test_should_throw_error_when_getting_not_existing_element(self):
        self.register_route('/api/v1/spaces/exampleSpace/buckets/users/elements/notExistingElement', 'GET', 404,
                            response_file='not_existing_element_response.json')

    def before_test_should_throw_error_when_filtering_elements_from_not_existing_space(self):
        self.register_route('/api/v1/spaces/notExistingSpace/buckets/users/elements', 'GET', 404,
                            response_file='not_existing_space_response.json', query_params={'limit': '20', 'offset': '0'})

    def before_test_should_throw_error_when_filtering_elements_from_not_existing_bucket(self):
        self.register_route('/api/v1/spaces/exampleSpace/buckets/notExistingBucket/elements', 'GET', 404,
                            response_file='not_existing_bucket_response.json', query_params={'limit': '20', 'offset': '0'})

    def before_test_should_filter_elements(self):
        self.register_route('/api/v1/spaces/exampleSpace/buckets/users/elements', 'GET', 200,
                            response_file='elements_paginated_response1.json', query_params={'limit': '2', 'offset': '0' })

        self.register_route('/api/v1/spaces/exampleSpace/buckets/users/elements', 'GET', 200,
                            response_file='elements_paginated_response2.json', query_params={'limit': '2', 'offset': '2' })

    def test_should_create_bucket(self):
        # when
        self.loop.run_until_complete(self.easydb_client.create_bucket('exampleSpace', 'users'))

        # then
        self.assertEqual(self.verify(self.build_bucket_url('exampleSpace'), 'POST', 'bucket.json'), 1)

    def test_should_throw_error_when_creating_bucket_in_not_existing_space(self):
        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.create_bucket('notExistingSpace', 'users'))

        # and
        self.assertEqual(self.verify(self.build_bucket_url('notExistingSpace'), 'POST', 'bucket.json'), 1)

    def test_should_throw_error_when_creating_already_existing_bucket(self):
        # expect
        with self.assertRaises(BucketAlreadyExistsException):
            self.loop.run_until_complete(self.easydb_client.create_bucket('exampleSpace', 'users'))

        # and
        self.assertEqual(self.verify(self.build_bucket_url('exampleSpace'), 'POST', 'bucket.json'), 1)

    def test_should_add_element_to_bucket(self):
        # when
        created_element = self.loop.run_until_complete(self.easydb_client. \
                                                       add_element('exampleSpace', 'users',
                                                                   MultipleElementFields()
                                                                   .add_field('firstName', 'John')
                                                                   .add_field('lastName', 'Smith')))

        # then
        self.assertEqual(self.verify('/api/v1/spaces/exampleSpace/buckets/users/elements', 'POST', 'element_request.json'), 1)
        self.assertEqual(created_element,
                         Element('elementId')
                         .add_field('firstName', 'John')
                         .add_field('lastName', 'Smith'))

    def test_should_throw_error_when_adding_element_in_not_existing_space(self):
        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client. \
                                         add_element('notExistingSpace', 'users',
                                                     MultipleElementFields()
                                                     .add_field('firstName', 'John')
                                                     .add_field('lastName', 'Smith')))

        # and
        self.assertEqual(self.verify('/api/v1/spaces/notExistingSpace/buckets/users/elements', 'POST', 'element_request.json'), 1)

    def test_should_throw_error_when_adding_element_in_not_existing_bucket(self):
        # expect
        with self.assertRaises(BucketDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client. \
                                         add_element('exampleSpace', 'notExistingBucket',
                                                     MultipleElementFields()
                                                     .add_field('firstName', 'John')
                                                     .add_field('lastName', 'Smith')))

        # and
        self.assertEqual(self.verify(self.build_element_url('exampleSpace', 'notExistingBucket'), 'POST', 'element_request.json'), 1)

    def test_should_delete_bucket(self):
        # when
        self.loop.run_until_complete(self.easydb_client.delete_bucket('exampleSpace', 'users'))

        # then
        self.assertEqual(self.verify('/api/v1/spaces/exampleSpace/buckets/users', 'DELETE'), 1)

    def test_should_throw_error_when_deleting_bucket_in_not_existing_space(self):
        # except
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.delete_bucket('notExistingSpace', 'users'))

        # and
        self.assertEqual(self.verify('/api/v1/spaces/notExistingSpace/buckets/users', 'DELETE'), 1)

    def test_should_throw_error_when_deleting_not_existing_bucket(self):
        # except
        with self.assertRaises(BucketDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.delete_bucket('exampleSpace', 'notExistingBucket'))

        # and
        self.assertEqual(self.verify('/api/v1/spaces/exampleSpace/buckets/notExistingBucket', 'DELETE'), 1)

    def test_should_delete_element(self):
        # when
        self.loop.run_until_complete(self.easydb_client.delete_element('exampleSpace', 'users', 'elementId'))

        # then
        self.assertEqual(self.verify('/api/v1/spaces/exampleSpace/buckets/users/elements/elementId', 'DELETE'), 1)

    def test_should_throw_error_when_deleting_element_from_not_existing_space(self):
        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.delete_element('notExistingSpace', 'users', 'elementId'))

        # and
        self.assertEqual(self.verify('/api/v1/spaces/notExistingSpace/buckets/users/elements/elementId', 'DELETE'), 1)

    def test_should_throw_error_when_deleting_element_from_not_existing_bucket(self):
        # expect
        with self.assertRaises(BucketDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.delete_element('exampleSpace', 'notExistingBucket', 'elementId'))

        # and
        self.assertEqual(self.verify('/api/v1/spaces/exampleSpace/buckets/notExistingBucket/elements/elementId', 'DELETE'), 1)

    def test_should_throw_error_when_deleting_not_existing_element(self):
        # expect
        with self.assertRaises(ElementDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.delete_element('exampleSpace', 'users', 'notExistingElement'))

        # and
        self.assertEqual(self.verify('/api/v1/spaces/exampleSpace/buckets/users/elements/notExistingElement', 'DELETE'), 1)

    def test_should_update_element(self):
        # when
        self.loop.run_until_complete(self.easydb_client.update_element('exampleSpace', 'users', 'elementId',
                                                                       MultipleElementFields()
                                                                       .add_field('firstName', 'John')
                                                                       .add_field('lastName', 'Smith')))
        # then
        self.assertEqual(self.verify('/api/v1/spaces/exampleSpace/buckets/users/elements/elementId', 'PUT', 'element_request.json'), 1)

    def test_should_throw_error_when_updating_element_from_not_existing_space(self):
        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.update_element('notExistingSpace', 'users', 'elementId',
                                                  MultipleElementFields()
                                                  .add_field('firstName', 'John')
                                                  .add_field('lastName', 'Smith')))

        # and
        self.assertEqual(self.verify('/api/v1/spaces/notExistingSpace/buckets/users/elements/elementId', 'PUT', 'element_request.json'), 1)

    def test_should_throw_error_when_updating_element_from_not_existing_bucket(self):
        # expect
        with self.assertRaises(BucketDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.update_element('exampleSpace', 'notExistingBucket', 'elementId',
                                                  MultipleElementFields()
                                                  .add_field('firstName', 'John')
                                                  .add_field('lastName', 'Smith')))

        # and
        self.assertEqual(
            self.verify('/api/v1/spaces/exampleSpace/buckets/notExistingBucket/elements/elementId', 'PUT', 'element_request.json'), 1)

    def test_should_throw_error_when_updating_not_existing_element(self):
        # expect
        with self.assertRaises(ElementDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.update_element('exampleSpace', 'users', 'notExistingElement',
                                                  MultipleElementFields()
                                                  .add_field('firstName', 'John')
                                                  .add_field('lastName', 'Smith')))

        # and
        self.assertEqual(self.verify('/api/v1/spaces/exampleSpace/buckets/users/elements/notExistingElement', 'PUT', 'element_request.json'), 1)

    def test_should_get_element(self):
        # when
        element = self.loop.run_until_complete(self.easydb_client.get_element('exampleSpace', 'users', 'elementId'))

        # then
        self.assertEqual(element, Element('elementId').add_field('firstName', 'John').add_field('lastName', 'Smith'))
        self.assertEqual(self.verify('/api/v1/spaces/exampleSpace/buckets/users/elements/elementId', 'GET'), 1)

    def test_should_throw_error_when_getting_element_from_not_existing_space(self):
        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.get_element('notExistingSpace', 'users', 'elementId'))

        # and
        self.assertEqual(self.verify('/api/v1/spaces/notExistingSpace/buckets/users/elements/elementId', 'GET'), 1)

    def test_should_throw_error_when_getting_element_from_not_existing_bucket(self):
        # expect
        with self.assertRaises(BucketDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.get_element('exampleSpace', 'notExistingBucket', 'elementId'))

        # and
        self.assertEqual(self.verify('/api/v1/spaces/exampleSpace/buckets/notExistingBucket/elements/elementId', 'GET'), 1)

    def test_should_throw_error_when_getting_not_existing_element(self):
        # expect
        with self.assertRaises(ElementDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.get_element('exampleSpace', 'users', 'notExistingElement'))

        # and
        self.assertEqual(self.verify('/api/v1/spaces/exampleSpace/buckets/users/elements/notExistingElement', 'GET'), 1)

    def test_should_filter_elements(self):
        # when
        query = FilterQuery('exampleSpace', 'users', offset=0, limit=2)
        paginated_elements = self.loop.run_until_complete(
            self.easydb_client.filter_elements_by_query(query))

        # then
        self.assertEqual(paginated_elements.elements,
                         [Element('id1').add_field('firstName', 'Chandler')
                                  .add_field('lastName', 'Bing'),
                          Element('id2').add_field('firstName', 'Joe').add_field('lastName', 'Tribbiani')])

        self.assertEqual(self.verify('/api/v1/spaces/exampleSpace/buckets/users/elements', 'GET', query_params={'limit': '2', 'offset': '0'}), 1)

        # and when
        paginated_elements = self.loop.run_until_complete(
            self.easydb_client.filter_elements_by_link(paginated_elements.next_link))

        # then
        self.assertEqual(paginated_elements.elements,
                         [Element('id3').add_field('firstName', 'Monica')
                                  .add_field('lastName', 'Geller')])

        self.assertEqual(self.verify('/api/v1/spaces/exampleSpace/buckets/users/elements', 'GET', query_params={'limit': '2', 'offset': '0'}), 1)

    def test_should_throw_error_when_filtering_elements_from_not_existing_space(self):
        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.filter_elements_by_query(FilterQuery('notExistingSpace', 'users')))

        # and
        self.assertEqual(self.verify('/api/v1/spaces/notExistingSpace/buckets/users/elements', 'GET', query_params={'limit': '20', 'offset': '0'}), 1)

    def test_should_throw_error_when_filtering_elements_from_not_existing_bucket(self):
        # expect
        with self.assertRaises(BucketDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.filter_elements_by_query(FilterQuery('exampleSpace', 'notExistingBucket')))

        # and
        self.assertEqual(self.verify('/api/v1/spaces/exampleSpace/buckets/notExistingBucket/elements', 'GET', query_params={'limit': '20', 'offset': '0'}), 1)

    def build_space_url(self, space_name=""):
        return self.without_ending_slash('/api/v1/spaces/%s' % space_name)

    def build_bucket_url(self, space_name, bucket_name=""):
        return self.without_ending_slash('%s/buckets/%s' % (self.build_space_url(space_name), bucket_name))

    def build_element_url(self, space_name, bucket_name, element_name=""):
        return self.without_ending_slash('%s/elements/%s' % (self.build_bucket_url(space_name, bucket_name), element_name))

    @staticmethod
    def without_ending_slash(s: str):
        return s.rstrip('/')