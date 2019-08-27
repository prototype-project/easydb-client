from aioresponses import aioresponses

from easydb import EasydbClient, MultipleElementFields, Element, SpaceDoesNotExistException, \
    BucketDoesNotExistException, ElementDoesNotExistException, FilterQuery, BucketAlreadyExistsException
from tests.base_test import BaseTest


class BucketTests(BaseTest):
    def setUp(self):
        super().setUp()
        self.easydb_client = EasydbClient(self.server_url)

    def elements_url(self, space_name, bucket_name=None):
        base_url = "%s/api/v1/spaces/%s/buckets" % (self.server_url, space_name)
        if bucket_name:
            base_url += "/" + bucket_name + "/elements"
        return base_url

    @aioresponses()
    def test_should_create_bucket(self, mocked: aioresponses):
        # given
        mocked.post(self.elements_url("exampleSpace"), status=201)

        # when
        self.loop.run_until_complete(self.easydb_client.create_bucket('exampleSpace', 'users'))

    @aioresponses()
    def test_should_throw_error_when_creating_bucket_in_not_existing_space(self, mocked: aioresponses):
        # given
        mocked.post(self.elements_url("notExistingSpace"), status=404, payload={
            "errorCode": "SPACE_DOES_NOT_EXIST",
            "status": "NOT_FOUND",
            "message": "Space notExistingSpace doues not exist"
        })

        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.create_bucket('notExistingSpace', 'users'))

    @aioresponses()
    def test_should_throw_error_when_creating_already_existing_bucket(self, mocked: aioresponses):
        # given
        mocked.post(self.elements_url("exampleSpace"), status=400, payload={
            "errorCode": "BUCKET_ALREADY_EXISTS",
            "status": "BAD_REQUEST",
            "message": "Bucket already exists"
        })

        # expect
        with self.assertRaises(BucketAlreadyExistsException):
            self.loop.run_until_complete(self.easydb_client.create_bucket('exampleSpace', 'users'))

    @aioresponses()
    def test_should_add_element_to_bucket(self, mocked: aioresponses):
        # given
        mocked.post(self.elements_url("exampleSpace", "users"), status=200, payload={
            "id": "elementId",
            "fields": [
                {"name": "firstName", "value": "John"},
                {"name": "lastName", "value": "Smith"}
            ]
        })

        # when
        created_element = self.loop.run_until_complete(self.easydb_client. \
                                                       add_element('exampleSpace', 'users',
                                                                   MultipleElementFields()
                                                                   .add_field('firstName', 'John')
                                                                   .add_field('lastName', 'Smith')))

        # then
        self.assertEqual(created_element,
                         Element("elementId").add_field('firstName', 'John').add_field('lastName', 'Smith'))

    @aioresponses()
    def test_should_throw_error_when_adding_element_in_not_existing_space(self, mocked: aioresponses):
        # given
        mocked.post(self.elements_url("notExistingSpace", "users"), status=404, payload={
            "errorCode": "SPACE_DOES_NOT_EXIST",
            "status": "NOT_FOUND",
            "message": "Space notExistingSpace doues not exist"
        })

        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client. \
                                         add_element('notExistingSpace', 'users',
                                                     MultipleElementFields()
                                                     .add_field('firstName', 'John')
                                                     .add_field('lastName', 'Smith')))

    @aioresponses()
    def test_should_throw_error_when_adding_element_in_not_existing_bucket(self, mocked: aioresponses):
        # given
        mocked.post(self.elements_url("exampleSpace", "notExistingBucket"), status=404, payload={
            "errorCode": "BUCKET_DOES_NOT_EXIST",
            "status": "NOT_FOUND",
            "message": "Bucket notExistingBucket does not exist"
        })

        # expect
        with self.assertRaises(BucketDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client. \
                                         add_element('exampleSpace', 'notExistingBucket',
                                                     MultipleElementFields()
                                                     .add_field('firstName', 'John')
                                                     .add_field('lastName', 'Smith')))

    @aioresponses()
    def test_should_delete_bucket(self, mocked: aioresponses):
        # given
        mocked.delete(self.elements_url("exampleSpace") + "/users", status=200)

        # expect
        self.loop.run_until_complete(self.easydb_client.delete_bucket('exampleSpace', 'users'))

    @aioresponses()
    def test_should_throw_error_when_deleting_bucket_in_not_existing_space(self, mocked: aioresponses):
        # given
        mocked.delete(self.elements_url("notExistingSpace") + "/users", status=404, payload={
            "errorCode": "SPACE_DOES_NOT_EXIST",
            "status": "NOT_FOUND",
            "message": "Space notExistingSpace doues not exist"
        })

        # except
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.delete_bucket('notExistingSpace', 'users'))

    @aioresponses()
    def test_should_throw_error_when_deleting_not_existing_bucket(self, mocked: aioresponses):
        # given
        mocked.delete(self.elements_url("exampleSpace") + "/notExistingBucket", status=404, payload={
            "errorCode": "BUCKET_DOES_NOT_EXIST",
            "status": "NOT_FOUND",
            "message": "Bucket notExistingBucket does not exist"
        })

        # except
        with self.assertRaises(BucketDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.delete_bucket('exampleSpace', 'notExistingBucket'))

    @aioresponses()
    def test_should_delete_element(self, mocked: aioresponses):
        # given
        mocked.delete(self.elements_url("exampleSpace", "users") + "/elementId")

        # expect
        self.loop.run_until_complete(self.easydb_client.delete_element('exampleSpace', 'users', 'elementId'))

    @aioresponses()
    def test_should_throw_error_when_deleting_element_from_not_existing_space(self, mocked: aioresponses):
        # given
        mocked.delete(self.elements_url("notExistingSpace", "users") + "/elementId", status=404, payload={
            "errorCode": "SPACE_DOES_NOT_EXIST",
            "status": "NOT_FOUND",
            "message": "Space notExistingSpace doues not exist"
        })

        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.delete_element('notExistingSpace', 'users', 'elementId'))

    @aioresponses()
    def test_should_throw_error_when_deleting_element_from_not_existing_bucket(self, mocked: aioresponses):
        # given
        mocked.delete(self.elements_url("exampleSpace", "notExistingBucket") + "/elementId", status=404, payload={
            "errorCode": "BUCKET_DOES_NOT_EXIST",
            "status": "NOT_FOUND",
            "message": "Bucket notExistingBucket does not exist"
        })

        # expect
        with self.assertRaises(BucketDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.delete_element('exampleSpace', 'notExistingBucket', 'elementId'))

    @aioresponses()
    def test_should_throw_error_when_deleting_not_existing_element(self, mocked: aioresponses):
        # given
        mocked.delete(self.elements_url("exampleSpace", "users") + "/notExistingElement", status=404, payload={
            "errorCode": "ELEMENT_DOES_NOT_EXIST",
            "status": "NOT_FOUND",
            "message": "Element with id notExistingElement does not exist in bucket users"
        })

        # expect
        with self.assertRaises(ElementDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.delete_element('exampleSpace', 'users', 'notExistingElement'))

    @aioresponses()
    def test_should_update_element(self, mocked=aioresponses):
        # given
        mocked.put(self.elements_url("exampleSpace", "users") + "/elementId", status=200)

        # expect
        self.loop.run_until_complete(self.easydb_client.update_element('exampleSpace', 'users', 'elementId',
                                                                       MultipleElementFields()
                                                                       .add_field('firstName', 'John')
                                                                       .add_field('lastName', 'Smith')))

    @aioresponses()
    def test_should_throw_error_when_updating_element_from_not_existing_space(self, mocked: aioresponses):
        # given
        mocked.put(self.elements_url("notExistingSpace", "users") + "/elementId", status=404, payload={
            "errorCode": "SPACE_DOES_NOT_EXIST",
            "status": "NOT_FOUND",
            "message": "Space notExistingSpace doues not exist"
        })

        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.update_element('notExistingSpace', 'users', 'elementId',
                                                  MultipleElementFields()
                                                  .add_field('firstName', 'John')
                                                  .add_field('lastName', 'Smith')))

    @aioresponses()
    def test_should_throw_error_when_updating_element_from_not_existing_bucket(self, mocked: aioresponses):
        # given
        mocked.put(self.elements_url("exampleSpace", "notExistingBucket") + "/elementId", status=404, payload={
            "errorCode": "BUCKET_DOES_NOT_EXIST",
            "status": "NOT_FOUND",
            "message": "Bucket notExistingBucket does not exist"
        })

        # expect
        with self.assertRaises(BucketDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.update_element('exampleSpace', 'notExistingBucket', 'elementId',
                                                  MultipleElementFields()
                                                  .add_field('firstName', 'John')
                                                  .add_field('lastName', 'Smith')))

    @aioresponses()
    def test_should_throw_error_when_updating_not_existing_element(self, mocked: aioresponses):
        # given
        mocked.put(self.elements_url("exampleSpace", "users") + "/notExistingElement", status=404, payload={
            "errorCode": "ELEMENT_DOES_NOT_EXIST",
            "status": "NOT_FOUND",
            "message": "Element with id notExistingElement does not exist in bucket users"
        })

        # expect
        with self.assertRaises(ElementDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.update_element('exampleSpace', 'users', 'notExistingElement',
                                                  MultipleElementFields()
                                                  .add_field('firstName', 'John')
                                                  .add_field('lastName', 'Smith')))

    @aioresponses()
    def test_should_get_element(self, mocked: aioresponses):
        # given
        mocked.get(self.elements_url("exampleSpace", "users") + "/elementId", status=200, payload={
            "id": "elementId",
            "fields": [
                {"name": "firstName", "value": "John"},
                {"name": "lastName", "value": "Smith"}
            ]
        })

        # when
        element = self.loop.run_until_complete(self.easydb_client.get_element('exampleSpace', 'users', 'elementId'))

        # then
        self.assertEqual(element, Element('elementId').add_field('firstName', 'John').add_field('lastName', 'Smith'))

    @aioresponses()
    def test_should_throw_error_when_getting_element_from_not_existing_space(self, mocked: aioresponses):
        # given
        mocked.get(self.elements_url("notExistingSpace", "users") + "/elementId", status=404, payload={
            "errorCode": "SPACE_DOES_NOT_EXIST",
            "status": "NOT_FOUND",
            "message": "Space notExistingSpace doues not exist"
        })

        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.get_element('notExistingSpace', 'users', 'elementId'))

    @aioresponses()
    def test_should_throw_error_when_getting_element_from_not_existing_bucket(self, mocked: aioresponses):
        # given
        mocked.get(self.elements_url("exampleSpace", "notExistingBucket") + "/elementId", status=404, payload={
            "errorCode": "BUCKET_DOES_NOT_EXIST",
            "status": "NOT_FOUND",
            "message": "Bucket notExistingBucket does not exist"
        })

        # expect
        with self.assertRaises(BucketDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.get_element('exampleSpace', 'notExistingBucket', 'elementId'))

    @aioresponses()
    def test_should_throw_error_when_getting_not_existing_element(self, mocked: aioresponses):
        # given
        mocked.get(self.elements_url("exampleSpace", "users") + "/notExistingElement", status=404, payload={
            "errorCode": "ELEMENT_DOES_NOT_EXIST",
            "status": "NOT_FOUND",
            "message": "Element with id notExistingElement does not exist in bucket users"
        })

        # expect
        with self.assertRaises(ElementDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.get_element('exampleSpace', 'users', 'notExistingElement'))

    @aioresponses()
    def test_should_filter_elements_without_query(self, mocked: aioresponses):
        # given
        mocked.get(self.elements_url("exampleSpace", "users") + "?limit=2&offset=0", status=200, payload={
            "nextPageLink": self.elements_url("exampleSpace", "users") + "?limit=2&offset=2",
            "results": [
                {
                    "id": "id1",
                    "fields": [
                        {
                            "name": "firstName",
                            "value": "Chandler"
                        },
                        {
                            "name": "lastName",
                            "value": "Bing"
                        }
                    ]
                },
                {
                    "id": "id2",
                    "fields": [
                        {
                            "name": "firstName",
                            "value": "Joe"
                        },
                        {
                            "name": "lastName",
                            "value": "Tribbiani"
                        }
                    ]
                }
            ]
        })

        mocked.get(self.elements_url("exampleSpace", "users") + "?limit=2&offset=2", status=200, payload={
            "nextPageLink": None,
            "results": [
                {
                    "id": "id3",
                    "fields": [
                        {
                            "name": "firstName",
                            "value": "Monica"
                        },
                        {
                            "name": "lastName",
                            "value": "Geller"
                        }
                    ]
                }
            ]
        })

        # when
        query = FilterQuery('exampleSpace', 'users', offset=0, limit=2)
        paginated_elements = self.loop.run_until_complete(
            self.easydb_client.filter_elements_by_query(query))

        # then
        self.assertEqual(paginated_elements.elements,
                         [Element('id1').add_field('firstName', 'Chandler')
                         .add_field('lastName', 'Bing'),
                          Element('id2').add_field('firstName', 'Joe').add_field('lastName', 'Tribbiani')])

        # and when
        paginated_elements = self.loop.run_until_complete(
            self.easydb_client.filter_elements_by_link(paginated_elements.next_link))

        # then
        self.assertEqual(paginated_elements.elements,
                         [Element('id3').add_field('firstName', 'Monica')
                         .add_field('lastName', 'Geller')])

    @aioresponses()
    def test_should_filter_elements_using_query(self, mocked: aioresponses):
        # given
        graphql_query = """
        {
            elements {
                id
                fields {
                    name
                    value
                }
            }
        }"""

        mocked.get(self.elements_url("exampleSpace", "users") + "?limit=2&offset=0&query=" + graphql_query, status=200,
                   payload={
                       "nextPageLink": None,
                       "results": [
                           {
                               "id": "id1",
                               "fields": [
                                   {
                                       "name": "firstName",
                                       "value": "Chandler"
                                   },
                                   {
                                       "name": "lastName",
                                       "value": "Bing"
                                   }
                               ]
                           }
                       ]
                   })

        # when
        query = FilterQuery('exampleSpace', 'users', offset=0, limit=2, query=graphql_query)
        paginated_elements = self.loop.run_until_complete(
            self.easydb_client.filter_elements_by_query(query))

        # then
        self.assertEqual(paginated_elements.elements,
                         [Element('id1').add_field('firstName', 'Chandler').add_field('lastName', 'Bing')])

    @aioresponses()
    def test_should_throw_error_when_filtering_elements_from_not_existing_space(self, mocked: aioresponses):
        # given
        mocked.get(self.elements_url("notExistingSpace", "users") + "?limit=20&offset=0", status=404, payload={
            "errorCode": "SPACE_DOES_NOT_EXIST",
            "status": "NOT_FOUND",
            "message": "Space notExistingSpace doues not exist"
        })

        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.filter_elements_by_query(FilterQuery('notExistingSpace', 'users')))

    @aioresponses()
    def test_should_throw_error_when_filtering_elements_from_not_existing_bucket(self, mocked: aioresponses):
        # given
        mocked.get(self.elements_url("exampleSpace", "notExistingBucket") + "?limit=20&offset=0", status=404, payload={
            "errorCode": "BUCKET_DOES_NOT_EXIST",
            "status": "NOT_FOUND",
            "message": "Bucket notExistingBucket does not exist"
        })

        # expect
        with self.assertRaises(BucketDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.filter_elements_by_query(FilterQuery('exampleSpace', 'notExistingBucket')))
