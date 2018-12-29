from easydb import EasydbClient, SpaceDoesNotExistException, TransactionOperation, ElementField, OperationResult, \
    Element, TransactionDoesNotExistException, BucketDoesNotExistException, ElementDoesNotExistException,\
    UnknownOperationException
from easydb.domain import TransactionAbortedException
from tests.base_test import HttpTest


class TransactionTest(HttpTest):
    def setUp(self):
        super().setUp()
        self.easydb_client = EasydbClient(self.server_url, retry_backoff_millis=1, retries_number=0)

    def before_test_should_begin_transaction(self):
        self.register_route('/api/v1/transactions/exampleSpace', 'POST', 201, response_file='transaction_created.json')

    def before_test_should_throw_error_when_beginning_transaction_in_not_existing_space(self):
        self.register_route('/api/v1/transactions/notExistingSpace', 'POST', 404,
                            response_file='not_existing_space_response.json')

    def before_test_should_add_operation_to_transaction_and_return_empty_result(self):
        self.register_route('/api/v1/transactions/exampleTransactionId/add-operation', 'POST', 201,
                            request_file='add_update_operation_request.json',
                            response_file='add_update_operation_response.json')

    def before_test_should_add_operation_to_transaction_and_return_not_empty_result(self):
        self.register_route('/api/v1/transactions/exampleTransactionId/add-operation', 'POST', 201,
                            request_file='add_read_operation_request.json',
                            response_file='add_read_operation_response.json')

    def before_test_should_throw_error_when_adding_operation_to_not_existing_transaction(self):
        self.register_route('/api/v1/transactions/notExistingTransactionId/add-operation', 'POST', 404,
                            request_file='add_read_operation_request.json',
                            response_file='not_existing_transaction_response.json')

    def before_test_should_throw_error_when_adding_operation_to_not_existing_bucket(self):
        self.register_route('/api/v1/transactions/exampleTransactionId/add-operation', 'POST', 404,
                            request_file='add_read_operation_not_existing_bucket_request.json',
                            response_file='not_existing_bucket_response.json')

    def before_test_should_throw_error_when_adding_operation_to_not_existing_element(self):
        self.register_route('/api/v1/transactions/exampleTransactionId/add-operation', 'POST', 404,
                            request_file='add_read_operation_not_existing_element_request.json',
                            response_file='not_existing_element_response.json')

    def before_test_should_commit_transaction(self):
        self.register_route('/api/v1/transactions/exampleTransactionId/commit', 'POST', 201)

    def before_test_should_throw_error_when_committing_not_existing_transaction(self):
        self.register_route('/api/v1/transactions/notExistingTransactionId/commit', 'POST', 404,
                            response_file='not_existing_transaction_response.json')

    def before_test_should_throw_error_when_transaction_aborted(self):
        self.register_route('/api/v1/transactions/exampleTransactionId/commit', 'POST', 200,
                            response_file='transaction_aborted_response.json')

    def before_test_should_throw_error_when_transaction_aborted_during_adding_operation(self):
        self.register_route('/api/v1/transactions/exampleTransactionId/add-operation', 'POST', 200,
                            request_file='add_read_operation_request.json',
                            response_file='transaction_aborted_response.json')

    def test_should_begin_transaction(self):
        # when
        transaction = self.loop.run_until_complete(self.easydb_client.begin_transaction('exampleSpace'))

        # expect
        self.assertEqual(transaction.transaction_id, 'exampleTransactionId')
        self.assertEqual(self.verify('/api/v1/transactions/exampleSpace', 'POST'), 1)

    def test_should_throw_error_when_beginning_transaction_in_not_existing_space(self):
        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.begin_transaction('notExistingSpace'))

        # and
        self.assertEqual(self.verify('/api/v1/transactions/notExistingSpace', 'POST'), 1)

    def test_should_add_operation_to_transaction_and_return_empty_result(self):
        # given
        operation = TransactionOperation('UPDATE', 'users', 'exampleElementId', [ElementField('username', 'Mirek')])

        # when
        operation_result = self.loop.run_until_complete(
            self.easydb_client.add_operation('exampleTransactionId', operation))

        # then
        self.assertTrue(operation_result.is_empty())
        self.assertEqual(self.verify('/api/v1/transactions/exampleTransactionId/add-operation', 'POST',
                                     request_file='add_update_operation_request.json'), 1)

    def test_should_add_operation_to_transaction_and_return_not_empty_result(self):
        # given
        operation = TransactionOperation('READ', 'users', 'exampleElementId')

        # when
        operation_result = self.loop.run_until_complete(
            self.easydb_client.add_operation('exampleTransactionId', operation))

        # then
        self.assertFalse(operation_result.is_empty())
        self.assertEqual(operation_result, OperationResult(Element('exampleElementId').add_field('username', 'Heniek')))
        self.assertEqual(self.verify('/api/v1/transactions/exampleTransactionId/add-operation', 'POST',
                                     request_file='add_read_operation_request.json'), 1)

    def test_should_throw_error_when_adding_operation_to_not_existing_transaction(self):
        # given
        operation = TransactionOperation('READ', 'users', 'exampleElementId')

        # expect
        with self.assertRaises(TransactionDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.add_operation('notExistingTransactionId', operation))

        # and
        self.assertEqual(self.verify('/api/v1/transactions/notExistingTransactionId/add-operation', 'POST',
                                     request_file='add_read_operation_request.json'), 1)

    def test_should_throw_error_when_adding_operation_to_not_existing_bucket(self):
        # given
        operation = TransactionOperation('READ', 'notExistingBucket', 'exampleElementId')

        # expect
        with self.assertRaises(BucketDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.add_operation('exampleTransactionId', operation))

        # and
        self.assertEqual(self.verify('/api/v1/transactions/exampleTransactionId/add-operation', 'POST',
                                     request_file='add_read_operation_not_existing_bucket_request.json'), 1)

    def test_should_throw_error_when_adding_operation_to_not_existing_element(self):
        # given
        operation = TransactionOperation('READ', 'users', 'notExistingElement')

        # expect
        with self.assertRaises(ElementDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.add_operation('exampleTransactionId', operation))

        # and
        self.assertEqual(self.verify('/api/v1/transactions/exampleTransactionId/add-operation', 'POST',
                                     request_file='add_read_operation_not_existing_element_request.json'), 1)

    def test_should_throw_error_when_adding_operation_with_not_existing_type(self):
        # given
        operation = TransactionOperation('UNKNOWN', 'users', 'notExistingElement')

        # expect
        with self.assertRaises(UnknownOperationException):
            self.loop.run_until_complete(self.easydb_client.add_operation('exampleTransactionId', operation))

    def test_should_commit_transaction(self):
        # given
        self.loop.run_until_complete(self.easydb_client.commit_transaction('exampleTransactionId'))

        # expect
        self.assertEqual(self.verify('/api/v1/transactions/exampleTransactionId/commit', 'POST'), 1)

    def test_should_throw_error_when_committing_not_existing_transaction(self):
        # expect
        with self.assertRaises(TransactionDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.commit_transaction('notExistingTransactionId'))

        # and
        self.assertEqual(self.verify('/api/v1/transactions/notExistingTransactionId/commit', 'POST'), 1)

    def test_should_throw_error_when_transaction_aborted(self):
        # expect
        with self.assertRaises(TransactionAbortedException):
            self.loop.run_until_complete(self.easydb_client.commit_transaction('exampleTransactionId'))

        # and
        self.assertEqual(self.verify('/api/v1/transactions/exampleTransactionId/commit', 'POST'), 1)

    def test_should_throw_error_when_transaction_aborted_during_adding_operation(self):
        # given
        easydb_client = EasydbClient(self.server_url, retry_backoff_millis=1, retries_number=2)
        operation = TransactionOperation('READ', 'users', 'exampleElementId')

        # expect
        with self.assertRaises(TransactionAbortedException):
            self.loop.run_until_complete(easydb_client.add_operation('exampleTransactionId', operation))

        # and
        self.assertEqual(self.verify('/api/v1/transactions/exampleTransactionId/add-operation', 'POST',
                                     request_file='add_read_operation_request.json'), 3)