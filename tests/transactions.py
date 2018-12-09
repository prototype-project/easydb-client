from client import EasydbClient, SpaceDoesNotExistException, TransactionOperation, ElementField, OperationResult, \
    Element
from tests.base_test import HttpTest


class TransactionsTest(HttpTest):
    def setUp(self):
        super().setUp()
        self.easydb_client = EasydbClient(self.server_url)

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
