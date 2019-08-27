from aioresponses import aioresponses

from easydb import EasydbClient, SpaceDoesNotExistException, TransactionOperation, OperationResult, \
    Element, TransactionDoesNotExistException, BucketDoesNotExistException, ElementDoesNotExistException, \
    UnknownOperationException
from easydb.domain import TransactionAbortedException, MultipleElementFields
from tests.base_test import BaseTest


class TransactionTest(BaseTest):
    def setUp(self):
        super().setUp()
        self.easydb_client = EasydbClient(self.server_url, retry_backoff_millis=1, retries_number=0)

    def transactions_url(self, space_name, transaction_id=None):
        base_url = "%s/api/v1/spaces/%s/transactions" % (self.server_url, space_name)
        if transaction_id:
            base_url += "/" + transaction_id
        return base_url

    @aioresponses()
    def test_should_begin_transaction(self, mocked: aioresponses):
        # given
        mocked.post(self.transactions_url("exampleSpace"), status=201, payload={
            "transactionId": "exampleTransactionId"
        })

        # when
        transaction = self.loop.run_until_complete(self.easydb_client.begin_transaction('exampleSpace'))

        # expect
        self.assertEqual(transaction.transaction_id, 'exampleTransactionId')

    @aioresponses()
    def test_should_throw_error_when_beginning_transaction_in_not_existing_space(self, mocked: aioresponses):
        # given
        mocked.post(self.transactions_url("notExistingSpace"), status=404, payload={
            "errorCode": "SPACE_DOES_NOT_EXIST",
            "status": "NOT_FOUND",
            "message": "Space notExistingSpace doues not exist"
        })

        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.begin_transaction('notExistingSpace'))

    @aioresponses()
    def test_should_add_update_operation_to_transaction_and_return_empty_result(self, mocked: aioresponses):
        # given
        operation = TransactionOperation('UPDATE', 'users', 'exampleElementId',
                                         MultipleElementFields().add_field('username', 'Mirek'))

        mocked.post(self.transactions_url('users', 'exampleTransactionId') + "/add-operation", status=200, payload={
            "element": None
        })

        # when
        operation_result = self.loop.run_until_complete(
            self.easydb_client.add_operation('users', 'exampleTransactionId', operation))

        # then
        self.assertTrue(operation_result.is_empty())

    @aioresponses()
    def test_should_add_operation_to_transaction_and_return_not_empty_result(self, mocked: aioresponses):
        # given
        operation = TransactionOperation('READ', 'users', 'exampleElementId')

        mocked.post(self.transactions_url('users', 'exampleTransactionId') + "/add-operation", status=200, payload={
            "element": {
                "id": "exampleElementId",
                "fields": [
                    {
                        "name": "username",
                        "value": "Heniek"
                    }
                ]
            }
        })

        # when
        operation_result = self.loop.run_until_complete(
            self.easydb_client.add_operation('users', 'exampleTransactionId', operation))

        # then
        self.assertFalse(operation_result.is_empty())
        self.assertEqual(operation_result, OperationResult(Element('exampleElementId').add_field('username', 'Heniek')))

    @aioresponses()
    def test_should_throw_error_when_adding_operation_to_not_existing_transaction(self, mocked: aioresponses):
        # given
        operation = TransactionOperation('READ', 'users', 'exampleElementId')

        mocked.post(self.transactions_url('users', 'notExistingTransactionId') + "/add-operation", status=404, payload={
            "errorCode": "TRANSACTION_DOES_NOT_EXIST",
            "status": "NOT_FOUND",
            "message": "Transaction notExistingTransaction does not exist"
        })

        # expect
        with self.assertRaises(TransactionDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.add_operation('users', 'notExistingTransactionId', operation))

    @aioresponses()
    def test_should_throw_error_when_adding_operation_to_not_existing_bucket(self, mocked: aioresponses):
        # given
        operation = TransactionOperation('READ', 'notExistingBucket', 'exampleElementId')

        mocked.post(self.transactions_url('users', 'exampleTransactionId') + '/add-operation', status=404, payload={
            "errorCode": "BUCKET_DOES_NOT_EXIST",
            "status": "NOT_FOUND",
            "message": "Bucket notExistingBucket does not exist"
        })

        # expect
        with self.assertRaises(BucketDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.add_operation('users', 'exampleTransactionId', operation))

    @aioresponses()
    def test_should_throw_error_when_adding_operation_for_not_existing_element(self, mocked: aioresponses):
        # given
        operation = TransactionOperation('READ', 'users', 'notExistingElement')

        mocked.post(self.transactions_url('users', 'exampleTransactionId') + '/add-operation', status=404, payload={
            "errorCode": "ELEMENT_DOES_NOT_EXIST",
            "status": "NOT_FOUND",
            "message": "Element with id notExistingElement does not exist in bucket users"
        })

        # expect
        with self.assertRaises(ElementDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.add_operation('users', 'exampleTransactionId', operation))

    def test_should_throw_error_when_adding_operation_with_not_existing_type(self):
        # given
        operation = TransactionOperation('UNKNOWN', 'users', 'notExistingElement')

        # expect
        with self.assertRaises(UnknownOperationException):
            self.loop.run_until_complete(self.easydb_client.add_operation('users', 'exampleTransactionId', operation))

    @aioresponses()
    def test_should_commit_transaction(self, mocked: aioresponses):
        # given
        mocked.post(self.transactions_url('users', 'exampleTransactionId') + '/commit', status=202)

        # when
        self.loop.run_until_complete(self.easydb_client.commit_transaction('users', 'exampleTransactionId'))

        # then no exception thrown

    @aioresponses()
    def test_should_throw_error_when_committing_not_existing_transaction(self, mocked: aioresponses):
        # given
        mocked.post(self.transactions_url('users', 'notExistingTransactionId') + '/commit', status=404, payload={
            "errorCode": "TRANSACTION_DOES_NOT_EXIST",
            "status": "NOT_FOUND",
            "message": "Transaction notExistingTransaction does not exist"
        })

        # expect
        with self.assertRaises(TransactionDoesNotExistException):
            self.loop.run_until_complete(
                self.easydb_client.commit_transaction('users', 'notExistingTransactionId')) @ aioresponses()

    @aioresponses()
    def test_should_throw_error_when_committing_transaction_in_not_existing_space(self, mocked: aioresponses):
        # given
        mocked.post(self.transactions_url('notExistingSpace', 'exampleTransactionId') + '/commit', status=404, payload={
            "errorCode": "SPACE_DOES_NOT_EXIST",
            "status": "NOT_FOUND",
            "message": "Space notExistingSpace doues not exist"
        })

        # expect
        with self.assertRaises(SpaceDoesNotExistException):
            self.loop.run_until_complete(self.easydb_client.commit_transaction('notExistingSpace', 'exampleTransactionId'))

    @aioresponses()
    def test_should_throw_error_when_transaction_aborted(self, mocked: aioresponses):
        # given
        mocked.post(self.transactions_url('users', 'exampleTransactionId') + '/commit', status=409, payload={
            "errorCode": "TRANSACTION_ABORTED",
            "status": "TRANSACTION_ABORTED",
            "message": "Transaction was aborted. Possible many conflicting transactions running at the same time. Try later again"
        })

        # expect
        with self.assertRaises(TransactionAbortedException):
            self.loop.run_until_complete(self.easydb_client.commit_transaction('users', 'exampleTransactionId'))

    @aioresponses()
    def test_should_throw_error_when_transaction_aborted_during_adding_operation(self, mocked: aioresponses):
        # given
        operation = TransactionOperation('READ', 'users', 'exampleElementId')

        mocked.post(self.transactions_url('users', 'exampleTransactionId') + '/add-operation', status=409, payload={
            "errorCode": "TRANSACTION_ABORTED",
            "status": "TRANSACTION_ABORTED",
            "message": "Transaction was aborted. Possible many conflicting transactions running at the same time. Try later again"
        })

        # expect
        with self.assertRaises(TransactionAbortedException):
            self.loop.run_until_complete(self.easydb_client.add_operation('users', 'exampleTransactionId', operation))