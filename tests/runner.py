import unittest

from tests.space import SpaceTests
from tests.bucket import BucketTests
from tests.transactions import TransactionTest


def suite():
    test_suite = unittest.TestSuite()
    test_suite.addTest(SpaceTests("SpaceTests"))
    test_suite.addTest(BucketTests("BucketTests"))
    test_suite.addTest(TransactionTest("TransactionTests"))
    return suite


def run_tests():
    unittest.main()


if __name__ == '__main__':
    run_tests()