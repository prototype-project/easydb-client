import unittest

import asyncio


class BaseTest(unittest.TestCase):
    def setUp(self):
        self.server_port = 9000
        self.server_url = 'http://localhost:' + str(self.server_port)
        self.loop = asyncio.get_event_loop()
