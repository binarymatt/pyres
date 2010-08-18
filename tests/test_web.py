import os
from resweb.webapp import app
import unittest

class ReswebTestCase(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()
    
    def test_index(self):
        rv  = self.app.get("/")
        assert 'failed' in rv.data