from unittest import TestCase
import os


class TestEncryptionDriver(TestCase):



    def test_encrypt_data(self):
        self.assertTrue(os.path.exists('C:\\Users\\Prasad\\Pictures\\visa\\encrypted_data.txt'))






