from unittest import TestCase
from src.Encryption import EncryptionDriver
import os
from src.ConfigProcessor import get_config_details


class TestEncryptionDriver(TestCase):



    def test_encrypt_data(self):
        #ed = EncryptionDriver
        self.assertTrue(os.path.exists('C:\\Users\\Prasad\\Pictures\\visa\\encrypted_data.txt'))






