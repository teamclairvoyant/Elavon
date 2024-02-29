from unittest import TestCase
from src.Decryption import DecryptionDriver
import os
from src.ConfigProcessor import get_config_details


class TestDecryptionDriver(TestCase):

    def test_encrypt_data(self):
        ed = DecryptionDriver
        self.assertTrue(os.path.exists('C:\\Users\\Prasad\\Pictures\\visa\\decrypted_data.json'))

    def test_file_sizes_match(self):
        # Get the size of the encrypted file
        encrypted_size = os.path.getsize("C:\\Users\\Prasad\\Pictures\\visa\\encrypted_data.txt")
        print(encrypted_size)

        # Get the size of the decrypted file

        decrypted_size = os.path.getsize('C:\\Users\\Prasad\\Pictures\\visa\\decrypted_data.json')
        print(decrypted_size)

        # Assert that the sizes of the encrypted and decrypted files match
        self.assertLess(decrypted_size,encrypted_size)







