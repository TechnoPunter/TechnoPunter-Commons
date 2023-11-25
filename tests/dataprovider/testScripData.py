import logging
import os
import unittest

if os.path.exists('/var/www/TechnoPunter-Commons'):
    REPO_PATH = '/var/www/TechnoPunter-Commons/'
    TEST_RESOURCE_DIR = '/var/www/TechnoPunter-Commons/commons/resources/test'
else:
    REPO_PATH = '/Users/pralhad/Documents/99-src/98-trading/TechnoPunter-Commons/'
    TEST_RESOURCE_DIR = "/Users/pralhad/Documents/99-src/98-trading/TechnoPunter-Commons/commons/resources/test"

os.environ['RESOURCE_PATH'] = os.path.join(REPO_PATH, 'resources/config')
os.environ['GENERATED_PATH'] = os.path.join(REPO_PATH, 'dummy')

logger = logging.getLogger(__name__)
from commons.dataprovider.ScripData import ScripData


class TestScripData(unittest.TestCase):
    sd = ScripData()

    def test_record_load(self):
        res = self.sd.get_scrip_data("DUMMY")
        self.assertIsNotNone(res)


if __name__ == "__main__":
    unittest.main()
