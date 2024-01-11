import unittest
import logging
from core.plugin import PluginCore


def create_test_home_dir(dir_name, logger):
    pass


class TestPluginCore(unittest.TestCase):
    def setUp(self):
        logger = logging.getLogger(__name__)
        self.plugin_core = PluginCore(logger, "test_plugins", create_test_home_dir)

    def test_loaded_plugins(self):
        plugins = self.plugin_core.load_plugins()
        # 2 plugin folders exist in the configured plugin path.
        self.assertEqual(len(plugins), 2)


if __name__ == "__main__":
    unittest.main()
