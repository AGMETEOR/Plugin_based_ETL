from core.plugin import PluginInterface
from .etl.etl import ETL


class Sample(PluginInterface):
    def __init__(self, path, logger):
        pass

    def load_plugin_config(self, path):
        pass

    def execute(self):
        ETL()

    def stop(self):
        pass
