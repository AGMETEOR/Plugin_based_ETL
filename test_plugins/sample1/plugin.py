from core.plugin import PluginInterface


class Sample1(PluginInterface):
    def __init__(self, path, logger):
        pass

    def load_plugin_config(self, path):
        pass

    def execute(self):
        pass

    def stop(self):
        pass
