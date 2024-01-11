import os
import yaml
import logging
from typing import Any
from core.plugin import PluginInterface
from .etl.etl import ETL


class GHOTOPOSTGRES(PluginInterface):
    def __init__(self, path: str, logger: logging.Logger):
        self._config = self.load_plugin_config(path)
        self.logger = logger

    def load_plugin_config(self, plugin_path: str) -> Any:
        def database_constructor(loader: yaml.SafeLoader, node: yaml.nodes.ScalarNode) -> str:
            """Construct a database url"""
            db_string = os.environ.get('GHOTopOSTGRES_DATABASE_URL')
            return f"{db_string}{loader.construct_scalar(node)}"

        def get_loader():
            """Add constructors to PyYAML loader."""
            loader = yaml.SafeLoader
            loader.add_constructor("!GHOTopOSTGRES_DATABASE_URL", database_constructor)
            return loader

        config_path = os.path.join(plugin_path, "config.yaml")
        plugin_config = {}
        with open(config_path, 'r') as yaml_file:
            try:
                plugin_config = yaml.load(yaml_file, Loader=get_loader())
            except yaml.YAMLError as e:
                self.logger.error(f"Error reading YAML file: {e}")
        return plugin_config

    def execute(self):
        self.logger.info("Starting GHOTOPOSTGRES plugin")
        etls_from_config = self._config["etl"]
        indicator = self._config["run"]
        etl = ETL(etls_from_config[indicator], indicator, self.logger)
        etl.start()
        self.logger.info("Finishing execution of GHOTOPOSTGRES plugin")

    def stop(self):
        self.logger.info("Configured to pause...")
        self.logger.info("Exiting gracefully...")
