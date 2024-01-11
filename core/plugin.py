from abc import ABC, abstractmethod
import importlib.util
import os
from typing import List, Callable, Any
import inspect
import yaml
import logging
from core.constants import HOME_DIRECTORY_INTEGRATION_FOLDER
from core.exceptions import StatusFileReadError, StatusFileWriteError


class PluginInterface(ABC):
    """
    An abstract base class representing the interface for ETL plugins.
    """

    @abstractmethod
    def __init__(self, path: str, logger: logging.Logger):
        """
        Initialize the plugin with the given path and logger.
        """
        pass

    @abstractmethod
    def load_plugin_config(self, plugin_path: str):
        """
        Load and configure the plugin using the provided plugin path.
        """
        pass

    @abstractmethod
    def execute(self):
        """
        Execute the plugin ETL job.
        """
        pass

    @abstractmethod
    def stop(self):
        """
        Function to call when the ETL job is stopping.
        """
        pass


class PluginCore():
    """
    Manages plugin system functionality like discovery and loading plugins
    """
    def __init__(self,
                 logger: logging.Logger,
                 plugin_directory: str, create_home_dir_func: Callable[[str, logging.Logger], None]):
        self.logger = logger
        self.plugin_directory = plugin_directory
        create_home_dir_func(HOME_DIRECTORY_INTEGRATION_FOLDER, self.logger)

    def load_plugins(self) -> List[PluginInterface]:
        """
        Loads plugins given a path
        """
        plugins = []

        for plugin_name in os.listdir(self.plugin_directory):
            plugin_path = os.path.join(self.plugin_directory, plugin_name)

            if os.path.isdir(plugin_path):
                module_name = f"{self.plugin_directory}.{plugin_name}.plugin"
                plugin_module = importlib.import_module(module_name)

                plugin_classes = inspect.getmembers(plugin_module, inspect.isclass)

                for name, cls in plugin_classes:
                    if (
                        issubclass(cls, PluginInterface) and cls is not PluginInterface
                    ):
                        plugins.append(cls(plugin_path, self.logger))

        return plugins

    @staticmethod
    def read_status_file(file_path: str, logger: logging.Logger):
        """
        Read a status file that lives in the home directory where plugins can write
        status information about the jobs they are running
        """
        try:
            full_path = os.path.join(os.path.expanduser("~"), file_path)

            if os.path.exists(full_path) and os.path.getsize(full_path) > 0:
                with open(full_path, 'r') as file:
                    status_data = yaml.safe_load(file)
            else:
                status_data = {'plugins': {}}

        except FileNotFoundError:
            logger.error(f"Error: File not found - {full_path}")
            raise StatusFileReadError(f"File not found - {full_path}")

        except yaml.YAMLError as e:
            logger.error(f"Error reading YAML file: {e}")
            raise StatusFileReadError(f"Error reading YAML file: {e}")

        except Exception as e:
            raise StatusFileReadError(f"An unexpected error occurred: {e}")

        return status_data

    @staticmethod
    def write_status_file(file_path: str, status_data: Any):
        """
        Plugin ETL jobs use this function tio write their status data
        """
        try:
            full_path = os.path.join(os.path.expanduser("~"), file_path)
            with open(full_path, 'w') as file:
                yaml.dump(status_data, file, default_flow_style=False)
        except Exception as e:
            raise StatusFileWriteError(f"An unexpected error occurred: {e}")
