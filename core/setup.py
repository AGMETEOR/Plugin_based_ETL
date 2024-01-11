import logging
import os
import sys


def configure_logger():
    """
    Configures a basic logger for the current module if no handlers are already set and returns it
    """

    logger = logging.getLogger(__name__)
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        console_handler.setFormatter(formatter)

        logger.addHandler(console_handler)

    return logger


def create_directory_with_empty_status_file(directory_name: str, logger: logging.Logger):
    """
    Create a directory in the user's home directory with an empty 'status.yaml' file.
    ETL plugins can write any status information about their runs in the 'status.yaml' file.
    For example, a plugin can use it to pause and resume processing of data.

    :param directory_name: name of the hidden directory that holds data about this integration.
    :param logger: configured logger
    """

    try:
        home_directory = os.path.expanduser("~")
        new_directory_path = os.path.join(home_directory, directory_name)

        if not os.path.exists(new_directory_path):
            os.makedirs(new_directory_path)
            logger.info(f"Directory '{directory_name}' created in the home directory.")

            status_file_path = os.path.join(new_directory_path, "status.yaml")
            with open(status_file_path, "w"):
                pass

            logger.info("Empty status.yaml file created.")
        else:
            logger.debug(f"Directory '{directory_name}' already exists in the home directory.")

    except FileNotFoundError:
        logger.error("Error: Home directory not found.")
    except PermissionError:
        logger.error("Error: Permission denied while creating the directory or file.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
