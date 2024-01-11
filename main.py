from core.plugin import PluginCore
from core.thread import StoppableThread
from core.setup import configure_logger, create_directory_with_empty_status_file
from core.constants import PLUGINS_DIRECTORY


def run():
    """
    run is the start of the integration, a collection of different ETL plugins
    """

    logger = configure_logger()

    core = PluginCore(logger, PLUGINS_DIRECTORY, create_directory_with_empty_status_file)
    plugins = core.load_plugins()

    threads = []

    for plugin in plugins:
        # plugins run ETL jobs that are independent of each other, therefore run them in threads
        thread = StoppableThread(target=plugin.execute, stop=plugin.stop)
        threads.append(thread)

    try:
        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all threads to finish
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        # one way to pause a plugin ETL job is to listen to interrupt CTRL + C
        # we can then call stop method on our custom thread instance, which will
        # run some stop function like saving the current status of the ETL job
        # before exiting gracefully.
        # TODO(allan): Investigate this mechanism further and improve
        for thread in threads:
            thread.stop()


if __name__ == "__main__":
    run()
