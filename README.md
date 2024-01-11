# Psystem
This data integration using plugin system for flexibilty and easy scaling. Each ETL pipeline can easily be added to the integration and run separately. This design is based on the thinking that there can be multiple Sources, Transformations and Destinations of data, and each combination of these is unique.

The goal is to make adding more plugins as easy as adding the implementation in a `plugins` directory. It's even possible to have a plugin store where an ETL can easily be installed from.

Each plugin has a configuration that is used to customize some things about it. The configuration can include a url for the API/database that contains the data, some transformations to apply or not and configuration for the destination of this data. 

For demonstration, 2 plugins are developed in the assignment, "sample" and "ghotopostgres" 

## Implementing the assignment
``
Build an ETL pipeline application that uses Python and a PostgreSQL database. The application should extract  data from the [GHO OData API](https://www.who.int/data/gho/info/gho-odata-api) and process that data in any way you see fit, finally ingesting that into the postgres database.
``

With the plugin system implemented, the above ETL pipeline is implemented as a plugin called "ghotopostgres". This plugin and its files are simply added to the plugins directory and the application will pick it up, apply it's configuration and execute it.

### ghotopostgres plugin
Below is the configuration file for this plugin in yaml format
```yaml
name: GHOTopOSTGRES
creator: Zenysis
enabled: true
run: NCDMORT3070
etl:
  NCD_CCS_BreastCancer:
    indicator: NCD_CCS_BreastCancer
    api: https://ghoapi.azureedge.net/api
    transform:
      columns:
        Id: Id
        SpatialDim: Country
        TimeDimType: TimeDimType
        TimeDim: TimeDim
        ParentLocationCode: Region
        Value: Value
        NumericValue: NumericValue
        Date: Date
    destination:
      postgres:
        url: !GHOTopOSTGRES_DATABASE_URL
  NCDMORT3070:
    indicator: NCDMORT3070
    api: https://ghoapi.azureedge.net/api
    transform:
      columns:
        Id: Id
        Value: Value
    destination:
      postgres:
        url: !GHOTopOSTGRES_DATABASE_URL 

```
With this configuration, it's possible to switch the behavior of the ETL pipeline run by this plugin. Let's walk through. The `name` and `creator` field provides some metadata about the plugin. `enabled` provides a way for the plugin system to enable or disable the plugin. One use case can be from a commercial point of view, where a paid plugin can be disabled and then renabled when payment is done.
`run` specifies which ETL configuration to run from the list in `etl` field.

The configuration format is very much guided by the characteristics of the ETL process, and source data. Here, the `etl` field specifies a key for each GHO data indicator, specifies the extract point in `api` field, then the transformations to perform in the `transform` field. In this case, `columns` means the columns that will be extracted from the data fetched and kept in the destination. Finally there's `destination` field that has configuration for the postgres database. For security, only a representation of the postgres url is kept in the yaml configuration. A custom loader within the plugin understands this and replaces it with the actual database url from a safe location like an environment variable.

This is a snippet of such a loader.
```python
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
```

## Pausing and resuming ghotopostgres plugin ETL jobs
Pausing and resuming of ETL jobs is implemented using python thread events, listening to interrupt signal from the keyboard and a status file, `status.yaml` located in a hidden folder `.psystem` in the home directory. The plugin fetches GHO indicator data in pages to **preserve memory**, and after every page is processed, it writes to the status the next page it'll process. When the thread is interrupted by the keyboard interrupt signal i.e. pressing `CTRL+C` on mac, the status is written and program gracefully exits. Starting the program back up reads the plugin status data and picks up the page it needs to resume with. Some edge cases to this mechanism are highlighted below.

## Edge cases
An edge case can occur when the status has not changed, and a new database instance is connected. In such a case, the database table will contain partial data. One remedy is to edit the status file to start from the `page 1` . If this was not done, then some SQL would have to be written to append the missing data to the top of the table.

Ideally, the right thing to do is safe guard against such a scenario, making a check of the database to see which data was last fetched and then synchronizing with the status file.
 
Other edge cases include varying data lengths which affect how we create the postgres table columns

## Setting up
This project has a plugin `ghotopostgres` that needs a postgres database to run. You can run this in your terminal to set an environment variable `GHOTopOSTGRES_DATABASE_URL` with a link to your database.
```
export GHOTopOSTGRES_DATABASE_URL=<your_postgres_database_url>
```

A quick way to setup a test database on your machine is to use docker. Run this command
```
docker run --name ghotopostgres-pg -p 5432:5432 -v /absolute_path_to_a_directory/pg:/var/lib/postgresql/data -e POSTGRES_PASSWORD=mysecretpassword postgres
```
You can then connect to that postgres instance using
```
export GHOTopOSTGRES_DATABASE_URL=postgresql://postgres:mysecretpassword@localhost:5432/postgres
```
To run the project. Set up a virtual env and pip install all requiremts.
```
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
python main.py
```


## Running tests and linting
A `tox.ini` file has been provided and using `tox` command from root of the project, you can run all the test files, flake8 linting and mypy typechecks

## OS specs
Any OS that has a python interpreter can run this program
