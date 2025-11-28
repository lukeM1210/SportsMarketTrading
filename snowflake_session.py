import os
from snowflake.snowpark import Session


def get_session() -> Session:
    """
    Build and return a Snowpark Session using env vars.
    Set these in your shell or VS Code run config:
      SNOW_ACCOUNT, SNOW_USER, SNOW_PASSWORD,
      SNOW_ROLE, SNOW_WAREHOUSE
    """
    connection_parameters = {
        "account": os.environ["SNOW_ACCOUNT"],
        "user": os.environ["SNOW_USER"],
        "password": os.environ["SNOW_PASSWORD"],
        "role": os.environ.get("SNOW_ROLE", "SYSADMIN"),
        "warehouse": os.environ.get("SNOW_WAREHOUSE", "COMPUTE_WH"),
        "database": os.environ.get("SNOW_DATABASE", "DEMO"),
        "schema": os.environ.get("SNOW_SCHEMA", "NFL_ODDS"),
    }

    return Session.builder.configs(connection_parameters).create()


if __name__ == "__main__":
    session = get_session()
    print(session.sql("SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()").collect())
