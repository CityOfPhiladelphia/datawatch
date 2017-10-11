import logging
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import click
import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from .models import BaseModel
from .testers import test_table

def get_logger():
    FORMAT = '[%(asctime)-15s] %(levelname)s [%(name)s] %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.INFO)

    def exception_handler(type, value, tb):
        logger.exception("Uncaught exception: {}".format(str(value)), exc_info=(type, value, tb))

    sys.excepthook = exception_handler

    return logging.getLogger()

def get_db_engine(sql_alchemy_connection):
    connection_string = sql_alchemy_connection or os.getenv('SQL_ALCHEMY_CONNECTION')

    return create_engine(connection_string)

@click.group()
def main():
    pass

@main.command()
@click.option('--sql-alchemy-connection')
def init_db(sql_alchemy_connection):
    engine = get_db_engine(sql_alchemy_connection)

    BaseModel.metadata.create_all(engine)

@main.command()
@click.argument('CONFIG_PATH', type=click.Path(exists=True))
@click.option('--sql-alchemy-connection')
def run(config_path, sql_alchemy_connection):
    logger = get_logger()

    with open(config_path) as file:
        config = yaml.load(file)

    engine = get_db_engine(sql_alchemy_connection)

    Session = sessionmaker(bind=engine)
    session = Session()

    for table_config in config['tables']:
        try:
            test_table(logger, session, config, table_config)
        except:
            logger.error('Error testing table', table_config['name'])
            ## TODO: trigger monitoring

    ## TODO: cloudwatch heartbeat
