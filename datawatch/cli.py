import logging
import sys
import os

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
from .connections import close_connections
from .alerts import Alerts

def get_logger():
    FORMAT = '[%(asctime)-15s] %(levelname)s [%(name)s] %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.INFO)

    def exception_handler(type, value, tb):
        logger.exception("Uncaught exception: {}".format(str(value)), exc_info=(type, value, tb))

    sys.excepthook = exception_handler

    return logging.getLogger()

def get_db_engine(sql_alchemy_connection):
    connection_string = sql_alchemy_connection or os.getenv('SQL_ALCHEMY_CONNECTION')

    if not connection_string:
        raise Exception('Missing DataWatch database connection string')

    return create_engine(connection_string)

def get_config(config_path):
    with open(config_path) as file:
        return yaml.load(file)

def get_alerts(logger, config, use_alerts):
    alerts_instance = None
    if use_alerts:
        if 'alerts' not in config:
            raise Exception('`alerts` not found in config')
        alerts_instance = Alerts(logger, config['alerts'])
    return alerts_instance

def run_test(logger, session, config, table_config, alerts):
    data_test = None

    try:
        data_test = test_table(logger, session, config, table_config)
    except:
        logger.exception('Error testing table', table_config['test_name'])
        ## TODO: trigger alert
        ## TODO: if more than say 5 trigger alerts, kill the process and trigger alert

    if data_test and data_test.error_status:
        if alerts:
            alerts.alert(table_config, data_test)
        return False
    return True

@click.group()
def main():
    pass

@main.command('init-db')
@click.option('--sql-alchemy-connection')
def init_db(sql_alchemy_connection):
    engine = get_db_engine(sql_alchemy_connection)

    BaseModel.metadata.create_all(engine)

@main.command('run-test')
@click.argument('TEST_NAME')
@click.argument('CONFIG_PATH', type=click.Path(exists=True))
@click.option('--sql-alchemy-connection')
@click.option('--alerts/--no-alerts', is_flag=True, default=False)
def run_single_test(test_name, config_path, sql_alchemy_connection, alerts):
    logger = get_logger()

    config = get_config(config_path)

    engine = get_db_engine(sql_alchemy_connection)

    Session = sessionmaker(bind=engine)
    session = Session()

    alerts_instance = get_alerts(logger, config, alerts)

    target_table_config = None
    for table_config in config['table_tests']:
        if table_config['test_name'] == test_name:
            target_table_config = table_config
            break

    if not target_table_config:
        raise Exception('`{}` test config not found'.format(test_name))

    success = run_test(logger, session, config, table_config, alerts_instance)

    close_connections()

    if not success:
        sys.exit(1)

@main.command('run-all')
@click.argument('CONFIG_PATH', type=click.Path(exists=True))
@click.option('--sql-alchemy-connection')
@click.option('--alerts/--no-alerts', is_flag=True, default=False)
def run_all(config_path, sql_alchemy_connection, alerts):
    logger = get_logger()

    config = get_config(config_path)

    engine = get_db_engine(sql_alchemy_connection)

    Session = sessionmaker(bind=engine)
    session = Session()

    alerts_instance = get_alerts(logger, config, alerts)

    for table_config in config['table_tests']:
        run_test(logger, session, config, table_config, alerts_instance)

    close_connections()

    ## TODO: cloudwatch heartbeat
