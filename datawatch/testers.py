from datetime import datetime

from .connections import get_connection
from .models import DataTest

def get_test_error(exception):
    if isinstance(exception, requests_exceptions.ConnectionError):
        return 'connection_error', 'Error reaching the host', None
    elif isinstance(exception, requests_exceptions.HTTPError):
        return 'http_error', exception.response.text, exception.response.status_code
    elif isinstance(exception, requests_exceptions.SSLError):
        return 'connection_error', 'Issue with connection SSL', None
    elif isinstance(exception, requests_exceptions.ConnectTimeout):
        return 'connection_error', 'Timeout initiating connection', None
    elif isinstance(exception, requests_exceptions.ReadTimeout):
        return 'server_error', 'Timeout loading data from response', None
    elif isinstance(exception, requests_exceptions.RequestException):
        http_status = None
        if hasattr(exception, 'response') and hasattr(exception.response, 'status_code'):
            http_status = exception.response.status_code
        return 'http_error', 'Unknown exception from HTTP request', http_status
    ## TODO: sqlalchemy exceptions
    else:
        if hasattr(exception, 'args') and len(exception.args) > 0:
            error_message = args[0]
        else:
            error_message = str(exception)
        return 'exception', error_message, None

def test_table(logger, session, config, table_config):
    connection = get_connection(config, table_config['connection'])

    error_status = None
    error_message = None
    query_error_status = None
    query_error_message = None

    ## start test

    start = datetime.utcnow()

    try:
        rows, http_status = connection.query(table_config['test_query'])
    except Exception as exception:
        logger.exception('Exception testing', table_config['test_name'])
        query_error_status, query_error_message, http_status = get_test_error(exception)

    end = datetime.utcnow()

    elapsed_time = int((end - start).total_seconds() * 1000)

    if query_error_status:
        error_status = 'test_query_failed'
        error_message = 'The test query failed'

    ## end test

    ## start counts

    try:
        count = connection.count(table_config['table'])
    except:
        logger.exception('Error counting table', table_config['test_name'])
        if not error_status:
            error_status = 'count_error'
            error_message = 'Error counting table'

    try:
        source_connection = get_connection(config, table_config['source_connection'])
        source_count =  source_connection.count(table_config['source_table'])
    except:
        logger.exception('Error counting source table', table_config['source_table'])
        if not error_status:
            error_status = 'count_error'
            error_message = 'Error counting source table'

    if not error_status and count != source_count:
        error_status = 'count_error'
        error_message = 'The `{}` and `{}` counts do not match'.format(
            table_config['table'], table_config['source_table'])

    ## end counts

    last_count = None
    if table_config['append_only']:
        last_data_test = session.query(DataTest.count)\
            .filter(DataTest.test_name == table_config['test_name'])\
            .order_by(DataTest.timestamp.desc())\
            .first()
        if last_data_test:
            last_count = last_data_test.count

    if not error_status and last_count != None and count < last_count:
        error_status = 'count_error'
        error_message = '`{}` is append only but has a lower count than the last test'.format(
            table_config['test_name'])

    data_test = DataTest(
        test_name=table_config['test_name'],
        timestamp=start,
        error_status=error_status,
        error_message=error_message,
        query_started_at=start,
        query_ended_at=end,
        query_time_elapsed_ms=elapsed_time,
        query_error_status=query_error_status,
        query_error_message=query_error_message,
        query_http_status=http_status,
        count=count,
        source_count=source_count,
        last_count=last_count)
    session.add(data_test)
    session.commit()

    return data_test
