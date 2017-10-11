from datetime import datetime

from .connections import get_connection
from .models import DataPing

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

    ## start test

    start = datetime.utcnow()

    error_status = None
    error_message = None

    try:
        rows, http_status = connection.query(table_config['test_query'])
    except Exception as exception:
        logger.exception('Exception testing', table_config['name'])
        error_status, error_message, http_status = get_test_error(exception)

    end = datetime.utcnow()

    elapsed_time = int((end - start).total_seconds() * 1000)

    ## end test

    ## start counts

    try:
        count = connection.count(table_config['table'])
    except:
        logger.exception('Error counting table', table_config['name'])
        error_status = 'count_error'
        error_message = 'Error counting table'

    try:
        source_connection = get_connection(table_config['source_connection'])
        source_count =  source_connection.count(table_config['source_table'])
    except:
        logger.exception('Error counting source table', table_config['source_table'])
        error_status = 'count_error'
        error_message = 'Error counting source table'

    ## end counts

    ## TODO: append only check - create counts table and compare it to the last check

    data_ping = DataPing(
        data_source=table_config['name'],
        test_started_at=start,
        test_ended_at=end,
        test_time_elapsed_ms=elapsed_time,
        test_error_status=error_status,
        test_error_message=error_message,
        test_http_status=http_status,
        count=count,
        source_count=source_count)
    session.add(data_ping)
    session.commit()

    return data_ping
