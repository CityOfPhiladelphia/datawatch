import os
import json
import hashlib
from datetime import datetime

import requests

from .models import Alert as AlertModel

class Alerts(object):
    hash_fields = [
        'table',
        'source_table',
        'test_query',
        'error_status',
        'error_message',
        'query_error_status',
        'query_error_message',
        'query_http_status',
        'count',
        'source_count',
        'last_count'
    ]

    def __init__(self, logger, alerts_config, session=None, default_timeout=1440):
        self.logger = logger
        self.alerters = {}
        self.session = session
        self.default_timeout = default_timeout

        for alert_config_name in alerts_config:
            if alert_config_name == 'default':
                continue

            alert_config = alerts_config[alert_config_name]
            alerter_type = alerter_types[alert_config['type']]
            alerter_params = dict(alert_config)
            del alerter_params['type']
            self.alerters[alert_config_name] = alerter_type(**alerter_params)

        if 'default' in alerts_config:
            default = alerts_config['default']
        else:
            default = alerts_config.keys()[0]

        self.default = default

    def get_alert_hash(self, table_config, data_test):
        m = hashlib.sha256()

        for key in self.hash_fields:
            if key in table_config:
                value = table_config[key]
            elif hasattr(data_test, key):
                value = getattr(data_test, key)
            else:
                continue

            if isinstance(value, str):
                m.update(value.encode('utf-8'))
            else:
                m.update(value)

        return m.hexdigest()

    def check_last_alert(self, table_config, alert_hash):
        last_alert = (
            self.session.query(AlertModel)
            .filter(AlertModel.test_name == table_config['test_name'])
            .order_by(AlertModel.timestamp.desc())
            .first()
        )

        if not last_alert:
            return False

        if 'alert_timeout' in table_config:
            timeout = table_config['alert_timeout']
        else:
            timeout = self.default_timeout

        time_delta = datetime.utcnow() - last_alert.timestamp
        time_delta_minutes = round(time_delta.total_seconds() / 60)

        return (
            (last_alert.alert_hash == alert_hash) and
            (time_delta_minutes <= timeout)
        )

    def record_alert(self, table_config, data_test, alert_hash):
        alert_record = AlertModel(
            data_test_id=data_test.id if data_test else None,
            timestamp=data_test.timestamp if data_test else datetime.utcnow(),
            test_name=table_config['test_name'],
            alert_hash=alert_hash)
        self.session.add(alert_record)
        self.session.commit()

    def alert(self, table_config, data_test):
        if 'alerts' in table_config:
            if table_config['alerts'] == None or table_config['alerts'] == False:
                return
            else:
                alerters = table_config['alerts']
        else:
            alerters = [self.default]

        alert_hash = self.get_alert_hash(table_config, data_test)

        skip_alert = self.check_last_alert(table_config, alert_hash)

        if skip_alert:
            if self.logger:
                self.logger.info('Skipping alert for {}'.format(repr(data_test or table_config['test_name'])))
            return

        for alerter_name in alerters:
            try:
                if alerter_name not in self.alerters:
                    raise Exception('`{}` alert type not found'.format(alerter_name))
                self.alerters[alerter_name].alert(table_config, data_test)
            except:
                if self.logger:
                    self.logger.exception('Exception sending alert using', alerter_name)

        try:
            self.record_alert(table_config, data_test, alert_hash)
        except:
            if self.logger:
                self.logger.exception('Exception recording alert')

class Alerter(object):
    def alert(self, table_config, data_test):
        raise NotImplementedError()

class EmailAlerter(Alerter):
    pass
    ## TODO: implement

class SlackAlerter(Alerter):
    def __init__(self, *args, at_channel=False, slack_url=None, **kwargs):
        if slack_url == None:
            slack_url = os.getenv('SLACK_WEBHOOK_URL')
        elif slack_url[0] == '$':
            slack_url = os.getenv(slack_url[1:])

        self.slack_url = slack_url
        self.at_channel = at_channel

    def get_datatest_message(self, table_config, data_test):
        attachments = [
            {
                'color': '#ff0000',
                'fields': [
                    {
                        'title': 'Table',
                        'value': table_config['table'],
                        'short': True
                    },
                    {
                        'title': 'Source Table',
                        'value': table_config['source_table'],
                        'short': True
                    },
                    {
                        'title': 'Error Status',
                        'value': data_test.error_status,
                        'short': True
                    },
                    {
                        'title': 'Error Message',
                        'value': data_test.error_message
                    }
                ]
            }
        ]

        if data_test.error_status == 'test_query_failed':
            attachments.append({
                'title': 'Query Failure',
                'color': '#ff0000',
                'fields': [
                    {
                        'title': 'Query',
                        'value': table_config['test_query']
                    },
                    {
                        'title': 'Query Error Status',
                        'value': data_test.query_error_status,
                        'short': True
                    },
                    {
                        'title': 'Query Error Message',
                        'value': data_test.query_error_message
                    },
                    {
                        'title': 'Query Elapsed Time',
                        'value': str(data_test.query_time_elapsed_ms) + ' ms',
                        'short': True
                    },
                    {
                        'title': 'Query HTTP Status',
                        'value': str(data_test.query_http_status),
                        'short': True
                    }
                ]
            })

        if data_test.error_status == 'count_error':
            count_attachment = {
                'title': 'Count Failure',
                'color': '#ff0000',
                'fields': [
                    {
                        'title': 'Count',
                        'value': '{:,}'.format(data_test.count) if data_test.count else 'Unavailable',
                        'short': True
                    },
                    {
                        'title': 'Source Count',
                        'value': '{:,}'.format(data_test.source_count) if data_test.source_count else 'Unavailable',
                        'short': True
                    }
                ]
            }

            if table_config['append_only']:
                count_attachment['fields'].append({
                    'title': 'Last Count (Append Only)',
                    'value': '{:,}'.format(data_test.last_count) if data_test.last_count else 'Unavailable',
                    'short': True
                })

            attachments.append(count_attachment)

        at_channel_str = ''
        if self.at_channel:
            at_channel_str = '<!channel> '

        return {
            'text': '{}DataWatch Test Failure - *{}*'.format(at_channel_str, table_config['test_name']),
            'attachments': attachments
        }

    def get_failed_test_message(self, table_config):
        at_channel_str = ''
        if self.at_channel:
            at_channel_str = '<!channel> '

        return {
            'text': '{}DataWatch Test Failure - *Unable to test* - *{}*'.format(at_channel_str, table_config['test_name'])
        }

    def alert(self, table_config, data_test):
        if data_test == None:
            message = self.get_failed_test_message(table_config)
        else:
            message = self.get_datatest_message(table_config, data_test)
        requests.post(
            self.slack_url,
            data=json.dumps(message),
            headers={'Content-Type': 'application/json'})

alerter_types = {
    'email': EmailAlerter,
    'slack': SlackAlerter
}
