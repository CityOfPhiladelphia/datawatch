import os
import json

import requests

class Alerts(object):
    def __init__(self, logger, alerts_config):
        self.logger = logger
        self.alerters = {}

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

    def alert(self, table_config, data_test):
        if 'alerts' in table_config:
            if table_config['alerts'] == None or table_config['alerts'] == False:
                return
            else:
                alerters = table_config['alerts']
        else:
            alerters = [self.default]

        for alerter_name in alerters:
            try:
                if alerter_name not in self.alerters:
                    raise Exception('`{}` alert type not found'.format(alerter_name))
                self.alerters[alerter_name].alert(table_config, data_test)
            except:
                if self.logger:
                    self.logger.exception('Exception sending alert using', alerter_name)

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

    def get_message(self, table_config, data_test):
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
                        'value': table_config['query']
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
                        'value': '{:,}'.format(data_test.count),
                        'short': True
                    },
                    {
                        'title': 'Source Count',
                        'value': '{:,}'.format(data_test.source_count),
                        'short': True
                    }
                ]
            }

            if table_config['append_only']:
                count_attachment['fields'].append({
                    'title': 'Last Count (Append Only)',
                    'value': '{:,}'.format(data_test.last_count),
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

    def alert(self, table_config, data_test):
        message = self.get_message(table_config, data_test)
        requests.post(
            self.slack_url,
            data=json.dumps(message),
            headers={'Content-Type': 'application/json'})

alerter_types = {
    'email': EmailAlerter,
    'slack': SlackAlerter
}
