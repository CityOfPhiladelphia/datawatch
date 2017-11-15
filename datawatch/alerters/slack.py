import os
import json

import requests

from datawatch.alerts import Alerter

class SlackAlerter(Alerter):
    name = 'slack'

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
