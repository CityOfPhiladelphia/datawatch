import os
import hashlib
from datetime import datetime

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

    def __init__(self, logger, alerts_config, alerter_types=None, session=None, default_timeout=1440):
        self.logger = logger
        self.alerter_types = {}
        self.alerters = {}
        self.session = session
        self.default_timeout = default_timeout

        for alerter_type in alerter_types:
            self.alerter_types[alerter_type.name] = alerter_type

        for alert_config_name in alerts_config:
            if alert_config_name == 'default':
                continue

            alert_config = alerts_config[alert_config_name]
            alerter_type = self.alerter_types[alert_config['type']]
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
