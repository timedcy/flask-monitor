# -*- coding: utf-8 -*-
from __future__ import absolute_import
from flask_monitor import ObserverMetrics

from influxdb import InfluxDBClient
from influxdb.client import InfluxDBClientError


class ObserverInfluxdb(ObserverMetrics):

    def __init__(self, host, port, user, password, db, ssl=False, verify_ssl=False, measure='flask', skip_path=None, *args, **kw):
        ObserverMetrics.__init__(self, *args, **kw)
        self._data = [
            {
                "measurement": measure,
                "tags": {},
                "fields": {},
            }
        ]
        self.field_set = set(['remote_addr', 'delta', 'start', 'asctime'])
        self.skip_path = set(skip_path if skip_path else [])
        try:
            self.db = InfluxDBClient(host=host,
                                     port=port,
                                     username=user,
                                     password=password,
                                     database=db,
                                     ssl=ssl,
                                     verify_ssl=verify_ssl)
        except InfluxDBClientError:
            self.logger.critical("Cannot connect to InfluxDB database '%s'" % db)


    def action(self, event):
        try:
            data = self._data
            if event.dict and event.dict.get('path') in skip_path:
                return
            data[0]['tags'] = {k: v for k, v in event.dict.items() if k not in self.field_set}
            data[0]['fields'] = {k: v for k, v in event.dict.items() if k in self.field_set}
            data[0]['fields']["cost"] = event.timing*1000.0
            self.db.write_points(data)
        except InfluxDBClientError as e:
            self.logger.critical("Error InfluxDB '%s'" % str(e))
        except Exception as e:
            self.logger.critical("Error Unknow on InfluxDB '%s'" % str(e))
