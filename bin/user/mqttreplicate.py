''' Replicate WeeWX dstabases using MQTT request/response functionality.'''
# pylint: disable=fixme, too-many-instance-attributes, too-many-arguments
import abc
import argparse
import json
import logging
import queue
import random
import threading
import time

import paho
import paho.mqtt
import paho.mqtt.client

import weecfg
import weeutil
import weeutil.logger
import weewx
import weewx.drivers
import weewx.engine

from weeutil.weeutil import to_bool, to_int

VERSION = '0.0.1'
DRIVER_NAME = 'MQTTRequester'
DRIVER_VERSION = VERSION
REQUEST_TOPIC = 'replicate/request'
RESPONSE_TOPIC = 'replicate/response'
ARCHIVE_TOPIC = 'replicate/archive'

class Logger():
    ''' Manage the logging '''
    def __init__(self):
        self.log = logging.getLogger(__name__)

    def logdbg(self, msg):
        """ log debug messages """
        self.log.debug(msg)

    def loginf(self, msg):
        """ log informational messages """
        self.log.info(msg)

    def logerr(self, msg):
        """ log error messages """
        self.log.error(msg)

class MQTTClient(abc.ABC):
    ''' Abstract class that wraps paho mqtt to protect from breaking changes. '''
    @classmethod
    def get_client(cls, logger, client_id, userdata):
        ''' Factory method to get appropriate MQTTClient for paho mqtt version. '''
        if hasattr(paho.mqtt.client, 'CallbackAPIVersion'):
            return MQTTClientV2(logger, client_id, userdata)

        raise ValueError("paho mqtt v2 is required.")

    def connect(self, host, port, keepalive):
        ''' Connect to the MQTT server. '''
        raise NotImplementedError("Method 'connect' is not implemented")

    def disconnect(self):
        ''' Connect to the MQTT server. '''
        raise NotImplementedError("Method 'disconnect' is not implemented")

    def loop_start(self):
        ''' Connect to the MQTT server. '''
        raise NotImplementedError("Method 'loop_start' is not implemented")

    def loop(self):
        ''' Connect to the MQTT server. '''
        raise NotImplementedError("Method 'loop' is not implemented")

    def loop_stop(self):
        ''' Connect to the MQTT server. '''
        raise NotImplementedError("Method 'loop_stop' is not implemented")

    def subscribe(self, topic, qos):
        ''' Subscribe to the MQTT topic. '''
        raise NotImplementedError("Method 'subscribe' is not implemented")

    def publish(self, topic, data, qos, retain, properties=None):
        ''' Publish the MQTT message. '''
        raise NotImplementedError("Method 'publish' is not implemented")

class MQTTClientV2(MQTTClient):
    ''' MQTTClient that communicates with paho mqtt v2. '''
    def __init__(self, logger, client_id, userdata):
        self.logger = logger
        self.client_id = client_id
        self.client = paho.mqtt.client.Client(
            callback_api_version=paho.mqtt.client.CallbackAPIVersion.VERSION2,
            protocol=paho.mqtt.client.MQTTv5,
            client_id=self.client_id,
            userdata=userdata)

        self._on_connect = None
        self._on_connect_fail = None
        self._on_disconnect = None
        self._on_log = None
        self._on_message = None
        self._on_publish = None
        self._on_subscribe = None

    # Properties for each supported callback

    @property
    def on_connect(self):
        ''' The on_connect call back function. '''
        return self._on_connect

    @on_connect.setter
    def on_connect(self, value):
        self._on_connect = value
        if value:
            self.client.on_connect = self._client_on_connect
        else:
            self.client.on_connect = None

    @property
    def on_connect_fail(self):
        ''' The on_connect_fail call back function. '''
        return self._on_connect_fail

    @on_connect_fail.setter
    def on_connect_fail(self, value):
        self._on_connect_fail = value
        if value:
            self.client.on_connect_fail = self._client_on_connect_fail
        else:
            self.client.on_connect_fail = None

    @property
    def on_disconnect(self):
        ''' The on_disconnect call back function. '''
        return self._on_disconnect

    @on_disconnect.setter
    def on_disconnect(self, value):
        self._on_disconnect = value
        if value:
            self.client.on_disconnect = self._client_on_disconnect
        else:
            self.client.on_disconnect = None

    @property
    def on_log(self):
        ''' The on_log call back function. '''
        return self._on_log

    @on_log.setter
    def on_log(self, value):
        self._on_log = value
        if value:
            self.client.on_log = self._client_on_log
        else:
            self.client.on_log = None

    @property
    def on_message(self):
        ''' The on_message call back function. '''
        return self._on_message

    @on_message.setter
    def on_message(self, value):
        self._on_message = value
        if value:
            self.client.on_message = self._client_on_message
        else:
            self.client.on_message = None

    @property
    def on_publish(self):
        ''' The on_publish call back function. '''
        return self._on_publish

    @on_publish.setter
    def on_publish(self, value):
        self._on_publish = value
        if value:
            self.client.on_publish = self._client_on_publish
        else:
            self.client.on_publish = None

    @property
    def on_subscribe(self):
        ''' The on_subscribe call back function. '''
        return self._on_subscribe

    @on_subscribe.setter
    def on_subscribe(self, value):
        self._on_subscribe = value
        if value:
            self.client.on_subscribe = self._client_on_subscribe
        else:
            self.client.on_subscribe = None

    # The wrappers of the  client methods are next

    def connect(self, host, port, keepalive):
        self.client.connect(host, port, keepalive)

    def disconnect(self):
        self.client.disconnect()

    def loop(self):
        self.client.loop()

    def loop_start(self):
        self.client.loop_start()

    def loop_stop(self):
        self.client.loop_stop()

    def publish(self, topic, data, qos, retain, properties=None):
        return self.client.publish(topic, data, qos, retain, properties)

    def subscribe(self, topic, qos):
        return self.client.subscribe(topic, qos)

    # The  wrappers of the callbacks are next

    def _client_on_connect(self, _client, userdata, flags, reason_code, _properties):
        self.logger.logdbg(f"Client {self.client_id} connected with result code {reason_code}")
        self.logger.logdbg((f"Client {self.client_id}"
                            f" connected with result code {int(reason_code.value)}"))
        self.logger.logdbg(f"Client {self.client_id} connected flags {str(flags)}")
        self._on_connect(userdata)

    def _client_on_connect_fail(self, _client, userdata):
        self._on_connect_fail(userdata)

    def _client_on_disconnect(self, _client, userdata, _flags, reason_code, _properties):
        self.logger.logdbg((f"Client {self.client_id}"
                            f" disconnected with result code {reason_code.value}"))
        self._on_disconnect(userdata, reason_code.value)

    def _client_on_log(self, _client, userdata, level, msg):
        """ The on_log callback. """
        self._on_log(userdata, level, msg)

    def _client_on_message(self, _client, userdata, msg):
        self._on_message(userdata, msg)

    def _client_on_publish(self, _client, userdata, mid, _reason_codes, _properties):
        """ The on_publish callback. """
        self.logger.logdbg(f"Client {self.client_id} published  ({int(time.time())}): {mid}")
        self._on_publish(userdata)

    def _client_on_subscribe(self, _client, userdata, mid, _reason_code_list, _properties):
        self._on_subscribe(userdata, mid)

class MQTTResponder(weewx.engine.StdService):
    ''' The "server" that sends the replication data to the requester/client. '''
    def __init__(self, engine, config_dict):
        super().__init__(engine, config_dict)
        self.logger = Logger()

        if not to_bool(config_dict.get('MQTTReplicate', {})\
                       .get('Responder', {})\
                        .get('enable', True)):
            self.logger.loginf("Responder not enabled, exiting.")
            return

        self.client_id = 'MQTTReplicateRespond-' + str(random.randint(1000, 9999))

        service_dict = config_dict.get('MQTTReplicate', {}).get('Responder', {})
        self.request_topic = service_dict.get('request_topic', REQUEST_TOPIC)
        self.archive_topic = service_dict.get('archive_topic', ARCHIVE_TOPIC)
        delta = service_dict.get('delta', 60)
        host = service_dict.get('host', 'localhost')
        port = service_dict.get('port', 1883)
        keepalive = service_dict.get('keepalive', 60)

        self.data_bindings = {}
        for database_name in service_dict['databases']:
            _data_binding = service_dict['databases'][database_name]['data_binding']
            self.data_bindings[_data_binding] = {}
            self.data_bindings[_data_binding]['delta'] = \
            service_dict['databases'][database_name].get('delta', delta)
            self.data_bindings[_data_binding]['type'] = \
                service_dict['databases'][database_name].get('type', 'secondary')
            manager_dict = weewx.manager.get_manager_dict_from_config(config_dict, _data_binding)
            self.data_bindings[_data_binding]['dbmanager'] = \
                weewx.manager.open_manager(manager_dict)

        self.mqtt_logger = {
            paho.mqtt.client.MQTT_LOG_INFO: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_NOTICE: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_WARNING: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_ERR: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_DEBUG: self.logger.loginf
        }

        self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)

        self.data_queue = queue.Queue()
        self._thread = MQTTResponderThread(self.logger,
                                           delta,
                                           config_dict,
                                           False,
                                           self.data_queue,
                                           host,
                                           port,
                                           keepalive)
        self._thread.start()

        self.mqtt_client = MQTTClient.get_client(self.logger, self.client_id, None)

        self.mqtt_client.on_connect = self._on_connect
        if service_dict.get('log_mqtt', False):
            self.mqtt_client.on_log = self._on_log
        self.mqtt_client.on_message = self._on_message

        self.mqtt_client.connect(host, port, keepalive)
        self.mqtt_client.loop_start()

    def shut_down(self):
        ''' Perform operations to terminate MQTT.'''
        for _, data_binding in self.data_bindings.items():
            data_binding['dbmanager'].close()
        self.logger.loginf(f'Client {self.client_id} shutting down the MQTT client.')
        self.mqtt_client.disconnect()
        self.mqtt_client.loop_stop()
        self._thread.shut_down()

    def new_archive_record(self, event):
        ''' Handle the new_archive_record event.'''
        for data_binding_name, data_binding in self.data_bindings.items():
            qos = 0
            if data_binding['type'] == 'main':
                payload = json.dumps(event.record)
            else:
                timestamp = event.record['dateTime']
                # some extensions do not force the timestamp to be on an interval
                record = data_binding['dbmanager'].getRecord(timestamp,
                                                             max_delta=data_binding['delta'])
                if record:
                    payload = json.dumps(record)
                else:
                    self.logger.loginf((f'Client {self.client_id}'
                                       f' binding {data_binding_name}'
                                       f' timestamp {timestamp} no record.'))
                    continue

            properties = paho.mqtt.client.Properties(paho.mqtt.client.PacketTypes.PUBLISH)
            properties.UserProperty = [
                ('data_binding', data_binding_name)
                ]
            self.logger.logdbg((f'Client {self.client_id}'
                                f' publishing binding: {data_binding_name},'
                                f' payload: {payload}.'))
            mqtt_message_info = self.mqtt_client.publish(self.archive_topic,
                                                        payload,
                                                        qos,
                                                        False,
                                                        properties=properties)
            self.logger.logdbg((f"Client {self.client_id}"
                                f" binding {data_binding_name}"
                                f" publishing ({int(time.time())}):"
                                f" {mqtt_message_info.mid} {qos} {self.archive_topic}"))

    def _on_connect(self, _userdata):
        (result, mid) = self.mqtt_client.subscribe(self.request_topic, 0)
        self.logger.loginf((f"Client {self.client_id}"
                    f" subscribing to {self.request_topic}"
                    f" has a mid {int(mid)}"
                    f" and rc {int(result)}"))

    def _on_log(self, _client, _userdata, level, msg):
        self.mqtt_logger[level](f"Client {self.client_id} MQTT log: {msg}")

    def _on_message(self, _userdata, msg):
        self.logger.logdbg((f"Client {self.client_id}:"
                            f" topic: {msg.topic},"
                            f" QOS: {int(msg.qos)},"
                            f" retain: {msg.retain},"
                            f" payload: {msg.payload},"
                            f" properties: {msg.properties}"))            

        if not hasattr(msg.properties,'UserProperty'):
            self.logger.logerr(f'Client {self.client_id} has no "UserProperty"')
            self.logger.logerr(f'Client {self.client_id}'
                               f' skipping topic: {msg.topic} payload: {msg.payload}')
            return

        user_property = msg.properties.UserProperty
        data_binding = None
        for keyword_value in user_property:
            if keyword_value[0] == 'data_binding':
                data_binding = keyword_value[1]
                break

        if not data_binding:
            self.logger.logerr(f'Client {self.client_id} has no "data_binding" UserProperty')
            self.logger.logerr(f'Client {self.client_id}'
                               f' skipping topic: {msg.topic} payload: {msg.payload}')
            return

        if data_binding not in self.data_bindings:
            self.logger.logerr(f'Client {self.client_id} has unknown data_binding {data_binding}')
            self.logger.logerr(f'Client {self.client_id}'
                               f' skipping topic: {msg.topic} payload: {msg.payload}')
            return

        response_topic = msg.properties.ResponseTopic

        properties = paho.mqtt.client.Properties(paho.mqtt.client.PacketTypes.PUBLISH)
        properties.UserProperty = [
            ('data_binding', data_binding)
            ]

        start_timestamp = int(msg.payload.decode('utf-8'))

        self.logger.logdbg(f'Client {self.client_id} received msg: {msg}')
        self.logger.logdbg((f'Client {self.client_id}'
                            f' responding on response topic: {response_topic}'))

        data = {'topic': response_topic,
                'start_timestamp': start_timestamp,
                'data_binding': data_binding,
                'properties': properties}               
        self.data_queue.put(data)
        self.logger.logdbg(self.logger.logdbg(f'Client {self.client_id} queued: {data}'))

class MQTTResponderThread(threading.Thread):
    '''  Publish the requested data. '''
    def __init__(self, logger, delta, config_dict, log_mqtt, data_queue, host, port, keepalive):
        threading.Thread.__init__(self)
        self.logger = logger
        self.config_dict = config_dict
        self.data_queue = data_queue
        self.client_id = 'MQTTReplicateRespondThread-' + str(random.randint(1000, 9999))
        self.data_bindings = {}

        service_dict = config_dict.get('MQTTReplicate', {}).get('Responder', {})
        for database_name in service_dict['databases']:
            _data_binding = service_dict['databases'][database_name]['data_binding']
            self.data_bindings[_data_binding] = {}
            self.data_bindings[_data_binding]['delta'] = \
            service_dict['databases'][database_name].get('delta', delta)
            self.data_bindings[_data_binding]['type'] = \
                service_dict['databases'][database_name].get('type', 'secondary')
            manager_dict = weewx.manager.get_manager_dict_from_config(config_dict, _data_binding)
            self.data_bindings[_data_binding]['dbmanager'] = \
                weewx.manager.open_manager(manager_dict)

        self.mqtt_logger = {
            paho.mqtt.client.MQTT_LOG_INFO: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_NOTICE: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_WARNING: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_ERR: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_DEBUG: self.logger.loginf
        }

        self.mqtt_client = MQTTClient.get_client(self.logger, self.client_id, None)

        if log_mqtt:
            self.mqtt_client.on_log = self._on_log

        self.mqtt_client.connect(host, port, keepalive)
        self.mqtt_client.loop_start()

    def shut_down(self):
        ''' Perform operations to terminate MQTT.'''
        self.logger.loginf(f'Client {self.client_id} shutting down the MQTT client.')
        self.mqtt_client.disconnect()

    def run(self):
        self.logger.logdbg(f"Client {self.client_id} starting queue loop")
        for data_binding_name, data_binding in self.data_bindings.items():
            manager_dict = weewx.manager.get_manager_dict_from_config(self.config_dict,
                                                                      data_binding_name)
            data_binding['dbmanager'] = weewx.manager.open_manager(manager_dict)

        while True:
            try:
                data = self.data_queue.get(True, 5)
                record_count = 0
                for record in self.data_bindings[data['data_binding']]['dbmanager']\
                                                 .genBatchRecords(data['start_timestamp']):
                    record_count += 1
                    payload = json.dumps(record)
                    qos = 0
                    self.logger.logdbg((f'Client {self.client_id} {data["topic"]} {data["data_binding"]}'
                                        f' publishing is: {payload}.'))
                    mqtt_message_info = self.mqtt_client.publish(data['topic'],
                                                                 payload,
                                                                 qos,
                                                                 False,
                                                                 properties=data['properties'])
                    self.logger.logdbg((f"Client {self.client_id} {data['topic']}"
                                        f"  published {mqtt_message_info.mid} {qos}"))                
                self.logger.loginf((f"Client {self.client_id} {data['topic']} {data['properties']}"
                                    f"  published {record_count} records."))                            
            except queue.Empty:
                pass

        self.logger.logdbg(f"Client {self.client_id} queue loop ended")
        for data_binding_name, data_binding in self.data_bindings.items():
            data_binding['dbmanager'].close()

    def _on_log(self, _client, _userdata, level, msg):
        self.mqtt_logger[level](f"Client {self.client_id} MQTT log: {msg}")

def loader(config_dict, engine):
    """ Load and return the driver. """
    return MQTTRequester(config_dict, engine) # pragma: no cover

class MQTTRequester(weewx.drivers.AbstractDevice):
    # (methods not used) pylint: disable=abstract-method
    ''' The "client" class that data ts replicated to. '''
    def __init__(self, config_dict, _engine):
        self.logger = Logger()
        stn_dict = config_dict['MQTTReplicate']['Requester']

        self.the_time = time.time()
        self.loop_interval = float(stn_dict.get('loop_interval', 2.5))
        self._archive_interval = to_int(stn_dict.get('archive_interval', 300))
        self.wait_before_retry = float(stn_dict.get('wait_before_retry', 10))
        self.archive_topic = stn_dict.get('archive_topic', ARCHIVE_TOPIC)

        self.client_id = 'MQTTReplicateRequest-' + str(random.randint(1000, 9999))
        self.response_topic = stn_dict.get('response_topic',
                                               f'{RESPONSE_TOPIC}/{self.client_id}')
        self.request_topic = stn_dict.get('request_topic', REQUEST_TOPIC)

        self.data_bindings = {}
        for database_name in stn_dict['databases']:
            _primary_data_binding = stn_dict['databases'][database_name]['primary_data_binding']
            _secondary_data_binding = \
                stn_dict['databases'][database_name]['secondary_data_binding']
            self.data_bindings[_primary_data_binding] = {}
            self.data_bindings[_primary_data_binding]['type'] = \
                stn_dict['databases'][database_name].get('type', 'secondary')
            self.data_bindings[_primary_data_binding]['manager_dict'] = \
                weewx.manager.get_manager_dict_from_config(config_dict, _secondary_data_binding)
            self.data_bindings[_primary_data_binding]['dbmanager'] = None

        self.mqtt_logger = {
            paho.mqtt.client.MQTT_LOG_INFO: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_NOTICE: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_WARNING: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_ERR: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_DEBUG: self.logger.loginf
        }

        self.mqtt_client = MQTTClient.get_client(self.logger, self.client_id, None)

        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_disconnect = self._on_disconnect
        if stn_dict.get('log_mqtt', False):
            self.mqtt_client.on_log = self._on_log
        self.mqtt_client.on_message = self._on_message

        self.mqtt_client.connect(stn_dict.get('host', 'localhost'),
                                 stn_dict.get('port', 1883),
                                 stn_dict.get('keepalive', 60))

        self.data_queue = queue.Queue()

        self.mqtt_client.loop_start()

    @property
    def hardware_name(self):
        """ The name of the hardware driver. """
        return DRIVER_NAME

    @property
    def archive_interval(self):
        """ The archive interval. """
        return self._archive_interval

    def genArchiveRecords(self, _lastgood_ts):
        while True:
            try:
                # ToDo: needs rework
                yield self.data_queue.get(True ,self.wait_before_retry)
            except queue.Empty:
                break

    def genLoopPackets(self):
        while True:
            sleep_time = self.the_time + self.loop_interval - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)

            self.the_time += self.loop_interval
            yield {'dateTime': int(self.the_time+0.5),
                   'usUnits' : weewx.US }

    def closePort(self):
        """Run when an engine shutdown is requested."""
        self.mqtt_client.disconnect()
        self.mqtt_client.loop_stop()

    def genStartupRecords(self, last_ts):
        ''' Request the missing data. '''
        qos = 0

        for data_binding_name, _data_binding in self.data_bindings.items():
            properties = paho.mqtt.client.Properties(paho.mqtt.client.PacketTypes.PUBLISH)
            properties.ResponseTopic = self.response_topic
            properties.UserProperty = [
                ('data_binding', data_binding_name)
                ]

            mqtt_message_info = self.mqtt_client.publish(self.request_topic,
                                                        last_ts,
                                                        qos,
                                                        False,
                                                        properties=properties)
            self.logger.loginf((f"Client {self.client_id}"
                        f"  topic ({self.request_topic}):"            
                        f"  data_binding ({data_binding_name}):"
                        f"  publishing ({last_ts}):"
                        f"  properties ({properties}):"                        
                        f" {mqtt_message_info.mid} {qos}"))

        yield from()

    def _on_connect(self, _userdata):
        (result, mid) = self.mqtt_client.subscribe(self.response_topic, 0)
        self.logger.loginf((f"Client {self.client_id}"
                         f" subscribing to {self.response_topic}"
                         f" has a mid {int(mid)}"
                         f" and rc {int(result)}"))

        (result, mid) = self.mqtt_client.subscribe(self.archive_topic, 0)
        self.logger.loginf((f"Client {self.client_id}"
                         f" subscribing to {self.archive_topic}"
                         f" has a mid {int(mid)}"
                         f" and rc {int(result)}"))     

        # dbmanager needs to be created in same thread as on_message called
        for _, data_binding in self.data_bindings.items():
            if not data_binding['dbmanager']:
                data_binding['dbmanager'] = weewx.manager.open_manager(data_binding['manager_dict'])

    def _on_disconnect(self, _userdata, rc):
        if rc == 0:
            for data_binding_name, data_binding in self.data_bindings.items():
                data_binding['dbmanager'].close()
                self.logger.logdbg(f"Client {self.client_id} closed db {data_binding_name}.")

    def _on_log(self, _client, _userdata, level, msg):
        self.mqtt_logger[level](f"Client {self.client_id} MQTT log: {msg}")

    def _on_message(self, _userdata, msg):
        self.logger.logdbg((f"Client {self.client_id}:"
                            f" topic: {msg.topic},"
                            f" QOS: {int(msg.qos)},"
                            f" retain: {msg.retain},"
                            f" payload: {msg.payload},"
                            f" properties: {msg.properties}"))

        if not hasattr(msg.properties,'UserProperty'):
            self.logger.logerr(f'Client {self.client_id} has no "UserProperty"')
            self.logger.logerr(f'Client {self.client_id}'
                               f' skipping topic: {msg.topic} payload: {msg.payload}')
            return

        user_property = msg.properties.UserProperty
        data_binding = None
        for keyword_value in user_property:
            if keyword_value[0] == 'data_binding':
                data_binding = keyword_value[1]
                break

        if not data_binding:
            self.logger.logerr(f'Client {self.client_id} has no "data_binding" UserProperty')
            self.logger.logerr(f'Client {self.client_id}'
                               f' skipping topic: {msg.topic} payload: {msg.payload}')
            return

        if data_binding not in self.data_bindings:
            self.logger.logerr(f'Client {self.client_id} has unknown data_binding {data_binding}')
            self.logger.logerr(f'Client {self.client_id}'
                               f' skipping topic: {msg.topic} payload: {msg.payload}')
            return

        record = json.loads(msg.payload.decode('utf-8'))
        if msg.topic == self.archive_topic and self.data_bindings[data_binding]['type'] == 'main':
            self.data_queue.put(record)
        else:
            # Add the record directly to the database
            # This means a new_archive_record is not fired
            # But it is much faster
            self.data_bindings[data_binding]['dbmanager'].addRecord(record)

if __name__ == '__main__':
    def add_request_parser(parser):
        ''' Add the requester parser, '''
        description = '''
'''
        subparser = parser.add_parser('request',
                                      description=description,
                                      formatter_class=argparse.RawDescriptionHelpFormatter)

        subparser.add_argument("--conf",
                            required=True,
                            help="The WeeWX configuration file. Typically weewx.conf.")
        subparser.add_argument('--timestamp',
                               type=int,
                               help='The timestamp to replicate from.')
        subparser.add_argument('--host',
                               default='localhost',
                               required=True,
                               help='The MQTT broker.')
        subparser.add_argument('--primary-binding',
                               required=True,
                               help='The primarary data binding.')
        subparser.add_argument('--secondary-binding',
                               required=True,
                               help='The secondary data binding.')

        return subparser

    def add_respond_parser(parser):
        ''' Add the requester parser, '''
        description = '''
'''
        subparser = parser.add_parser('respond',
                                      description=description,
                                      formatter_class=argparse.RawDescriptionHelpFormatter)

        subparser.add_argument("--conf",
                            required=True,
                            help="The WeeWX configuration file. Typically weewx.conf.")

    def main():
        """ Run it."""

        arg_parser = argparse.ArgumentParser()
        arg_parser.add_argument('--version',
                                action='version',
                                 version=f"mqttreplicate version is {VERSION}")

        subparsers = arg_parser.add_subparsers(dest='command')
        add_request_parser(subparsers)
        add_respond_parser(subparsers)
        options = arg_parser.parse_args()

        _config_path, config_dict = weecfg.read_config(options.conf)
        weewx.debug = 1
        weeutil.logger.setup('weewx', config_dict)

        del config_dict['Engine']
        config_dict['Engine'] = {}
        config_dict['Engine']['Services'] = {}

        if options.command == 'request':
            engine = weewx.engine.DummyEngine(config_dict)
            mqtt_requester = MQTTRequester(config_dict, engine)
            # ToDO: Hack to wait for connect to happen
            # ToDo: Should I put some logic in MQTTRequester?
            time.sleep(10)
            for record in mqtt_requester.genStartupRecords(options.timestamp):
                print(record)
            time.sleep(1)
            print('done')
        elif options.command == 'respond':
            #config_dict.merge(configobj.ConfigObj(replicator_config_dict))
            if 'enable' in config_dict['MQTTReplicate']['Responder']:
                del config_dict['MQTTReplicate']['Responder']['enable']

            engine = weewx.engine.DummyEngine(config_dict)
            mqtt_responder = MQTTResponder(engine, config_dict)
            try:
                while True:
                    time.sleep(2)
            except KeyboardInterrupt:
                mqtt_responder.shutDown()
        else:
            arg_parser.print_help()

        print("done")

    main()
