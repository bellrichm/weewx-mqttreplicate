#
#    Copyright (c) 2020-2024 Rich Bell <bellrichm@gmail.com>
#
#    See the file LICENSE.txt for your full rights.
#
''' Replicate WeeWX dstabases using MQTT request/response functionality.'''
# pylint: disable=fixme, too-many-lines, too-many-instance-attributes, too-many-arguments
import abc
import argparse
import json
import logging
import queue
import random
import threading
import time
import traceback

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

class ThreadError(Exception):
    ''' Exception raised when a critical error occurs in a child thread. '''

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

    def loop_forever(self):
        ''' Connect to the MQTT server. '''
        raise NotImplementedError("Method 'loop_forever' is not implemented")

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

    # The  wrappers of the callbacks are next

    def _client_on_connect(self, _client, userdata, flags, reason_code, _properties):
        self.logger.logdbg(f"Client: {self.client_id} connected with result code {reason_code}")
        self.logger.logdbg((f"Client: {self.client_id}"
                            f" connected with result code {int(reason_code.value)}"))
        self.logger.logdbg(f"Client: {self.client_id} connected flags {str(flags)}")
        self._on_connect(userdata)

    def _client_on_connect_fail(self, _client, userdata):
        self._on_connect_fail(userdata)

    def _client_on_disconnect(self, _client, userdata, _flags, reason_code, _properties):
        self.logger.logdbg((f"Client: {self.client_id}"
                            f" disconnected with result code {reason_code.value}"))
        self._on_disconnect(userdata, reason_code.value)

    def _client_on_log(self, _client, userdata, level, msg):
        """ The on_log callback. """
        self._on_log(userdata, level, msg)

    def _client_on_message(self, _client, userdata, msg):
        self._on_message(userdata, msg)

    def _client_on_publish(self, _client, userdata, mid, _reason_codes, _properties):
        """ The on_publish callback. """
        self._on_publish(userdata, mid)

    def _client_on_subscribe(self, _client, userdata, mid, _reason_code_list, _properties):
        self._on_subscribe(userdata, mid)

    # The wrappers of the  client methods are next

    def connect(self, host, port, keepalive):
        self.client.connect(host, port, keepalive)

    def disconnect(self):
        self.client.disconnect()

    def loop_forever(self):
        self.client.loop_forever()

    def publish(self, topic, data, qos, retain, properties=None):
        return self.client.publish(topic, data, qos, retain, properties)

    def subscribe(self, topic, qos):
        return self.client.subscribe(topic, qos)

class MQTTResponder(weewx.engine.StdService):
    ''' The "server" that sends the replication data to the requester/client. '''
    def __init__(self, engine, config_dict):
        super().__init__(engine, config_dict)
        self.logger = Logger()
        self.thread_id = threading.get_native_id()

        if not to_bool(config_dict.get('MQTTReplicate', {})\
                       .get('Responder', {})\
                       .get('enable', True)):
            self.logger.loginf("Responder not enabled, exiting.")
            return

        self.client_id = 'MQTTReplicateRespond-' + str(random.randint(1000, 9999))

        service_dict = config_dict.get('MQTTReplicate', {}).get('Responder', {})
        if len(service_dict.sections)> 1:
            raise AttributeError("Only one instance is allowed.")

        instance_name = service_dict.sections[0]
        self.request_topic = service_dict.get('request_topic', f'{REQUEST_TOPIC}/{instance_name}')
        self.archive_topic = service_dict.get('archive_topic', ARCHIVE_TOPIC)
        delta = service_dict.get('delta', 60)
        host = service_dict.get('host', 'localhost')
        port = service_dict.get('port', 1883)
        keepalive = service_dict.get('keepalive', 60)
        self.subscribe_qos = to_int(service_dict.get('subscribe_qos', 1))
        self.publish_qos = to_int(service_dict.get('publish_qos', 1))
        self.max_responder_threads = service_dict.get('max_responder_threads', 1)
        log_mqtt = to_bool(service_dict.get('log_mqtt', False))

        self.data_bindings = {}
        for data_binding_name, data_binding in service_dict[instance_name].items():
            data_binding_key = f"{instance_name}/{data_binding_name}"
            self.data_bindings[data_binding_key] = {}
            self.data_bindings[data_binding_key]['delta'] = data_binding.get('delta', delta)
            self.data_bindings[data_binding_key]['type'] = data_binding.get('type', 'secondary')
            manager_dict = weewx.manager.get_manager_dict_from_config(config_dict,
                                                                      data_binding_name)
            self.data_bindings[data_binding_key]['dbmanager'] = \
                weewx.manager.open_manager(manager_dict)
            if self.data_bindings[data_binding_key]['type'] == 'main':
                self.main_data_binding = data_binding_key

        self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)

        self.data_queue = queue.Queue()
        self.threads = []
        self.thread_error = False
        for _i in range(self.max_responder_threads):
            thread = MQTTResponderThread(self.logger,
                                         self.thread_error,
                                         self.data_queue,
                                         delta,
                                         config_dict,
                                         False,
                                         host,
                                         port,
                                         keepalive,
                                         self.publish_qos)
            thread.start()
            self.threads.append(thread)

        self.mqtt_client = MQTTClient.get_client(self.logger, self.client_id, None)

        self.loop_thread = MQTTResponderLoopThread(self.logger,
                                              self.mqtt_client,
                                              self.client_id,
                                              log_mqtt,
                                              self.data_queue,
                                              self.data_bindings,
                                              self.request_topic,
                                              self.subscribe_qos,
                                              host,
                                              port,
                                              keepalive)
        self.loop_thread.start()

    def shutDown(self):
        self.logger.loginf(f'Client: {self.client_id} thread: {self.thread_id} shutting down.')
        for _, data_binding in self.data_bindings.items():
            data_binding['dbmanager'].close()

        self.mqtt_client.disconnect()
        self.loop_thread.join(20.0)
        if self.loop_thread.is_alive():
            self.logger.logerr(f"Unable to shut down {self.loop_thread.native_id} thread")
        else:
            self.logger.loginf(f"Shut down {self.loop_thread.native_id} thread")

        for i in range(self.max_responder_threads):
            self.data_queue.put(None)

        for i in range(self.max_responder_threads):
            self.threads[i].join(20)
        if self.threads[i].is_alive():
            self.logger.logerr(f"Unable to shut down {self.threads[i].native_id} thread")
        else:
            self.logger.loginf(f"Shut down {self.threads[i].native_id} thread")

    def new_archive_record(self, event):
        ''' Handle the new_archive_record event.'''
        if not self.loop_thread.is_alive():
            raise ThreadError(f'Client: {self.client_id} thread: {self.thread_id}'
                              ' Loop thread abnormally ended')
        if self.thread_error:
            raise ThreadError(f'Client: {self.client_id} thread: {self.thread_id}'
                              ' A thread pool thread abnormally ended.')          
        for data_binding_name, data_binding in self.data_bindings.items():
            if data_binding['type'] == 'main':
                continue

            timestamp = event.record['dateTime']
            # some extensions do not force the timestamp to be on an interval
            record = data_binding['dbmanager'].getRecord(timestamp,max_delta=data_binding['delta'])
            if record:
                payload = json.dumps(record)
                self.publish_payload(data_binding_name, self.publish_qos, payload)
            else:
                self.logger.loginf((f'Client: {self.client_id} thread: {self.thread_id}'
                                    f' binding {data_binding_name}'
                                    f' timestamp {timestamp} no record.'))

        payload = json.dumps(event.record)
        self.publish_payload(self.main_data_binding, self.publish_qos, payload)

    def publish_payload(self, data_binding_name, qos, payload):
        ''' Publish the record. '''
        properties = paho.mqtt.client.Properties(paho.mqtt.client.PacketTypes.PUBLISH)
        properties.UserProperty = [
            ('data_binding', data_binding_name)
            ]
        self.logger.logdbg((f'Client: {self.client_id} thread: {self.thread_id}'
                            f' publishing binding: {data_binding_name},'
                            f' payload: {payload}.'))
        mqtt_message_info = self.mqtt_client.publish(self.archive_topic,
                                                     payload,
                                                     qos,
                                                     False,
                                                     properties=properties)
        self.logger.logdbg((f"Client: {self.client_id} thread: {self.thread_id}"
                            f" binding {data_binding_name}"
                            f" {mqtt_message_info.mid} {qos} {self.archive_topic}"))

class MQTTResponderLoopThread(threading.Thread):
    ''' The MQTT 'loop' thread. '''
    def __init__(self,
                 logger,
                 mqtt_client,
                 client_id,
                 log_mqtt,
                 data_queue,
                 data_bindings,
                 request_topic,
                 subscribe_qos,
                 host,
                 port,
                 keepalive):
        threading.Thread.__init__(self)
        self.thread_id = 0
        self.logger = logger
        self.mqtt_client = mqtt_client
        self.client_id = client_id
        self.data_queue = data_queue
        self.data_bindings = data_bindings
        self.request_topic = request_topic
        self.subscribe_qos = subscribe_qos

        self.mqtt_client.on_connect = self._on_connect
        if log_mqtt:
            self.mqtt_client.on_log = self._on_log
        self.mqtt_client.on_message = self._on_message

        self.mqtt_logger = {
            paho.mqtt.client.MQTT_LOG_INFO: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_NOTICE: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_WARNING: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_ERR: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_DEBUG: self.logger.loginf
        }
        mqtt_client.connect(host, port, keepalive)

    def run(self):
        self.thread_id = threading.get_native_id()
        self.mqtt_client.loop_forever()

    def _on_connect(self, _userdata):
        (result, mid) = self.mqtt_client.subscribe(self.request_topic, self.subscribe_qos)
        self.logger.loginf((f"Client: {self.client_id} thread: {self.thread_id}"
                            f" subscribing to {self.request_topic}"
                            f" has a mid {int(mid)}"
                            f" and rc {int(result)}"))

    def _on_log(self, _client, _userdata, level, msg):
        self.mqtt_logger[level](f"Client: {self.client_id} thread: {self.thread_id}"
                                f" MQTT log: {msg}")

    def _on_message(self, _userdata, msg):
        self.logger.logdbg((f"Client: {self.client_id} thread: {self.thread_id} received:"
                            f" topic: {msg.topic},"
                            f" QOS: {int(msg.qos)},"
                            f" retain: {msg.retain},"
                            f" payload: {msg.payload},"
                            f" properties: {msg.properties}"))            

        if not hasattr(msg.properties,'UserProperty'):
            self.logger.logerr((f'Client: {self.client_id} thread: {self.thread_id}'
                                ' has no "UserProperty"'))
            self.logger.logerr(f'Client: {self.client_id} thread: {self.thread_id}'
                               f' skipping topic: {msg.topic} payload: {msg.payload}')
            return

        user_property = msg.properties.UserProperty
        data_binding = None
        for keyword_value in user_property:
            if keyword_value[0] == 'data_binding':
                data_binding = keyword_value[1]
                break

        if not data_binding:
            self.logger.logerr(f'Client: {self.client_id} thread: {self.thread_id}'
                               ' has no "data_binding" '
                               f' UserProperty: {msg.properties.UserProperty}')
            self.logger.logerr(f'Client: {self.client_id} thread: {self.thread_id}'
                               f' skipping topic: {msg.topic}'
                               f' UserProperty: {msg.properties.UserProperty}'
                               f' payload: {msg.payload}')
            return

        if data_binding not in self.data_bindings:
            self.logger.logerr(f'Client: {self.client_id} thread: {self.thread_id}'
                               f' has unknown data_binding {data_binding}')
            self.logger.logerr(f'Client: {self.client_id} thread: {self.thread_id}'
                               f' skipping topic: {msg.topic}'
                               f' UserProperty: {msg.properties.UserProperty}'
                               f' payload: {msg.payload}')
            return

        response_topic = msg.properties.ResponseTopic

        properties = paho.mqtt.client.Properties(paho.mqtt.client.PacketTypes.PUBLISH)
        properties.UserProperty = [
            ('data_binding', data_binding)
            ]

        start_timestamp = int(msg.payload.decode('utf-8'))

        data = {'topic': response_topic,
                'start_timestamp': start_timestamp,
                'data_binding': data_binding,
                'properties': properties}
        self.data_queue.put(data)
        self.logger.logdbg(f'Client: {self.client_id} thread: {self.thread_id} submitted:'
                           f' {data_binding} {response_topic} queued: {data}')

class MQTTResponderThread(threading.Thread):
    '''  Publish the requested data. '''
    def __init__(self,
                 logger,
                 thread_error,
                 data_queue,
                 delta,
                 config_dict,
                 log_mqtt,
                 host,
                 port,
                 keepalive,
                 publish_qos):
        threading.Thread.__init__(self)
        self.thread_id = threading.get_native_id()
        self.logger = logger
        self.thread_error = thread_error
        self.data_queue = data_queue
        self.config_dict = config_dict
        self.host = host
        self.port = port
        self.keepalive = keepalive
        self.publish_qos = publish_qos
        self.client_id = 'MQTTReplicateRespondThread-' + str(random.randint(1000, 9999))
        self.data_bindings = {}

        service_dict = config_dict.get('MQTTReplicate', {}).get('Responder', {})
        instance_name = service_dict.sections[0]

        for data_binding_name, data_binding in service_dict[instance_name].items():
            data_binding_key = f"{instance_name}/{data_binding_name}"
            self.data_bindings[data_binding_key] = {}
            self.data_bindings[data_binding_key]['delta'] = data_binding.get('delta', delta)
            self.data_bindings[data_binding_key]['type'] = data_binding.get('type', 'secondary')
            self.data_bindings[data_binding_key]['manager_dict'] = \
                weewx.manager.get_manager_dict_from_config(config_dict, data_binding_name)

        self.mqtt_logger = {
            paho.mqtt.client.MQTT_LOG_INFO: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_NOTICE: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_WARNING: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_ERR: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_DEBUG: self.logger.loginf
        }

        self.mids = {}
        self.mqtt_client = MQTTClient.get_client(self.logger, self.client_id, None)

        self.mqtt_client.on_connect = self._on_connect
        if log_mqtt:
            self.mqtt_client.on_log = self._on_log
        self.mqtt_client.on_publish = self._on_publish

    def run(self):
        ''' Publish the data. '''
        for _data_binding_key, data_binding in self.data_bindings.items():
            data_binding['dbmanager'] = weewx.manager.open_manager(data_binding['manager_dict'])

        while True:
            data = self.data_queue.get()
            if data:
                self.logger.logdbg((f"Client: {self.client_id} thread: {self.thread_id}"
                                    f" In MQTTResponderThread.run data: {data}"))

                self.mqtt_client.connect(self.host, self.port, self.keepalive)

                record_count = 0
                for record in self.data_bindings[data['data_binding']]['dbmanager']\
                                                    .genBatchRecords(data['start_timestamp']):
                    record_count += 1
                    payload = json.dumps(record)
                    self.logger.logdbg((f'Client: {self.client_id} thread: {self.thread_id}'
                                        f' {data["topic"]}'
                                        f' {data["data_binding"]}'
                                        f' publishing is: {payload}.'))
                    mqtt_message_info = self.mqtt_client.publish(data['topic'],
                                                                 payload,
                                                                 self.publish_qos,
                                                                 False,
                                                                 properties=data['properties'])
                    self.logger.logdbg((f"Client: {self.client_id} thread: {self.thread_id}"
                                        f" {data['topic']}"
                                        f"  published {mqtt_message_info.mid} {self.publish_qos}"))
                    self.mids[mqtt_message_info.mid] = {}
                    self.mids[mqtt_message_info.mid]['time_stamp'] = time.time()
                    self.mids[mqtt_message_info.mid]['qos'] = self.publish_qos

                self.logger.loginf((f"Client: {self.client_id} thread: {self.thread_id}"
                                    f" {data['topic']} {data['properties']}"
                                    f"  published {record_count} records."))
                # Wait for all messages to be published
                if len(self.mids) > 0:
                    self.mqtt_client.loop_forever()
            else:
                break

        for _data_binding_key, data_binding in self.data_bindings.items():
            data_binding['dbmanager'].close()

    def _on_connect(self, _userdata):
        pass

    def _on_publish(self, _userdata, mid):
        if mid in self.mids:
            time_stamp = self.mids[mid]['time_stamp']
            qos = self.mids[mid]['qos']
            del self.mids[mid]
        if len(self.mids) > 0:
            self.logger.logdbg((f"Client: {self.client_id} thread: {self.thread_id}"
                                f" Published (int(time.time())): {time_stamp} {mid} {qos}"))
            self.logger.logdbg((f"Client: {self.client_id} thread: {self.thread_id}"
                                f" Inflight ({int(time.time())}): {self.mids}"))
        else:
            self.mqtt_client.disconnect()

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
        self.thread_id = threading.get_native_id()
        stn_dict = config_dict['MQTTReplicate']['Requester']

        self.the_time = time.time()
        self.loop_interval = float(stn_dict.get('loop_interval', 2.5))
        self._archive_interval = to_int(stn_dict.get('archive_interval', 300))
        self.startup_max_tries = float(stn_dict.get('startup_max_tries', 2))
        self.startup_wait_before_retry = float(stn_dict.get('startup_wait_before_retry', 10))
        self.archive_max_tries = float(stn_dict.get('archive_max_tries', 2))
        self.archive_wait_before_retry = float(stn_dict.get('archive_wait_before_retry', 10))
        self.subscribe_qos = to_int(stn_dict.get('subscribe_qos', 1))
        self.publish_qos = to_int(stn_dict.get('publish_qos', 1))
        self.archive_topic = stn_dict.get('archive_topic', ARCHIVE_TOPIC)
        host = stn_dict.get('host', 'localhost')
        port = stn_dict.get('port', 1883)
        keepalive = stn_dict.get('keepalive', 60)
        log_mqtt = stn_dict.get('log_mqtt', False)

        self.client_id = 'MQTTReplicateRequest-' + str(random.randint(1000, 9999))
        self.response_topic = stn_dict.get('response_topic',
                                           f'{RESPONSE_TOPIC}/{self.client_id}')
        request_topic = stn_dict.get('request_topic', REQUEST_TOPIC)

        self.main_data_binding = None
        self.data_bindings = {}

        for instance_name in stn_dict.sections:
            for  primary_name, binding in stn_dict[instance_name].items():
                secondary_name = binding['secondary_data_binding']
                data_binding_key = f'{instance_name}/{primary_name}'
                self.data_bindings[data_binding_key] = {}
                self.data_bindings[data_binding_key]['request_topic'] = (f'{request_topic}/'
                                                                        f'{instance_name}')
                self.data_bindings[data_binding_key]['type'] = binding.get('type', 'secondary')
                self.data_bindings[data_binding_key]['manager_dict'] = \
                    weewx.manager.get_manager_dict_from_config(config_dict, secondary_name)
                # 5/1/2024 - The DBBinder is instantiated after the driver
                #db_manager = engine.db_binder.get_manager(data_binding=secondary_name,
                #                                          initialize=False)
                db_manager = \
                    weewx.manager.open_manager(self.data_bindings[data_binding_key]['manager_dict'])
                self.data_bindings[data_binding_key]['last_good_timestamp'] = \
                    db_manager.lastGoodStamp()
                self.data_bindings[data_binding_key]['dbmanager'] = None
                if self.data_bindings[data_binding_key]['type'] == 'main':
                    self.main_data_binding = data_binding_key

        if stn_dict.get('command_line'):
            self.data_bindings[self.main_data_binding]['type'] = 'secondary'
            self.main_data_binding = None
            instance_name = stn_dict.sections[0]
            database_name = stn_dict[instance_name].sections[0]
            timestamp = stn_dict[instance_name][database_name].get('timestamp')
            if timestamp:
                next(iter(self.data_bindings.values()))['last_good_timestamp'] = timestamp

        #self.exception = None

        self.mqtt_client = MQTTClient.get_client(self.logger, self.client_id, None)

        self.data_queue = queue.PriorityQueue()

        loop_thread = MQTTRequesterLoopThread(self.logger,
                                              self.mqtt_client,
                                              self.client_id,
                                              log_mqtt,
                                              self.data_queue,
                                              self.data_bindings,
                                              self.response_topic,
                                              self.archive_topic,
                                              self.subscribe_qos,
                                              host,
                                              port,
                                              keepalive)
        loop_thread.start()

        self.logger.loginf(f"Client {self.client_id}: Thread: {self.thread_id}"
                           f" Waiting for MQTT subscription.")
        while not loop_thread.subscribed:
            time.sleep(1)

        # Request any possible missing records
        # Do it now, so hopefully queue is primed when genStartupRecords is called
        for data_binding_name, data_binding in self.data_bindings.items():
            if data_binding['type'] == 'main':
                continue
            self.request_records(data_binding_name,
                                 self.publish_qos,
                                 data_binding['request_topic'],
                                 data_binding['last_good_timestamp'])

        if self.main_data_binding:
            # Request 'main' last, so new_archive_record event fired after other DBs are updated
            self.request_records(self.main_data_binding,
                                self.publish_qos,
                                self.data_bindings[self.main_data_binding]['request_topic'],
                                self.data_bindings[self.main_data_binding]['last_good_timestamp'])

    @property
    def hardware_name(self):
        """ The name of the hardware driver. """
        return DRIVER_NAME

    @property
    def archive_interval(self):
        """ The archive interval. """
        return self._archive_interval

    def genStartupRecords(self, _lastgood_ts):
        for record in self.gen_replica_record(self.startup_max_tries,
                                              self.startup_wait_before_retry):
            yield record

    def genArchiveRecords(self, _lastgood_ts):
        for record in self.gen_replica_record(self.archive_max_tries,
                                              self.archive_wait_before_retry):
            yield record

    def gen_replica_record(self, max_tries, wait_before_retry):
        ''' Generator to return the records that are in the queue. '''
        record_count = 0
        tries = 0
        while True:
            try:
                record = self.data_queue.get(True, wait_before_retry)[1]
                record_count += 1
                tries = 0
                yield record
            except queue.Empty:
                tries += 1
                if tries >= max_tries:
                    self.logger.loginf((f"Client {self.client_id}: Thread: {self.thread_id}"
                                        f" After {tries} with a wait of {wait_before_retry},"
                                        " queue is still empty."))
                    break

    def genLoopPackets(self):
        while True:
            #if self.exception:
            #    raise ThreadError from self.exception
            sleep_time = self.the_time + self.loop_interval - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)

            self.the_time += self.loop_interval
            yield {'dateTime': int(self.the_time+0.5),
                   'usUnits' : weewx.US }

    def closePort(self):
        """Run when an engine shutdown is requested."""
        self.mqtt_client.disconnect()

    def request_records(self, data_binding_name, qos, topic, last_ts):
        ''' Request the missing data. '''
        properties = paho.mqtt.client.Properties(paho.mqtt.client.PacketTypes.PUBLISH)
        properties.ResponseTopic = self.response_topic
        properties.UserProperty = [
            ('data_binding', data_binding_name)
            ]

        mqtt_message_info = self.mqtt_client.publish(topic,
                                                        last_ts,
                                                        qos,
                                                        False,
                                                        properties=properties)
        self.logger.loginf((f"Client {self.client_id}: Thread: {self.thread_id}"
                            f"  topic ({topic}):"            
                            f"  data_binding ({data_binding_name}):"
                            f"  publishing ({last_ts}):"
                            f"  properties ({properties}):"                        
                            f" {mqtt_message_info.mid} {qos}"))

class MQTTRequesterLoopThread(threading.Thread):
    ''' The MQTT 'loop' thread. '''
    def __init__(self,
                 logger,
                 mqtt_client,
                 client_id,
                 log_mqtt,
                 data_queue,
                 data_bindings,
                 response_topic,
                 archive_topic,
                 subscribe_qos,
                 host,
                 port,
                 keepalive):
        threading.Thread.__init__(self)
        self.thread_id = 0
        self.logger = logger
        self.mqtt_client = mqtt_client
        self.client_id = client_id
        self.data_queue = data_queue
        self.data_bindings = data_bindings
        self.response_topic = response_topic
        self.archive_topic = archive_topic
        self.subscribe_qos = subscribe_qos

        self.response_topic_mid = None
        self.subscribed = False

        self.mqtt_logger = {
            paho.mqtt.client.MQTT_LOG_INFO: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_NOTICE: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_WARNING: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_ERR: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_DEBUG: self.logger.loginf
        }

        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_disconnect = self._on_disconnect
        if log_mqtt:
            self.mqtt_client.on_log = self._on_log
        self.mqtt_client.on_message = self._on_message
        self.mqtt_client.on_subscribe = self._on_subscribe

        self.mqtt_client.connect(host, port, keepalive)

    def run(self):
        self.thread_id = threading.get_native_id()
        self.mqtt_client.loop_forever()

    def _on_connect(self, _userdata):
        (result, mid) = self.mqtt_client.subscribe(self.response_topic, self.subscribe_qos)
        self.logger.loginf((f"Client {self.client_id}: Thread: {self.thread_id}"
                         f" subscribing to {self.response_topic}"
                         f" has a mid {int(mid)}"
                         f" and rc {int(result)}"))
        self.response_topic_mid = mid

        (result, mid) = self.mqtt_client.subscribe(self.archive_topic, self.subscribe_qos)
        self.logger.loginf((f"Client {self.client_id}: Thread: {self.thread_id}"
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
                self.logger.logdbg(f"Client {self.client_id}: Thread: {self.thread_id}"
                                   f" closed db {data_binding_name}.")

    def _on_log(self, _client, _userdata, level, msg):
        self.mqtt_logger[level](f"Client: {self.client_id} Thread: {self.thread_id}"
                                f" MQTT log: {msg}")

    def _on_message(self, _userdata, msg):
        # ToDo: Fine tune exception handling
        try:
            self.logger.logdbg((f"Client {self.client_id}: Thread: {self.thread_id}"
                                f" topic: {msg.topic},"
                                f" QOS: {int(msg.qos)},"
                                f" retain: {msg.retain},"
                                f" payload: {msg.payload},"
                                f" properties: {msg.properties}"))

            if not hasattr(msg.properties,'UserProperty'):
                self.logger.logerr((f'Client {self.client_id}: Thread: {self.thread_id}'
                                    ' has no "UserProperty"'))
                self.logger.logerr(f'Client {self.client_id}: Thread: {self.thread_id}'
                                   f' skipping topic: {msg.topic} payload: {msg.payload}')
                return

            user_property = msg.properties.UserProperty
            data_binding = None
            for keyword_value in user_property:
                if keyword_value[0] == 'data_binding':
                    data_binding = keyword_value[1]
                    break

            if not data_binding:
                self.logger.logerr((f'Client {self.client_id}: Thread: {self.thread_id}'
                                    ' has no "data_binding" UserProperty'))
                self.logger.logerr(f'Client {self.client_id}: Thread: {self.thread_id}'
                                   f' skipping topic: {msg.topic} payload: {msg.payload}')
                return

            if data_binding not in self.data_bindings:
                self.logger.logerr((f'Client {self.client_id}: Thread: {self.thread_id}'
                                    f' has unknown data_binding {data_binding}'))
                self.logger.logerr(f'Client {self.client_id}: Thread: {self.thread_id}'
                                   f' skipping topic: {msg.topic} payload: {msg.payload}')
                return

            record = json.loads(msg.payload.decode('utf-8'))
            if self.data_bindings[data_binding]['type'] == 'main':
                # For all records from the 'main' db, create an archive_record
                self.data_queue.put((record['dateTime'], record))
            else:
                self.data_bindings[data_binding]['dbmanager'].addRecord(record)
        except Exception as exception: # (want to catch all ) pylint: disable=broad-exception-caught
            self.logger.logerr((f"Client {self.client_id}: Thread: {self.thread_id}"
                                f" Failed with {type(exception)} and reason {exception}."))
            self.logger.logerr((f"Client {self.client_id}: Thread: {self.thread_id}"
                                f" {traceback.format_exc()}"))
            #self.exception = exception

    def _on_subscribe(self, _userdata, mid):
        if mid == self.response_topic_mid:
            self.subscribed = True

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
            config_dict['MQTTReplicate']['Requester']['command_line'] = True
            engine = weewx.engine.DummyEngine(config_dict)
            mqtt_requester = MQTTRequester(config_dict, engine)
            try:
                for _packet in mqtt_requester.genLoopPackets():
                    pass
            except (KeyboardInterrupt, Exception): # pylint: disable=broad-exception-caught
                mqtt_requester.closePort()
            print('done')
        elif options.command == 'respond':
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
