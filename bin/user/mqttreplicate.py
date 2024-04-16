''' Replicate WeeWX dstabases using MQTT request/response functionality.'''
import abc
import argparse
import json
import logging
import os
import random
import threading
import time

import configobj
import paho
import paho.mqtt
import paho.mqtt.client

import weecfg
import weeutil
import weeutil.logger
import weewx
import weewx.drivers
import weewx.engine

from weeutil.weeutil import to_bool

VERSION = '0.0.1'
REQUEST_TOPIC = 'replicate/request'
RESPONSE_TOPIC = 'replicate/response'

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

        self._thread = MQTTResponderThread(self.logger, self.client_id, config_dict)
        self._thread.start()

    def shutDown(self):
        """Run when an engine shutdown is requested."""
        if self._thread:
            self.logger.loginf(f"Client {self.client_id} SHUTDOWN - thread initiated")
            self._thread.shut_down()

class MQTTResponderThread(threading.Thread):
    ''' Manage the MQTT communication for the "server" that sends the data. '''
    def __init__(self, logger, client_id, config_dict):
        threading.Thread.__init__(self)
        service_dict = config_dict.get('MQTTReplicate', {}).get('Responder', {})
        self.logger = logger
        self.client_id = client_id
        self.request_topic = service_dict.get('request_topic', REQUEST_TOPIC)

        self.data_bindings = {}
        for database_name in service_dict['databases']:
            _data_binding = service_dict['databases'][database_name]['data_binding']
            self.data_bindings[_data_binding] = {}
            self.data_bindings[_data_binding]['manager_dict'] = \
                weewx.manager.get_manager_dict_from_config(config_dict, _data_binding)

        self.mqtt_logger = {
            paho.mqtt.client.MQTT_LOG_INFO: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_NOTICE: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_WARNING: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_ERR: self.logger.loginf,
            paho.mqtt.client.MQTT_LOG_DEBUG: self.logger.loginf
        }

        self.mqtt_client = MQTTClient.get_client(self.logger, self.client_id, None)

        self.mqtt_client.on_connect = self._on_connect
        if service_dict.get('log_mqtt', False):
            self.mqtt_client.on_log = self._on_log
        self.mqtt_client.on_message = self._on_message

        self.mqtt_client.connect(service_dict.get('host', 'localhost'),
                                 service_dict.get('port', 1883),
                                 service_dict.get('keepalive', 60))

    def run(self):
        self.logger.logdbg(f"Client {self.client_id} starting MQTT loop")
        # Need to get the database manager in the thread that is used
        for _, data_binding in self.data_bindings.items():
            data_binding['dbmanager'] = weewx.manager.open_manager(data_binding['manager_dict'])

        self.mqtt_client.client.loop_forever()

        for _, data_binding in self.data_bindings.items():
            data_binding['dbmanager'].close()
        self.logger.logdbg(f"Client {self.client_id} MQTT loop ended.")

    def shut_down(self):
        ''' Perform operations to terminate MQTT.'''
        self.logger.loginf(f'Client {self.client_id} shutting down the MQTT client.')
        self.mqtt_client.disconnect()

    def _on_connect(self, _userdata):
        (result, mid) = self.mqtt_client.subscribe(self.request_topic, 0)
        self.logger.logdbg((f"Client {self.client_id}"
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

        for record in self.data_bindings[data_binding]['dbmanager']\
                                        .genBatchRecords(start_timestamp):
            payload = json.dumps(record)
            qos = 0
            self.logger.logdbg(f'Client {self.client_id} response is: {payload}.')
            mqtt_message_info = self.mqtt_client.publish(response_topic,
                                                        payload,
                                                        0,
                                                        False,
                                                        properties=properties)
            self.logger.logdbg((f"Client {self.client_id}"
                                f"  publishing ({int(time.time())}):"
                                f" {mqtt_message_info.mid} {qos} {response_topic}"))

class MQTTRequester(weewx.engine.StdService):
    ''' The "client" class that data ts replicated to. '''
    def __init__(self, engine, config_dict):
        super().__init__(engine, config_dict)
        self.logger = Logger()
        service_dict = config_dict.get('MQTTReplicate', {}).get('Requester', {})

        enable = to_bool(service_dict.get('enable', True))
        if not enable:
            self.logger.loginf("Requester not enabled, exiting.")
            return

        self.client_id = 'MQTTReplicateRequest-' + str(random.randint(1000, 9999))
        self.response_topic = service_dict.get('response_topic',
                                               f'{RESPONSE_TOPIC}/{self.client_id}')
        self.request_topic = service_dict.get('request_topic', REQUEST_TOPIC)

        self.data_bindings = {}
        for database_name in service_dict['databases']:
            _primary_data_binding = service_dict['databases'][database_name]['primary_data_binding']
            _secondary_data_binding = \
                service_dict['databases'][database_name]['secondary_data_binding']
            self.data_bindings[_primary_data_binding] = {}
            self.data_bindings[_primary_data_binding]['manager_dict'] = \
                weewx.manager.get_manager_dict_from_config(config_dict, _secondary_data_binding)
            self.data_bindings[_primary_data_binding]['dbmanager'] = None
            if 'timestamp' in service_dict['databases'][database_name]:
                self.data_bindings[_primary_data_binding]['last_good_timestamp'] = \
                    service_dict['databases'][database_name]['timestamp']
            else:
                dbmanager = engine.db_binder.get_manager(_secondary_data_binding)
                # Find out when the database was last updated.
                self.data_bindings[_primary_data_binding]['last_good_timestamp'] = \
                    dbmanager.lastGoodStamp()

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
        if service_dict.get('log_mqtt', False):
            self.mqtt_client.on_log = self._on_log
        self.mqtt_client.on_message = self._on_message

        self.mqtt_client.connect(service_dict.get('host', 'localhost'),
                                 service_dict.get('port', 1883),
                                 service_dict.get('keepalive', 60))

        self.mqtt_client.loop_start()

        self.bind(weewx.STARTUP, self.request_catchup)

    def shutDown(self):
        """Run when an engine shutdown is requested."""
        self.mqtt_client.disconnect()
        self.mqtt_client.loop_stop()

    def request_catchup(self, _event):
        ''' Request the missing data. '''
        qos = 0

        for data_binding_name, data_binding in self.data_bindings.items():
            properties = paho.mqtt.client.Properties(paho.mqtt.client.PacketTypes.PUBLISH)
            properties.ResponseTopic = self.response_topic
            properties.UserProperty = [
                ('data_binding', data_binding_name)
                ]

            mqtt_message_info = self.mqtt_client.publish(self.request_topic,
                                                        data_binding['last_good_timestamp'],
                                                        qos,
                                                        False,
                                                        properties=properties)
            self.logger.logdbg((f"Client {self.client_id}"
                        f"  publishing ({int(time.time())}):"
                        f" {mqtt_message_info.mid} {qos} {self.request_topic}"))

    def _on_connect(self, _userdata):
        (result, mid) = self.mqtt_client.subscribe(self.response_topic, 0)
        self.logger.logdbg((f"Client {self.client_id}"
                         f" subscribing to {self.response_topic}"
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

        config_path, config_dict = weecfg.read_config(options.conf)
        weewx.debug = 1
        weeutil.logger.setup('weewx', config_dict)

        del config_dict['Engine']
        replicator_config_dict = {}
        replicator_config_dict['Engine'] = {}
        replicator_config_dict['Engine']['Services'] = {}

        if options.command == 'request':
            replicator_config_dict['MQTTReplicate'] = {}
            replicator_config_dict['MQTTReplicate']['Requester'] = {}
            replicator_config_dict['MQTTReplicate']['Requester']['host'] = options.host
            replicator_config_dict['MQTTReplicate']['Requester']['databases'] = {}
            replicator_config_dict['MQTTReplicate']['Requester']['databases']['weewx'] = {}
            if options.timestamp:
                replicator_config_dict['MQTTReplicate']['Requester']['databases']['weewx']\
                    ['timestamp'] = options.timestamp
            replicator_config_dict['MQTTReplicate']['Requester']['databases']['weewx']\
                ['primary_data_binding'] = options.primary_binding
            replicator_config_dict['MQTTReplicate']['Requester']['databases']['weewx']\
                ['secondary_data_binding'] = options.secondary_binding

            del config_dict['MQTTReplicate']['Requester']
            config_dict.merge(configobj.ConfigObj(replicator_config_dict))

            engine = weewx.engine.DummyEngine(config_dict)
            mqtt_requester = MQTTRequester(engine, config_dict)
            # ToDO: Hack to wait for connect to happen
            # ToDo: Should I put some logic in MQTTRequester?
            time.sleep(10)
            mqtt_requester.request_catchup(None)
            try:
                while True:
                    time.sleep(2)
            except KeyboardInterrupt:
                mqtt_requester.shutDown()
        elif options.command == 'respond':
            config_dict.merge(configobj.ConfigObj(replicator_config_dict))
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
