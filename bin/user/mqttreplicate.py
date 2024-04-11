import abc
import logging
import queue
import random
import threading
import time

import json
import paho
import paho.mqtt
import paho.mqtt.client
import weewx
import weewx.drivers
import weewx.engine

from weeutil.weeutil import to_bool

VERSION = '0.0.1'

class Logger(object):
    '''
    Manage the logging
    '''
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
    @classmethod
    def get_client(cls, logger, mqtt_options):
        ''' Factory method to get appropriate MQTTClient for paho mqtt version. '''
        if hasattr(paho.mqtt.client, 'CallbackAPIVersion'):
            return MQTTClientV2(logger, mqtt_options)
        
        raise ValueError("paho mqtt v2 is required.")

    def connect(self, mqtt_options):
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
    def __init__(self, logger, mqtt_options):
        self.logger = logger
        self.client_id = mqtt_options['client_id']
        self.client = paho.mqtt.client.Client(callback_api_version=paho.mqtt.client.CallbackAPIVersion.VERSION2, 
                                       protocol=paho.mqtt.client.MQTTv5,
                                       client_id=self.client_id,
                                       userdata=mqtt_options['userdata'])
        
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
        return self._on_subscribe
        
    @on_subscribe.setter
    def on_subscribe(self, value):
        self._on_subscribe = value
        if value:
            self.client.on_subscribe = self._client_on_subscribe
        else:
            self.client.on_subscribe = None

    # The wrappers of the  client methods are next

    def connect(self, mqtt_options):
        self.client.connect(mqtt_options['host'], mqtt_options['port'], mqtt_options['keepalive'])
        #self.client.loop(timeout=0.1)

    def disconnect(self):
        """ shut it down """
        self.client.disconnect()

    def loop(self):
        """ the loop """
        self.client.loop()

    def loop_start(self):
        """ start the loop """
        self.client.loop_start()

    def loop_stop(self):
        """ stop the loop """
        self.client.loop_stop()
        
    def subscribe(self, topic, qos):
        (result, mid) = self.client.subscribe(topic, qos)
        print(f"Subscribing to {topic} has a mid {int(mid)} and rc {int(result)}")
        #self.client.loop(timeout=0.1)
       
    def publish(self, topic, data, qos, retain, properties=None):
        mqtt_message_info = self.client.publish(topic, data, qos, retain, properties)
        print("Publishing (%s): %s %s %s" % (int(time.time()), mqtt_message_info.mid, qos, topic))
        #self.client.loop(timeout=0.1)

    # The  wrappers of the callbacks are next

    def _client_on_connect(self, _client, userdata, flags, reason_code, _properties):
        self.logger.logdbg(f"Client {self.client_id} connected with result code {reason_code}")
        print(f"Connected with result code {int(reason_code.value)}")
        print(f"Connected flags {str(flags)}")
        self.on_connect(userdata)
    
    def _client_on_connect_fail(self, client, userdata):
        self.on_connect_fail(userdata)

    def _client_on_disconnect(self, _client, userdata, _flags, reason_code, _properties):
        print("Disconnected with result code %i" % int(reason_code.value))
        self.on_disconnect(userdata)

    def _client_on_log(self, _client, userdata, level, msg):
        """ The on_log callback. """
        print("MQTT log: %s" %msg)
        self.on_log(userdata, level, msg)        

    def _client_on_message(self, client, userdata, msg):
        print(f"topic: {msg.topic}, QOS: {int(msg.qos)}, retain: {msg.retain}, payload: {msg.payload} properties: {msg.properties}")
        self.on_message(userdata, msg)

    def _client_on_publish(self, _client, userdata, mid, reason_codes, properties):
        """ The on_publish callback. """
        print("Published  (%s): %s" % (int(time.time()), mid))
        self.on_publish(userdata)

    def _client_on_subscribe(self, client, userdata, mid, reason_code_list, properties):
        self.on_subscribe(userdata, mid)

class MQTTResponder(weewx.engine.StdService):
    def __init__(self, engine, config_dict):
        super().__init__(engine, config_dict)
        self.logger = Logger()
        service_dict = config_dict.get('MQTTReplicate', {}).get('Responder', {})

        enable = to_bool(service_dict.get('enable', True))
        if not enable:
            self.logger.loginf("Not enabled, exiting.")
            return

        _data_binding = service_dict.get('data_binding', 'wx_binding')
        _manager_dict = weewx.manager.get_manager_dict_from_config(config_dict, _data_binding)

        userdata = {}
        self.mqtt_options = {}
        self.mqtt_options['userdata'] = userdata
        self.mqtt_options['log_mqtt'] = False
        self.mqtt_options['host'] = 'rmbell-v01.local'
        self.mqtt_options['port'] = 1883
        self.mqtt_options['keepalive'] = 60
        self.mqtt_options['client_id'] = 'MQTTReplicateRespond-' + str(random.randint(1000, 9999))
        self.mqtt_options['clean_start'] = False
        self.client_id = self.mqtt_options['client_id']
        
        self._thread = MQTTResponderThread(self.logger, _manager_dict, self.mqtt_options)
        self._thread.start()

    def shutDown(self):
        """Run when an engine shutdown is requested."""
        if self._thread:
            self.logger.loginf(f"Client {self.client_id} SHUTDOWN - thread initiated")
            self._thread.shutDown()

class MQTTResponderThread(threading.Thread):
    def __init__(self, logger, manager_dict, mqtt_options):
        threading.Thread.__init__(self)
        self.logger = logger
        self.client_id = mqtt_options['client_id']
        self.manager_dict = manager_dict
        self.mqtt_options = mqtt_options

        self.mqtt_client = MQTTClient.get_client(self.logger, self.mqtt_options)
        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_message = self._on_message        
        self.mqtt_client.connect(self.mqtt_options)
        
    def run(self):
        self.logger.logdbg(f"Client {self.client_id} starting MQTT loop")
        with weewx.manager.open_manager(self.manager_dict) as _manager:
            self.dbmanager = _manager
            self.mqtt_client.client.loop_forever()
        self.logger.logdbg(f"Client {self.client_id} MQTT loop ended.")

    def shutDown(self):
        self.logger.loginf(f'Client {self.client_id} shutting down the MQTT client.')
        self.mqtt_client.disconnect()

    def _on_connect(self, userdata):
        userdata['connect'] = True
        self.mqtt_client.subscribe('replicate/request', 0)
            
    def _on_message(self, userdata, msg):
        response_topic = msg.properties.ResponseTopic
        self.logger.logdbg(f'Client {self.client_id} received msg: {msg}')
        start_timestamp = int(msg.payload.decode('utf-8'))        
        self.logger.logdbg('Responding on response topic:', response_topic)
        for record in self.dbmanager.genBatchRecords(start_timestamp):
            payload = json.dumps(record)
            self.logger.logdbg(f'Client {self.client_id} response is: {payload}.')
            self.mqtt_client.publish(response_topic, payload, 0, False)

class MQTTRequester(weewx.engine.StdService):
    def __init__(self, engine, config_dict):
        super().__init__(engine, config_dict)
        self.logger = Logger()
        service_dict = config_dict.get('MQTTReplicate', {}).get('Requester', {})
        self.manager_dict = weewx.manager.get_manager_dict_from_config(config_dict, 'wx_binding')
        self.dbmanager = None

        enable = to_bool(service_dict.get('enable', True))
        if not enable:
            print("Not enabled, exiting.")
            return

        _data_binding = service_dict.get('data_binding', 'wx_binding')
        
        userdata = {}
        self.mqtt_options = {}
        self.mqtt_options['userdata'] = userdata
        self.mqtt_options['log_mqtt'] = False
        self.mqtt_options['host'] = 'rmbell-v01.local'
        self.mqtt_options['port'] = 1883
        self.mqtt_options['keepalive'] = 60
        self.mqtt_options['client_id'] = 'MQTTReplicateRequest-' + str(random.randint(1000, 9999))
        self.mqtt_options['clean_start'] = False

        self.mqtt_client = MQTTClient.get_client(self.logger, self.mqtt_options)
        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_message = self._on_message        
        self.mqtt_client.connect(self.mqtt_options)
        self.mqtt_client.connect(self.mqtt_options)    
        # needed to get on_message called, probably getting disconnected?
        self.mqtt_client.loop_start()
        
        self.dbmanager = engine.db_binder.get_manager(_data_binding)
        # Find out when the database was last updated.
        self.lastgood_ts = self.dbmanager.lastGoodStamp()
        self.bind(weewx.STARTUP, self.request_catchup)

    def shutDown(self):
        """Run when an engine shutdown is requested."""
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()
        
    def request_catchup(self, event):
        properties = paho.mqtt.client.Properties(paho.mqtt.client.PacketTypes.PUBLISH)
        properties.ResponseTopic = f'replicate/{self.mqtt_options["client_id"]}/catchup'      
        self.mqtt_client.publish('replicate/request', self.lastgood_ts, 0, False, properties=properties)

    def _on_connect(self, userdata):
        userdata['connect'] = True
        self.mqtt_client.subscribe(f'replicate/{self.mqtt_options["client_id"]}/catchup', 0)
        # dbmanager needs to be created in same thread as on_message called
        if not self.dbmanager:
            self.dbmanager = weewx.manager.open_manager(self.manager_dict)

    def _on_message(self, userdata, msg):
        print(f'handle message: {msg}')
        record = json.loads(msg.payload.decode('utf-8'))
        self.dbmanager.addRecord(record)

if __name__ == '__main__':
    print('start')
    #mqtt_responder = MQTTResponder(None, None)

    mqtt_requester = MQTTRequester(None, None)
    mqtt_requester.request_catchup(None)

    mqtt_requester.shutDown()
    print('done')