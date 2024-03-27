import abc
import random
import time

import paho
import paho.mqtt
import paho.mqtt.client
import weewx
import weewx.drivers
import weewx.engine

class MQTTClient(abc.ABC):
    @classmethod
    def get_client(cls, mqtt_options):
        ''' Factory method to get appropriate MQTTClient for paho mqtt version. '''
        if hasattr(paho.mqtt.client, 'CallbackAPIVersion'):
            return MQTTClientV2(mqtt_options)
        
        raise ValueError("paho mqtt v2 is required.")

    def connect(self, mqtt_options):
        ''' Connect to the MQTT server. '''
        raise NotImplementedError("Method 'connect' is not implemented")

    def disconnect(self):
        ''' Connect to the MQTT server. '''
        raise NotImplementedError("Method 'disconnect' is not implemented")

    def subscribe(self, topic, qos):
        ''' Subscribe to the MQTT topic. '''
        raise NotImplementedError("Method 'subscribe' is not implemented")
       
    def publish(self, topic, data, qos, retain, properties=None):
        ''' Publish the MQTT message. '''
        raise NotImplementedError("Method 'publish' is not implemented")
        
class MQTTClientV2(MQTTClient):
    ''' MQTTClient that communicates with paho mqtt v2. '''
    def __init__(self, mqtt_options):
        self.client = paho.mqtt.client.Client(callback_api_version=paho.mqtt.client.CallbackAPIVersion.VERSION2, 
                                       protocol=paho.mqtt.client.MQTTv5,
                                       client_id=mqtt_options['client_id'],
                                       userdata=mqtt_options['userdata'])
        
        self.client.on_connect = self._on_connect
        self.on_connect = None
        self.client.on_disconnect = self._on_disconnect
        self.on_disconnect = None        
        self.client.on_publish = self._on_publish        
        self.on_publish = None
        self.client.on_message = self._on_message        
        self.on_message = None
        
        if mqtt_options['log_mqtt']:
            self.client.on_log = self._on_log
    
    def connect(self, mqtt_options):
        self.client.connect(mqtt_options['host'], mqtt_options['port'], mqtt_options['keepalive'])
        #self.client.loop(timeout=0.1)
        # needed to get on_message called, probably getting disconnected?
        self.client.loop_start()

    def disconnect(self):
        """ shut it down """
        self.client.loop_stop()
        self.client.disconnect()
        
    def subscribe(self, topic, qos):
        (result, mid) = self.client.subscribe(topic, qos)
        print(f"Subscribing to {topic} has a mid {int(mid)} and rc {int(result)}")
        #self.client.loop(timeout=0.1)
       
    def publish(self, topic, data, qos, retain, properties=None):
        mqtt_message_info = self.client.publish(topic, data, qos, retain, properties)
        print("Publishing (%s): %s %s %s" % (int(time.time()), mqtt_message_info.mid, qos, topic))
        #self.client.loop(timeout=0.1)

    def _on_log(self, _client, _userdata, level, msg):
        """ The on_log callback. """
        print("MQTT log: %s" %msg)
        
    def _on_connect(self, _client, userdata, flags, reason_code, _properties):
        print(f"Connected with result code {reason_code}")
        print(f"Connected with result code {int(reason_code.value)}")
        print(f"Connected flags {str(flags)}")

        userdata['connect'] = True

        if self.on_connect:
            self.on_connect(userdata)
    
    def _on_disconnect(self, _client, userdata, _flags, reason_code, _properties):
        print("Disconnected with result code %i" % int(reason_code.value))
        if self.on_disconnect:
            self.on_disconnect(userdata)

    def _on_publish(self, _client, userdata, mid, reason_codes, properties):
        """ The on_publish callback. """
        print("Published  (%s): %s" % (int(time.time()), mid))
        if self.on_publish:
            self.on_publish(userdata)

    def _on_message(self, client, userdata, msg):
        print(f"topic: {msg.topic}, QOS: {int(msg.qos)}, retain: {msg.retain}, payload: {msg.payload} properties: {msg.properties}")

        if self.on_message:
            self.on_message(msg)

class MQTTPublisher():
    pass

class MQTTSubscriber():
    pass

class MQTTResponder(weewx.engine.StdService):
    def __init__(self, engine, config_dict):
        super().__init__(engine, config_dict)
        userdata = {}
        self.mqtt_options = {}
        self.mqtt_options['userdata'] = userdata
        self.mqtt_options['log_mqtt'] = False
        self.mqtt_options['host'] = 'localhost'
        self.mqtt_options['port'] = 1883
        self.mqtt_options['keepalive'] = 60
        self.mqtt_options['client_id'] = 'MQTTReplicate-' + str(random.randint(1000, 9999))
        self.mqtt_options['clean_start'] = False

        self.mqtt_client = MQTTClient.get_client(self.mqtt_options)
        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_message = self._on_message        
        self.mqtt_client.connect(self.mqtt_options)
        
    def _on_connect(self, userdata):
        userdata['connect'] = True
        self.mqtt_client.subscribe('replicate/request', 0)
        #self.mqtt_client.client.loop(timeout=2.0)
            
    def _on_message(self, msg):
        response_topic = msg.properties.ResponseTopic
        print('Responding on response topic:', response_topic)
        self.mqtt_client.publish(response_topic, 'response test', 0, False)

class MQTTRequester(weewx.engine.StdService):
    def __init__(self, engine, config_dict):
        super().__init__(engine, config_dict)
        userdata = {}
        self.mqtt_options = {}
        self.mqtt_options['userdata'] = userdata
        self.mqtt_options['log_mqtt'] = False
        self.mqtt_options['host'] = 'localhost'
        self.mqtt_options['port'] = 1883
        self.mqtt_options['keepalive'] = 60
        self.mqtt_options['client_id'] = 'MQTTReplicate-' + str(random.randint(1000, 9999))
        self.mqtt_options['clean_start'] = False

        self.mqtt_client = MQTTClient.get_client(self.mqtt_options)
        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_message = self._on_message        
        self.mqtt_client.connect(self.mqtt_options)
        
        self.dbmanager = self.engine.db_binder.get_manager('wx_binding')
        # Find out when the database was last updated.
        self.lastgood_ts = self.dbmanager.lastGoodStamp()
        self.bind(weewx.STARTUP, self.request_catchup)
        
    def request_catchup(self, event):
        properties = paho.mqtt.client.Properties(paho.mqtt.client.PacketTypes.PUBLISH)
        properties.ResponseTopic = f'replicate/{self.mqtt_options["client_id"]}/catchup'      
        self.mqtt_client.publish('replicate/request', 'request test', 0, False, properties=properties)
        #self.mqtt_client.client.loop(timeout=2.0)

    def _on_connect(self, userdata):
        userdata['connect'] = True
        self.mqtt_client.subscribe(f'replicate/{self.mqtt_options["client_id"]}/catchup', 0)

    def _on_message(self, msg):
        print('handle message')
        # self.dbm.addRecord(record)

if __name__ == '__main__':
    print('start')
    mqtt_requester = MQTTRequester(None, None)
    mqtt_responder = MQTTResponder(None, None)
    mqtt_requester.request_catchup(None)
    mqtt_requester.mqtt_client.client.loop(timeout=2.0)
    # proof of concept hack
    #time.sleep(5)
    #mqtt_responder.mqtt_client.disconnect()
    #mqtt_requester.mqtt_client.disconnect()
    print('done')