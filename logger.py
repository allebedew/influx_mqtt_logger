#!/usr/bin/env python

import json
from pprint import pprint
import sys
import paho.mqtt.client as mqtt_client
import coloredlogs, logging
from influxdb import InfluxDBClient
import time
import argparse

config = {}     # key: value
topics = {}     # topic: details

payloads = {}   # topic: payload
last_write = {} # topic: last write time
is_new = set()  # topic: last update time

mqtt = None     # mqtt client
influx = None   # influx client

######################### MQTT Callbacks #########################

def on_connect(mqtt, userdata, flags, result):
    if result is not mqtt_client.MQTT_ERR_SUCCESS:
        logging.error('Error connecting to MQTT server (res={})'.format(result))
        mqtt.disconnect()
        return
    logging.info('Connected as {}'.format(mqtt._client_id))
    for key in topics:
        logging.debug('Subscr {} {}'.format(key,topics))
        mqtt.subscribe(key)

def on_disconnect(mqtt, userdata, rc):
    logging.info('Disconnected')

def on_message(mqtt, userdata, message):
    payloads[message.topic] = message.payload
    is_new.add(message.topic)

#################### Local ####################

# calls every second
def process_pool():
    for topic, payload in payloads.items():
        if topics[topic]['momentary'] == True:
            if topic in is_new or time.time() - last_write[topic] > config["min_write_interval"]:
              influx_write(topic, payload)
        else:
            if topic not in last_write or time.time() - last_write[topic] > config["min_write_interval"]:
                influx_write(topic, payload)

def influx_write(topic, payload):
    logging.debug('Writing to influx: {} = {}'.format(topic, payload))
    topic_info = topics[topic]

    if topic_info['type'] == 'string': value = payload
    elif topic_info['type'] == 'int': value = int(payload)
    elif topic_info['type'] == 'bool': value = bool(payload != '0')
    else: value = float(payload)

    data = {
        "measurement": topic_info['measurement'],
        "tags": topic_info['tags'],
        "fields": { 'value': value }
    }
    influx.write_points([data])

    last_write[topic] = time.time()
    is_new.remove(topic)


def load_config():
    global config, topics
    logging.info('Loading config {}...'.format(args.c))

    with open(args.c) as conf_file:
        conf_file = json.load(conf_file)

    config = conf_file["config"]
    topics = conf_file["topics"]

def parse_arguments():
    global args
    parser = argparse.ArgumentParser(description='Description string')
    parser.add_argument('-c', metavar='FILE', required=True, help='Comfig file')
    parser.add_argument('-d', default=False, type=bool, help='Enable debug logging')
    args = parser.parse_args()
    print(args)

def main():
    global mqtt, influx
    parse_arguments()
    if args.d:
        coloredlogs.install(level=logging.DEBUG, fmt='%(asctime)s %(levelname)s %(message)s')
    else:
        coloredlogs.install(level=logging.INFO, fmt='%(asctime)s %(levelname)s %(message)s')
    
    load_config()
    #pprint(config)
    #pprint(topics)

    logging.info('Influx connecting to {}:{} with {} database...'.format(config["influx_host"], config["influx_port"], config["influx_database"]))
    influx = InfluxDBClient(config["influx_host"], config["influx_port"], 'root', 'root', config["influx_database"])
    influx.create_database(config["influx_database"])

    logging.info('MQTT connecting to {}:{}...'.format(config["mqtt_server"], config["mqtt_port"]))
    mqtt = mqtt_client.Client(config["mqtt_client_id"])
    mqtt.on_connect = on_connect
    mqtt.on_disconnect = on_disconnect
    mqtt.on_message = on_message
    if 'mqtt_user' in config and 'mqtt_pass' in config:
        mqtt.username_pw_set(config["mqtt_user"], config["mqtt_pass"])
    mqtt.enable_logger(logging)
    mqtt.connect(config["mqtt_server"], config["mqtt_port"])
    mqtt.loop_start()

    while True:
        loop()

def loop():
    process_pool()
    time.sleep(1)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.info('Disconnecting...')
        mqtt.disconnect()
        sys.exit(0)
