#!/usr/bin/env python
#-*- coding: UTF-8 -*-

# autor: Carlos Rueda
# date: 2015-12-21
# mail: carlos.rueda@deimos-space.com
# version: 1.0

##################################################################################
# version 1.0 release notes:
# Initial version
##################################################################################

import time
import datetime
import os
import sys
import utm
import logging, logging.handlers

import pika
import time

import threading
import SocketServer, socket
import binascii
import base64

########################################################################
# configuracion y variables globales
from configobj import ConfigObj
config = ConfigObj('./ais_dispatcher.properties')

LOG = config['directory_logs'] + "/ais_dispatcher.log"
LOG_FOR_ROTATE = 10

RABBITMQ_HOST = config['rabbitMQ_HOST']
QUEUE_NAME = config['queue_name']

KCS_HOST = config['KCS_HOST']
KCS_PORT = config['KCS_PORT']

SLEEP_TIME = float(config['sleep_time'])

PID = "/var/run/ais_dispatcher"

#### VARIABLES #########################################################



########################################################################

# Se definen los logs internos que usaremos para comprobar errores
try:
    logger = logging.getLogger('ais_dispatcher')
    loggerHandler = logging.handlers.TimedRotatingFileHandler(LOG, 'midnight', 1, backupCount=10)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    loggerHandler.setFormatter(formatter)
    logger.addHandler(loggerHandler)
    logger.setLevel(logging.DEBUG)
except:
    print '------------------------------------------------------------------'
    print '[ERROR] Error writing log at %s' % LOG
    print '[ERROR] Please verify path folder exits and write permissions'
    print '------------------------------------------------------------------'
    exit()

########################################################################

if os.access(os.path.expanduser(PID), os.F_OK):
        print "Checking if ais_dispatcher process is already running..."
        pidfile = open(os.path.expanduser(PID), "r")
        pidfile.seek(0)
        old_pd = pidfile.readline()
        # process PID
        if os.path.exists("/proc/%s" % old_pd) and old_pd!="":
            print "You already have an instance of the ais_dispatcher process running"
            print "It is running as process %s" % old_pd
            sys.exit(1)
        else:
            print "Trying to start ais_dispatcher process..."
            os.remove(os.path.expanduser(PID))

#This is part of code where we put a PID file in the lock file
pidfile = open(os.path.expanduser(PID), 'a')
print "ais_dispatcher process started with PID: %s" % os.getpid()
pidfile.write(str(os.getpid()))
pidfile.close()

########################################################################

########################################################################
# Definicion de clases
#
########################################################################

socketKCS = None
rabbitMQconnection = None

def connectKCS():
    global socketKCS

    # conexion a KCS
    socketKCS = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socketKCS.settimeout(0.1)
    try:
        socketKCS.connect((KCS_HOST, int(KCS_PORT)))
        logger.info('Connected to KCS')
        socketKCS.settimeout(0.05)
    except Exception, error:
        logger.error('Error connecting to KCS: %s', error)

def connectRabbitMQ():
    global rabbitMQconnection

    # conexión a rabbitMQ
    try:
        rabbitMQconnection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        return True
    except Exception, error:
        logger.error('Error connecting to rabbitMQ: %s', error)
    return False

def send2kcs(message):
    global socketKCS
    try:
        socketKCS.send(message)
        logger.info("[->] Sent to KCS")
        return True
    except Exception, error:
        logger.error('Error sending data to KCS: %s',error)
        logger.info('Trying reconnection to KCS')
        try:
            socketKCS.settimeout(0.1)
            socketKCS.connect((KCS_HOST, int(KCS_PORT)))
            socketKCS.settimeout(0.05)
            socketKCS.send(message)
            logger.info("[->] Sent to KCS")
            return True
        except:
            logger.info('Failed reconnection to KCS')
    return False

def main():
    global rabbitMQconnection

    # Bucle de conexion a la cola
    resultRabbitMQ = False
    while resultRabbitMQ != True:
        resultRabbitMQ = connectRabbitMQ()
        time.sleep(0.5)

    channel = rabbitMQconnection.channel()
    channel.queue_declare(queue='AIS', durable=True)
    logger.info(' [*] Waiting for messages. To exit press CTRL+C')

    def callback(ch, method, properties, body):
        logger.debug(" [x] Received %r" % body)
        resultKCSK = False
        while resultKCSK != True:
            resultKCSK = send2kcs(body)
            time.sleep(0.5)
        ch.basic_ack(delivery_tag = method.delivery_tag)
        time.sleep(0.15)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback,
                      queue='AIS')

    channel.start_consuming()


if __name__ == '__main__':
    connectKCS()
    main()