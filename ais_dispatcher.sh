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

import fiona
import utm
import ogr, osr

from shapely.geometry import shape

########################################################################
# configuracion y variables globales
from configobj import ConfigObj
config = ConfigObj('./tracking-push.properties')

LOG = config['directory_logs'] + "/push.log"
LOG_FOR_ROTATE = 10

DB_FRONTEND_IP = config['mysql_host']
DB_FRONTEND_PORT = config['mysql_port']
DB_FRONTEND_NAME = config['mysql_db_name']
DB_FRONTEND_USER = config['mysql_user']
DB_FRONTEND_PASSWORD = config['mysql_passwd']

REMOTE_IP = config['remote_ip']
REMOTE_PORT = config['remote_port']

FLEET_ID = config['fleet_id']
SLEEP_TIME = float(config['sleep_time'])

PID = "/var/run/tracking-push/tracking-push"

#### VARIABLES #########################################################
vRoute = []

LOG_FOLDER = "./shp2kyros.log"

queryHeader = "INSERT INTO routes (SHAPE) VALUES ( GeomFromText( \' LineString("
queryFooter = ") \' ) )"

# Spatial Reference System
inputEPSG = 3857
outputEPSG = 4326

########################################################################

# definimos los logs internos que usaremos para comprobar errores
try:
    logger = logging.getLogger('shp2kyros')
    loggerHandler = logging.handlers.TimedRotatingFileHandler(LOG_FOLDER, 'midnight', 1, backupCount=10)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    loggerHandler.setFormatter(formatter)
    logger.addHandler(loggerHandler)
    logger.setLevel(logging.DEBUG)
except:
    print '------------------------------------------------------------------'
    print '[ERROR] Error writing log at %s' % LOG_FOLDER
    print '[ERROR] Please verify path folder exits and write permissions'
    print '------------------------------------------------------------------'
    exit()

########################################################################

if os.access(os.path.expanduser(PID), os.F_OK):
        print "Checking if tracking-push process is already running..."
        pidfile = open(os.path.expanduser(PID), "r")
        pidfile.seek(0)
        old_pd = pidfile.readline()
        # process PID
        if os.path.exists("/proc/%s" % old_pd) and old_pd!="":
            print "You already have an instance of the tracking-push process running"
            print "It is running as process %s" % old_pd
            sys.exit(1)
        else:
            print "Trying to start tracking-push process..."
            os.remove(os.path.expanduser(PID))

#This is part of code where we put a PID file in the lock file
pidfile = open(os.path.expanduser(PID), 'a')
print "Tracking-push process started with PID: %s" % os.getpid()
pidfile.write(str(os.getpid()))
pidfile.close()

########################################################################

########################################################################
# Definicion de clases
#
########################################################################

if __name__ == '__main__':
    main()

Control de parametros:
if len(sys.argv) == 1:
    print "--------------------------------------------------------"
    print "Este programa necesita parametros:"
    print " --> fichero.csv"
    print "Ejemplo: fia-test-estres.sh ruta1.csv"
    exit()

if len(sys.argv) < 2:
    print "ERROR: Numero de parámetros incorrecto"
    exit()