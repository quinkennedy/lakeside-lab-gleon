# using pymongo to connect to the MongoDB
import pymongo
from pymongo import MongoClient
# using built-in libraries for fetching from HTTPS
import urllib.request
import json
# allow for changing log levels
import logging
# we want to timestamp the readings
import datetime
# storing sensitive data in an external config file
import configparser

# converts a string log level (read in from the config)
# to a log LEVEL
def toLogLevel(level):
  return getattr(logging, level)

# read and parse the config file
config = configparser.ConfigParser()
config.read('config')

# set up constants
# some of this can be relocated to environment variables
dataURL = config['api']['uri']
mongoUser = config['mongo']['user']
mongoPW = config['mongo']['pass']
mongoHost = config['mongo']['host']
mongoPort = config['mongo']['port']
mlabDeployment = config['mongo']['deployment']
logLevel = toLogLevel(config['logging']['level'])
logFile = config['logging']['file']
logFormat = '%(asctime)s %(levelname)s: %(message)s'
# assemble the mongoURI from its parts
mongoURI = "mongodb://%s:%s@%s:%s/%s" % (mongoUser, 
                                         mongoPW, 
                                         mongoHost, 
                                         mongoPort, 
                                         mlabDeployment)

# configure logging
logging.basicConfig(format=logFormat,
                    filename=logFile,
                    level=logLevel)

logging.info('starting')

logging.info('connecting to MongoDB')
# connect to our MongoDB
client = MongoClient(mongoURI)
db = client.gleon
# and get the appropriate collections
orgs = db.organizations
sites = db.sites
readings = db.readings

logging.info('fetching data from remote service')
# assemble the HTTP request
req = urllib.request.Request(dataURL)

# get & parse the response into an object
res = urllib.request.urlopen(req).read()
data = json.loads(res.decode('utf-8'))

# convenience function for optionally adding document to collection
def findDoc( collection, document ):
  dbDoc = collection.find_one(document)
  dbId = None
  if dbDoc is None:
    logging.debug('adding document to collection')
    dbResult = collection.insert_one(document)
    dbId = dbResult.inserted_id
  else:
    logging.debug('found document in collection')
    dbId = dbDoc['_id']
  return dbId

# use one time for all readings
time = datetime.datetime.utcnow()


# TODO: what if the service is down?
# TODO: there is a top-level 'status' field..?

# for efficiency, we will do bulk inserts
# so we will store all measurement updates in this array
# for inserting all-together at the end
allReadings = []

logging.info('parsing returned data')
# data is organized by 'orgarization' at the top-level
for org in data['list']:
  # construct our minimal org-level data
  currOrg = {'name': org['name'],
             'id'  : org['id']}
  logging.debug('organization %s: %s', org['id'], org['name'])
  dbOrgId = findDoc(orgs, currOrg)
  
  # each organization can have multiple sites
  for site in org['sites']:
    # construct our site-level data
    currSite = {'name': site['name'],
                'id'  : site['id'],
                'org' : dbOrgId}
    logging.debug('  site %s: %s', site['id'], site['name'])
    dbSiteId = findDoc(sites, currSite)

    # add all the readings for this site
    numR = 0
    for reading in site['data']:
      # we will just store the raw reading data provided
      # but there are a couple of fields we want to add
      reading['site'] = dbSiteId
      reading['fetchTime'] = time
      # cache the document for bulk insertion at the end
      allReadings.append(reading)
      numR = numR + 1
    logging.debug("    readings: %s", numR)

# now bulk-insert all the readings
insertResult = readings.insert_many(allReadings)

logging.info('inserted %s readings', len(insertResult.inserted_ids))
logging.info('done')
