# for fetching config values from an external file
import configparser
# for calculating which map tiles to download
import math
# used for downloading images
import requests
# for some log management
import logging
# so we can check if files exist
import os

def toLogLevel(level):
  return getattr(logging, level)

# read in and parse the config file
config = configparser.ConfigParser()
config.read('config')

if (config['logging']['console']):
  logging.basicConfig(level=toLogLevel(config['logging']['level']))
else:
  logging.basicConfig(filename=config['logging']['filename'], 
                      level=toLogLevel(config['logging']['level']))

logging.info('starting')

mapUriPrefix = config['graphicMap']['uriPrefix']
mapUriSuffix = '?access_token=%s' % (config['graphicMap']['token'])
satUriPrefix = config['satMap']['uriPrefix']
saveDir = config['location']['name']

# converts lat/lng/zoom to tile x/y
# from http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Derivation_of_tile_names
def deg2num(lat_deg, lon_deg, zoom):
  lat_rad = math.radians(lat_deg)
  n = 2.0 ** zoom
  xtile = int((lon_deg + 180.0) / 360.0 * n)
  ytile = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
  logging.debug('converted %s, %s at zoom %s to %s, %s', lat_deg, lon_deg, zoom, xtile, ytile)
  return (xtile, ytile)

# download an image from the provided uri 
# and give it the specified filename
def download_file(uri, filename):
  # if the file already exists, lets not download it again
  if os.path.isfile(filename):
    logging.debug('file %s already exists, skipping download' % (filename))
  else:
    logging.debug('downloading %s to %s', uri, filename)
    # open the file for writing to
    with open(filename, 'wb') as handle:
      # fetch the image from online
      response = requests.get(uri, stream=True)

      if not response.ok:
        logging.error(response)

      for block in response.iter_content(1024):
        if not block:
          break

        handle.write(block)


# download the specified satellite map tile
def download_sat(z, x, y):
  pic_url = '%s&z=%s&x=%s&y=%s' % (satUriPrefix, z, x, y)
  pic_file = '%s/sat/tile_%s_%s_%s.jpg' % (saveDir, z, x, y)
  download_file(pic_url, pic_file)

# download the specified graphic map tile
def download_vec(z, x, y):
  # assemble the full URI and filename for this tile
  pic_url = '%s%s/%s/%s%s' % (mapUriPrefix, z, x, y, mapUriSuffix)
  pic_file = '%s/vec/tile_%s_%s_%s.png' % (saveDir, z, x, y)
  download_file(pic_url, pic_file)


# we will download all tiles between these zoom levels
startzoom = int(config['location']['minzoom'])
endzoom = int(config['location']['maxzoom'])

# get the tile position of the start zoom
placeName = config['location']['name']
startTilePos = deg2num(float(config[placeName]['latitude']), 
                       float(config[placeName]['longitude']), 
                       startzoom)

# recursively download a tile at a given zoom level,
# and if we are not at the lowest zoom level,
# re-enter for each of this tiles four immediate children
def getAllTiles(zoom, tilex, tiley, endzoom):
  download_vec(zoom, tilex, tiley)
  download_sat(zoom, tilex, tiley)
  # if we are not at the lowest zoom,
  # then recurse down a level
  if (zoom < endzoom):
    nextZoom = zoom + 1
    nextTileX = tilex * 2
    nextTileY = tiley * 2
    getAllTiles(nextZoom, nextTileX, nextTileY, endzoom)
    getAllTiles(nextZoom, nextTileX + 1, nextTileY, endzoom)
    getAllTiles(nextZoom, nextTileX, nextTileY + 1, endzoom)
    getAllTiles(nextZoom, nextTileX + 1, nextTileY + 1, endzoom)

# kick off recursively downloading tiles
getAllTiles(startzoom, startTilePos[0], startTilePos[1], endzoom)

logging.info('done')
