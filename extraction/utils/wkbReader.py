import struct
import numpy as np

# Function to decypher the WKB header
def wkbHeader(raw):
    # See http://trac.osgeo.org/postgis/browser/trunk/raster/doc/RFC2-WellKnownBinaryFormat

    header = {}

    header['endianess'] = struct.unpack('B', raw[0])[0]
    header['version'] = struct.unpack('H', raw[1:3])[0]
    header['nbands'] = struct.unpack('H', raw[3:5])[0]
    header['scaleX'] = struct.unpack('d', raw[5:13])[0]
    header['scaleY'] = struct.unpack('d', raw[13:21])[0]
    header['ipX'] = struct.unpack('d', raw[21:29])[0]
    header['ipY'] = struct.unpack('d', raw[29:37])[0]
    header['skewX'] = struct.unpack('d', raw[37:45])[0]
    header['skewY'] = struct.unpack('d', raw[45:53])[0]
    header['srid'] = struct.unpack('i', raw[53:57])[0]
    header['width'] = struct.unpack('H', raw[57:59])[0]
    header['height'] = struct.unpack('H', raw[59:61])[0]

    return header

# Function to decypher the WKB raster data
def wkbImage(raw):
    h = wkbHeader(raw)
    img = [] # array to store image bands
    offset = 61 # header raw length in bytes
    for i in range(h['nbands']):
        # Determine pixtype for this band
        pixtype = struct.unpack('B', raw[offset])[0]>>4
        # For now, we only handle unsigned byte
        if pixtype == 4:
            band = np.frombuffer(raw, dtype='uint8', count=h['width']*h['height'], offset=offset+1)
            img.append((np.reshape(band, ((h['height'], h['width'])))))
            offset = offset + 2 + h['width']*h['height']
        # to do: handle other data types

    return h['ipX'], h['ipY'], img

