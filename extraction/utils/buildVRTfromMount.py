# buildVRTfromMount -- S3 store is mounted, so not need to transfer first
#
import glob
import os
from datetime import datetime
from osgeo import gdal

from . import updateImageStatus as uis

# CREODIAS public S3 archive access


def buildVRTfromMount(cand, cardtype, bands, mountdir):
    # cand is a tuple
    oid = cand[0]
    reference = cand[1]
    obstime = cand[2]

    imglist = []

    if cardtype == 'bs':
        vrt_options = gdal.BuildVRTOptions(separate=True, srcNodata=0, VRTNodata=0)
        for b in bands:
            # Get the image
            imgpath = f"/{mountdir}/Sentinel-1/SAR/CARD-BS/{datetime.strftime(obstime, '%Y/%m/%d')}/{reference}/{reference}.data/Gamma0_{b}.img"
            hdrpath = f"/{mountdir}/Sentinel-1/SAR/CARD-BS/{datetime.strftime(obstime, '%Y/%m/%d')}/{reference}/{reference}.data/Gamma0_{b}.hdr"
            if not os.path.exists(imgpath):
                print(f"Resource {imgpath} not available in mounted S3 storage (FATAL)")
                if (uis.updateImageStatus(oid, f"!S3 {b}.img", 'inprogress')):
                    return False
                return False
            elif not os.path.exists(hdrpath):
                print(f"Resource {hdrpath} not available in mounted S3 storage (FATAL)")
                if (uis.updateImageStatus(oid, f"!S3 {b}.hdr", 'inprogress')):
                    return False
                return False
            else:
                imglist.append(imgpath)

    elif cardtype == 'c6':
        vrt_options = gdal.BuildVRTOptions(srcNodata=0, VRTNodata=0)
        # the CARD-COH6 image is a single GeoTIFF with 2 bands
        imgpath = f"/{mountdir}/Sentinel-1/SAR/CARD-COH6/{datetime.strftime(obstime, '%Y/%m/%d')}/{reference}/{reference}.tif"
        if not os.path.exists(imgpath):
            print(f"Resource {imgpath} not available in mounted S3 storage (FATAL)")
            if (uis.updateImageStatus(oid, f"!S3 .tif", 'inprogress')):
                return False
            return False
        else:
            imglist.append(imgpath)
    elif cardtype == 'c1':
        return True
    elif cardtype == 's2':
        vrt_options = gdal.BuildVRTOptions(separate=True, srcNodata=0, VRTNodata=0)
        # the CARD-COH6 image is a single GeoTIFF with 2 bands
        imgpath = f"/{mountdir}/Sentinel-2/MSI/L2A/{datetime.strftime(obstime, '%Y/%m/%d')}/{reference}/GRANULE/"
        # We first need to retrieve the subdir
        flist = glob.glob(f"{imgpath}/*")

        if not flist:
            print(f"Resource {imgpath} not available in S3 storage (FATAL)")
            if (uis.updateImageStatus(oid, f"!S3 subdir", 'inprogress')):
                return False
            return False

        subdir = flist[0].replace(imgpath,'').split('/')[0]

        mgrs_tile = reference.split('_')[5]
        full_tstamp = reference.split('_')[2]

        for b in bands:
            res = 0
            if b in ['B02', 'B03', 'B04', 'B08']:
                res = 10
            elif b in ['B05', 'B06', 'B07', 'B8A', 'B11', 'B12', 'SCL']:
                res = 20
            else:
                print(f"Band {b} in neither 10 nor 20 m resolution")
                return False
            selection = f"R{res}m/{mgrs_tile}_{full_tstamp}_{b}_{res}m.jp2"
            fullpath = f"{imgpath}{subdir}/IMG_DATA/{selection}"
            if not os.path.exists(fullpath):
                print(f"Resource {fullpath} not available in mounted S3 storage (FATAL)")
                if (uis.updateImageStatus(oid, f"!S3 .jp2", 'inprogress')):
                    return False
                return False
            else:
                imglist.append(fullpath)
    gdal.BuildVRT(f"data/{reference}.vrt", imglist, options=vrt_options)
    return True
