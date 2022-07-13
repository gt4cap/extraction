import sys
import os
import glob
import shutil

from utils import findImageCandidate, updateImageStatus, buildVRTfromMount, indexedNumbaWindowedRasterStats, indexedNumbaHistogram

if not os.path.ismount('/eodata'):
    print("This script requires a mounted /eodata subdir (FATAL)")
    sys.exit(1)

tstype = sys.argv[1]
res = int(sys.argv[2])

g = findImageCandidate.findImageCandidate(tstype)

if g:
    if tstype == 's2':
        if res == -1:
            bands = ['SCL']
        elif res == 10:
            bands = ['B02', 'B03', 'B04', 'B08']
        else:
            bands = ['B05', 'B11']
    elif tstype == 'bs':
        bands = ['VV', 'VH']
    elif tstype == 'c1':
        bands = ['VV']

    if buildVRTfromMount.buildVRTfromMount(g, tstype, bands, 'eodata'):
        print(f"{g[0]} transferred to disk and VRT built")
        if res == -1:
            if indexedNumbaHistogram.indexRasterHistogram(g) > 0:
                updateImageStatus.updateImageStatus(g[0], 'extracted', 'inprogress')
            else:
                updateImageStatus.updateImageStatus(g[0], 'No parcels', 'inprogress')
        else:
            status = indexedNumbaWindowedRasterStats.indexRasterStats(g[0], g[1], bands, tstype)
            updateImageStatus.updateImageStatus(g[0], status, 'inprogress')
    else:
        updateImageStatus.updateImageStatus(g[0], 'eodata issue', 'inprogress')

    flist = glob.glob(f"data/{g[1]}*")
    for f in flist:
        os.remove(f)
    cachelist = glob.glob(f"/1/DIAS/**/{g[1]}*", recursive = True)
    if len(cachelist) > 0:
        shutil.rmtree(cachelist[0])

else:
   print("No candidate ingested image found")
