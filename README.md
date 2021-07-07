# raster_reclass
Tool to reclassify raster values according to probabilistic remap table.

This is an improved version of my raster reclassification (peppering) script. 

The underlying algorithm is identical to before. However, the overall architecture is totally different. Now it's a python module, and can be imported or run as a shell command. It's also significantly faster due to improved thread scheduling. 

To use it in a jupyter notebook (or other python script):

```python
from raster_reclass import *

hrupath = '/path/to/original/raster.tif'
remap_hrupath = '/path/to/new/raster.tif' #if this file doesn't exist, the script will automatically make a new raster
reptable = pd.read_csv('/path/to/remaptable.csv') # this should be a pandas dataframe with three columns: torep (original hru value), repval (replacement value), 
												 # and pct (percentage of original hru cells that will be replaced with the replacement value)
blocksize = 2000 #the size of chunks the script will process the raster with
logpath = '/path/to/logs' #this is just the directory, NOT the log file itself

reclassraster(hrupath=hrupath, remap_hrupath=remap_hrupath, complete_reptable=reptable, blocksize=blocksize, logpath=logpath)
```

To use it as a shell command:

```bash
python distributed_raster_reclass.py --hrupath "/path/to/original/raster.tif" --remaphrupath "/path/to/new/raster.tif" --tablepath "/path/to/remaptable.csv" --blocksize 2000 -logpath "/var/log"

```