# raster_reclass
This is an improved version of my raster reclassification (peppering) script. 

The underlying algorithm is identical to before. However, the overall architecture is totally different. Now it's a python module, and can be imported or run as a shell command. It's also significantly faster due to improved thread scheduling. 

# Required Inputs and Formats
Two inputs are required:
1) A baseline raster to be reclassified.
2) A csv table with the raster cell values to be reclassified, the replacement value, and the percentage of original cell values that will be reclassified.

An example input raster and reclassification table are in the Example_data folder.

# Running
To use it in a jupyter notebook (or other python script):

```python
from raster_reclass import *

hrupath = '/path/to/original/raster.tif'

#if this file doesn't exist, the script will automatically make a new raster
remap_hrupath = '/path/to/new/raster.tif' 

# this should be a pandas dataframe with three columns: torep (original hru value),
# repval (replacement value), and pct (percentage of original hru cells that will
# be replaced with the replacement value)
reptable = pd.read_csv('/path/to/remaptable.csv') 

#the size of chunks the script will process the raster with
blocksize = 2000 

#this is just the directory, NOT the log file itself
logpath = '/path/to/logs' 

reclassraster(hrupath=hrupath, remap_hrupath=remap_hrupath, complete_reptable=reptable, 
				blocksize=blocksize, logpath=logpath)
```

To use it as a shell command:

```bash
python distributed_raster_reclass.py --hrupath "/path/to/original/raster.tif" --remaphrupath "/path/to/new/raster.tif" --tablepath "/path/to/remaptable.csv" --blocksize 2000 -logpath "/var/log"

```

# Packages
pepper_by_sws working with:
- numpy=1.20.1
- pandas=1.2.3
- rasterio=1.2.1
