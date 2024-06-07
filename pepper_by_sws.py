#%%
"""
Benjamin Bowes, 11-24-2021

This script reclassifies a raster based on a table with counts of cells to be reclassified.
Peppering is by SWS, which speeds up processing but requires all peppered SWS be merged after peppering.
"""

import os
import time
import rasterio as rio
import pandas as pd
import numpy as np
from rasterio.windows import Window  # This function is for I/O on subsets of geotiff files
from numpy.random import choice

# set up paths
hrupath = r"F:\Auckland\CombineRaster\FutureHRU_Peppered_Raster\Combine_FHRU.tif"
# SWS map path
swspath = r"F:\Auckland\CombineRaster\FutureHRU_Peppered_Raster\SWS_v1_rcls.tif"
# remap_hrupath: create a copy of the original raster and rename it prior to running
remap_hrupath = r"F:\Auckland\CombineRaster\FutureHRU_Peppered_Raster\FHRU_Peppered.tif"
reptable_path = r"F:\Auckland\CombineRaster\FutureHRU_Peppered_Raster\_FOR_PEPPER_HRU.csv"

# this should be a pandas dataframe with three columns: torep (original hru value),
# repval (replacement value), and pct (count of original hru cells that will
# be replaced with the replacement value)
complete_reptable = pd.read_csv(reptable_path)[['Value', 'MODEL', 'CELLS']]
# Formatting reptable.
complete_reptable.columns = ['torep', 'repval', 'pct']

#%%

# Convert torep column to negative.
# This is so that we don't accidentally remap the same HRU twice.
# complete_reptable['torep'] = complete_reptable.torep * -1

# get original raster shape
with rio.open(hrupath) as src:
    hru_numrows, hru_numcols = src.shape
    # nodata = src.nodata
    nodata = 0
    # print(nodata)
    profile = src.profile

rows = np.linspace(0,hru_numrows,10).astype(int)
cols = np.linspace(0,hru_numcols,10).astype(int)

#%%

with rio.open(swspath) as src:
    sws_numrows, sws_numcols = src.shape
    sws_nodata = src.nodata
    print("sws no data value:", sws_nodata)
    sws_profile = src.profile
    print('loading SWS raster')
    sws = src.read(1).astype(np.uint16)
print('getting unique SWS IDs')
swsid = np.unique(sws)
swsid = swsid[swsid!=sws_nodata]
# swsid = [144, 333, 346, 330, 433, 564, 613, 719, 750, 859]

#%%
for si,s in enumerate(swsid):
    # if si > 1092:
        # break
    print("SWS {} of {}, {}".format(si+1,len(swsid), time.ctime()))
    print('finding indices of SWS')
    sws_row_inds, sws_col_inds = np.where(sws==s)
    read_window = Window.from_slices(
        (sws_row_inds.min(),sws_row_inds.max()+1),
        (sws_col_inds.min(),sws_col_inds.max()+1))
    with rio.open(hrupath) as src:
        print('reading raster smash for SWS')
        hru_block = src.read(1, window=read_window)
        remapProfile = src.profile
        remapProfile.update({
            'height': read_window.height,
            'width': read_window.width,
            'transform': src.window_transform(read_window)})
    hru_block[sws[sws_row_inds.min():sws_row_inds.max()+1,sws_col_inds.min():sws_col_inds.max()+1]!=s] = nodata
    print('finding unique smash IDs')
    smashID = np.unique(hru_block)
    smashID = smashID[smashID!=nodata]
    smashcount = len(smashID)
    print('flatten raster smash array')
    hru_flat = hru_block.flatten()
    hru_flat_data = hru_flat!=nodata
    hru_flat_where, = np.where(hru_flat_data)
    if len(hru_flat_where) == 0:
        continue
    hru_flat_True = hru_flat[hru_flat_data]
    new_flat = nodata*np.ones_like(hru_flat).astype(int)
    for v,val in enumerate(smashID):
        print('value: {} ({} of {})'.format(val,v+1,smashcount),end='\r')
        new_value = complete_reptable.loc[complete_reptable["torep"] == val]
        val_inds, = np.where(hru_flat_True==val)
        #
        if new_value['pct'].sum() > len(val_inds):
            proportion = len(val_inds)/new_value['pct'].sum()
            new_pct = (new_value['pct']*proportion)[:-1].round().to_numpy()
            new_value = new_value.assign(pct=np.append(new_pct,len(val_inds)-new_pct.sum()).astype(int))
        #
        if len(new_value) == 1:  # VALUE does not have to be split between multiple HRUs
            # hru_block[row_indices, col_indices] = int(new_value["repval"])  # values can be replaced without randomizing
            new_flat[hru_flat_where[val_inds]] = int(new_value["repval"])  # values can be replaced without randomizing
        #
        if len(new_value) > 1:  # VALUE has to be split between multiple HRUs
            inds = np.arange(0, len(val_inds))  # Create empty vector to fill later with replacement locations.
            # Iterate through the table of replacement values, replacing the correct
            # percentage of the HRU with its replacement values.
            for i in range(len(new_value)):
                repval = new_value.iloc[i]["repval"]  # new HRU number
                numrep = int(new_value.iloc[i]["pct"])  # number of cells to replace, use this if count of cells is given in reptable

                # This function randomly selects points in the empty vector (inds),
                # which will be replaced with the current replacement value.
                inds_torep = choice(inds, numrep, replace=False)

                # Dropping the portions of the empty vector that are being replaced with the replacement value.
                inds = np.setdiff1d(inds, inds_torep)

                # rows_torep = row_indices[inds_torep]  # Converting from 1D to 2D indices.
                # cols_torep = col_indices[inds_torep]

                new_flat[hru_flat_where[val_inds[inds_torep]]] = repval  # This is where the HRU block values are actually replaced.
    print('writing new HRU raster for SWS {}'.format(s))
    # with rio.open(remap_hrupath.replace('.tif','_{:06d}.tif'.format(s)),'w',**remapProfile) as dst:
    with rio.open(os.path.join(r"F:\Auckland\CombineRaster\FutureHRU_Peppered_Raster\Peppered_by_SWS",
                               'SWS_{:06d}.tif'.format(s)),'w',**remapProfile) as dst:
        dst.write(np.reshape(new_flat,hru_block.shape).astype(int),indexes=1)
print('done!')

# #%%
# ci = 0

# for i,r in enumerate(rows[:-1]):

#     # if i > 0:
#         # continue

#     for j,c in enumerate(cols[2:-1]):

#         # if j > 0:
#             # continue

#         print('\n')
#         print('i: {} of {}'.format(i+1,len(rows)))
#         print('j: {} of {}'.format(j+1,len(cols)))

#         read_window = Window(cols[j],rows[i],cols[j+1],rows[i+1])

#         print('reading raster ... ',end='')
#         with rio.open(hrupath) as src:
#             hru_block = src.read(1, window=read_window)
#         print('done!')

#         vals = np.unique(hru_block)
#         vals = vals[vals!=nodata]

#         blank_raster = np.ndarray(hru_block.shape).astype(int)
        
#         if len(vals) == 0:
#             continue
#             ci += 1
        
#         for x,v in enumerate(vals):
#             if (x == 0) or ((x%10)==9):
#                 print('value: {} ({} of {})'.format(v,x+1,len(vals)),end='\r')

#             new_value = complete_reptable.loc[complete_reptable["torep"] == v]
#             row_indices, col_indices = np.where(hru_block == v)

#             if new_value['pct'].sum() > len(row_indices):
#                 proportion = len(row_indices)/new_value['pct'].sum()
#                 # new_value = new_value.multiply([1,1,proportion]).round().astype(int)
#                 new_pct = (new_value['pct']*proportion)[:-1].round().to_numpy()
#                 new_value = new_value.assign(pct=np.append(new_pct,len(row_indices)-new_pct.sum()).astype(int))
#                 complete_reptable.loc[complete_reptable["torep"] == v,'pct'] = complete_reptable.loc[complete_reptable["torep"] == v,'pct']-new_value['pct']


#             if len(new_value) == 1:  # VALUE does not have to be split between multiple HRUs
#                 # hru_block[row_indices, col_indices] = int(new_value["repval"])  # values can be replaced without randomizing
#                 blank_raster[row_indices, col_indices] = int(new_value["repval"])  # values can be replaced without randomizing

#             if len(new_value) > 1:  # VALUE has to be split between multiple HRUs
#                 inds = np.arange(0, len(row_indices))  # Create empty vector to fill later with replacement locations.
#                 # Iterate through the table of replacement values, replacing the correct
#                 # percentage of the HRU with its replacement values.
#                 for i in range(len(new_value)):
#                     repval = new_value.iloc[i]["repval"]  # new HRU number
#                     numrep = int(new_value.iloc[i]["pct"])  # number of cells to replace, use this if count of cells is given in reptable

#                     # This function randomly selects points in the empty vector (inds),
#                     # which will be replaced with the current replacement value.
#                     inds_torep = choice(inds, numrep, replace=False)

#                     # Dropping the portions of the empty vector that are being replaced with the replacement value.
#                     inds = np.setdiff1d(inds, inds_torep)

#                     rows_torep = row_indices[inds_torep]  # Converting from 1D to 2D indices.
#                     cols_torep = col_indices[inds_torep]

#                     blank_raster[rows_torep, cols_torep] = repval  # This is where the HRU block values are actually replaced.
#         print('\n')
#         print('writing to raster file ... ',end='')
#         if os.path.isfile(remap_hrupath):
#             # write new raster
#             with rio.open(remap_hrupath, 'r+') as dst:
#                 dst.write(blank_raster, window=read_window, indexes=1)
#         else:
#             # create raster
#             with rio.open(remap_hrupath, 'w', **profile) as dst:
#                 dst.write(blank_raster, window=read_window, indexes=1)
#         print('done!')
#         ci += 1
