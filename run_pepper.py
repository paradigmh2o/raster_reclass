"""
Benjamin Bowes, 11-24-2021

This script reclassifies a raster based on a table with counts of cells to be reclassified.
"""

import time
import rasterio as rio
import pandas as pd
import numpy as np
from rasterio.windows import Window  # This function is for I/O on subsets of geotiff files
from numpy.random import choice

# set up paths
hrupath = r"C:\Users\Ben\Desktop\Shasta_Pepper\HRU_BASE_FINAL\HRU_BASE_FINAL.tif"
# remap_hrupath: create a copy of the original raster and rename it prior to running
remap_hrupath = r"C:\Users\Ben\Desktop\Shasta_Pepper\pepper_out3\reclassed_raster4.tif"
reptable_path = r"C:\Users\Ben\Desktop\Shasta_Pepper\HRU_BASE_FINAL\KA_Peppering_FINAL.csv"

# this should be a pandas dataframe with three columns: torep (original hru value),
# repval (replacement value), and pct (count of original hru cells that will
# be replaced with the replacement value)
complete_reptable = pd.read_csv(reptable_path)[['VALUE', 'HRUCODE', 'CELLS']]
# Formatting reptable.
complete_reptable.columns = ['torep', 'repval', 'pct']

# Convert torep column to negative.
# This is so that we don't accidentally remap the same HRU twice.
complete_reptable['torep'] = complete_reptable.torep * -1

# get original raster shape
with rio.open(hrupath) as src:
    hru_numrows, hru_numcols = src.shape

# read the original raster
read_window = Window(0, 0, hru_numcols, hru_numrows)
with rio.open(hrupath) as src:
    hru_block = src.read(1, window=read_window)
hru_block = hru_block * -1

# Create a blank array for the new values to avoid overwriting previously written values
blank_raster = np.ndarray((hru_numrows, hru_numcols))

# loop over unique values in VALUE
value_tracker = []
count = 1
start_time = time.time()
for value in complete_reptable["torep"].unique():
    if count % 100 == 0:
        print("replacing ", value, ": ", count, " of ", len(complete_reptable["torep"].unique()))
    new_value = complete_reptable.loc[complete_reptable["torep"] == value]

    # Find locations in the block where the current value exists.
    row_indices, col_indices = np.where(hru_block == value)

    if len(new_value) == 1:  # VALUE does not have to be split between multiple HRUs
        # hru_block[row_indices, col_indices] = int(new_value["repval"])  # values can be replaced without randomizing
        blank_raster[row_indices, col_indices] = int(new_value["repval"])  # values can be replaced without randomizing

    if len(new_value) > 1:  # VALUE has to be split between multiple HRUs
        inds = np.arange(0, len(row_indices))  # Create empty vector to fill later with replacement locations.
        # Iterate through the table of replacement values, replacing the correct
        # percentage of the HRU with its replacement values.
        # for i, row in new_value.iterrows():
        for i in range(len(new_value)):
            repval = new_value.iloc[i]["repval"]  # new HRU number
            # numrep = int(row.pct*len(row_indices))  # this converts percent to count of cells
            numrep = int(new_value.iloc[i]["pct"])  # number of cells to replace, use this if count of cells is given in reptable

            # This function randomly selects points in the empty vector (inds),
            # which will be replaced with the current replacement value.
            inds_torep = choice(inds, numrep, replace=False)

            # Dropping the portions of the empty vector that are being replaced with the replacement value.
            inds = np.setdiff1d(inds, inds_torep)

            rows_torep = row_indices[inds_torep]  # Converting from 1D to 2D indices.
            cols_torep = col_indices[inds_torep]

            blank_raster[rows_torep, cols_torep] = repval  # This is where the HRU block values are actually replaced.

    if len(new_value) > 2:
        value_tracker.append(value)
    if new_value["pct"].sum() != len(row_indices):
        print("reclassified ", value, "sum of cells to replace:", new_value["pct"].sum(), "sum of cells replaced: ",
              len(row_indices))

    count += 1

# write new raster
with rio.open(remap_hrupath, "r+") as dst:
    dst.write(blank_raster, window=read_window, indexes=1)

print("processing time: ", time.time() - start_time)
