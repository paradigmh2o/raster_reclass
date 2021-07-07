#! /home/eric/envs/taunton  /home/eric/miniconda3/bin/python
import threading
import queue
import time
import sys
import os
import logging
import shutil
import platform
import rasterio as rio
import pandas as pd
import numpy as np
import argparse
from threading import Thread
from rasterio.windows import Window # This function is for I/O on subsets of geotiff files
from numpy.random import choice
from multiprocessing import cpu_count

def main():
    parser = argparse.ArgumentParser(description='Files necessary for remap process.')
    parser.add_argument('-hp','--hrupath', help='Path to hru geotiff',required=False)
    parser.add_argument('-rhp','--remaphrupath', help='Path to new remapped hru geotiff',required=False)
    parser.add_argument('-tp','--tablepath', help='Path to remap table',required=False)
    parser.add_argument('-bs','--blocksize', help='Blocksize (integer)',required=False)
    parser.add_argument('-lp','--logpath', help='Path to write logs to',required=False)
    args = parser.parse_args()

    hrupath = args.hrupath
    remap_hrupath = args.remaphrupath
    reptable_path = args.tablepath
    blocksize = args.blocksize
    logpath = args.logpath


    if not hrupath:
        hrupath = 'hru.tif'
    if not remap_hrupath:
        remap_hrupath = 'remaphru.tif'
    if not reptable_path:
        reptable_path = 'remaptable.csv'
    if not blocksize:
        blocksize = 2000
    if not logpath:
        ostype = platform.system()
        if ostype=='Linux':
            logpath = '/var/log'
        else:
            logpath = os.getcwd()

    complete_reptable = pd.read_csv(reptable_path)
    # Formatting reptable.
    complete_reptable.columns = ['torep','repval','pct']
    # Convert torep column to negative. 
    # This is so that we don't accidentally remap the same HRU twice.
    complete_reptable['torep'] = complete_reptable.torep * -1
    # There are a lot of HRU replacements that are a tiny fraction (like 1E-17), which we can ignore.
    # This line filters those out.
    complete_reptable = complete_reptable[complete_reptable.pct*100>=1]
    # There are some remap percentages that exceed 100% because whoever made the remap table did a bad job.
    # This rounds down to 100%. 
    complete_reptable.loc[complete_reptable.pct>1,'pct'] = 1
    # Sorting the table and resetting the index.
    complete_reptable = complete_reptable.sort_values('pct').reset_index(drop=True)

    reclassraster(hrupath=hrupath, remap_hrupath=remap_hrupath, complete_reptable=complete_reptable, blocksize=blocksize)


def setup_logger(logger_name, log_file, level):
    log_setup = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(asctime)s %(threadName)-17s %(levelname)-8s %(message)s')
    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setFormatter(formatter)
    log_setup.setLevel(level)
    log_setup.addHandler(file_handler)

def reclassraster(hrupath, remap_hrupath, complete_reptable, blocksize):

    if not os.path.exists(remap_hrupath):
        shutil.copyfile(hrupath, remap_hrupath)

    progress_logfile = os.path.join(logpath,'raster_reclass_progress.log')
    error_logfile = os.path.join(logpath,'raster_reclass_errors.log')

    setup_logger('progress', progress_logfile, logging.INFO)
    setup_logger('error', error_logfile, logging.ERROR)

    global errorlog
    errorlog = logging.getLogger('error')
    global progresslog
    progresslog = logging.getLogger('progress')

    pd.set_option('mode.chained_assignment', None)

    numcores = cpu_count()
    inputqueue = queue.Queue()
    outputqueue = queue.Queue()

    producer = WindowProducer(inputqueue=inputqueue, hrupath=hrupath, blocksize=blocksize)
    writer = BlockWriter(outputqueue=outputqueue, remap_hrupath=remap_hrupath)

    reclasscores = numcores - 2 # Leave 2 cores free: 1 for writing blocks and another so your computer doesn't freeze
    reclasslist = []
    for t in range(reclasscores):
        reclasslist.append(WindowProcessor(inputqueue=inputqueue, outputqueue=outputqueue, hrupath=hrupath,
                    complete_reptable=complete_reptable))

    producer.start()
    producer.join()
    for thread in reclasslist:
        thread.start()
    writer.start()

    starttime = time.time()
    while not (inputqueue.empty()) & (outputqueue.empty()):
        time.sleep(1)
        currenttime = time.time()
        elapsedtime = currenttime - starttime
        timestring = time.strftime("%H:%M:%S", time.gmtime(elapsedtime))
        sys.stdout.write(' Runtime: {} | Input queue: {} | Output queue: {} \r'.format(timestring,inputqueue.qsize(),outputqueue.qsize()))
        sys.stdout.flush()


    print('Raster reclassification complete.')

class WindowProducer(Thread):
    def __init__(self, inputqueue, hrupath, blocksize=1000):
        Thread.__init__(self)
        self.inputqueue = inputqueue
        self.blocksize = blocksize
        with rio.open(hrupath) as src:
            self.hru_numrows,self.hru_numcols = src.shape

    def run(self):

        colrange = range(0,self.hru_numcols,self.blocksize)
        rowrange = range(0,self.hru_numrows,self.blocksize)

        for row in rowrange:
            for col in colrange:
                if col == max(colrange):
                    block_width = self.hru_numcols - max(colrange)
                else:
                    block_width = self.blocksize
                if row == max(rowrange):
                    block_height = self.hru_numrows - max(rowrange)
                else:
                    block_height = self.blocksize

                # Window is a rasterio function we imported earlier. It converts the corner
                # coordinates we determined above into an object that rasterio uses to read
                # a chunk of the raster.
                read_window = Window(col,row,block_width,block_height)
                self.inputqueue.put(item=read_window, block=True, timeout=120)
                progresslog.info('READ WINDOW ADDED TO INPUT QUEUE-- {}, {}'.format(col,row))



class WindowProcessor(Thread):
    def __init__(self, inputqueue, outputqueue, complete_reptable, hrupath):
        Thread.__init__(self)
        self.inputqueue = inputqueue
        self.outputqueue = outputqueue
        self.stoprequest = threading.Event()

        self.complete_reptable = complete_reptable
        self.reptable = None
        self.read_window = None
        self.hru_block = None
        self.hrupath = hrupath
        self.nodata = None
        self.out_meta = None

    def run(self):
        while not self.stoprequest.isSet():
            try:
                self.read_window = self.inputqueue.get(block=True, timeout = 5)
                self.inputqueue.task_done()
            except queue.Empty:
                continue

            progresslog.info('READ WINDOW REMOVED FROM INPUT QUEUE-- {}'.format(self.read_window))

            try:
                self.hru_block, self.nodata, self.out_meta = self.readblock()
            except Exception as logerr:
                errorlog.error('{}----{}'.format(logerr,self.read_window))
            try:
                self.reclassblock()
            except Exception as logerr:
                errorlog.error('{}----{}'.format(logerr,self.read_window))
            try:
                self.outputblock()
            except Exception as logerr:
                errorlog.error('{}----{}'.format(logerr,self.read_window))


    def readblock(self):
        with rio.open(self.hrupath) as src:
            hru_block = src.read(1, window=self.read_window)
            nodata = src.nodata
            out_meta = src.meta.copy()

        hru_block[hru_block!=nodata] = hru_block[hru_block!=nodata]*-1
        progresslog.info('HRU BLOCK READ FROM GEOTIFF-- {}'.format(self.read_window))
        return (hru_block, nodata, out_meta)

    def reclassblock(self):
        vals_torep = np.unique(self.hru_block) # get every value we want to replace (every unique value)
        vals_torep = vals_torep[vals_torep!=self.nodata] #drop nodatas

        for val_torep in vals_torep:
            # Subset reptable so that we're only looking at replacement values for the current HRU.
            self.reptable = self.complete_reptable[self.complete_reptable.torep==val_torep]
            # Check to see if there are actually replacement values for the current HRU.
            if len(self.reptable)>0:

                row_indices,col_indices=np.where(self.hru_block==val_torep) # Find locations in the block where the current HRU exists.
                self.reptable['numrep'] = self.reptable['pct']*len(row_indices) # Calculate number of cells to replace with each replacement value.
                inds = np.arange(0,len(row_indices)) # Create empty vector to fill later with replacement locations.

                #Iterate through the table of replacement values, replacing the correct
                # percentage of the HRU with its replacement values.
                for i,row in self.reptable.iterrows():
                    repval = row.repval
                    numrep = int(row.numrep)

                    inds_torep = choice(inds,numrep,replace=False) # This function randomly selects points in the empty vector (inds),
                                                                   # which will be replaced with the current replacement value.

                    inds = np.setdiff1d(inds,inds_torep) # Dropping the portions of the empty vector
                                                         # that are being replaced with the replacement value.

                    rows_torep = row_indices[inds_torep] # Converting from 1D to 2D indices.
                    cols_torep = col_indices[inds_torep]

                    self.hru_block[rows_torep,cols_torep] = repval # This is where the HRU block values are actually replaced.

                # Because the replacement percentages may not alway sum exactly to 1, there may be a couple cells that
                # have not yet been replaced at this point. The three lines below handle those cases, by replacing them
                # with the last replacement value in the replacement table. It's technically less accurate than it could
                # be, but it will only ever be a couple cells out of thousands.
                row_indices,col_indices=np.where(self.hru_block==val_torep)
                if len(row_indices) > 0:
                    self.hru_block[row_indices,col_indices] = self.reptable.repval.tail(1).values

        progresslog.info('HRU BLOCK RECLASSIFIED-- {}'.format(self.read_window))



    def outputblock(self):
        output_list = [self.read_window, self.hru_block, self.out_meta]
        self.outputqueue.put(item=output_list, block=True, timeout=20)
        progresslog.info('RECLASSED BLOCK PLACED IN OUTPUT QUEUE-- {}'.format(self.read_window))


    def join(self, timeout=None):
        self.stoprequest.set()
        super(WindowProcessor, self).join(timeout)

class BlockWriter(Thread):
    def __init__(self, outputqueue, remap_hrupath):
        Thread.__init__(self)
        self.outputqueue = outputqueue
        self.stoprequest = threading.Event()
        self.remap_hrupath = remap_hrupath

    def run(self):
        while not self.stoprequest.isSet():
            try:
                self.read_window, self.hru_block, self.out_meta = self.outputqueue.get(block=True, timeout = 20)
                self.outputqueue.task_done()
            except queue.Empty:
                pass
            progresslog.info('RECLASSED BLOCK REMOVED FROM OUTPUT QUEUE-- {}'.format(self.read_window))
            try:
                with rio.open(self.remap_hrupath, "r+", **self.out_meta) as dst:
                    dst.write(self.hru_block,window=self.read_window,indexes=1)
            except Exception as logerr:
                errorlog.error(logerr)
            progresslog.info('RECLASSED BLOCK WRITTEN TO GEOTIFF-- {}'.format(self.read_window))

    def join(self, timeout=None):
        self.stoprequest.set()
        super(BlockWriter, self).join(timeout)

if __name__ == '__main__':
    main()
else:
    ostype = platform.system()
    if ostype=='Linux':
        logpath = '/var/log'
    else:
        logpath = os.getcwd()
