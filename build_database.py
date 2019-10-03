# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:34:50 2019

@author: GAllison

This script performs the overall task of creating a FracFocus database from
the raw excel collection and creating the output data sets.

Change the file handles at the top of this code to appropriate directories.

    
"""


import pandas as pd
import core.Read_FF as rff
import core.FF_stats as ffstats
import core.Parse_raw as parse_raw
import core.Flag_events as flag_ev
import core.Categorize_records as cat_rec
import core.Add_bg_columns as abc
import core.Process_mass as proc_mass
import core.Make_working_set as mws

#### -----------   File handles  -------------- ####

####### uncomment below for local runs
outdir = './out/'
sources = './sources/'
tempfolder = './tmp/'

### uncomment below for running on CodeOcean
#outdir = '../results/'
#sources = '../data/'
#tempfolder = '../results/'


####### zip input file
zfilename = sources+'currentData.zip'

####### output
raw_stats_fn = outdir+'ff_raw_stats.txt'

#### ----------    end File Handles ----------  ####

raw_df = rff.Read_FF(zname=zfilename).import_raw()
ffstats.FF_stats(raw_df,outfn=raw_stats_fn).calculate_all()
df = parse_raw.Parse_raw().clean_fields(raw_df)
raw_df = None  # we are done with this monster, get it out of memory
df = flag_ev.Flag_events().clean_events(df)
df = cat_rec.Categorize_CAS(df=df,sources=sources,outdir=outdir).do_all()
df = abc.Add_bg_columns(df,sources=sources).add_all_cols()
df = proc_mass.Process_mass(df).run()    
mws.save_master_df(df,outdir=outdir)
df = mws.get_filtered_df(df,outdir=outdir)
