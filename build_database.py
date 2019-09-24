# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:34:50 2019

@author: GAllison

This script performs the overall task of creating a FracFocus database from
the raw excel collection and creating the outputs.

run_modes  (edit below):  
    1 = Create full dataset from scratch including graphs. Runs everything
    2 = Recreate graphs from new user settings.
    
"""

##-------------- User selections  --------------------##

run_mode = 1 # see above for options
rerun_raw_stats = False # takes a good chunk of time, so can switch off here
focal_state = 'ohio'  # should be lower case
focal_CAS_number = '50-00-0'  # needs to match exactly
focal_Operator = 'Just a test' # needs to match exactly
focal_Supplier = 'halliburton' # must match exactly (lower case) 

## below are the fields kept from raw_df to include in the later tables.
keep_col = ['UploadKey','IngredientKey','JobEndDate','JobStartDate',
            'OperatorName','TotalBaseWaterVolume','TotalBaseNonWaterVolume',
            'APINumber','StateNumber','StateName','WellName','CountyName','CountyNumber',
            'Latitude','Longitude','TVD','TradeName','Supplier','Purpose',
            'IngredientName','CASNumber','PercentHFJob','MassIngredient',
            'ingkey','raw_filename']
## ------------- end User selections -----------------##

import pandas as pd
import core.Read_FF as rff
import core.FF_stats as ffstats
import core.Parse_raw as parse_raw
import core.Flag_events as flag_ev
import core.Categorize_records as cat_rec
import core.Add_bg_columns as abc
import core.Process_mass as proc_mass

#### -----------   File handles  -------------- ####

####### folders
outdir = './out/'
sources = './sources/'
tempfolder = './tmp/'

####### zip input file
zfilename = sources+'currentData.zip'
####### pickle locations - data stored in a python format for easier use
raw_pickle_fn = tempfolder+'raw_pickle.pkl'  # this is a huge file
full_pickle_fn = tempfolder+'full_pickle.pkl' # another big one

####### output
raw_stats_fn = outdir+'ff_raw_stats.txt'

#### ----------    end File Handles ----------  ####




if run_mode==1:
    raw_df = rff.Read_FF(zname=zfilename,
                         picklefn=raw_pickle_fn).import_raw()
    if rerun_raw_stats:
        ffstats.FF_stats(raw_df,outfn=raw_stats_fn).calculate_all()
    raw_df = raw_df[keep_col].copy()
    df = parse_raw.Parse_raw().clean_fields(raw_df)
    raw_df = None  # we are done with this monster, get it out of memory
    df = flag_ev.Flag_events().clean_events(df)
    df = cat_rec.Categorize_CAS(df=df,sources=sources,outdir=outdir).do_all()
    df = abc.Add_bg_columns(df).add_all_cols()
    df = proc_mass.Process_mass(df).run()    
    print('Pickling full data set')
    df.to_pickle(full_pickle_fn)
    
else:
    df = pd.read_pickle(full_pickle_fn)
