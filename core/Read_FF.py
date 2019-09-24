
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:15:03 2019

@author: GAllison

This module is used to read all the raw data in from a FracFocus excel zip file

Input is simply the name of the archive file.  We expect that file to be
in the "sources" directory of the parent folder.

All variables are read into the 'final' dataframe unless limited with the
"keep_list" input.

Re-typing and other processing is performed downstream in other modules.

"""
import zipfile
import re
import pandas as pd


class Read_FF():
    
    def __init__(self,zname='./sources/currentData.zip',
                 picklefn = './tmp/raw_df_pickle.pkl'):
        self.zname = zname
        self.pickle_fn = picklefn
        self._get_raw_cols_to_import()
        
    def _get_raw_cols_to_import(self):
        """just samples one file to retrieve the column list"""
        with zipfile.ZipFile(self.zname) as z:
            with z.open('FracFocusRegistry_1.csv') as f:
                t = pd.read_csv(f,low_memory=False,nrows=2,
                                keep_default_na=False,na_values='')
        self.import_cols = list(t.columns)

            
    def import_raw(self):
        """
        Because we are interested in documenting the different states of 'missing'
        data, we assign NaN values to only the empty cells (''), and keep characters
        entered as is.  Later in the process, the NaN will be transformed to a 
        string ('_empty_entry_') for non-numeric fields.
        """
        dflist = []
        with zipfile.ZipFile(self.zname) as z:
            inf = []
            for fn in z.namelist():
                # the files in the FF archive with the Ingredient records
                #  always start with this prefix...
                if fn[:17]=='FracFocusRegistry':
                    # need to extract number of file to correctly order them
                    num = int(re.search(r'\d+',fn).group())
                    inf.append((num,fn))
                    
            inf.sort()
            infiles = [x for _,x in inf]  # now we have a well-sorted list
            
            for fn in infiles:
                with z.open(fn) as f:
                    print(f' -- processing {fn}')
                    t = pd.read_csv(f,low_memory=False,
                                    usecols=self.import_cols, # currently, all columns
                                    # ignore pandas default_na values
                                    keep_default_na=False,na_values='')
                    
                    # the 'raw_filename' variable is used to make it easier to find the
                    # original source of data.
                    t['raw_filename'] = fn
                    dflist.append(t)
        final = pd.concat(dflist,sort=True)
        final.reset_index(drop=True,inplace=True) #  single integer as index
        final['ingkey'] = final.index.astype(int) #  create a basic integer index for easier reference
        final.to_pickle(self.pickle_fn) # stores the entire raw dataframe in a python pickle
        return final
        
    def get_raw_pickle(self):
        print('Fetching raw_df from pickle')
        return  pd.read_pickle(self.pickle_fn)
