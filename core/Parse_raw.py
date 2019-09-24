# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 13:32:22 2019

@author: GAllison

Parse_raw is used to take the raw dataframe, clean up empty cells, create
or update dictionaries of field labels and to separate it into tables, etc

# =============================================================================
# To facilitate an efficient work flow, we keep 'dictionaries' of all the different
# categories of selected fields (such as Supplier, Purpose, etc.)  This allows us
# to process the much shorter list of items in a dictionary and then later merge that
# information with the larger database.  For example, there are only about 3000 unique
# CASNumber entries for us to process, but there are more than 4,000,000 data records
# in the main dataframe. Working with the dictionary speeds up the process by >1000x.
# =============================================================================
"""

import pandas as pd
import numpy as np
#import csv


class Parse_raw():
    def __init__(self):  #,outdir = './out/'):
#        self.outdir = outdir
        self.blank_in = ['',None,np.NaN]
        self.blank_label = '_empty_entry_'
        self.blank_list = ['CASNumber','IngredientName','OperatorName',
                           'Supplier','Purpose','TradeName','StateName']
#        self.field_dic_name = self.outdir+'field_dic_pickle.pkl'

        
    def _adjust_API(self,row):
        """  Here we create a 10-character string version of the APINumber,
        which is an integer in the raw data and is susceptible to ambiguity
        when it comes to identifiying specific wells.  For example,
        state numbers < 10 are sometimes a problem; The first two digits
        of the API number should be the state number but they are sometimes
        wrong because the leading zero is dropped. Colorado must start '05'"""
        s = str(row.APINumber)
        if int(s[:2])==row.StateNumber:
            if int(s[2:5])==row.CountyNumber:
                return s[:10]  # this is the normal condition, no adjustment needed
        #print('Adjust_API problem...')
        s = '0'+s
        if int(s[:2])==row.StateNumber:
            if int(s[2:5])==row.CountyNumber:
                #self.fflog.add_to_well_log(s[:10],1)
                return s[:10]  # This should be the match; 
        else:
            # didn't work!
            s = str(row.APINumber)
            #self.fflog.add_to_well_log(s[:10],2)
            print(f'API10 adjustment failed: {row.APINumber}')
            return s[:10]
 
    
    def _normalize_empty_cells(self,df):
        print('Normalizing empty cells')
        for col in self.blank_list:
            if col in df.columns:
                df[col] = np.where(df[col].isin(self.blank_in),
                                  self.blank_label,df[col])
        return df
    
# =============================================================================
#     def _get_csv_df(self,fn):
#         """ get pandas df from csv with heavy quoting """
#         return pd.read_csv(fn,quotechar='$', quoting=csv.QUOTE_ALL,
#                               keep_default_na=False)
#     
#     def _put_csv_df(self,df,fn):
#         """ save pandas df into csv with heavy quoting """
#         df.to_csv(fn,quotechar='$', quoting=csv.QUOTE_ALL)
#     
# =============================================================================
    def _createAPI10(self,raw_df):
        print('Creating api10')
        api_df = raw_df.groupby('UploadKey',as_index=False)['APINumber',
                                            'StateNumber','CountyNumber'].first()
        api_df['api10'] = api_df.apply(lambda x: self._adjust_API(x),axis=1)
        api_df = api_df[['UploadKey','api10']]
        raw_df = pd.merge(raw_df,api_df,on='UploadKey',validate='m:1')
        return raw_df

    def _make_date_field(self,raw_df):
        print('Converting date')
        # drop the time portion of the datatime
        raw_df['d1'] = raw_df.JobEndDate.str.split().str[0]
        # fix some obvious typos that cause hidden problems
        raw_df['d2'] = raw_df.d1.str.replace('3012','2012')
        # instead of translating ALL records, just do uniques records ...
        tab = pd.DataFrame({'d2':list(raw_df.d2.unique())})
        tab['date'] = pd.to_datetime(tab.d2)
        # ... then merge back to whole data set
        raw_df = pd.merge(raw_df,tab,on='d2',how='left',validate='m:1')
        return raw_df.drop(['d1','d2'],axis=1)
    
    def clean_fields(self,raw_df):
        raw_df = self._normalize_empty_cells(raw_df)
        raw_df = self._createAPI10(raw_df)
        raw_df = self._make_date_field(raw_df)
        return raw_df

    
