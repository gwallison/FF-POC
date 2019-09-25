# -*- coding: utf-8 -*-
"""
Created on Sun Jul 21 16:46:40 2019

@author: Gary
"""

import pandas as pd

class Add_bg_columns():
    """This class contains the methods to add the 'curated' fields into
    the data set.  These fields require significant manual supervision to 
    correct typos, consolidate categories etc.  Here, we just use the
    translation files (fieldname_xlate.csv) to create the new columns
    """
    
    def __init__(self,df,sources='./sources/'):
        self.df = df
        self.fields = ['Supplier','OperatorName','StateName']
        self.masterdir = sources
        
    def _add_cols(self):
        for field in self.fields:
            print(f'Adding column {"bg"+field}')
            fn = self.masterdir+field+'_'+'xlate.csv'
            ref = pd.read_csv(fn,keep_default_na=False,na_values='',quotechar='$')
            self.df['original'] = self.df[field].str.strip().str.lower()
            ref.columns = ['bg'+field,'original']
            ref = ref[~ref.duplicated(subset='original',keep='last')]
            self.df = self.df.merge(ref,on='original',how='left',validate='m:1')
            self.df = self.df.drop('original',axis=1)
        return self.df
    
    def add_all_cols(self):
        self._add_cols()
        return self.df