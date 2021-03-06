# -*- coding: utf-8 -*-
"""
Created on Sat Jul  6 13:45:09 2019

@author: Gary Allison

"""
import pandas as pd

class FF_stats():
    """This class is used to generate some basic statistics on the raw FracFocus
    data.  This should be performed AFTER the main processing to take advantage of 
    the flags generated there.
    
    The stats here are generated for each field in the raw data and are divided
    into booleans, numerics and strings.  For all three groups, we report
    Percent_not_empty, that is what percent of records are not a "na" by 
    pandas' terms.

    For booleans, we also report the percent of records that are "true".

    For numerics, we also report the percent of records that are non-zero.

    For strings, we report the percent that are not one of the "not a value" 
    designator (see self.na_lst) as well as the number of unique values in the field.

    All fields that begin with an uppercase letter are original to the FracFocus
    raw data and remain unchanged in the output.  Fields that begin with 
    lowercase are generated by this code.    
    """   
    def __init__(self,df,outfn='./out/ff_raw_stats.txt'):
        self.raw_df = df
        self.cols = list(self.raw_df.columns)
        
        self.na_lst = ['na','n/a','none','null','unk','nan']
        self.df = self.raw_df
        self.outfn = outfn
        self._clear_outfile()            
        self._coerce_dtype()
        
    def _clear_outfile(self):
        with open(self.outfn,'w') as self.outf:
            self.outf.write('*'*20+'   FracFocus raw stats   '+'*'*20+'\n')

    def _add_to_outfile(self,txt):
        with open(self.outfn,'a') as self.outf:
            self.outf.write(txt)

    def _get_filtered_raw(self):
        self.flags = pd.read_csv('./out/raw_filtered_guide.csv')
        return pd.merge(self.flags,self.raw_df,on='ingkey',how='left')

    def _coerce_dtype(self):
        """ some columns need to be coerced into their native data type - that
        is, pandas assigns them to the wrong type when importing the raw.  """
        bools = ['FederalWell','IndianWell','IngredientMSDS']
        for b in bools:
            self.df[b] = self.df[b].astype('bool')
        
    def _show_bool_col_stat(self):
        out = '{:>25}: {:>12} {:>12}\n'.format('Field Name','% not empty',
                                             '% True')
        out +='     --------------------------------------------------------------\n'
        cols = list(self.df.columns)
        tot = len(self.df)
        for col in cols:
            if self.df[col].dtype=='bool':
                perc_non_na = len(self.df[~self.df[col].isna()])/tot *100
                perc_true = str(round(self.df[col].sum()/tot * 100,2))
                out+='{:>25}: {:>12} {:>12}\n'.format(col,
                                                    round(perc_non_na,2),
                                                    perc_true)
        self._add_to_outfile(out)

    def _show_numeric_col_stat(self):
        out='{:>25}: {:>12} {:>12}\n'.format('Field Name','% not empty',
                                             '% non zero')
        out +='     --------------------------------------------------------------\n'
        cols = list(self.df.columns)
        tot = len(self.df)
        for col in cols:
            if (self.df[col].dtype=='float64')|(self.df[col].dtype=='int64'):
                perc_non_na = len(self.df[~self.df[col].isna()])/tot *100
                if perc_non_na >0:
                    non_zero = round(len(self.df[self.df[col]!=0])/tot * 100,2)
                else:
                    non_zero = ' - '
                out+='{:>25}: {:>12} {:>12}\n'.format(col,
                                                    round(perc_non_na,2),
                                                    non_zero)
        self._add_to_outfile(out)


    def _show_string_col_stat(self):
        out='{:>25}: {:>12} {:>12} {:>12}\n'.format('Field Name','% not empty',
                                             '% not "N/A"','num unique')
        out+='     --------------------------------------------------------------\n'
        cols = list(self.df.columns)
        tot = len(self.df)
        for col in cols:
            try:
                if (self.df[col].dtype=='object'):
                    perc_non_na = len(self.df[~self.df[col].isna()])/tot *100
                    if perc_non_na >0:
                        non_zero = round(len(self.df[~self.df[col].str.lower().str.strip().isin(self.na_lst)])/tot * 100,2)
                        num_uni = len(self.df[col].unique())
                    else:
                        non_zero = ' - '
                        num_uni = ' - '
                    out+='{:>25}: {:>12} {:>12} {:>12}\n'.format(col,
                                                        round(perc_non_na,2),
                                                        non_zero,
                                                        num_uni)
            except:
                out+='{:>25}: {:>12} {:>12} {:>12}\n'.format(col,
                                                        '---','---','---')

        self._add_to_outfile(out)
        
    def calculate_all(self):
        self._add_to_outfile('\n        ################   Boolean fields   ####################\n')
        self._show_bool_col_stat()
        self._add_to_outfile('\n\n        ################   Numeric fields  ####################\n')
        self._show_numeric_col_stat()              
        self._add_to_outfile('\n\n        ################   String fields  ####################\n')
        self._show_string_col_stat()              
        
