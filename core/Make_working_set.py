# -*- coding: utf-8 -*-
"""
Created on Wed Sep 25 12:04:34 2019

@author: Gary

This module is used to store the master dataframe and 
create a new dataframe that has only cleaned data
and store it in the output directory as well as return it.
"""

import zipfile

def save_master_df(df,outdir='./out/'):
    # first drop temporary fields that are no longer useful
    df.drop(['perfect_match','proprietary','un_cas_like',
             'dup','redundant_rec','ok','no_redund','is_carrier','singular_carrier',
             'total_mass','tot_wi_range'],axis=1,inplace=True)
    df.to_csv(outdir+'full_df.csv')
    with zipfile.ZipFile(outdir+'full_df.zip','w') as z:
        z.write(outdir+'full_df.csv',compress_type=zipfile.ZIP_DEFLATED)
    
def save_single_state(df,outdir='./out/',state='ohio'): 
    """state must be lower case"""
    df[df.bgStateName==state].to_csv(outdir+state+'_only.csv')
    
def get_filtered_df(df,outdir = './out/',
                    keepcodes = 'M|3|A',removecodes= 'R|1|2|4|5',all_cols=False,
                    col_list=['UploadKey','CASNumber','IngredientName',
                              'Purpose','OperatorName',
                               'Supplier','MassIngredient','PercentHFJob',
                               'record_flags','FFVersion',
                               'bgCAS','bgIngredientName','bgMass','JobEndDate',
                               'JobStartDate','date','StateName','api10',
                               'bgSupplier','bgStateName','bgOperatorName',
                               'TotalBaseWaterVolume']):
    """return filtered dataset, optionally with a subset of columns"""
    print('Creating filtered version of data set')
    if all_cols: 
            # first drop temporary fields that are no longer useful
            df.drop(['perfect_match','proprietary','un_cas_like',
                     'dup','redundant_rec','ok','no_redund','is_carrier','singular_carrier',
                     'total_mass','tot_wi_range'],axis=1,inplace=True)
            df = df.copy()
    else: df = df[col_list].copy()
    print(f'Initial length:           {len(df)}')
    if keepcodes: df = df[df.record_flags.str.contains(keepcodes)]
    print(f'  after keepcodes({keepcodes}):   {len(df)}')
    if removecodes: df = df[~df.record_flags.str.contains(removecodes)]
    print(f'  after removecodes ({removecodes}): {len(df)}')
    df.to_csv(outdir+'filtered_df.csv')
    with zipfile.ZipFile(outdir+'filtered_df.zip','w') as z:
        z.write(outdir+'filtered_df.csv',compress_type=zipfile.ZIP_DEFLATED)
    return df.copy()
