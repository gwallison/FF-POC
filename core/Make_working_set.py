# -*- coding: utf-8 -*-
"""
Created on Wed Sep 25 12:04:34 2019

@author: Gary

This module is used to store the master dataframe and 
create a new dataframe that has only cleaned data
and store it in the output directory as well as return it.
"""

def save_master_df(df,outdir='./out/'):
    df.to_csv(outdir+'full_df.csv')
    
def save_single_state(df,outdir='./out/',state='ohio'): 
    """state must be lower case"""
    df[df.bgStateName==state].to_csv(outdir+state+'_only.csv')
    
def get_filtered_df(df,outdir = './out/',
                    keepcodes = 'M|3|A',removecodes= 'R|1|2|4|5',all_cols=False,
                    col_list=['UploadKey','CASNumber','IngredientName','Purpose','OperatorName',
                               'Supplier','MassIngredient','PercentHFJob','DQ_code','FFVersion',
                               'bgCAS','bgIngredientName','bgMass','JobStartDate','date','StateName','api10',
                               'bgSupplier','bgStateName','bgOperatorName','TotalBaseWaterVolume']):
    """return filtered dataset, optionally with a subset of columns"""
    print('Creating filtered version of data set')
    if all_cols: df = df.copy()
    else: df = df[col_list].copy()
    print(f'Initial length:           {len(df)}')
    if keepcodes: df = df[df.DQ_code.str.contains(keepcodes)]
    print(f'  after keepcodes({keepcodes}):   {len(df)}')
    if removecodes: df = df[~df.DQ_code.str.contains(removecodes)]
    print(f'  after removecodes ({removecodes}): {len(df)}')
    df.to_csv(outdir+'filtered_df.csv')
    return df.copy()
