# -*- coding: utf-8 -*-
"""
Created on Tue Jun 25 07:59:02 2019

@author: Gary Allison
"""
import pandas as pd
import numpy as np
import core.CAS_tools as ct
import core.process_CAS_ref_files

class Categorize_CAS():
    """ This class sorts all data in the dataframe into one of the following
    categories:
        - valid data record (has valid CAS id)
        - proprietary data
        - non-data 
        - hidden data (has quantity but identity is masked in some way)
        
    This work proceeds in phases:
    Phase I:    Find records which match CAS authority. 
    Phase II:   Mark the records that are explicitly labeled with some form
                of 'proprietary' (explicitly hidden) and other hidden labels
                (implicitly hidden)
    Phase III:  Mark records that are duplicates *within* a given event.  These
                records are apparently generated during the process of translating
                from the pdf files to the data files, and should be ignored. 
                There are over 70,000 of them.
    
        """
        
    def __init__(self,df,sources='./sources/',
                 outdir='./out/'):
        """df is the master FF data set being analyzed.
        praw is the Parse_raw object that set up the field_cat sets
        """
        self.df = df
        self.cas_ref_dict = core.process_CAS_ref_files.processAll(sources+'CAS_ref_files/')
        self._get_cas_field_cat()
        # the cas_labels file has a list of all the CASNumbers with a
        # column that identifies the labels that signify the 'proprietary' status
        # Those identifiers were added manually through inspection of the CAS list.
        self.cas_labels_fn = sources+'cas_labels.csv'
        #self.proprietary_list = sources+'CAS_labels_for_proprietary.csv'
        #self.refdir = './CAS_ref/out/'

    def _get_cas_field_cat(self):
        self.cas_field_cat = self.df.groupby('CASNumber',as_index=False)['UploadKey'].count()
        self.cas_field_cat.rename({'UploadKey':'total_cnt'},inplace=True,axis=1)
        print(f'Number of unique raw CASNumbers: {len(self.cas_field_cat)}')

###  Phase I - find valid records based on legimate CASNumber
        
    def _clean_CAS_for_comparison(self):
        #print('clean cas for comparison')
        #self.cas_field_cat.rename({'original':'CASNumber'},inplace=True,axis=1)
        self.cas_field_cat['cas_clean'] = self.cas_field_cat.CASNumber.str.replace(r'[^0-9-]','')
        self.cas_field_cat['zero_corrected'] = self.cas_field_cat.cas_clean.map(lambda x: ct.correct_zeros(x) )

    def _getIgName(self,cas):
        if cas in self.cas_ref_dict:
            return self.cas_ref_dict[cas][0]
        else:
            return 'name_unresolved'
        
    def _mark_if_perfect_match(self):
        self.cas_ref_lst = list(self.cas_ref_dict.keys())
        self.cas_field_cat['perfect_match'] = self.cas_field_cat.zero_corrected.isin(self.cas_ref_lst)
        self.cas_field_cat['bgCAS'] = np.where(self.cas_field_cat.perfect_match,
                                               self.cas_field_cat.zero_corrected,
                                               'cas_unresolved')
        self.cas_field_cat['bgIngredientName'] = self.cas_field_cat.bgCAS.map(lambda x: self._getIgName(x))
        tmp = self.cas_field_cat[['CASNumber','perfect_match','bgCAS','bgIngredientName']].copy()
        self.df = pd.merge(self.df,tmp,on='CASNumber',how='left',validate='m:1')
        
    def phaseI(self):
        self._clean_CAS_for_comparison()
        self._mark_if_perfect_match()
        print(f'Number of perfect matches from unique CAS: {self.cas_field_cat.perfect_match.sum()}')
        print(f'Total records affected:    {self.df.perfect_match.sum()}\n')
        self.df.record_flags = np.where(self.df.perfect_match,
                                   self.df.record_flags.str[:]+'-P',
                                   self.df.record_flags)
        
        
###  Phase II - Proprietary claims and other hidden labels
    def _add_proprietary_column(self):
        labels = pd.read_csv(self.cas_labels_fn,keep_default_na=False,na_values='')
        prop_lst = list(labels[labels.proprietary==1].clean.str.lower().str.strip().unique())
        self.cas_field_cat['proprietary'] = self.cas_field_cat.CASNumber.str.lower().str.strip().isin(prop_lst)
        tmp = self.cas_field_cat[['CASNumber','proprietary']].copy()
        self.df = pd.merge(self.df,tmp,on='CASNumber',how='left',validate='m:1')
        
    def _add_hiding_column(self):
        labels = pd.read_csv(self.cas_labels_fn,keep_default_na=False,na_values='')
        hiding_lst = list(labels[labels.hiding==1].clean.str.lower().str.strip().unique())
        self.cas_field_cat['un_cas_like'] = self.cas_field_cat.CASNumber.str.lower().str.strip().isin(hiding_lst)
        tmp = self.cas_field_cat[['CASNumber','un_cas_like']].copy()
        self.df = pd.merge(self.df,tmp,on='CASNumber',how='left',validate='m:1')

    def phaseII(self):
        """record_flags for explicit proprietary is 3
                    for non_cas_like CAS Number but with quantity = 4
                    for non_cas_like CAS Number but absent quantity = 5"""
        self._add_proprietary_column()
        print(f'Total Proprietary records= {self.df.proprietary.sum()}')
        
        self.df.record_flags = np.where(self.df.proprietary,
                                   self.df.record_flags.str[:]+'-3',
                                   self.df.record_flags)
        
        
        self._add_hiding_column()
        cond1 = self.df.PercentHFJob>0
        cond2 = self.df.MassIngredient>0
        has_quant = cond1 | cond2
        not_quant = ~has_quant
        cond3 = self.df.un_cas_like
        self.df.record_flags = np.where(cond3&has_quant,
                                self.df.record_flags.str[:]+'-4',
                                self.df.record_flags)
        self.df.record_flags = np.where(cond3&not_quant,
                                self.df.record_flags.str[:]+'-5',
                                self.df.record_flags)
        print(f'Total Non_caslike but quant = {len(self.df[self.df.record_flags.str.contains("4",regex=False)])}')
#        print(f'Total Non_caslike but not quant = {len(t[t.record_flags==5])}')
        
### Phase III - check for ingredient duplicates within events
    def _flag_duplicated_records(self):
        self.df['dup'] = self.df.duplicated(subset=['UploadKey','IngredientName',
                                       'CASNumber','MassIngredient','PercentHFJob'],
                                        keep=False)
        c0 = ~self.df.IngredientKey.isna()
        cP = self.df.record_flags.str.contains('P',regex=False)
        dups = self.df[(self.df.dup)&(c0)&(cP)].copy()
        c1 = dups.Supplier.str.lower().isin(['listed above'])
        c2 = dups.Purpose.str.lower().str[:9]=='see trade'
        dups['redundant_rec'] = c1&c2
        #print(f' Expected redundant total: {dups.redundant_rec.sum()}, {c1.sum()}, {c2.sum()}')
        #print(f'dups len = {len(dups)}, inKey len = {len(dups.IngredientKey.unique())}')
        self.df = pd.merge(self.df,dups[['IngredientKey','redundant_rec']],
                           on='IngredientKey',how='left',validate='m:1')

    def phaseIII(self):
        """ > 70000 records are duplicated within events apparently due to the
        process of converting the pdf files to the bulk download.  These duplicates
        are identifiable by their 'Supplier' and 'Purpose' fields. Here we identify
        all duplicates (by 5 fields) then flag those that have the supplier/purpose
        characteristic -- record_flags is R."""
        self._flag_duplicated_records()
        self.df.record_flags = np.where(self.df.redundant_rec==True,
                                self.df.record_flags.str[:]+'-R',
                                self.df.record_flags)
        print(f'Total redundant records flagged: {len(self.df[self.df.record_flags.str.contains("R",regex=False)])}')
        
    def do_all(self):
        self.phaseI()
        self.phaseII()
        self.phaseIII()
        return self.df