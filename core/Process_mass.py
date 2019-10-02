# -*- coding: utf-8 -*-
"""
Created on Wed Jul  3 16:52:41 2019

@author: Gary Allison
"""

import numpy as np
import pandas as pd

class Process_mass():
    """The methods of this class are used to calculate implied measures of
    mass of chemicals.  For this version, several criteria must be met for 
    chemical masses to be calculated:
        Within events:
            The sum of all PercentHFJob values must be within 5% tolerance of 
              100%.
            TotalBaseWaterVolume must be non-zero.
            A single 'carrier' or 'base fluid' record must be present that:
                - is water by bgCAS
                - is > 40% by PercentHFJob
    Overall, this yields mass calculations on roughly 2.8M records, 
    in about 87K events.  Higher yields can be accomplished with more rigorous
    checking and thorough curation.
    """
    
    def __init__(self, df):
        self.df = df
        # what is the acceptable upper and lower tolerance for total percentage
        self.upper_tolerance = 105
        self.lower_tolerance = 95
        
        # now use record_flags to label records that are workable
        print('Starting the Process_mass phase')
        self.df['ok'] = self.df.record_flags.str.contains('P|3') # perfect match or proprietary
        self.df.ok = np.where(self.df.record_flags.str.contains('R|1|2|4|5'),
                              False,self.df.ok)
        self.df['no_redund'] = ~self.df.record_flags.str.contains('R')
        print(f'Num events: {len(self.df[self.df.ok].UploadKey.unique())}')
        print(f'Total records: {len(self.df[self.df.ok])}')
        
    def _total_percent_within_range(self):
        """calculate the total percentange of materials within each event; must
        remove the redundant records first.  All others (including mislabled) 
        are kept in the calculation"""
        
        cnd1 = ~self.df.record_flags.str.contains('R')
        # only remove the redundant events - keep mislabeled for total % calculation
        gb = self.df[cnd1].groupby('UploadKey', as_index=False)['PercentHFJob'].sum()
        cnts = pd.cut(gb.PercentHFJob,[0,1,10,90,95,105,110,10000])
        print('Range of total % for "all" events')
        print(pd.value_counts(cnts))        
        cl = gb.PercentHFJob<=self.upper_tolerance
        ch = gb.PercentHFJob>=self.lower_tolerance
        ulist = list(gb[cl&ch].UploadKey.unique()) # all events within tolerance
        self.df.record_flags = np.where(self.df.UploadKey.isin(ulist),
                                   self.df.record_flags.str[:]+'-%',
                                   self.df.record_flags)
        
        
    def _get_carrier_names(self):
        """To find the ingredient records that have the PercentHFJob for the water
        reported in the TotalBaseWaterVolume, we need to find the purpose label 'carrier'
        or any of its variations that point to the record in a given event.  Because there
        are a lot of variations of 'carrier' and 'base fluid', we use a regular expression
        search for all the purpose categories that match.  However, those events that
        cram multiple purposes into a single cell have to be controlled for.  Because
        these multi-component purpose categories are long strings, we limit the
        length of the strings that are allowed in this list."""
        
        cn = pd.DataFrame({'Purpose':list(self.df.Purpose.unique())})
        cn['clean'] = cn.Purpose.str.strip().str.lower()
        cn['is_base'] = cn.clean.str.contains('base',regex=False)
        cn['is_carrier'] = cn.clean.str.contains('carrier',regex=False) | cn.is_base
        cn['is_short'] = cn.clean.str.len()<50 # long Purposes are multiple and should be excluded
        cn.is_carrier = cn.is_carrier&cn.is_short
        self.df = pd.merge(self.df,cn[['Purpose','is_carrier']],on='Purpose',how='left')
        
    def _mark_singular_carriers(self):
        """Many events mark more than one record as a carrier. While sometimes this
        is understandable, in many cases it muddies exactly what the carrier is and
        what its percentage is.  To keep things as simple as possible, we mark (and
        keep) only those event with a single record labeled as carrier. 
        
        """
        gb = self.df.groupby('UploadKey',as_index=False)['is_carrier'].sum() 
        gb['singular_carrier'] = np.where(gb.is_carrier==1,True,False)
        self.df = pd.merge(self.df,gb[['UploadKey','singular_carrier']],on='UploadKey',how='left')
        
    def _get_total_event_mass(self):
        cnd1 = self.df.record_flags.str.contains('%',regex=False)
        cnd2 = (self.df.PercentHFJob>40) & (self.df.bgCAS=='7732-18-5') 
        cnd3 = self.df.singular_carrier
        gb1 = self.df[self.df.ok & cnd1].groupby('UploadKey',as_index=False)['TotalBaseWaterVolume'].first()
        # don't really need sum here as it should be a single record, but...
        gb2 = self.df[self.df.no_redund & cnd1 & cnd2 & cnd3].groupby('UploadKey',as_index=False)['PercentHFJob'].sum()
        mg = pd.merge(gb1,gb2,on='UploadKey',how='left')
        mg['carrier_mass'] = mg.TotalBaseWaterVolume * 8.3  # reporting in lbs
        mg['total_mass'] = mg.carrier_mass/(mg.PercentHFJob/100)
        return mg
    
    def _calc_all_record_masses(self,totaldf):
        """apply 'M' to all filtered records that have mass and 'A' to all filtered
        records that have pres/abs.
        """
        # first limit to within reasonable range of 'carrier_mass'
        totaldf['tot_wi_range'] = (totaldf.PercentHFJob>40) & (totaldf.PercentHFJob<=100)
        self.df = pd.merge(self.df,totaldf[['UploadKey','total_mass','tot_wi_range']],
                           on='UploadKey',how='left',validate='m:1')
        self.df['bgMass'] = np.where(self.df.tot_wi_range,
                                    (self.df.PercentHFJob/100)*self.df.total_mass,
                                    np.NaN)
        
        self.df.record_flags = np.where((self.df.tot_wi_range)&(self.df.PercentHFJob>0),
                                   self.df.record_flags.str[:]+'-M',
                                   self.df.record_flags)
        self.df.record_flags = np.where(self.df.tot_wi_range,
                           self.df.record_flags.str[:]+'-A',
                           self.df.record_flags)

    def run(self):
        self._total_percent_within_range()
        self._get_carrier_names()
        self._mark_singular_carriers()
        totaldf = self._get_total_event_mass()
        self._calc_all_record_masses(totaldf)
        return self.df
    
    

