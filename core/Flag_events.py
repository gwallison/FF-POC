# -*- coding: utf-8 -*-
"""
Created on Tue Sep 24 08:06:49 2019

@author: Gary

This module flags events that are empty of indgredients (mostly because they
are FF version 1 and not supplied with the bulk download) and events that 
that have more than one disclosure associated with it.  In this proof-of-concept
version of the code, we filter out all of these events.  It is possible to 
squeeze information out of these flagged events, but not without more specific
analysis and curation than is appropriate here.
"""
import numpy as np

class Flag_events():
    def __init__(self):
        self.here = 'Flag_events'

    def _flag_empty_events(self,raw_df):
        """The earliest FracFocus data on the pdf website is not included in 
        the bulk download.  There are placeholder records in the data set that include
        'header' data such as location, operator name, and even the amount of water
        used.  However, there are no records about the chemical ingredients in 
        the frack.  (Through incredible effort, these early data were scraped from
        the pdfs by the organization, SkyTruth, and are currently available online.)
        The vast majority of these events are FFVersion 1.
        Because no chemical disclosure is given for these events, we flag these data
        for removal 
        from the data set we use for analysis. Keeping them in the data set would
        distort any estimates of 'presence/absence' of materials."""
        raw_df['record_flags'] = '0' # initialize the 'flag' code field.
        raw_df.record_flags = np.where(raw_df.IngredientKey.isna(),
                                  raw_df.record_flags.str[:]+'-1',
                                      raw_df.record_flags)
        return raw_df
    
    def _flag_duplicate_events(self,raw_df):
        """The FracFocus data set contains multiple versions of some fracking events.
        Here we first find the duplicates (using the API number and the fracking date).
        NOTE:  For this version of the FF database, we mark ALL duplicates for removal.
        While it may be possible to salvage some of the duplicates, there are no direct 
        indicators of the most 'correct' version and, according to Mark Layne,
        sometimes duplicates are not even replacements, but rather additions to 
        previous entries.  (It may be possible to use the position of data entry
        to indicate the most recent and presumably correct disclosure entry.)
        In this version, we are also not flagging those 'empty events' that are
        already flagged.
        """
        t = raw_df[raw_df.record_flags=='0'][['UploadKey','date','api10','ingkey']].copy()
        t = t.groupby(['UploadKey','date','api10'],as_index=False)['ingkey','UploadKey'].first()
        t = t.sort_values(by='ingkey')
        t['dupes'] = t.duplicated(subset=['api10','date'],keep=False)

        dupes = list(t[t.dupes].UploadKey.unique())
        raw_df.record_flags = np.where(raw_df.UploadKey.isin(dupes),
                                  raw_df.record_flags.str[:]+'-2',
                                  raw_df.record_flags)
        return raw_df
    

    def clean_events(self,raw_df):
        print('Finding and flagging empty and duplicate events')
        raw_df = self._flag_empty_events(raw_df)
        raw_df = self._flag_duplicate_events(raw_df)
        empty_ev = list(raw_df[raw_df.record_flags.str.contains('1',regex=False)].UploadKey.unique())
        dup_ev = list(raw_df[raw_df.record_flags.str.contains('2',regex=False)].UploadKey.unique())
        print(f'Flagged events: \n  -- empty: {len(empty_ev)}\n  -- duplicate: {len(dup_ev)}')
        return raw_df

