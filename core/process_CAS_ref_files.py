# -*- coding: utf-8 -*-
"""
Created on Wed Feb  6 19:41:04 2019

@author: GAllison

This set of routines is used to translate the ref files created with SciFinder
(online) to a reference dictionary used to validate the CASNumbers and 
find accepted synonyms for IngredientNames in the FracFocus dataset.

The ref files that are used as INPUT to this routine are created by 
searching manually for a given CAS number (the 
SciFinder website has heavy restrictions on automated searches) and then saving
the SciFinder results to a text file.  The routines below parse those 
text files for the infomation used later when comparing to CASNumber codes in 
FracFocus.  Each ref file may contain several CAS-registry records; that is an
artifact of how we performed the manual searches, and is handled by the code.

Note that 'primary name' is the name used by CAS as the main name for
a material; it is the first entry in the list of synonyms.


"""

import os

inputdir = './sources/CAS_ref_files/'
outputdir = './out/'

def processRecord(rec):
    """return tuple of (cas#,[syn])"""
    cas = 'Nope'
    prime = ''
    lst = []
    fields = rec.split('FIELD ')
    for fld in fields:
        if 'Registry Number:' in fld:
            start = fld.find(':')+1
            end = fld.find('\n')
            cas = fld[start:end]
        if 'CA Index Name:' in fld:
            start = fld.find(':')+1
            end = fld.find('\n')
            prime = fld[start:end].lower()
        if 'Other Names:' in fld:
            start = fld.find(':')+1
            lst = fld[start:].split(';')
    olst = [prime]
    for syn in lst:
        syn = syn.strip().lower()
        if len(syn)>0: 
            if syn not in olst:
                olst.append(syn)
    return (cas,olst)

def processFile(fn,ref_dict):
    with open(fn,'r') as f:
        whole = f.read()
    records = whole.split('END_RECORD')
    for rec in records:
        tup = processRecord(rec)
        ref_dict[tup[0]] = tup[1]   
    return ref_dict

def processAll(inputdir=inputdir):
    cas_ref_dict = {}
    fnlst = os.listdir(inputdir)
    for fn in fnlst:
        cas_ref_dict = processFile(inputdir+fn,cas_ref_dict)
    print(f'Number of CAS references collected: {len(cas_ref_dict)}')
    return(cas_ref_dict)

