README for FF-POC repository and project

This CodeOcean capsule is a Proof of Concept version of code to transform 
the online chemical disclosure site for hydraulic fracturing, FracFocus.org, 
into a usable database.  The code demonstrates cleaning, filtering, and 
curating techniques to yield organized data sets and sample analyses 
from a notoriously messy collection of chemical records.   
The sample analyses are available in the results section as jupyter notebooks 
and downloadable versions of the final data are also available there.  
For a majority of the records, the mass of the chemicals is calculated. 
(The FracFocus data used were downloaded June 25, 2019).

To be included in final data sets, 
   Fracking events must use water as carrier and percentages must be 
     consistent and within tolerance.
   Chemicals must be identified by a match with an authoritative CAS number 
     or be labeled proprietary.

Further, portions of the raw data that are filtered out include: 
- fracking events with no chemical records (mostly 2011-May 2013).
- fracking events with multiple entries (and no indication which entries 
    are correct).
- chemical records that are identified as redundant within the event.

Finally,  I clean up some of the labeling fields by consolidating multiple 
versions of a single category into an easily searchable name. For instance, 
I collapse the 80+ versions of the supplier 'Halliburton' to a single name.

By removing or cleaning the difficult data from this unique data source, 
I produce a data set that should facilitate more in-depth 
analyses of chemical use in the fracking industry.

