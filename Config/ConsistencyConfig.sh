## Location of all the files saved and read by the ConsistencyCheck
export ConsistencyCacheDirectory=/scratch/dabercro/ConsistencyCache

## Age (in sec) of uberftp output before walking again
export UberftpOutputAge=302400

## Age (in sec) of DatasetsInPhedex.dat file before it is redownloaded
export DatasetsInPhedexAge=86400

## Age (in sec) of list of files in Phedex before redownloading
export PhedexOutputAge=302400

## Location of webpages
export ConsistencyWebpages=/home/cmsprod/public_html/ConsistencyChecks

## List of SEs
## I should find a way to automate this
export SE_T2_AT_Vienna=hephyse.oeaw.ac.at
export SE_T2_CH_CSCS=storage01.lcg.cscs.ch
export SE_T2_US_MIT=se01.cmsaf.mit.edu
