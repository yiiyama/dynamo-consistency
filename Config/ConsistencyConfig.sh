## Location of all the files saved and read by the ConsistencyCheck
export ConsistencyCacheDirectory=/scratch/dabercro/ConsistencyCache

## Age (in sec) of uberftp output before walking again
export SiteDirListAge=30240

## Age (in sec) of DatasetsInPhedex.dat file before it is redownloaded
export DatasetsInPhedexAge=86400

## Maximum number of simultaneous wgets calls to Phedex
export NumPhedexThreads=2

## Age (in sec) of list of files in Phedex before redownloading
export PhedexOutputAge=30240

## Location of webpages
export ConsistencyWebpages=/home/cmsprod/public_html/ConsistencyChecks

## List of SEs
## I should find a way to automate this with the SiteDB API
export SE_T2_AT_Vienna=hephyse.oeaw.ac.at
export SE_T2_BE_IIHE=maite.iihe.ac.be
export SE_T2_BE_UCL=ingrid-se02.cism.ucl.ac.be
export SE_T2_BR_SPRACE=osg-se.sprace.org.br
export SE_T2_CH_CSCS=storage01.lcg.cscs.ch
export SE_T2_ES_CIEMAT=srm.ciemat.es
export SE_T2_US_Caltech=cit-se.ultralight.org
export SE_T2_US_Florida=srm.ihepa.ufl.edu
export SE_T2_US_MIT=se01.cmsaf.mit.edu
#export SE_T2_US_Nebraska=red-gridftp.unl.edu
export SE_T2_US_Nebraska=srm.unl.edu
export SE_T2_US_UCSD=bsrm-3.t2.ucsd.edu
export SE_T3_CH_PSI=t3se01.psi.ch
