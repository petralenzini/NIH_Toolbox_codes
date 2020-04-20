#note that there is a key part of this program that requires command line interaction...
#if you run this on files that already exist, you risk overwriting guids....
#proceed line by line

import pandas as pd
import os, datetime
from src.download.box import LifespanBox

verbose = True
#verbose = False
snapshotdate = datetime.datetime.today().strftime('%m_%d_%Y')
root_cache='/data/intradb/tmp/box2nda_cache/'
cache_space = os.path.join(root_cache)         
try: 
    os.mkdir(cache_space)
except:
    print("cache already exists")

root_store = '/home/shared/HCP/hcpinternal/ccf-nda-behavioral/store/'
store_space = os.path.join(root_store, 'eprime') #this will be the place to save any snapshots on the nrg servers
try: 
    os.mkdir(store_space) #look for store space before creating it here
except:
    print("store already exists")

#connect to Box
box = LifespanBox(cache=cache_space)

#subjects who already have guids:
haveguids=pd.read_csv('genguids/subjects_w_guids_16April2020IntraDBdownload.csv',header=0)

#merge with all redcap subject ids to find subjects who need guids
hcd=box.getredcapfields(['id'],study='hcpdchild')
hca=box.getredcapfields(['id'],study='hcpa')

hcd18=box.getredcapfields(['id'],study='hcpd18')

allsubjects=pd.concat([hcd,hca],axis=0)
#drop the withdrawns
allsubjects=allsubjects.loc[allsubjects.flagged.isnull()==True][['subject_id','id']]
all18subjects=hcd18.loc[hcd18.flagged.isnull()==True][['subject_id','id']]

#did this already....circling back to do the hcpd18 folks after moving the used psudo guids to 'used' and generating more new ones to fill holes
allwguids=pd.merge(haveguids,allsubjects,how='right',left_on='Subject',right_on='subject_id')
allwguids['pseudo_guid']=allwguids.nda_guid
#find how many need new ids
a=len(allwguids.loc[allwguids.pseudo_guid.isnull()==True])


all18wguids=pd.merge(haveguids,all18subjects,how='right',left_on='Subject',right_on='subject_id')
all18wguids['pseudo_guid']=all18wguids.nda_guid
#find how many need new ids
a18=len(all18wguids.loc[all18wguids.pseudo_guid.isnull()==True])

#############################
#need to automate this process based on a or a18 to call command line:
#first move or delete the existing guidout.txt file...otherwise you'll be appending to a file and grab guiweree already assigned
#depending on the length of a (or a18), run the following to get more guids (move them to 'used' status after youve assigned them to people)
#ran this command from nrgpackages/tools.release/intradb/ccf-nda-behavioral/ccf-nda-behavioarl/genguids, where I saved all the relevant jar files 
#I got with the wgets in get_guid_client.txt  from Mike Hodge (email 4/15/2020 also came with guidurl.properties)
#for i in {1..a}; do java -cp "./*" gov.nih.ndar.ws.guid.client.CmdLineGuidClient -a getInvalidGuid -u plenzini -p myndarpassword >> guidout.txt; done


#read the new ids from the guid generated output file
command='grep NDAR_ genguids/guidout.txt > newguids.txt'
os.system(command)
#newguids=pd.read_csv('newguids.txt',header=None,prefix='newguid')
newguids18=pd.read_csv('newguids.txt',header=None,prefix='newguid')
###############################


#trim to size and give it the allwguids index so you can plop it in to that dataframe
newguids=newguids.iloc[0:a]
newguids18=newguids18.iloc[0:a18]

newguids.newguid0.index=allwguids.loc[allwguids.pseudo_guid.isnull()==True,'pseudo_guid'].index
newguids18.newguid0.index=all18wguids.loc[all18wguids.pseudo_guid.isnull()==True,'pseudo_guid'].index

allwguids.loc[allwguids.pseudo_guid.isnull()==True,'pseudo_guid']=newguids.newguid0[0:a]
all18wguids.loc[all18wguids.pseudo_guid.isnull()==True,'pseudo_guid']=newguids18.newguid0[0:a18]

allwguids['redcap_event_name']='visit_1_arm_1'
all18wguids['redcap_event_name']='visit_arm_1'

hcaguids=allwguids.loc[allwguids.subject_id.str.contains('HCA')==True]
hcdguids=allwguids.loc[allwguids.subject_id.str.contains('HCD')==True]
hcd18guids=all18wguids.loc[all18wguids.subject_id.str.contains('HCD')==True]

hcaguids[['subject_id','id','redcap_event_name','pseudo_guid']].to_csv('HCA_PseudoGuids4REDCap_'+snapshotdate+'.csv',index=False)
hcdguids[['subject_id','id','redcap_event_name','pseudo_guid']].to_csv('HCD_PseudoGuids4REDCap_'+snapshotdate+'.csv',index=False)
hcd18guids[['subject_id','id','redcap_event_name','pseudo_guid']].to_csv('HCD18_PseudoGuids4REDCap_'+snapshotdate+'.csv',index=False)

#now move the generated guids to 'used' status
#mv guidout.txt used_guidout_for17Aprildump2redcap.txt
