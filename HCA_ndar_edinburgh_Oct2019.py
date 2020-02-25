import os, datetime
import csv
import pycurl
import sys
import shutil
from openpyxl import load_workbook
import pandas as pd
import download.box
from io import BytesIO
import numpy as np
import subprocess
from scipy import stats

redcapconfigfile="/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/.boxApp/redcapconfig.csv"

from download.box import LifespanBox
box_temp='/home/petra/UbWinSharedSpace1/boxtemp' #location of local copy of curated data
box = LifespanBox(cache=box_temp)

verbose = True
snapshotdate = datetime.datetime.today().strftime('%m_%d_%Y')
#catfromdate=max of last run--'2019-06-17'

#ped file will have all the nda vars from intradb, plus randomization status and
extrainfo='UnrelatedHCAHCD_w_STG_Image_and_pseudo_GUID09_25_2019.csv'
dev_peds_path='/home/petra/UbWinSharedSpace1/redcap2nda_Lifespan2019/Dev_pedigrees/'
hcplist=dev_peds_path+extrainfo

pathout="/home/petra/UbWinSharedSpace1/redcap2nda_Lifespan2019/HCA_crosswalk_docs/prepped_structures"


fieldlist=['racial','ethnic','iihandwr','iihandth','iihandsc','iihandto','iihandkn','iihandsp','iihandbr','iihandma','iihandbo','iihandfk','iihandey']
hca=box.getredcapfields(fieldlist, study='hcpa')
hca.loc[:,'nda_interview_date']=pd.to_datetime(hca['interview_date']) -pd.offsets.QuarterBegin(startingMonth=1)
hca=hca.rename(columns={'interview_age':'nda_interview_age'})
hca=hca.loc[hca.flagged.isnull()==True] #these already subset to v1 by redcap event pull
hcpids=pd.read_csv(hcplist,header=0)[['nda_guid','subjectped','final_pedid','unrelated_subset_October2019']]

hcaidsupdate=pd.merge(hcpids,hca,left_on='subjectped',right_on='subject',how='left')#.drop(columns=['YOB','Hand','flagged','gender','interview_date'])
hcaidsupdate=hcaidsupdate.loc[hcaidsupdate.subjectped.str.contains('HCA')] #842 as of 9/25

#for...ndarsubjects
#Convert numbers to strings for NDAR race, sex at birth, and ethnicity vars
#WARNING: these conversions are NOT THE SAME FOR EVERY REDCAP DB
dfnew=hcaidsupdate.copy()

dfnew['race']=dfnew.replace({'racial':
                                       {'1':'American Indian/Alaska Native',
                                        '2':'Asian',
                                        '3':'Black or African American',
                                        '4':'Hawaiian or Pacific Islander',
                                        '5':'White',
                                        '6':'More than one race',
                                        '99':'Unknown or not reported'}})['racial']

dfnew['ethnic_group']=dfnew.replace({'ethnic':
                                           {'1':'Hispanic or Latino',
                                            '2':'Not Hispanic or Latino',
                                            '3':'unknown or not reported'}})['ethnic']

dfnew['site_char']=dfnew.replace({'site':
                                      {'1': 'MGH', '2':'UCLA', '3':'UMinn', '4':'WashU'}})['site']



dfnew.loc[:,'phenotype']=pd.Series(("Healthy Subject" for i in range(dfnew.shape[0])),index=dfnew.index)
dfnew.loc[:,'phenotype_description']=pd.Series(("No diagnosed history of neurologic or major psychiatric disorder" for i in range(dfnew.shape[0])),index=dfnew.index)
dfnew.loc[:,'twins_study']=pd.Series(("No" for i in range(dfnew.shape[0])),index=dfnew.index)
dfnew.loc[:,'sibling_study']=pd.Series(("No" for i in range(dfnew.shape[0])),index=dfnew.index)
dfnew.loc[:,'family_study']=pd.Series(("No" for i in range(dfnew.shape[0])),index=dfnew.index)
dfnew.loc[:,'sample_taken']=pd.Series(("No" for i in range(dfnew.shape[0])),index=dfnew.index)

#check range of agemonths
z=stats.zscore(dfnew.nda_interview_age)
dfnew.iloc[z>3].copy()
dfnew.iloc[z<-3].copy()
#no 3SD outliers w.r.t age--output 2.5 outliers for checking.

#capture missing race
flagsz=pd.DataFrame(dfnew.loc[dfnew.race==''])
flags=flagsz[['subject_id','site_char',datevar]].copy()
flags.loc[:,'reason']=pd.Series(("No race" for i in range(flags.shape[0])),index=flags.index)
#assign these to 'Unknown or not reported' as confirmed with UCLA 3/22
dfnew.loc[dfnew.subject_id=='HCA6604264','race']='Unknown or not reported'
dfnew.loc[dfnew.subject_id=='HCA8580690','race']='Unknown or not reported'
dfnew.loc[dfnew.race=='']

#capture missing ethnicity
flagsez=pd.DataFrame(dfnew.loc[dfnew.ethnic_group==''])
flagse=flagsez[['subject_id','site_char',datevar]].copy()
flagse.loc[:,'reason']=pd.Series(("No ethnicity" for i in range(flagse.shape[0])),index=flagse.index)

#check no withdrawn subjects or other subjects that have underscores in their names indicative of withdrawn status
dfnew.loc[dfnew.flagged.isnull()==False]


#next for external use uploading to NDA
#remove unneccessary vars                                                                   and rename - note cant rename genderstring as gender until original numeric gender var is removed
dfslim=dfnew.drop(columns=['interview_date','site','dob','unrelated_subset_October2019','racial','ethnic','flagged','gender','subject','subject_id'])
df1=dfslim.rename(columns={"subjectped":"src_subject_id","nda_gender":"gender","nda_interview_age":"interview_age",
                           "nda_interview_date":"interview_date",
                           "final_pedid":"family_user_def_id","nda_guid":"subjectkey","site_char":"site"})
df1['interview_date']=pd.to_datetime(df1['interview_date']).dt.strftime('%m/%d/%Y')


#remove special case subjects from Mike email 10:09am 3/26
#HCA7030751 (has had structural preprocessing run)  HCA8848606  HCD0796168
#check these are gone (email from Mike 3/25/2019 - 12:38pm):
#HCD2049242 HCA7317064 HCA7477793 HCA7706378
#Remove HCA with ages>21 and <36 - email from Cindy, 3/25/2019, 9:24am
#HCA9814189 HCA8343272 HCA8715687 HCA9137577 HCA8858104 HCA6732980 HCA8738598 HCA6426670 HCA7411864 HCA8935904 HCA9808700
#three more from 9/25/2019  'HCA6780789','HCA6878504','HCA7025859'

#842
#these special should aready have been dropped by virtue of lack of stg data as of 9/25/2019, but check
nospecials=df1.loc[~df1['src_subject_id'].isin(['HCA7030751','HCA8848606','HCA7317064','HCA7477793','HCA7706378',
            'HCA9814189','HCA8343272','HCA8715687','HCA9137577','HCA8858104','HCA6732980','HCA8738598','HCA6426670',
                'HCA7411864','HCA8935904','HCA9808700','HCA6780789','HCA6878504','HCA7025859'])]


ndar=nospecials[['src_subject_id', 'family_user_def_id', 'gender', 'subjectkey', 'race', 'ethnic_group', 'phenotype','phenotype_description','family_study','twins_study', 'sibling_study', 'sample_taken', 'interview_date', 'interview_age','site']].copy()

#write out csv for validation
filePath=pathout+'/HCPA_restricted_ndar_subject_'+snapshotdate+'.csv'

if os.path.exists(filePath):
    os.remove(filePath)
else:
    print("Can not delete the file as it doesn't exists")

with open(filePath,'a') as f:
    f.write("ndar_subject,1\n")
    ndar.to_csv(f,index=False)


hand=nospecials[['src_subject_id','gender', 'subjectkey', 'interview_date', 'interview_age','iihandwr','iihandth','iihandsc','iihandto', 'iihandkn','iihandsp', 'iihandbr', 'iihandma','iihandbo','iihandfk','iihandey',]].copy()
varlist=["iihandwr","iihandth","iihandsc","iihandto","iihandkn","iihandsp","iihandbr","iihandma","iihandbo","iihandfk","iihandey"]
varlistnoeye=["iihandwr","iihandth","iihandsc","iihandto","iihandkn","iihandsp","iihandbr","iihandma","iihandbo","iihandfk"]

for var in varlist:
    print(hand.loc[hand[var]==''])

#assign these to median of their row for sum
hand.loc[hand.src_subject_id=='HCA7319472','iihandma']='1'
hand.loc[hand.src_subject_id=='HCA7605978','iihandfk']='5'

#convert to ints for sums
for var in varlist:
    hand[var]=hand[var].astype(int)


hand['handvarsum']=hand[varlistnoeye].sum(axis=1)
hand.loc[:,'handvarconstant']=pd.Series((3*len(varlistnoeye) for i in range(hand.shape[0])),index=hand.index)
hand['hcp_handedness_score']=5*(hand['handvarsum']-hand['handvarconstant'])
hand=hand.drop(['handvarsum','handvarconstant'],axis=1)


#assign these back to -99 Missing- (not possible to track down), and convert to string below

hand.loc[hand.src_subject_id=='HCA7319472','iihandma']=-99
hand.loc[hand.src_subject_id=='HCA7605978','iihandfk']=-99

for ii in range(len(varlist)):
    hand.loc[:,varlist[ii]]=hand.loc[:,varlist[ii]].astype(str)

for ii in range(len(varlist)):
    hand.loc[hand[varlist[ii]]=='-99',varlist[ii]]=''


#translate to nda values (e.g. #spr;right;spl;left;both;np;ne; oh
#SPR=Strongly Prefer Right; SPL=Strongly Prefer Left; ne =not experienced; np=no preference; oh=sometimes uses other hand
iihandlist=['iihandwr','iihandth','iihandsc','iihandto', 'iihandkn','iihandsp', 'iihandbr', 'iihandma','iihandbo','iihandfk','iihandey']
newhandlist=['newiihandwr','newiihandth','newiihandsc','newiihandto', 'newiihandkn','newiihandsp', 'newiihandbr', 'newiihandma','newiihandbo','newiihandfk','newiihandey']
for ii in range(len(iihandlist)):
    hand[newhandlist[ii]]=hand.replace({iihandlist[ii]:
                                       {'1':'left',
                                        '2':'spl',
                                        '3':'np',
                                        '4':'spr',
                                        '5':'right'}})[iihandlist[ii]]

hand=hand.drop(columns=iihandlist,axis=1)
dictnames=dict(zip(newhandlist,iihandlist))
hand=hand.rename(columns=dictnames)


filePath=pathout+'/HCPA_restricted_edinburgh_hand_'+snapshotdate+'.csv'
if os.path.exists(filePath):
    os.remove(filePath)
else:
    print("Can not delete the file as it doesn't exists")

with open(filePath,'a') as f:
    f.write("edinburgh_hand,1\n")
    hand.to_csv(f,index=False)


