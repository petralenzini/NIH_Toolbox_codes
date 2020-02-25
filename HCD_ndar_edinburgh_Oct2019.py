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
extrainfo='UnrelatedHCAHCD_w_STG_Image_and_pseudo_GUID09_27_2019.csv'
dev_peds_path='/home/petra/UbWinSharedSpace1/redcap2nda_Lifespan2019/Dev_pedigrees/'
hcplist=dev_peds_path+extrainfo

pathout="/home/petra/UbWinSharedSpace1/redcap2nda_Lifespan2019/HCD_crosswalk_docs/prepped_structures"

fieldlist=['racial','ethnic','hand1','hand2','hand3','hand4','hand5','hand6','hand7','hand8',
           'hand_total','iihandwr','iihandth','iihandsc','iihandto','iihandkn','iihandsp',
           'iihandbr','iihandma','iihandbo','iihandfk','iihandey']

hcdchild=box.getredcapfields(fieldlist, study='hcpdchild')
hcdchild.drop(columns=['dob'])
hcdchild.loc[hcdchild.gender=='1','genderstring']='M'
hcdchild.loc[hcdchild.gender=='2','genderstring']='F'

hcdchild['nda_interview_date']=pd.to_datetime(hcdchild['interview_date']) -pd.offsets.QuarterBegin(startingMonth=1)
hcdchild=hcdchild.rename(columns={'interview_age':'nda_interview_age'})

dfnewparent=box.getredcapfields(['child_id','p_c_race','p_c_latino','p_c_sex'] ,study='hcpdparent')

#HCPDchildrens parents can select multiple race options (checkbox)...need to convert those to 'more than one race' category of mutually exclusive
#version of race variable required by NDAR
#note this is innefficient because goes through the data multiple times instead of with elifs...
#12, NATIVE AMERICAN | 18, ASIAN |  11, BLACK/AFRICAN AMERICAN |  14, NATIVE HAWAIIAN or
#OTHER PACIFIC ISLANDER |10, WHITE | 25, MORE THAN ONE RACE |  99, DON'T KNOW
dfnewparent.p_c_race___10=dfnewparent.p_c_race___10.astype(int)
dfnewparent.p_c_race___11=dfnewparent.p_c_race___11.astype(int)
dfnewparent.p_c_race___12=dfnewparent.p_c_race___12.astype(int)
dfnewparent.p_c_race___14=dfnewparent.p_c_race___14.astype(int)
dfnewparent.p_c_race___18=dfnewparent.p_c_race___18.astype(int)
dfnewparent.p_c_race___25=dfnewparent.p_c_race___25.astype(int)
dfnewparent.p_c_race___99=dfnewparent.p_c_race___99.astype(int)

dfnewparent['sumcheckedrace']=dfnewparent[['p_c_race___10','p_c_race___11','p_c_race___12','p_c_race___14','p_c_race___18','p_c_race___25']].sum(axis=1)
dfnewparent.loc[(dfnewparent.sumcheckedrace==1) & (dfnewparent.p_c_race___10==1),'racial']=5
dfnewparent.loc[(dfnewparent.sumcheckedrace==1) & (dfnewparent.p_c_race___11==1),'racial']=3
dfnewparent.loc[(dfnewparent.sumcheckedrace==1) & (dfnewparent.p_c_race___12==1),'racial']=1
dfnewparent.loc[(dfnewparent.sumcheckedrace==1) & (dfnewparent.p_c_race___14==1),'racial']=4
dfnewparent.loc[(dfnewparent.sumcheckedrace==1) & (dfnewparent.p_c_race___18==1),'racial']=2
dfnewparent.loc[(dfnewparent.sumcheckedrace==1) & (dfnewparent.p_c_race___25==1),'racial']=6
dfnewparent.loc[(dfnewparent.sumcheckedrace==0) & (dfnewparent.p_c_race___99==1),'racial']=99
dfnewparent.loc[dfnewparent.sumcheckedrace>=2,'racial']=6

dfnewparent.loc[(dfnewparent.sumcheckedrace>=2) & ~(dfnewparent.p_c_race___25==1)][['child_id']] #.to_csv('multraceneedcheck.csv',index=False) #none of these

dfnewparent['race']=dfnewparent.replace({'racial':
                                       {1:'American Indian/Alaska Native',
                                        2:'Asian',
                                        3:'Black or African American',
                                        4:'Hawaiian or Pacific Islander',
                                        5:'White',
                                        6:'More than one race',
                                        99:'Unknown or not reported'}})['racial']

#1, Hispanic or Latino | 0, Not Hispanic or Latino | 9, Unknown or not reported
dfnewparent['ethnic_group']=dfnewparent.replace({'p_c_latino':
                                           {'1':'Hispanic or Latino',
                                            '0':'Not Hispanic or Latino',
                                            '9':'Unknown or not reported'}})['p_c_latino']



dfnewparent=dfnewparent.drop(columns=['dob', 'flagged', 'gender', 'interview_date', 'p_c_latino',
       'p_c_race___10', 'p_c_race___11', 'p_c_race___12', 'p_c_race___14',
       'p_c_race___18', 'p_c_race___25', 'p_c_race___99', 'p_c_sex',
       'parent_id', 'site', 'study', 'subject', 'interview_age','sumcheckedrace', 'racial'])

#merge parents and children
dfnew=pd.merge(dfnewparent,hcdchild,left_on='child_id',right_on='subject_id',how='inner')
dfnew.site=dfnew.site.astype(str).str.strip()
dfnew['site_char']=dfnew.replace({'site':
                                      {'1': 'Harvard', '2':'UCLA', '3':'UMinn', '4':'WashU'}})[['site']]
dfnew=dfnew.loc[dfnew.flagged.isnull()==True]
dfnew=dfnew.drop(columns=['child_id','subject_id','dob','gender','site','interview_date','study'])



##hcd18 and older
list18=['sub_race','sub_latino','iihandwr', 'iihandth', 'iihandsc', 'iihandto', 'iihandkn', 'iihandsp',
        'iihandbr', 'iihandma', 'iihandbo', 'iihandfk', 'iihandey']

hcpd18=box.getredcapfields(list18,study='hcpd18')
hcpd18['nda_interview_date']=pd.to_datetime(hcpd18['interview_date']) - pd.offsets.QuarterBegin(startingMonth=1)
hcpd18=hcpd18.rename(columns={'interview_age':'nda_interview_age'})

#convert gender and race/ethnicity to strings
hcpd18.loc[hcpd18.gender=='1','genderstring']='M'
hcpd18.loc[hcpd18.gender=='2','genderstring']='F'

# 12, NATIVE AMERICAN/ALASKA NATIVE | 18, ASIAN | 11, BLACK/AFRICAN AMERICAN |
# 14, NATIVE HAWAIIAN or OTHER PACIFIC ISLANDER | 10, WHITE | 25, More than one race | 99, Unknown or not reported
hcpd18.sub_race___10=hcpd18.sub_race___10.astype(int)
hcpd18.sub_race___11=hcpd18.sub_race___11.astype(int)
hcpd18.sub_race___12=hcpd18.sub_race___12.astype(int)
hcpd18.sub_race___14=hcpd18.sub_race___14.astype(int)
hcpd18.sub_race___18=hcpd18.sub_race___18.astype(int)
hcpd18.sub_race___25=hcpd18.sub_race___25.astype(int)
hcpd18.sub_race___99=hcpd18.sub_race___99.astype(int)


hcpd18['sumcheckedrace']=hcpd18[['sub_race___10','sub_race___11','sub_race___12','sub_race___14','sub_race___18','sub_race___25']].sum(axis=1)

#HCPD18 folks can select multiple race options (checkbox)...need to convert those to 'more than one race' category of mutually exclusive version of race variable required by NDAR
hcpd18.loc[(hcpd18.sumcheckedrace==1) & (hcpd18.sub_race___10==1),'racial']=5
hcpd18.loc[(hcpd18.sumcheckedrace==1) & (hcpd18.sub_race___11==1),'racial']=3
hcpd18.loc[(hcpd18.sumcheckedrace==1) & (hcpd18.sub_race___12==1),'racial']=1
hcpd18.loc[(hcpd18.sumcheckedrace==1) & (hcpd18.sub_race___14==1),'racial']=4
hcpd18.loc[(hcpd18.sumcheckedrace==1) & (hcpd18.sub_race___18==1),'racial']=2
hcpd18.loc[(hcpd18.sumcheckedrace==1) & (hcpd18.sub_race___25==1),'racial']=6
hcpd18.loc[(hcpd18.sumcheckedrace==0) & (hcpd18.sub_race___99==1),'racial']=99
hcpd18.loc[hcpd18.sumcheckedrace>=2,'racial']=6
#hcpd18[['sub_race___10','sub_race___11','sub_race___12','sub_race___14','sub_race___18','sub_race___25','sumcheckedrace','racial']].head(20)

hcpd18.loc[hcpd18.racial==1,'race']='American Indian/Alaska Native'
hcpd18.loc[hcpd18.racial==2,'race']='Asian'
hcpd18.loc[hcpd18.racial==3,'race']='Black or African American'
hcpd18.loc[hcpd18.racial==4,'race']='Hawaiian or Pacific Islander'
hcpd18.loc[hcpd18.racial==5,'race']='White'
hcpd18.loc[hcpd18.racial==6,'race']='More than one race'
hcpd18.loc[hcpd18.racial==99,'race']='Unknown or not reported'
hcpd18.loc[hcpd18.sub_latino=='1','ethnic_group']='Hispanic or Latino'
hcpd18.loc[hcpd18.sub_latino=='0','ethnic_group']='Not Hispanic or Latino'
hcpd18.loc[hcpd18.sub_latino=='9','ethnic_group']='Unknown or not reported'
hcpd18[['sub_race___10','sub_race___11','sub_race___12','sub_race___14','sub_race___18','sub_race___25','sumcheckedrace','racial','race']].head(20)

hcpd18.site=hcpd18.site.astype(str).str.strip()
hcpd18['site_char']=hcpd18.replace({'site':
                                      {'1': 'Harvard', '2':'UCLA', '3':'UMinn', '4':'WashU'}})[['site']]

hcpd18=hcpd18.drop(columns=['dob', 'gender', 'interview_date', 'sub_latino',
       'sub_race___10', 'sub_race___11', 'sub_race___12', 'sub_race___14',
       'sub_race___18', 'sub_race___25', 'sub_race___99',
       'site', 'study', 'sumcheckedrace', 'racial'])


hcpd18=hcpd18.loc[hcpd18.flagged.isnull()==True]
hcpd18=hcpd18.drop(columns=['subject_id'])

#concatenate hcdchild and hcd18...
hcpd18.columns #18 and older dataframe
dfnew.columns #(parent child dataframe)
hcdtogether=pd.concat([hcpd18,dfnew],axis=0,sort=True)

#QC
hcpd18.groupby('race').count()
dfnew.groupby('race').count()
hcdtogether.groupby('race').count()

#pull in the ndar fields
hcpids=pd.read_csv(hcplist,header=0)[['nda_guid','subjectped','final_pedid','unrelated_subset_October2019']]
hcdids=hcpids.loc[hcpids.subjectped.str.contains('HCD')] #783 as of 9/27

hcdtogether2=pd.merge(hcdids,hcdtogether,how='inner',left_on='subjectped',right_on='subject').drop(columns='subject')


hcdtogether2=hcdtogether2.rename(columns={"subjectped":"src_subject_id","genderstring":"gender","nda_interview_age":"interview_age",
                           "nda_interview_date":"interview_date",
                           "final_pedid":"family_user_def_id","nda_guid":"subjectkey","site_char":"site"})
hcdtogether2['interview_date']=pd.to_datetime(hcdtogether2['interview_date']).dt.strftime('%m/%d/%Y')
hcdtogether2['phenotype']=pd.Series(("Healthy Subject" for i in range(hcdtogether2.shape[0])),index=hcdtogether2.index)
hcdtogether2['phenotype_description']=pd.Series(("In good health" for i in range(hcdtogether2.shape[0])),index=hcdtogether2.index)
hcdtogether2['twins_study']=pd.Series(("No" for i in range(hcdtogether2.shape[0])),index=hcdtogether2.index)
hcdtogether2['sibling_study']=pd.Series(("No" for i in range(hcdtogether2.shape[0])),index=hcdtogether2.index)
hcdtogether2['family_study']=pd.Series(("No" for i in range(hcdtogether2.shape[0])),index=hcdtogether2.index)
hcdtogether2['sample_taken']=pd.Series(("No" for i in range(hcdtogether2.shape[0])),index=hcdtogether2.index)

ndar=hcdtogether2[['src_subject_id', 'family_user_def_id', 'gender', 'subjectkey', 'race', 'ethnic_group', 'phenotype','phenotype_description','family_study','twins_study', 'sibling_study', 'sample_taken', 'interview_date', 'interview_age','site']].copy()

#write out csv for validation
filePath=pathout+'/HCPD_ndar_subject_'+snapshotdate+'.csv'

if os.path.exists(filePath):
    os.remove(filePath)
else:
    print("Can not delete the file as it doesn't exists")

with open(filePath,'a') as f:
    f.write("ndar_subject,1\n")
    ndar.to_csv(f,index=False)


#lightson
#hcdtogether2.to_csv(pathout+'/test.csv',index=False)
################################


#hand
#last minute translation to valuess now requrested by NDA 4/8/2019
#spr;right;spl;left;both;np;ne; oh
#SPR=Strongly Prefer Right; SPL=Strongly Prefer Left; ne =not experienced; np=no preference; oh=sometimes uses other hand
#for ii in range(len(varlist)):
varlist=['hand1','hand2','hand3','hand4','hand5','hand6','hand7','hand8','iihandwr','iihandth','iihandsc','iihandto',
         'iihandkn','iihandsp','iihandbr','iihandma','iihandbo','iihandfk','iihandey']

flagshand=pd.DataFrame(columns=['src_subject_id','site','reason'])
for ii in range(len(varlist)):
    varhand=pd.DataFrame(hcdtogether2.loc[hcdtogether2[varlist[ii]]==''])
    varhand['reason']=varlist[ii]+' is missing'
    varhandslim=varhand[['src_subject_id','site','reason','interview_age']]
    flagshand=flagshand.append(varhandslim,sort=False)


flagshand.loc[(flagshand.interview_age<132) & (flagshand.reason.str.contains('iihand')==False)]
flagshand.loc[(flagshand.interview_age>=132) & (flagshand.reason.str.contains('iihand')==True)]


hcdtogether2.loc[hcdtogether2.src_subject_id=='HCD1246340','hand_total']=''
#other children only hand8 missing...set score from redcap to 8 for both instead of assuming missing means left handed
# #(both dominantly righthand, i.e. 7/7 non missing answers were already Rights)

hcdtogether2.loc[hcdtogether2.src_subject_id=='HCD0906250','hand_total']='8'
hcdtogether2.loc[hcdtogether2.src_subject_id=='HCD2884377','hand_total']='8'



varlistnoeye=['iihandwr','iihandth','iihandsc','iihandto','iihandkn','iihandsp','iihandbr','iihandma','iihandbo','iihandfk']
hcdtogetherolder = hcdtogether2.loc[(hcdtogether2.interview_age >= 132) & (hcdtogether2.src_subject_id != 'HCD0696265')].copy()
#assign these older subjects missings to subject's median for score calculation, and then reset them to missing again later
hcdtogetherolder.loc[hcdtogetherolder.src_subject_id=='HCD1807051','iihandsp']='4'
hcdtogetherolder.loc[hcdtogetherolder.src_subject_id=='HCD0036324','iihandfk']='5'
hcdtogetherolder.loc[hcdtogetherolder.src_subject_id=='HCD2415140','iihandfk']='4'
hcdtogetherolder.loc[hcdtogetherolder.src_subject_id=='HCD1502635','iihandto']='4'

#hcdtogetherolder.to_csv(pathout+'/test2.csv',index=False)
for var in varlistnoeye:
    hcdtogetherolder[var]=hcdtogetherolder[var].astype(int)

hcdtogetherolder['handvarsum']=hcdtogetherolder[varlistnoeye].sum(axis=1)
hcdtogetherolder.loc[:,'handvarconstant']=pd.Series((3*len(varlistnoeye) for i in range(hcdtogetherolder.shape[0])),index=hcdtogetherolder.index)
hcdtogetherolder['HCP Handedness Score']=5*(hcdtogetherolder['handvarsum']-hcdtogetherolder['handvarconstant'])
#hcdtogetherolder.to_csv(pathout+'/test3.csv',index=False)
hcdtogetherolder=hcdtogetherolder[['src_subject_id','HCP Handedness Score']]
hcdtogether2=pd.merge(hcdtogether2,hcdtogetherolder,on='src_subject_id',how='left')
hcdtogether2.loc[hcdtogether2.interview_age>=132,'hand_total']=''
#lightson
#hcdtogether2.to_csv(pathout+'/test4.csv',index=False)




hand=hcdtogether2[['src_subject_id','gender', 'subjectkey', 'interview_date', 'interview_age','hand1','hand2','hand3','hand4','hand5','hand6','hand7','hand8','hand_total','iihandwr','iihandth','iihandsc',
            'iihandto', 'iihandkn','iihandsp', 'iihandbr', 'iihandma','iihandbo','iihandfk','iihandey','HCP Handedness Score']].copy()

iihandlist=['iihandwr','iihandth','iihandsc','iihandto', 'iihandkn','iihandsp', 'iihandbr', 'iihandma','iihandbo','iihandfk','iihandey']
newhandlist=['newiihandwr','newiihandth','newiihandsc','newiihandto', 'newiihandkn','newiihandsp', 'newiihandbr', 'newiihandma','newiihandbo','newiihandfk','newiihandey']
for ii in range(len(iihandlist)):
    hand[newhandlist[ii]]=hand.replace({iihandlist[ii]:
                                       {'1':'left',
                                        '2':'spl',
                                        '3':'np',
                                        '4':'spr',
                                        '5':'right'}})[iihandlist[ii]]


handlist=['hand1','hand2','hand3','hand4','hand5','hand6','hand7','hand8']
newlist=['newhand1','newhand2','newhand3','newhand4','newhand5','newhand6','newhand7','newhand8']
for ii in range(len(handlist)):
    hand[newlist[ii]]=hand.replace({handlist[ii]:
                                       {'1':'right',
                                        '0':'left'}})[handlist[ii]]
#lightson
#hand.to_csv(pathout+'/test5.csv',index=False)

hand.loc[(hand.newhand1.str.strip()=='') | (hand.newhand1.isnull()==True),'newhand1']=hand.newiihandwr
hand.loc[(hand.newhand3.str.strip()=='') | (hand.newhand3.isnull()==True),'newhand3']=hand.newiihandth
hand.loc[(hand.newhand7.str.strip()=='') | (hand.newhand7.isnull()==True),'newhand7']=hand.newiihandsc
hand.loc[(hand.newhand4.str.strip()=='') | (hand.newhand4.isnull()==True),'newhand4']=hand.newiihandto
hand.loc[(hand.newhand6.str.strip()=='') | (hand.newhand6.isnull()==True),'newhand6']=hand.newiihandsp


#hand.to_csv(pathout+'/test7.csv',index=False)




#drop the hands (0s and 1s) and reame the newhands (lefts and rights) back to hands
#drop all the iihandlist variables (values 1::5)
#drop newiihandwr, newiihandth, newiihandsc, newiihandto, newiihandsp (values left,spl,np,spr,right) which are are now merged with hand1,hand3,hand7,hand4,hand6, respectively
#rename the remaining  newiihandkn, newiihandbr,newiihandma,newiihandbo,newiihandfk,newiihandey back to iihand counterparts

hand=hand.drop(columns=iihandlist+handlist+['newiihandwr', 'newiihandth', 'newiihandsc', 'newiihandto', 'newiihandsp'],axis=1)
dictnames=dict(zip(newlist,handlist))
hand=hand.rename(columns=dictnames)
hand=hand.rename(columns={'newiihandkn':'iihandkn','newiihandbr':'iihandbr','newiihandma':'iihandma','newiihandbo':'iihandbo','newiihandfk':'iihandfk','newiihandey':'iihandey'})

#4/19/2019 change again to avoid confusion of aliases:
#rename the collection to original ndar data dictionary elements-note hand5 is a new element...not renamed
hand=hand.rename(columns={'hand1':'writing','hand2':'hammer','hand3':'throwing','hand4':'toothbrush','hand6':'spoon','hand7':'scissors','hand8':'hand_15_drink',
'iihandkn':'knife_no_fork','iihandbr':'broom','iihandma':'match','iihandbo':'box','iihandfk':'foot','iihandey':'eye'})


filePath=pathout+'/HCPD_edinburgh_hand_'+snapshotdate+'.csv'
if os.path.exists(filePath):
    os.remove(filePath)
else:
    print("Can not delete the file as it doesn't exists")

with open(filePath,'a') as f:
    f.write("edinburgh_hand,1\n")
    hand.to_csv(f,index=False)

