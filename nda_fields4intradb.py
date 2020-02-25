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

redcapconfigfile="/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/.boxApp/redcapconfig.csv"

from download.box import LifespanBox
box_temp='/home/petra/UbWinSharedSpace1/boxtemp' #location of local copy of curated data
box = LifespanBox(cache=box_temp)

verbose = True
snapshotdate = datetime.datetime.today().strftime('%m_%d_%Y')
#catfromdate=max of last run--'2019-06-17'

hcalist='/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/ndafields2intradb/CCF_HCA_STG_6Sept2019.csv'
hcdlist='/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/ndafields2intradb/CCF_HCD_STG_23Sept2019.csv'

fieldlist=[]
hca=box.getredcapfields(['racial'], study='hcpa')
hca=box.getredcapfields(fieldlist,study='hcpa')
hca.loc[hca.gender.astype(int)==1,'nda_gender']='M'
hca.loc[hca.gender.astype(int)==2,'nda_gender']='F'
hca.loc[:,'nda_interview_date']=pd.to_datetime(hca['interview_date']) -pd.offsets.QuarterBegin(startingMonth=1)
hca=hca.rename(columns={'interview_age':'nda_interview_age'})
hca=hca.loc[hca.flagged.isnull()==True] #these already subset to v1 by redcap event pull
hcaids=pd.read_csv(hcalist,header=0)
hcaidsupdate=pd.merge(hcaids,hca,left_on='Subject',right_on='subject',how='left').drop(columns=['YOB','Hand','flagged','gender','interview_date'])

#check values of overlap
hcaidsupdate.loc[(hcaidsupdate.nda_gender_x.isnull()==False) & (hcaidsupdate.nda_gender_x != hcaidsupdate.nda_gender_y)][['nda_gender_x','nda_gender_y']]
#all genders the same...
hcaidsupdate.loc[(hcaidsupdate.nda_interview_age_x.isnull()==False) & (hcaidsupdate.nda_interview_age_x != hcaidsupdate.nda_interview_age_y)][['subject','nda_interview_age_x','nda_interview_age_y']]
#there are a couple of differences in the ages here...this is because the visit for the session came a few days later than the interview dayt in redcap.  Occasionally
#the days bridged a quarter cutoff.
hcaidsupdate.loc[(hcaidsupdate.nda_interview_date_x.isnull()==False) & (pd.to_datetime(hcaidsupdate.nda_interview_date_x) != hcaidsupdate.nda_interview_date_y)][['subject','nda_interview_date_x','nda_interview_date_y']]
#again there are a handful of differences in the interview dates here...
#dont overwrite the values already there...only create ones for those that dont have them
hcaidsupdate=hcaidsupdate.loc[hcaidsupdate.nda_gender_x.isnull()==True]
hcaidsupdate=hcaidsupdate.drop(columns=['nda_gender_x','nda_interview_age_x','nda_interview_date_x']).rename(
    columns={'nda_gender_y':'nda_gender','nda_interview_age_y':'nda_interview_age','nda_interview_date_y':'nda_interview_date'})

###make sure that HCA8828599_V1_MR isn't in the curl statement before running


#now prep the curl statement
f= open("/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/ndafields2intradb/curlhca.sh","w+")
for index,row in hcaidsupdate.iterrows():
   #put the ages
   f.write("curl -s -k --cookie JSESSIONID=$JSESSIONID 'https://hcpi-shadow20.nrg.wustl.edu/data/projects/CCF_HCA_STG/subjects/")
   f.write(row.Subject)
   f.write("/experiments/")
   f.write(row['MR ID'])
   f.write("?xsiType=xnat:mrSessionData&xnat:mrSessionData/fields/field%5Bname%3Dnda_interview_age%5D/field=")
   f.write(str(int(round(row.nda_interview_age))))
   f.write("' -X PUT")
   f.write("\n")
   #put the visit dates
   f.write("curl -s -k --cookie JSESSIONID=$JSESSIONID 'https://hcpi-shadow20.nrg.wustl.edu/data/projects/CCF_HCA_STG/subjects/")
   f.write(row.Subject)
   f.write("/experiments/")
   f.write(row['MR ID'])
   f.write("?xsiType=xnat:mrSessionData&xnat:mrSessionData/fields/field%5Bname%3Dnda_interview_date%5D/field=")
   f.write(row.nda_interview_date.strftime('%m/%d/%Y'))
   f.write("' -X PUT")
   f.write("\n")
   #put the genders - what is syntax for higher level?
   f.write("curl -s -k --cookie JSESSIONID=$JSESSIONID 'https://hcpi-shadow20.nrg.wustl.edu/data/projects/CCF_HCA_STG/subjects/")
   f.write(row.Subject)
   f.write("?xsiType=xnat:subjectData&xnat:subjectData/fields/field%5Bname%3Dnda_gender%5D/field=")
   f.write(row.nda_gender)
   f.write("' -X PUT")
   f.write("\n")

f.close()

#now hcd
hcdchild=box.getredcapfields(fieldlist, study='hcpdchild')
hcdchild=hcdchild.loc[hcdchild.flagged.isnull()==True]

hcdparent=box.getredcapfields(['p_c_sex','p_c_race','child_id'], study='hcpdparent')
parentchild=pd.merge(hcdchild,hcdparent,how='left',left_on='subject_id',right_on='child_id')
#check for redcap discrepancies
parentchild.loc[parentchild.gender_x!=parentchild.gender_y][['subject_id','gender_x','gender_y']] #one

#one here
flag1=parentchild.loc[parentchild.gender_x!=parentchild.p_c_sex][['site_x','subject_id','interview_date_x','gender_x','p_c_sex']] #one
flag2=parentchild.loc[parentchild.dob_x!=parentchild.dob_y][['site_x','subject_id','interview_date_x','dob_x','dob_y']] #one
flag3=parentchild.loc[parentchild.interview_date_x!=parentchild.interview_date_y][['site_x','subject_id','interview_date_x','interview_date_y']] #some
flag1['reason']='child gender not equal parent report of sex at birth'
flag2['reason']='child dob not equal parent report of dob'
flag3['reason']='interview date in parent database not the same as that reported in child database'
flags=pd.concat([flag1,flag2,flag3],axis=0)
flags.to_csv(box_temp+'/Flags4Intradb.csv',index=False)

#assuming flags have been resolved, the following dataset should be empty
parentchild.loc[parentchild.nb_months.apply(np.floor) !=parentchild.interview_age][['site_x','subject_id','interview_date_x','nb_months','interview_age']] #some

#now prep for merge
parentchildclean=parentchild[['flagged_x','gender_x', 'interview_date_x', 'site_x','study_x','subject_id', 'interview_age']]
parentchildclean=parentchildclean.rename(columns={'flagged_x':'flagged','gender_x':'gender','interview_date_x':'interview_date','site_x':'site','study_x':'study'})

#merge with the hcd18 subjects, and then check for intradb discrepancies
hcpd18=box.getredcapfields(['sub_race'], study='hcpd18')
hcpd18=hcpd18.loc[hcpd18.flagged.isnull()==True]
hcpd18=hcpd18[['flagged','gender', 'interview_date', 'site', 'study', 'subject_id','interview_age']]

hcpd=pd.concat([parentchildclean,hcpd18],axis=0,sort=True)
hcpd.loc[hcpd.gender.astype(int)==1,'nda_gender']='M'
hcpd.loc[hcpd.gender.astype(int)==2,'nda_gender']='F'
hcpd.loc[:,'nda_interview_date']=pd.to_datetime(hcpd['interview_date']) -pd.offsets.QuarterBegin(startingMonth=1)
hcpd=hcpd.rename(columns={'interview_age':'nda_interview_age'})
#flagged ones already dropped but for consistency...
hcpd=hcpd.loc[hcpd.flagged.isnull()==True]

#check genders for
hcdids=pd.read_csv(hcdlist,header=0)
hcdids=hcdids.drop(columns=['YOB','Hand','Handedness','Gender','M/F','Date of Birth','Race','Ethnicity','Date','Scanner','Scans'])
hcdidsupdate=pd.merge(hcdids,hcpd,left_on='Subject',right_on='subject_id',how='left').drop(columns=['flagged','gender','interview_date'])

#check values of overlap
hcdidsupdate.loc[(hcdidsupdate.nda_gender_x.isnull()==False) & (hcdidsupdate.nda_gender_x != hcdidsupdate.nda_gender_y)][['nda_gender_x','nda_gender_y']]
#all genders the same...
hcdidsupdate.loc[(hcdidsupdate.nda_interview_age_x.isnull()==False) & (hcdidsupdate.nda_interview_age_x != hcdidsupdate.nda_interview_age_y)][['subject_id','nda_interview_age_x','nda_interview_age_y']]
#there are a couple of differences in the ages here...this is because the visit for the session came a few days later than the interview dayt in redcap.
# Occasionally the days bridged a quarter cutoff, too which affects rounding of date quarters.
hcdidsupdate.loc[(hcdidsupdate.nda_interview_date_x.isnull()==False) & (pd.to_datetime(hcdidsupdate.nda_interview_date_x) != hcdidsupdate.nda_interview_date_y)][['subject_id','nda_interview_date_x','nda_interview_date_y']]
#again there are a handful of differences in the interview dates here...
#dont overwrite the values already there...only create ones for those that dont have them

hcdidsupdate=hcdidsupdate.loc[hcdidsupdate.nda_gender_x.isnull()==True]

#if the intersection of flags and hcdisupdate is empty, then can proceed with intradb update...as of 9/10/2019, waiting for UCLA
pd.merge(flags.loc[flags.subject_id!='HCDJanePractice']['subject_id'],hcdidsupdate,how='inner',on='subject_id')


#now prep the curl statement
f= open("/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/ndafields2intradb/curlhcd.sh","w+")
for index,row in hcdidsupdate.iterrows():
   #put the ages
   f.write("curl -s -k --cookie JSESSIONID=$JSESSIONID 'https://hcpi-shadow20.nrg.wustl.edu/data/projects/CCF_HCD_STG/subjects/")
   f.write(row.Subject)
   f.write("/experiments/")
   f.write(row['MR ID'])
   f.write("?xsiType=xnat:mrSessionData&xnat:mrSessionData/fields/field%5Bname%3Dnda_interview_age%5D/field=")
   f.write(str(int(round(row.nda_interview_age))))
   f.write("' -X PUT")
   f.write("\n")
   #put the visit dates
   f.write("curl -s -k --cookie JSESSIONID=$JSESSIONID 'https://hcpi-shadow20.nrg.wustl.edu/data/projects/CCF_HCD_STG/subjects/")
   f.write(row.Subject)
   f.write("/experiments/")
   f.write(row['MR ID'])
   f.write("?xsiType=xnat:mrSessionData&xnat:mrSessionData/fields/field%5Bname%3Dnda_interview_date%5D/field=")
   f.write(row.nda_interview_date.strftime('%m/%d/%Y'))
   f.write("' -X PUT")
   f.write("\n")
   #put the genders - what is syntax for higher level?
   f.write("curl -s -k --cookie JSESSIONID=$JSESSIONID 'https://hcpi-shadow20.nrg.wustl.edu/data/projects/CCF_HCD_STG/subjects/")
   f.write(row.Subject)
   f.write("?xsiType=xnat:subjectData&xnat:subjectData/fields/field%5Bname%3Dnda_gender%5D/field=")
   f.write(row.nda_gender)
   f.write("' -X PUT")
   f.write("\n")

f.close()

