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

#new unrelateds program
#take curated pedigree (curated to best knowledge), subset to those in HCA or HCD stg...
#merge with the list of unrelateds already selected last time around.
#fill in the blanks.

#latest pedigree
dev_peds_path='/home/petra/UbWinSharedSpace1/redcap2nda_Lifespan2019/Dev_pedigrees/'
pedsfile='redcapandexcelpeds09_23_2019.csv'
peds=pd.read_csv(dev_peds_path+pedsfile)
peds=peds[['subjectped','nda_guid','HCD_ID','HCA_ID','final_pedid']]


#nda vars from staging
hcalist='/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/ndafields2intradb/CCF_HCA_STG_23Sept2019.csv'
hcaids=pd.read_csv(hcalist,header=0)
hcaids=hcaids[['Subject','nda_guid','nda_interview_age','nda_interview_date','nda_gender']]

hcdlist='/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/ndafields2intradb/CCF_HCD_STG_23Sept2019.csv'
hcdids=pd.read_csv(hcdlist,header=0)
hcdids=hcdids[['Subject','nda_guid','nda_interview_age','nda_interview_date','nda_gender']]

HCDHCAlist=pd.concat([hcaids,hcdids],axis=0,sort=True)

#merge pedigree and staging info
hcpandpeds=pd.merge(HCDHCAlist,peds,left_on='Subject',right_on='subjectped',how='inner')
#check
#hcpandpeds.loc[(hcpandpeds.nda_guid_y.isnull()==False) & (hcpandpeds.nda_guid_x == hcpandpeds.nda_guid_y)]
#keep the vars from staging
hcpandpeds=hcpandpeds.drop(columns=['nda_guid_y','Subject'])
hcpandpeds=hcpandpeds.rename(columns={'nda_guid_x':'nda_guid'})



#old unrelateds var to update
unrelateds='UnrelatedHCAHCD_w_STG_Image_and_pseudo_GUID03_25_2019_slim4share.csv'
unrels=pd.read_csv(dev_peds_path+unrelateds)
unrels=unrels[['HCA_ID','HCD_ID','final_ped','nda_guid','subject','unrelated_subset','orig_list_Sandy']].rename(columns={'final_ped':'final_ped_old'})


hcpandpedunrel=pd.merge(hcpandpeds,unrels,left_on='subjectped',right_on='subject',how='left')
#check # three relationships change because discovered that HCA is also HCD
hcpandpedunrel.loc[(hcpandpedunrel.HCD_ID_x.isnull()==False) & (hcpandpedunrel.HCD_ID_x != hcpandpedunrel.HCD_ID_y)][['HCD_ID_x','HCD_ID_y','HCA_ID_x','HCA_ID_y']]
hcpandpedunrel.loc[(hcpandpedunrel.HCD_ID_y.isnull()==False) & (hcpandpedunrel.HCD_ID_x.isnull()==False) & (hcpandpedunrel.HCD_ID_x != hcpandpedunrel.HCD_ID_y)][['HCD_ID_x','HCD_ID_y','HCA_ID_x','HCA_ID_y']]
hcpandpedunrel.loc[(hcpandpedunrel.nda_guid_y.isnull()==False) & (hcpandpedunrel.nda_guid_x != hcpandpedunrel.nda_guid_y)][['subjectped','HCA_ID_x','HCD_ID_x','nda_guid_x','nda_guid_y']]
hcpandpedunrel=hcpandpedunrel.loc[~((hcpandpedunrel.nda_guid_y.isnull()==False) & (hcpandpedunrel.nda_guid_x != hcpandpedunrel.nda_guid_y))]


hcpandpedunrel=hcpandpedunrel.drop(columns=['HCA_ID_y','HCD_ID_y','nda_guid_y','subject','final_ped_old']).rename(
    columns={'nda_guid_x':'nda_guid','HCD_ID_x':'HCD_ID','HCA_ID_x':'HCA_ID'})
hcpandpedunrel['new_unrelated_subset']=hcpandpedunrel.unrelated_subset
hcpandpedunrel.loc[hcpandpedunrel.subjectped=='HCD2939275','new_unrelated_subset']='no'
hcpandpedunrel.loc[hcpandpedunrel.subjectped=='HCA7351165','new_unrelated_subset']='no'
changelist=['HCD2939275','HCA7351165','HCD0906250','HCD2692671']
#check
hcpandpedunrel.loc[hcpandpedunrel.subjectped.isin(changelist)][['subjectped','unrelated_subset','new_unrelated_subset']]
#now assign the new_unrelated_subset where missing
counts=hcpandpedunrel.groupby('final_pedid').count()
counts=counts.reset_index()[['final_pedid','nda_guid']].rename(columns={'nda_guid':'pedsize'})
hcpandpedunrelsizes=pd.merge(hcpandpedunrel,counts,how='left',on='final_pedid')
hcpandpedunrelsizes.loc[(hcpandpedunrelsizes.new_unrelated_subset.isnull()==True) & (hcpandpedunrelsizes.pedsize==1),'new_unrelated_subset']='yes'

#lightson - hcpandpedunrelsizes.to_csv(dev_peds_path+'testout2.csv')

#find famids of those fams who already have someone selected (conversly
#find fams that need to have person selected)
selected=hcpandpedunrelsizes.loc[hcpandpedunrelsizes.new_unrelated_subset=='yes'].drop_duplicates(subset='final_pedid',keep='last')[['final_pedid']]
selected['already_selected']=1
#set rest of family to 'no'
hcpandpedunrelsizes2=pd.merge(hcpandpedunrelsizes,selected,on='final_pedid',how='left')
#lightson - hcpandpedunrelsizes2.to_csv(dev_peds_path+'testout3.csv')





#of 128 pedigrees with size 2 or more only handful are actual new pedigrees
piece1=hcpandpedunrelsizes2.loc[hcpandpedunrelsizes2.already_selected.isnull()==False]
subset1=hcpandpedunrelsizes2.loc[hcpandpedunrelsizes2.already_selected.isnull()==True].copy()
subset1=subset1.loc[~(subset1.subjectped=='HCD0906250')]  #this one is special case already set to NO (all others were missing)
piece2=hcpandpedunrelsizes2.loc[hcpandpedunrelsizes2.subjectped=='HCD0906250']
np.random.seed(0)
subset1['rand']=np.random.ranf(subset1.shape[0])
subset1=subset1.sort_values(by=['final_pedid','rand']).copy()

selectednew=subset1.drop_duplicates(subset='final_pedid',keep='last')[['subjectped','new_unrelated_subset']]
selectednew['new_unrelated_subset']='yes'
subset1=pd.merge(subset1.drop(columns='new_unrelated_subset'),selectednew,how='left',on='subjectped')
subset1.loc[subset1.new_unrelated_subset.isnull()==True,'new_unrelated_subset']='no'

#put them together

pedsimage3=pd.concat([piece1,piece2,subset1],axis=0)
#anything still not filled is because famid already had a selected member
pedsimage3.loc[(pedsimage3.new_unrelated_subset.isnull()==True) & (pedsimage3.already_selected==1.0),'new_unrelated_subset']='yes'
pedsimage3=pedsimage3.drop(columns=['rand'])


#lightson - pedsimage3.to_csv(dev_peds_path+'testout4.csv')
pedsimage3.groupby('new_unrelated_subset').count()
#remove the young HCAs
pedsimage3=pedsimage3.loc[~(pedsimage3.subjectped.isin(['HCA6780789','HCA6878504','HCA7025859']))]
pedsimage3=pedsimage3.drop(columns='already_selected').rename(columns={'unrelated_subset':'unrelated_subset_April2019','new_unrelated_subset':'unrelated_subset_October2019'})


#lightson - pedsimage3.to_csv(dev_peds_path+'testout4.csv')

fileout='UnrelatedHCAHCD_w_STG_Image_and_pseudo_GUID09_25_2019.csv'
pedsimage3.to_csv(dev_peds_path+fileout,index=False)



