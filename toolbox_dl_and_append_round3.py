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

from download.box import LifespanBox

verbose = True
snapshotdate = datetime.datetime.today().strftime('%m_%d_%Y')
box_temp='/home/petra/UbWinSharedSpace1/boxtemp' #location of local copy of curated data
box = LifespanBox(cache=box_temp)
redcapconfigfile="/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/.boxApp/redcapconfig.csv"

#grab stuff from corrected and curated
#get list of filenames
##########################

#folderlistlabels=['WashU_HCAorBoth','WashU_HCD', 'UCLA_HCAorBoth','UCLA_HCD',  'UMN_HCAorBoth','UMN_HCD',   'MGH_HCAorBoth','Harvard_HCD']
#folderlistnums=  [82804729845,       82804015457,82807223120,      82805124019, 82803665867,    82805151056,82761770877,     82803734267]


#Harvard
Harv=82803734267
Harvattn=96013516511

MGH2=82761770877
MGHattn=96148925420

WashUD=82804015457
WashUDattn=96147128675

WashUA=82804729845
WashUAattn=96149947498

UMNA=82803665867
UMNAattn=96153923311

UMND=82805151056
UMNDattn=96155708581

UCLAA=82807223120
UCLAAattn=96154919803

UCLAD=82805124019
UCLADattn=96162759127


harvcleandata, harvcleanscore=curatedandcorrected(Harv,Harvattn)
mghcleandata, mghcleanscore=curatedandcorrected(MGH2,MGHattn)
washudcleandata,washudcleanscore=curatedandcorrected(WashUD,WashUDattn)
washuacleandata,washuacleanscore=curatedandcorrected(WashUA,WashUAattn)
umnacleandata,umnacleanscore=curatedandcorrected(UMNA,UMNAattn)
umndcleandata,umndcleanscore=curatedandcorrected(UMND,UMNDattn)
uclaacleandata,uclaacleanscore=curatedandcorrected(UCLAA,UCLAAattn)
ucladcleandata,ucladcleanscore=curatedandcorrected(UCLAD,UCLADattn)


###stopped here

harvcleandata.to_csv(box_temp+'/Harvard_HCDonly_Toolbox_Raw_Combined_'+snapshotdate+'.csv')
#box.update_file(497579203898,box_temp+'/Harvard_HCDonly_Toolbox_Raw_Combined_'+snapshotdate+'.csv')
harvcleanscore.to_csv(box_temp+'/Harvard_HCDonly_Toolbox_Scored_Combined_'+snapshotdate+'.csv')
#box.update_file(497530866864,box_temp+'/Harvard_HCDonly_Toolbox_Scored_Combined_'+snapshotdate+'.csv')

mghcleandata.to_csv(box_temp+'/MGH_HCAorBoth_Toolbox_Raw_Combined_'+snapshotdate+'.csv')
mghcleanscore.to_csv(box_temp+'/MGH_HCAorBoth_Toolbox_Scored_Combined_'+snapshotdate+'.csv')
#update box files by hand

washudcleandata.to_csv(box_temp+'/WashU_HCDonly_Toolbox_Raw_Combined_'+snapshotdate+'.csv')
washudcleanscore.to_csv(box_temp+'/WashU_HCDonly_Toolbox_Scored_Combined_'+snapshotdate+'.csv')

washuacleandata.to_csv(box_temp+'/WashU_HCAorBoth_Toolbox_Raw_Combined_'+snapshotdate+'.csv')
washuacleanscore.to_csv(box_temp+'/WashU_HCAorBoth_Toolbox_Scored_Combined_'+snapshotdate+'.csv')

umnacleandata.to_csv(box_temp+'/UMN_HCAorBoth_Toolbox_Raw_Combined_'+snapshotdate+'.csv')
umnacleanscore.to_csv(box_temp+'/UMN_HCAorBoth_Toolbox_Scored_Combined_'+snapshotdate+'.csv')

umndcleandata.to_csv(box_temp+'/UMN_HCDonly_Toolbox_Raw_Combined_'+snapshotdate+'.csv')
umndcleanscore.to_csv(box_temp+'/UMN_HCDonly_Toolbox_Scored_Combined_'+snapshotdate+'.csv')

uclaacleandata.to_csv(box_temp+'/UCLA_HCAorBoth_Toolbox_Raw_Combined_'+snapshotdate+'.csv')
uclaacleanscore.to_csv(box_temp+'/UCLA_HCAorBoth_Toolbox_Scored_Combined_'+snapshotdate+'.csv')

ucladcleandata.to_csv(box_temp+'/UCLA_HCDonly_Toolbox_Raw_Combined_'+snapshotdate+'.csv')
ucladcleanscore.to_csv(box_temp+'/UCLA_HCDonly_Toolbox_Scored_Combined_'+snapshotdate+'.csv')


#concatenate cleandata for snapshotdate - putting read_csv here in case not loaded into memory
harvcleandata=pd.read_csv(box_temp+'/Harvard_HCDonly_Toolbox_Raw_Combined_12_12_2019.csv',header=0,low_memory=False)
mghcleandata=pd.read_csv(box_temp+'/MGH_HCAorBoth_Toolbox_Raw_Combined_12_12_2019.csv',header=0,low_memory=False)
washudcleandata=pd.read_csv(box_temp+'/WashU_HCDonly_Toolbox_Raw_Combined_12_12_2019.csv',header=0,low_memory=False)
washuacleandata=pd.read_csv(box_temp+'/WashU_HCAorBoth_Toolbox_Raw_Combined_12_12_2019.csv',header=0,low_memory=False)
umnacleandata=pd.read_csv(box_temp+'/UMN_HCAorBoth_Toolbox_Raw_Combined_12_12_2019.csv',header=0,low_memory=False)
umndcleandata=pd.read_csv(box_temp+'/UMN_HCDonly_Toolbox_Raw_Combined_12_12_2019.csv',header=0,low_memory=False)
uclaacleandata=pd.read_csv(box_temp+'/UCLA_HCAorBoth_Toolbox_Raw_Combined_12_12_2019.csv',header=0,low_memory=False)
ucladcleandata=pd.read_csv(box_temp+'/UCLA_HCDonly_Toolbox_Raw_Combined_12_12_2019.csv',header=0,low_memory=False)

allrawdataHCAorBoth=pd.concat([mghcleandata,washuacleandata,umnacleandata,uclaacleandata],axis=0)
allrawdataHCD=pd.concat([harvcleandata,washudcleandata,umndcleandata,ucladcleandata],axis=0)

harvcleanscore=pd.read_csv(box_temp+'/Harvard_HCDonly_Toolbox_Scored_Combined_12_12_2019.csv',header=0,low_memory=False)
mghcleanscore=pd.read_csv(box_temp+'/MGH_HCAorBoth_Toolbox_Scored_Combined_12_12_2019.csv',header=0,low_memory=False)
washudcleanscore=pd.read_csv(box_temp+'/WashU_HCDonly_Toolbox_Scored_Combined_12_12_2019.csv',header=0,low_memory=False)
washuacleanscore=pd.read_csv(box_temp+'/WashU_HCAorBoth_Toolbox_Scored_Combined_12_12_2019.csv',header=0,low_memory=False)
umnacleanscore=pd.read_csv(box_temp+'/UMN_HCAorBoth_Toolbox_Scored_Combined_12_12_2019.csv',header=0,low_memory=False)
umndcleanscore=pd.read_csv(box_temp+'/UMN_HCDonly_Toolbox_Scored_Combined_12_12_2019.csv',header=0,low_memory=False)
uclaacleanscore=pd.read_csv(box_temp+'/UCLA_HCAorBoth_Toolbox_Scored_Combined_12_12_2019.csv',header=0,low_memory=False)
ucladcleanscore=pd.read_csv(box_temp+'/UCLA_HCDonly_Toolbox_Scored_Combined_12_12_2019.csv',header=0,low_memory=False)

allscoresHCAorBoth=pd.concat([mghcleanscore,washuacleanscore,umnacleanscore,uclaacleanscore],axis=0)
allscoresHCD=pd.concat([harvcleanscore,washudcleanscore,umndcleanscore,ucladcleanscore],axis=0)


#make csv
allrawdataHCAorBoth.to_csv(box_temp+'/HCAorBoth_Toolbox_Raw_Combined_'+snapshotdate+'.csv')
allrawdataHCD.to_csv(box_temp+'/HCD_Toolbox_Raw_Combined_'+snapshotdate+'.csv')
allscoresHCAorBoth.to_csv(box_temp+'/HCAorBoth_Toolbox_Scored_Combined_'+snapshotdate+'.csv')
allscoresHCD.to_csv(box_temp+'/HCD_Toolbox_Scored_Combined_'+snapshotdate+'.csv')


def curatedandcorrected(curatedfolderid,needsattnfolder):
    harvardfiles, harvardfolders=foldercontents(curatedfolderid)
    #dont grab files that need attention
    harvardfolders=harvardfolders.loc[~(harvardfolders.foldername.str.contains('needs_attention'))]
    harvardfiles2, harvardfolders2=folderlistcontents(harvardfolders.foldername,harvardfolders.folder_id)
    harvardfiles=pd.concat([harvardfiles,harvardfiles2],axis=0,sort=True)

    data4process=harvardfiles.loc[~(harvardfiles.filename.str.upper().str.contains('SCORE')==True)]
    scores4process=harvardfiles.loc[harvardfiles.filename.str.upper().str.contains('SCORE')==True]
    box.download_files(data4process.file_id)
    box.download_files(scores4process.file_id)

    #trick the catcontents macro to create catable dataset, but dont actually cat until you remove the
    #PINS in the corrected file from the curated file
    #step1 - separate data4process/scores4process into corrected and old curated data
    cdata=data4process.loc[data4process.filename.str.contains('corrected')]
    cscores=scores4process.loc[scores4process.filename.str.contains('corrected')]
    olddata=data4process.loc[~(data4process.filename.str.contains('corrected'))]
    oldscores=scores4process.loc[~(scores4process.filename.str.contains('corrected'))]
    #create catable dataset for corrected data
    hdatainitcorr=catcontents(cdata,box_temp)
    hscoreinitcorr=catcontents(cscores,box_temp)
    #get list of ids in this corrected data  #60 for Harvard
    corrl=findpairs(hdatainitcorr,hscoreinitcorr) #this is the list of ids in both scored and raw corrected data

    #create catable dataset for old curated data
    hdatainitold=catcontents(olddata,box_temp)
    hscoreinitold=catcontents(oldscores,box_temp)
    #remove the data with PINS from corrected
    hdatainitoldsub=hdatainitold[~(hdatainitold.PIN.isin(corrl))]
    hscoreinitoldsub=hscoreinitold[~(hscoreinitold.PIN.isin(corrl))]

    #now cat the two datasets together
    hdatainit=pd.concat([hdatainitcorr,hdatainitoldsub],axis=0,sort=True) #these have 60 more unique pins than before...good
    hscoreinit=pd.concat([hscoreinitcorr,hscoreinitoldsub],axis=0,sort=True) #these have 60 more than before...good

    l=findpairs(hdatainit,hscoreinit) #this is the list of ids in both scored and raw data
    #set aside those who arebnt in both and those that are in dlist or slist
    notbothdatalist=hdatainit[~(hdatainit.PIN.isin(l))]
    notbothscorelist=hscoreinit[~(hscoreinit.PIN.isin(l))]
    nbs=list(notbothscorelist.PIN.unique())
    nbd=list(notbothdatalist.PIN.unique())

    hdatainit2=hdatainit[hdatainit.PIN.isin(l)]
    hscoreinit2=hscoreinit[hscoreinit.PIN.isin(l)]
    #check that this is same as above -- it is
    #hdatainit2qc=hdatainit[~(hdatainit.PIN.isin(nbs+nbd))]
    #hscoreinit2qc=hscoreinit[~(hscoreinit.PIN.isin(nbs+nbd))]

    #find instrument duplications that are not identical
    dlist,slist=findwierdos(hdatainit2,hscoreinit2)
    dslist=pd.concat([dlist,slist],axis=0)
    wierdlist=list(dslist.PIN.unique())
    #set aside those who are in the wierdlist
    nonidenticaldupdata=hdatainit2.loc[hdatainit2.PIN.isin(wierdlist)]
    nonidenticaldupscore=hscoreinit2.loc[hscoreinit2.PIN.isin(wierdlist)]
    wierdd=list(dlist.PIN.unique())
    wierds=list(slist.PIN.unique())
    #so we have the notinboth lists and the wierdlists
    #Already set aside the notinbothlists
    #if we exclude any wierdlist PINs from both, this should get rid of everything that isnt one-to-one
    hdatainit3=hdatainit2.loc[~(hdatainit2.PIN.isin(wierdlist))]
    hscoreinit3=hscoreinit2.loc[~(hscoreinit2.PIN.isin(wierdlist))]
    #both have 580 unique ids - make them into a list
    l3=findpairs(hdatainit3,hscoreinit3) #this is the list of ids in both scored and raw data

    dlist,slist=findwierdos(hdatainit3,hscoreinit3)
    #now delete any identical duplicates check for issues finding wierdos
    if dlist.empty and slist.empty:
        hdatainit3=hdatainit3.drop_duplicates(subset={'PIN','Inst','ItemID','Position'},keep='first')
        hscoreinit3=hscoreinit3.drop_duplicates(subset={'PIN','Inst'})
    else:
        print('Found Non-Identical Duplications')
        print(dlist)
        print(slist)

    #export scores and data for all pins in dslist or nbs or nbd with flags
    notbothdatalist.to_csv(box_temp+'/Toolbox_notinboth_Data_'+snapshotdate+'.csv')
    notbothscorelist.to_csv(box_temp+'/Toolbox_notinboth_Scores_'+snapshotdate+'.csv')
    box.upload_file(box_temp+'/Toolbox_notinboth_Data_'+snapshotdate+'.csv',needsattnfolder)
    box.upload_file(box_temp+'/Toolbox_notinboth_Scores_'+snapshotdate+'.csv',needsattnfolder)

    nonidenticaldupdata.to_csv(box_temp+'/Toolbox_NonidentDups_Data_'+snapshotdate+'.csv')
    nonidenticaldupscore.to_csv(box_temp+'/Toolbox_NonidentDups_Scores_'+snapshotdate+'.csv')
    box.upload_file(box_temp+'/Toolbox_NonidentDups_Data_'+snapshotdate+'.csv',needsattnfolder)
    box.upload_file(box_temp+'/Toolbox_NonidentDups_Scores_'+snapshotdate+'.csv',needsattnfolder)

    #last but not least...set aside ids not in REDCap, and IDs that need visit numbers
    #get reds from hdatatinit3 (should be same as list from hscoreinit3)
    #generate hdatainit4 and hscoreinit4 which is relieved of these ids
    hdatainit4=subjectsvisits(hdatainit3)
    hscoreinit4=subjectsvisits(hscoreinit3)
    mv=hscoreinit4.loc[~(hscoreinit4.visit.isin(['V1','V2','V3','X1','X2','X3']))].copy()
    mvs=list(mv.subject.unique())  #list of PINs without visit numbers

    check=subjectpairs(hdatainit4,hscoreinit4) #this number will be fewer because V1 and V2 PINs for same subject only counted once)
    redids=box.getredcapids()
    dfcheck=pd.DataFrame(check,columns=['subject'])
    boxids=pd.merge(dfcheck,redids,how='left',on='subject',indicator=True)
    reds=list(boxids.loc[boxids._merge=='left_only'].subject) #subjects not in redcap
    boxandredcap=boxids.loc[boxids._merge=='both'].subject

    #export the otherwise cleanest data ready for snapshotting as the new updated curated file -- then run this for all sites befo
    #write code here - has only ids with visit numbers and one to one scores and data correspondence and no wierd duplications
    #but check one last time that hdatainit5 and hscoreinit5 is super clean
    hdatainit5=hdatainit4.loc[~(hdatainit4.subject.isin(mvs+reds))]
    hscoreinit5=hscoreinit4.loc[~(hscoreinit4.subject.isin(mvs+reds))]


    #export the lists of ids and reasons they were excluded
    df=pd.DataFrame(columns=['reason','affectedIDs'])
    df=df.append({'reason': 'PIN In Scores but not Data', 'affectedIDs': nbs}, ignore_index=True)
    df=df.append({'reason': 'PIN In Data but not Scores', 'affectedIDs': nbd}, ignore_index=True)
    df=df.append({'reason': 'PIN/Instrument Non-identical Duplication in Data', 'affectedIDs': wierdd}, ignore_index=True)
    df=df.append({'reason': 'PIN/Instrument Non-identical Duplication in Scores', 'affectedIDs': wierds}, ignore_index=True)
    df=df.append({'reason': 'PIN/subject in Scores and Data but missing visit', 'affectedIDs': mvs}, ignore_index=True)
    df=df.append({'reason': 'subject in Scores and Data but not REDCap ', 'affectedIDs': reds}, ignore_index=True)


    df.to_csv(box_temp+'/List_of_IDs_and_Reasons_they_in_these_files_'+snapshotdate+'.csv')
    box.upload_file(box_temp+'/List_of_IDs_and_Reasons_they_in_these_files_'+snapshotdate+'.csv',needsattnfolder)

    return hdatainit5,hscoreinit5



#get subject and visit from a PIN in a dataframe
def subjectsvisits(hdatainit3):
    hdatainit3['subject']=hdatainit3.PIN.str.strip().str[:10]
    hdatainit3['visit']=''
    hdatainit3.loc[hdatainit3.PIN.str.contains('v1',case=False),'visit']='V1'
    hdatainit3.loc[hdatainit3.PIN.str.contains('v2',case=False),'visit']='V2'
    hdatainit3.loc[hdatainit3.PIN.str.contains('v3',case=False),'visit']='V3'
    hdatainit3.loc[hdatainit3.PIN.str.contains('x1',case=False),'visit']='X1'
    hdatainit3.loc[hdatainit3.PIN.str.contains('x2',case=False),'visit']='X2'
    hdatainit3.loc[hdatainit3.PIN.str.contains('x3',case=False),'visit']='X3'
    return hdatainit3



#pull id visit combos that arent in both scores and data files
def findpairs(hdatainit,hscoreinit):
    pinsinboth=[]
    for i in hscoreinit.PIN.unique():
        if i in hdatainit.PIN.unique() and isinstance(i,str):
            pinsinboth=pinsinboth+[i]
        else:
            print('the following PINs in scores but not data:')
            print(i)

    for i in hdatainit.PIN.unique():
        if i in hscoreinit.PIN.unique():
            pass
        else:
            print('the following PINs in data but not scores:')
            print(i)
    return pinsinboth

def subjectpairs(hdatainit,hscoreinit):
    pinsinboth=[]
    for i in hscoreinit.subject.unique():
        if i in hdatainit.subject.unique() and isinstance(i,str):
            pinsinboth=pinsinboth+[i]
        else:
            print('the following subjects in scores but not data:')
            print(i)

    for i in hdatainit.subject.unique():
        if i in hscoreinit.subject.unique():
            pass
        else:
            print('the following subjectss in data but not scores:')
            print(i)
    return pinsinboth



def findwierdos(hdatainit,hscoreinit):
    #compare the two types of sort to identify which files have non-identical duplications
    sort1data=hdatainit.drop_duplicates(subset={'PIN','Inst','ItemID','Position'},keep='first')
    sort1score=hscoreinit.drop_duplicates(subset={'PIN','Inst'})
    sort2data=hdatainit.drop_duplicates(subset=set(hdatainit.columns).difference({'filename','file_id'}))
    sort2score=hscoreinit.drop_duplicates(subset=set(hscoreinit.columns).difference({'filename','file_id'}))
    s1d=sort1data.groupby('PIN').count()
    s2d=sort2data.groupby('PIN').count()
    databoth=pd.merge(s1d.reset_index()[['PIN','DeviceID']], s2d.reset_index()[['PIN','DeviceID']],on=['PIN','DeviceID'],how='outer',indicator=True)
    wierd_data=databoth.loc[databoth._merge!='both'].rename(columns={'DeviceID':'Number of Rows'})
    s1s=sort1score.groupby('PIN').count()
    s2s=sort2score.groupby('PIN').count()
    scoreboth=pd.merge(s1s.reset_index()[['PIN','DeviceID']], s2s.reset_index()[['PIN','DeviceID']],on=['PIN','DeviceID'],how='outer',indicator=True)
    wierd_score=scoreboth.loc[scoreboth._merge!='both'].rename(columns={'DeviceID':'Number of Rows'})
    return wierd_data,wierd_score


def catcontents(files,cache_space): #dataframe that has filename and file_id as columns
    scoresfiles=files.copy()
    scoresinit=pd.DataFrame()
    for i in scoresfiles.filename:
        filepath=os.path.join(cache_space,i)
        filenum=scoresfiles.loc[scoresfiles.filename==i,'file_id']
        try:
            temp=pd.read_csv(filepath,header=0,low_memory=False)
            temp['filename']=i
            temp['file_id']=pd.Series(int(filenum.values[0]),index=temp.index)
            temp['raw_cat_date']=snapshotdate
            scoresinit=pd.concat([scoresinit,temp],axis=0,sort=False)
        except:
            print(filepath+' wouldnt import')
            temp=pd.DataFrame()
            temp['filename']=pd.Series(i,index=[0])
            temp['file_id']=pd.Series(int(filenum.values[0]),index=[0])
            temp['raw_cat_date']=snapshotdate
            scoresinit=pd.concat([scoresinit,temp],axis=0,sort=False)
    return scoresinit


def catfromlocal(endpoint_temp,scores2cat): #dataframe that has filenames
    scoresfiles=scores2cat.copy()
    scoresinit=pd.DataFrame()
    for i in scoresfiles.fname:
        filepath=os.path.join(endpoint_temp,i)
        try:
            temp=pd.read_csv(filepath,header=0,low_memory=False)
            temp['filename']="endpointmachine/"+i
            temp['raw_cat_date']=snapshotdate
            scoresinit=pd.concat([scoresinit,temp],axis=0,sort=False)
        except:
            print(filepath+' wouldnt import')
            temp=pd.DataFrame()
            temp['filename']=pd.Series("endpointmachine/"+i,index=[0])
            temp['raw_cat_date']=snapshotdate
            scoresinit=pd.concat([scoresinit,temp],axis=0,sort=False)
    return scoresinit



def folderlistcontents(folderslabels,folderslist):
    bdasfilelist=pd.DataFrame()
    bdasfolderlist=pd.DataFrame()
    for i in range(len(folderslist)):
        print('getting file and folder contents of box folder ' +folderslabels[i])
        subfiles,subfolders=foldercontents(folderslist[i]) #foldercontents generates two dfs: a df with names and ids of files and a df with names and ids of folders
        bdasfilelist=bdasfilelist.append(subfiles)
        bdasfolderlist=bdasfolderlist.append(subfolders)
    return bdasfilelist,bdasfolderlist


def foldercontents(folder_id):
    filelist=[]
    fileidlist=[]
    folderlist=[]
    folderidlist=[]
    WUlist=box.client.folder(folder_id=folder_id).get_items(limit=None, offset=0, marker=None, use_marker=False, sort=None, direction=None, fields=None)
    for item in WUlist:
        if item.type == 'file':
            filelist.append(item.name)
            fileidlist.append(item.id)
        if item.type == 'folder':
            folderlist.append(item.name)
            folderidlist.append(item.id)
    files=pd.DataFrame({'filename':filelist, 'file_id':fileidlist})
    folders=pd.DataFrame({'foldername':folderlist, 'folder_id':folderidlist})
    return files,folders

