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

studyids=box.getredcapids()
hcpa=box.getredcapfields(fieldlist=['misscat','data_status'],study='hcpa')
ssaga=box.getredcapfields(fieldlist=[],study='ssaga')
check=pd.merge(hcpa[['flagged','gender','site','study','subject','misscat___8','data_status']],ssaga[['hcpa_id','subject']],on='subject',how='outer',indicator=True)
check.loc[(check._merge=='left_only') & (check.flagged.isnull()==True) & (check.misscat___8=='0')][['site','subject','data_status']]
#SEE SSAGA TRACKING EMAIL FROM CINDY 3/31/20



##########################
#nothing new in corrected this round.
#stopped here -- keeping code below to pick up where left off...which is ...not at all
#figure out the ids we have and see what we need to pull from elsewhere


#These are the subjects we're expecting (misscat___3=1 for perm missing toolb0x)
hcpa=box.getredcapfields(fieldlist=['misscat', 'data_status'], study='hcpa')
hcpa=hcpa.loc[hcpa.flagged.isnull()==True].copy()
hcpa=hcpa.loc[(hcpa.data_status.isin(['1','2'])) & (~(hcpa.misscat___3=='1'))].copy()

hcpd=box.getredcapfields(fieldlist=['misscat', 'data_status'], study='hcpdchild')
hcpd18=box.getredcapfields(fieldlist=['misscat', 'data_status'], study='hcpd18')
hcpdparent=box.getredcapfields(fieldlist=[], study='hcpdparent')
hcpdparent=hcpdparent[['parent_id','subject','study']].rename(columns={'subject':'child_id'})

hcpd=pd.concat([hcpd,hcpd18],axis=0)
hcpd=hcpd.loc[hcpd.flagged.isnull()==True].copy()
hcpd=hcpd.loc[(hcpd.data_status.isin(['1','2'])) & (~(hcpd.misscat___3=='1'))].copy()


#Harvard
Harv=82803734267
Harvattn=96013516511
Harvcorr=84800505740
harvcleandata,harvcleanscores=box2dataframe(fileid=Harv)
len(harvcleandata.PIN.unique()) #551
len(harvcleanscores.PIN.unique()) #551
H=pd.DataFrame(harvcleanscores.PIN.unique(),columns={"PIN"})
H['site']='Harvard'

MGH2=82761770877
MGHattn=96148925420
MHGcorr=84799213727
mghdata,mghscores=box2dataframe(fileid=MGH2)
len(mghdata.PIN.unique())
len(mghscores.PIN.unique()) #230 in each now
M=pd.DataFrame(mghscores.PIN.unique(),columns={"PIN"})
M['site']='MGH'


WashUD=82804015457
WashUDattn=96147128675
WUDcorr=84801037257
wuddata,wudscores=box2dataframe(fileid=WashUD) #301 in each now
len(wuddata.PIN.unique())
len(wudscores.PIN.unique())
WD=pd.DataFrame(wudscores.PIN.unique(),columns={"PIN"})
WD['site']='WashUD'


WashUA=82804729845
WashUAattn=96149947498
WUAcorr=84799623206
wuadata,wuascores=box2dataframe(fileid=WashUA)
len(wuadata.PIN.unique()) #238 in each now
len(wuascores.PIN.unique()) #238 in each now
WA=pd.DataFrame(wuascores.PIN.unique(),columns={"PIN"})
WA['site']='WashUA'


UMNA=82803665867
UMNAattn=96153923311
UMNAcorr=84799599800
umnadata,umnascores=box2dataframe(fileid=UMNA)
len(umnadata.PIN.unique()) #288 in each now
len(umnascores.PIN.unique()) #288 in each now
UMA=pd.DataFrame(umnascores.PIN.unique(),columns={"PIN"})
UMA['site']='UMNA'


UMND=82805151056
UMNDattn=96155708581
UMNDcorr=84799525828
umnddata,umndscores=box2dataframe(fileid=UMND)
len(umnddata.PIN.unique()) #270 in each now
len(umndscores.PIN.unique()) #270 in each now
UMD=pd.DataFrame(umndscores.PIN.unique(),columns={"PIN"})
UMD['site']='UMND'



UCLAA=82807223120
UCLAAattn=96154919803
UCLAAcorr=84799075673
uclaadata,uclaascores=box2dataframe(fileid=UCLAA)
len(uclaadata.PIN.unique()) #207
len(uclaascores.PIN.unique())
UCA=pd.DataFrame(uclaascores.PIN.unique(),columns={"PIN"})
UCA['site']='UCLAA'


UCLAD=82805124019
UCLADattn=96162759127
UCLADcorr=84800272537
ucladdata,ucladscores=box2dataframe(fileid=UCLAD)
len(ucladdata.PIN.unique()) #350
len(ucladscores.PIN.unique())
UCD=pd.DataFrame(ucladscores.PIN.unique(),columns={"PIN"})
UCD['site']='UCLAD'

allcurated=pd.concat([H,M,WD,WA,UMA,UMD,UCA,UCD],axis=0)


###########################################

ucladdata,ucladscores
uclaadata,uclaascores
umnddata,umndscores
umnadata,umnascores
wuadata,wuascores
wuddata,wudscores
mghdata,mghscores
harvcleandata,harvcleanscores

#concatenate cleandata for snapshotdate - putting read_csv here in case not loaded into memory
#raw:
allrawdataHCAorBoth=pd.concat([uclaadata,umnadata,wuadata,mghdata],axis=0)
allrawdataHCD=pd.concat([ucladdata,umnddata,wuddata,harvcleandata],axis=0)

#scores:
allscoresHCAorBoth=pd.concat([uclaascores,umnascores,wuascores,mghscores],axis=0)
allscoresHCD=pd.concat([ucladscores,umndscores,wudscores,harvcleanscores],axis=0)

######################
#make csv
allrawdataHCAorBoth.to_csv(box_temp+'/HCAorBoth_Toolbox_Raw_Combined_'+snapshotdate+'.csv')
allrawdataHCD.to_csv(box_temp+'/HCD_Toolbox_Raw_Combined_'+snapshotdate+'.csv')
allscoresHCAorBoth.to_csv(box_temp+'/HCAorBoth_Toolbox_Scored_Combined_'+snapshotdate+'.csv')
allscoresHCD.to_csv(box_temp+'/HCD_Toolbox_Scored_Combined_'+snapshotdate+'.csv')
##############################





def box2dataframe(fileid):
    harvardfiles, harvardfolders = foldercontents(fileid)
    data4process = harvardfiles.loc[~(harvardfiles.filename.str.upper().str.contains('SCORE') == True)]
    scores4process = harvardfiles.loc[harvardfiles.filename.str.upper().str.contains('SCORE') == True]
    data4process=data4process.reset_index()
    scores4process = scores4process.reset_index()
    box.download_files(data4process.file_id)
    box.download_files(scores4process.file_id)
    harvcleandata = pd.read_csv(box_temp+'/'+ data4process.filename[0], header=0,   low_memory=False)
    harvcleanscores = pd.read_csv(box_temp+'/'+ scores4process.filename[0], header=0,  low_memory=False)
    return harvcleandata,harvcleanscores

def inventory(hdata,hscores,study,site,v):
    curated = findpairs(hdata, hscores)  # this is the list of ids in both scored and raw corrected data
    curatedDF = pd.DataFrame(curated, columns={'PIN'})
    curatedDF[['subject','visit']]=curatedDF.PIN.str.split("_",1,expand=True)
    curatedvisit=curatedDF.loc[curatedDF.visit==v]
    curatedvisit=pd.merge(curatedvisit,study.loc[study.site==site],on='subject',how='outer',indicator=True)
    findelsewhere=curatedvisit.loc[curatedvisit._merge=='right_only'] #these are the ones that I need to get from endpoint
    return findelsewhere[['subject','visit','site','study']]

def grabfromallsites(pdfind,pdscores,pddata,pairs):
    catscores=pd.DataFrame()
    catdata=pd.DataFrame()
    pdfind['PIN']=pdfind['subject']+'_V1'
    grablist=pdfind.PIN.unique()
    for pinno in grablist:
        if pinno in pairs:
            print("Found PIN:" + pinno)
            catscores=pd.concat([catscores,pdscores.loc[pdscores.PIN==pinno]],axis=0)
            catdata = pd.concat([catdata,pddata.loc[pddata.PIN == pinno]],axis=0)
    return catscores,catdata

def sendtocorrected(scoresfound,datafound,fname,fnumber):
    scoresfound.to_csv(box_temp+'/'+fname+'_scores_'+snapshotdate+'.csv',index=False)
    box.upload_file(box_temp+'/'+fname+'_scores_'+snapshotdate+'.csv',fnumber)
    datafound.to_csv(box_temp+'/'+fname+'_data_'+snapshotdate+'.csv',index=False)
    box.upload_file(box_temp+'/'+fname+'_data_'+snapshotdate+'.csv',fnumber)




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

