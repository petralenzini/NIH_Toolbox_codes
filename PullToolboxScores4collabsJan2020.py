#the LifespanBox class from box.py imported below is in the downloads folder of V1 ccf-behavioral github organization

inout='/secretlocation/DataRequests/secretlocation/'
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
import json

from download.box import LifespanBox

verbose = True
redcapconfigfile="/secretlocation/redcapconfig.csv"

box_temp=inout
box = LifespanBox(cache=box_temp)

subjectlist=pd.read_csv(inout+'hcpaids_4_collabs.csv')

files,folders=foldercontents(snapshotfolder)
files.loc[files.filename.str.contains('Score')]
scores=587003317792 #new corrected data from sites (all combined)
scordataclean=Box2dataframe(scores,box_temp)

#first grab anyone from the clean scored data
cleanhisp=pd.merge(scordataclean,subjectlist,left_on='subject',right_on='subject_id',how='inner')
cleanhispv1=cleanhisp.loc[cleanhisp.visit=='V1']
#subset to requested instruments

instrumentlist=['NIH Toolbox Picture Vocabulary Test Age 3+ v2.0',
       'NIH Toolbox Flanker Inhibitory Control and Attention Test Age 12+ v2.1',
       'NIH Toolbox List Sorting Working Memory Test Age 7+ v2.1',
       'NIH Toolbox Dimensional Change Card Sort Test Age 12+ v2.1',
       'NIH Toolbox Pattern Comparison Processing Speed Test Age 7+ v2.1',
       'NIH Toolbox Picture Sequence Memory Test Age 8+ Form A v2.1',
       'NIH Toolbox Oral Reading Recognition Test Age 3+ v2.0']
cleanhispv1sub=cleanhispv1[cleanhispv1['Inst'].isin(instrumentlist)]
gotlist=pd.DataFrame(cleanhispv1sub.subject.unique().tolist(),columns=['subject'])
#now see if you can find anyother ids from other locations - exclude the ones you already found
stillneededidlist=pd.merge(subjectlist,gotlist,how='left',left_on='subject_id', right_on='subject',indicator=True)
stillneededlist=stillneededidlist.loc[stillneededidlist._merge=='left_only'][['subject_id']]

allsitesfolder=79549890229
files,folders=foldercontents(snapshotfolder)
listoffiles=files.loc[files.filename.str.contains('Score')]
extras=pd.DataFrame()
for i in listoffiles.file_id:
    scoredat=Box2dataframe(i,box_temp)
    scoredat['subject']=scoredat.PIN.str[0:10]
    scoredat['visit']=scoredat.PIN.str[11:13]
    xhisp=pd.merge(scoredat,stillneededlist,left_on='subject',right_on='subject_id',how='inner')
    print(xhisp.shape)
    if xhisp.empty == False:
        try:
            xhispv1=xhisp.loc[xhisp.visit=='V1']
            xhispv1sub=xhispv1[xhispv1['Inst'].isin(instrumentlist)]
            extras=pd.concat([extras,xhispv1sub],axis=0,sort=True)
        except:  #else:
            pass

#remove dups by subject/instrument (these were already removed from the cleanhispv1sub by a separate program)
sort1score = extras.drop_duplicates(subset={'subject', 'Inst'})

#put these together
alltoolbox=pd.concat([cleanhispv1sub,sort1score],axis=0,sort=True)
alltoolbox.drop_duplicates(subset={'subject', 'Inst'})
alltoolbox.to_csv(inout+'HCA_ToolboxScores.csv')

def Box2dataframe(curated_fileid_start,cache_space):#,study,site,datatype,boxsnapshotfolderid,boxsnapshotQCfolderid):
    #get current best curated data from BOX and read into pandas dataframe for QC
    raw_fileid=curated_fileid_start
    rawobject=box.download_file(raw_fileid)
    data_path = os.path.join(cache_space, rawobject.get().name)
    raw=pd.read_csv(data_path,header=0,low_memory=False, encoding = 'ISO-8859-1')
    #raw['DateCreatedDatetime']=pd.to_datetime(raw.DateCreated).dt.round('min')
    #raw['InstStartedDatetime']=pd.to_datetime(raw.InstStarted).dt.round('min')
    #raw['InstEndedDatetime']=pd.to_datetime(raw.InstEnded).dt.round('min')
    return raw


def folderlistcontents(folderslabels, folderslist):
    bdasfilelist = pd.DataFrame()
    bdasfolderlist = pd.DataFrame()
    for i in range(len(folderslist)):
        print(
            'getting file and folder contents of box folder ' +
            folderslabels[i])
        # foldercontents generates two dfs: a df with names and ids of files
        # and a df with names and ids of folders
        subfiles, subfolders = foldercontents(folderslist[i])
        bdasfilelist = bdasfilelist.append(subfiles)
        bdasfolderlist = bdasfolderlist.append(subfolders)
    return bdasfilelist, bdasfolderlist

def foldercontents(folder_id):
    filelist = []
    fileidlist = []
    folderlist = []
    folderidlist = []
    WUlist = box.client.folder(
        folder_id=folder_id).get_items(
        limit=None,
        offset=0,
        marker=None,
        use_marker=False,
        sort=None,
        direction=None,
        fields=None)
    for item in WUlist:
        if item.type == 'file':
            filelist.append(item.name)
            fileidlist.append(item.id)
        if item.type == 'folder':
            folderlist.append(item.name)
            folderidlist.append(item.id)
    files = pd.DataFrame({'filename': filelist, 'file_id': fileidlist})
    folders = pd.DataFrame(
        {'foldername': folderlist, 'folder_id': folderidlist})
    return files, folders
