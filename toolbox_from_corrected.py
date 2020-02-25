import os, datetime
import pandas as pd

from download.box import LifespanBox

verbose = True
#verbose = False
snapshotdate = datetime.datetime.today().strftime('%m_%d_%Y')

#Two types of files to curate...the so called raw data from which scores are generated and the scores themeselves.
#connect to Box (to get latest greatest curated stuff)
box_temp='/home/petra/UbWinSharedSpace1/boxtemp' #location of local copy of curated data
box = LifespanBox(cache=box_temp)
redcapconfigfile="/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/.boxApp/redcapconfig.csv"

#start with data that is result of extensive QC effort from sites.
#keep track of expected and observed IDs
#curate list of TBX issues.
#pull in data (by ID) that not on list of issues


#get list of filenames
##########################
Harvard=84800505740
harvardfiles, harvardfolders=foldercontents(Harvard)
harvardfiles2, harvardfolders2=folderlistcontents(harvardfolders.foldername,harvardfolders.folder_id)
harvardfiles=pd.concat([harvardfiles,harvardfiles2],axis=0,sort=True)

data4process=harvardfiles.loc[harvardfiles.filename.str.contains('Data')==True]
scores4process=harvardfiles.loc[harvardfiles.filename.str.contains('Score')==True]
box.download_files(data4process.file_id)
box.download_files(scores4process.file_id)

hdatainit=catcontents(data4process,box_temp)
hscoreinit=catcontents(scores4process,box_temp)

#check if these are empty
dlist,slist=findwierdos(hdatainit,hscoreinit)

#if so, just delete any identical duplicates
if dlist.empty and slist.empty:
    hdatainit=hdatainit.drop_duplicates(subset={'PIN','Inst','ItemID','Position'},keep='first')
    hscoreinit=hscoreinit.drop_duplicates(subset={'PIN','Inst'})
else:
    print('Found Non-Identical Duplications')
    print(dlist)
    print(slist)

l=findpairs(hdatainit,hscoreinit) #this is the list of ids in both scored and raw data
#keep the ones that have no nan pins
hdatainit=hdatainit[hdatainit.PIN.isin(l)]
hscoreinit=hscoreinit[hscoreinit.PIN.isin(l)]

#upload the concatenated files to site directory in box and move other files to incorporated
hdatainit.to_csv(box_temp+'/harvard_corrected_data'+snapshotdate+'.csv')
hscoreinit.to_csv(box_temp+'/harvard_corrected_scores'+snapshotdate+'.csv')
box.upload_file(box_temp+'/harvard_corrected_data'+snapshotdate+'.csv',Harvard)
box.upload_file(box_temp+'/harvard_corrected_scores'+snapshotdate+'.csv',Harvard)

#all files associated with this snapshotdate moved to incorporated_snapshotdate folder under this
#corrected folder


#########################################
MGH=84799213727
mghfiles, mghfolders=foldercontents(MGH)
data4process=mghfiles.loc[(mghfiles.filename.str.contains('Data')==True) | (mghfiles.filename.str.contains('Raw')==True)]
scores4process=mghfiles.loc[mghfiles.filename.str.contains('Score')==True]
box.download_files(data4process.file_id)
box.download_files(scores4process.file_id)

mdatainit=catcontents(data4process,box_temp)
mscoreinit=catcontents(scores4process,box_temp)

dlist,slist=findwierdos(mdatainit,mscoreinit)

if dlist.empty and slist.empty:
    mdatainit=mdatainit.drop_duplicates(subset={'PIN','Inst','ItemID','Position'},keep='first')
    mscoreinit=mscoreinit.drop_duplicates(subset={'PIN','Inst'})
else:
    print('Found Non-Identical Duplications')
    print(dlist)
    print(slist)

m=findpairs(mdatainit,mscoreinit) #this is the list of ids in both scored and raw data

#upload the concatenated files to site directory in box and move other files to incorporated
mdatainit.to_csv(box_temp+'/mgh_corrected_data'+snapshotdate+'.csv')
mscoreinit.to_csv(box_temp+'/mgh_corrected_scores'+snapshotdate+'.csv')
box.upload_file(box_temp+'/mgh_corrected_data'+snapshotdate+'.csv',MGH)
box.upload_file(box_temp+'/mgh_corrected_scores'+snapshotdate+'.csv',MGH)

#all files associated with this snapshotdate moved to incorporated_snapshotdate folder under this
#corrected folder


##########################
WashuD=84801037257
wudfiles, wudfolders=foldercontents(WashuD)
wudfiles2, wudfolders2=folderlistcontents(wudfolders.foldername,wudfolders.folder_id)
wudfiles=pd.concat([wudfiles,wudfiles2],axis=0,sort=True)
data4process=wudfiles.loc[(wudfiles.filename.str.contains('Data')==True) | (wudfiles.filename.str.contains('Raw')==True)]
scores4process=wudfiles.loc[wudfiles.filename.str.contains('Score')==True]
box.download_files(data4process.file_id)
box.download_files(scores4process.file_id)

wdatainit=catcontents(data4process,box_temp)
wscoreinit=catcontents(scores4process,box_temp)

dlist,slist=findwierdos(wdatainit,wscoreinit)

if dlist.empty and slist.empty:
    wdatainit=wdatainit.drop_duplicates(subset={'PIN','Inst','ItemID','Position'},keep='first')
    wscoreinit=wscoreinit.drop_duplicates(subset={'PIN','Inst'})
else:
    print('Found Non-Identical Duplications')
    print(dlist)
    print(slist)
wd=findpairs(wdatainit,wscoreinit) #this is the list of ids in both scored and raw data

wdatainit.to_csv(box_temp+'/wud_corrected_data'+snapshotdate+'.csv')
wscoreinit.to_csv(box_temp+'/wud_corrected_scores'+snapshotdate+'.csv')
box.upload_file(box_temp+'/wud_corrected_data'+snapshotdate+'.csv',WashuD)
box.upload_file(box_temp+'/wud_corrected_scores'+snapshotdate+'.csv',WashuD)

##########################
WashuA=84799623206
wuafiles, wuafolders=foldercontents(WashuA)
data4process=wuafiles.loc[(wuafiles.filename.str.contains('Data')==True) | (wuafiles.filename.str.contains('Raw')==True)]
scores4process=wuafiles.loc[wuafiles.filename.str.contains('Score')==True]
box.download_files(data4process.file_id)
box.download_files(scores4process.file_id)

wadatainit=catcontents(data4process,box_temp)
wascoreinit=catcontents(scores4process,box_temp)

dlist,slist=findwierdos(wadatainit,wascoreinit)

if dlist.empty and slist.empty:
    wadatainit=wadatainit.drop_duplicates(subset={'PIN','Inst','ItemID','Position'},keep='first')
    wascoreinit=wascoreinit.drop_duplicates(subset={'PIN','Inst'})
else:
    print('Found Non-Identical Duplications')
    print(dlist)
    print(slist)

wa=findpairs(wadatainit,wascoreinit) #this is the list of ids in both scored and raw data
wadatainit=wadatainit[wadatainit.PIN.isin(wa)]
wascoreinit=wascoreinit[wascoreinit.PIN.isin(wa)]


wadatainit.to_csv(box_temp+'/wua_corrected_data'+snapshotdate+'.csv')
wascoreinit.to_csv(box_temp+'/wua_corrected_scores'+snapshotdate+'.csv')
box.upload_file(box_temp+'/wua_corrected_data'+snapshotdate+'.csv',WashuA)
box.upload_file(box_temp+'/wua_corrected_scores'+snapshotdate+'.csv',WashuA)


######################################################


umnD=84799525828
umnDfiles, umnDfolders=foldercontents(umnD)

data4process=umnDfiles.loc[(umnDfiles.filename.str.contains('Data')==True) | (umnDfiles.filename.str.contains('Raw')==True)]
scores4process=umnDfiles.loc[umnDfiles.filename.str.contains('Score')==True]
box.download_files(data4process.file_id)
box.download_files(scores4process.file_id)

udatainit=catcontents(data4process,box_temp)
uscoreinit=catcontents(scores4process,box_temp)
dlist,slist=findwierdos(udatainit,uscoreinit)

if dlist.empty and slist.empty:
    udatainit=udatainit.drop_duplicates(subset={'PIN','Inst','ItemID','Position'},keep='first')
    uscoreinit=uscoreinit.drop_duplicates(subset={'PIN','Inst'})
else:
    print('Found Non-Identical Duplications')
    print(dlist)
    print(slist)

ud=findpairs(udatainit,uscoreinit) #this is the list of ids in both scored and raw data

udatainit.to_csv(box_temp+'/umnd_corrected_data'+snapshotdate+'.csv')
uscoreinit.to_csv(box_temp+'/umnd_corrected_scores'+snapshotdate+'.csv')
box.upload_file(box_temp+'/umnd_corrected_data'+snapshotdate+'.csv',umnD)
box.upload_file(box_temp+'/umnd_corrected_scores'+snapshotdate+'.csv',umnD)


######################################################
umnA=84799599800
umnafiles, umnafolders=foldercontents(umnA)
data4process=umnafiles.loc[umnafiles.filename.str.contains('RAW')==True]
scores4process=umnafiles.loc[umnafiles.filename.str.contains('Score')==True]
box.download_files(data4process.file_id)
box.download_files(scores4process.file_id)
#subset to file_ids corresponding to non-sas rescored data
data4process=data4process.loc[data4process.file_id !='518546218453']
scores4process=scores4process.loc[scores4process.file_id!='518542244267']
umadatainit=catcontents(data4process,box_temp)
umascoreinit=catcontents(scores4process,box_temp)
dlist,slist=findwierdos(umadatainit,umascoreinit)

if dlist.empty and slist.empty:
    umadatainit=umadatainit.drop_duplicates(subset={'PIN','Inst','ItemID','Position'},keep='first')
    umascoreinit=umascoreinit.drop_duplicates(subset={'PIN','Inst'})
else:
    print('Found Non-Identical Duplications')
    print(dlist)
    print(slist)


uma=findpairs(umadatainit,umascoreinit) #this is the list of ids in both scored and raw data

umadatainit.to_csv(box_temp+'/umna_corrected_data'+snapshotdate+'.csv')
umascoreinit.to_csv(box_temp+'/umna_corrected_scores'+snapshotdate+'.csv')
box.upload_file(box_temp+'/umna_corrected_data'+snapshotdate+'.csv',umnA)
box.upload_file(box_temp+'/umna_corrected_scores'+snapshotdate+'.csv',umnA)

######################################################
uclaA=84799075673
uclaAfiles, uclaAfolders=foldercontents(uclaA)
data4process=uclaAfiles.loc[uclaAfiles.filename.str.contains('Data')==True]
scores4process=uclaAfiles.loc[uclaAfiles.filename.str.contains('Score')==True]
box.download_files(data4process.file_id)
box.download_files(scores4process.file_id)

uadatainit=catcontents(data4process,box_temp)
uascoreinit=catcontents(scores4process,box_temp)
uadatainit['PIN']=uadatainit.PIN.str.strip()
uascoreinit['PIN']=uascoreinit.PIN.str.strip()

dlist,slist=findwierdos(uadatainit,uascoreinit)

if dlist.empty and slist.empty:
    uadatainit=uadatainit.drop_duplicates(subset={'PIN','Inst','ItemID','Position'},keep='first')
    uascoreinit=uascoreinit.drop_duplicates(subset={'PIN','Inst'})
else:
    print('Found Non-Identical Duplications')
    print(dlist)
    print(slist)

uca=findpairs(uadatainit,uascoreinit) #this is the list of ids in both scored and raw data
#keep the ones that have no nan pins
uadatainit=uadatainit[uadatainit.PIN.isin(uca)]
uascoreinit=uascoreinit[uascoreinit.PIN.isin(uca)]

uadatainit.to_csv(box_temp+'/uclaa_corrected_data'+snapshotdate+'.csv')
uascoreinit.to_csv(box_temp+'/uclaa_corrected_scores'+snapshotdate+'.csv')
box.upload_file(box_temp+'/uclaa_corrected_data'+snapshotdate+'.csv',uclaA)
box.upload_file(box_temp+'/uclaa_corrected_scores'+snapshotdate+'.csv',uclaA)

######################################################
uclaD=84800272537
uclaDfiles, uclaDfolders=foldercontents(uclaD)
data4process=uclaDfiles.loc[uclaDfiles.filename.str.contains('Raw')==True]
scores4process=uclaDfiles.loc[uclaDfiles.filename.str.contains('Score')==True]
box.download_files(data4process.file_id)
box.download_files(scores4process.file_id)

uddatainit=catcontents(data4process,box_temp)
udscoreinit=catcontents(scores4process,box_temp)
uddatainit['PIN']=uddatainit.PIN.str.strip()
udscoreinit['PIN']=udscoreinit.PIN.str.strip()


dlist,slist=findwierdos(uddatainit,udscoreinit)

if dlist.empty and slist.empty:
    uddatainit=uddatainit.drop_duplicates(subset={'PIN','Inst','ItemID','Position'},keep='first')
    udscoreinit=udscoreinit.drop_duplicates(subset={'PIN','Inst'})
else:
    print('Found Non-Identical Duplications')
    print(dlist)
    print(slist)
uddatainit=uddatainit[uddatainit.PIN.isin(ucd)]
udscoreinit=udscoreinit[udscoreinit.PIN.isin(ucd)]
ucd=findpairs(uddatainit,udscoreinit) #this is the list of ids in both scored and raw data

uddatainit.to_csv(box_temp+'/uclad_corrected_data'+snapshotdate+'.csv')
udscoreinit.to_csv(box_temp+'/uclad_corrected_scores'+snapshotdate+'.csv')
box.upload_file(box_temp+'/uclad_corrected_data'+snapshotdate+'.csv',uclaD)
box.upload_file(box_temp+'/uclad_corrected_scores'+snapshotdate+'.csv',uclaD)

###########################
#altogether

#Harvard l hdatainit hscoreinit
#MGH m mdatainit mscoreinit
#WashuD wd wdatainit wscoreinit
#WashUA wa wadatainit wascoreinit
#UMND ud udatainit uscoreinit
#UMNA uma umadatainit umascoreinit
#UCLAA uca uadatainit uascoreinit
#UCLAD ucd uddatainit udscoreinit


#raw
correctedraw=pd.concat([hdatainit, mdatainit, wdatainit, wadatainit, udatainit, umadatainit, uadatainit, uddatainit],axis=0,sort=True)
correctedraw=correctedraw.loc[correctedraw.PIN.isnull()==False]

#scores
correctedscores=pd.concat([hscoreinit, mscoreinit, wscoreinit, wascoreinit, uscoreinit, umascoreinit, uascoreinit, udscoreinit],axis=0,sort=True)
correctedscores=correctedscores.loc[correctedscores.PIN.isnull()==False]


#check tallies - all 168
len(ucd)+len(uca)+len(wa)+len(wd)+len(ud)+len(uma)+len(l)+len(m)
len(correctedraw.PIN.unique())
len(correctedscores.PIN.unique())

#lightson

dlist,slist=findwierdos(correctedraw,correctedscores)

if dlist.empty and slist.empty:
    correctedraw=correctedraw.drop_duplicates(subset={'PIN','Inst','ItemID','Position'},keep='first')
    correctedscores=correctedscores.drop_duplicates(subset={'PIN','Inst'})
else:
    print('Found Non-Identical Duplications')
    print(dlist)
    print(slist)

correctedraw['subject']=correctedraw.PIN.str.split("_",expand=True)[0]
correctedraw['visit']=correctedraw.PIN.str.split("_",expand=True)[1]
correctedscores['subject']=correctedscores.PIN.str.split("_",expand=True)[0]
correctedscores['visit']=correctedscores.PIN.str.split("_",expand=True)[1]

correctedraw.to_csv(box_temp+'/allsites_corrected_data.csv')
correctedscores.to_csv(box_temp+'/allsites_corrected_scores.csv')



#hdatainit mdatainit wdatainit wadatainit udatainit uadatainit uddatainit
#hscoreinit mscoreinit wscoreinit wascoreinit uscoreinit uascoreinit udscoreinit

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

