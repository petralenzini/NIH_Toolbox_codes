import os, datetime
import pandas as pd

from download.box import LifespanBox
import sys


verbose = True
#verbose = False
snapshotdate = datetime.datetime.today().strftime('%m_%d_%Y')

#Two types of files to curate...the so called raw data from which scores are generated and the scores themeselves.
#connect to Box (to get latest greatest curated stuff)
box_temp='/home/petra/UbWinSharedSpace1/boxtemp' #location of local copy of curated data
box = LifespanBox(cache=box_temp)
redcapconfigfile="/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/.boxApp/redcapconfig.csv"


###########################################
#Washu
Dcurated=82804015457
Acurated=82804729845
specialfolder=114625442112
specialfoldername='WashU: https://wustl.app.box.com/folder/114625442112'

#def grabcurated(curated,pin,special):
Acurdata, Acurscores = box2dataframe(fileid=Acurated)
Dcurdata, Dcurscores = box2dataframe(fileid=Dcurated)

#Done: pinD='HCD3105534_V1'; pinA='HCA8793607_V1'
#Done: pinD='HCD4004533_V1'; pinA='HCA6086470_V1'
#Done: pinD='HCD4286973_V1'; pinA='HCA7305663_V1'
#Done: pinD="HCD4534360_V1"; pinA='HCA9037876_V1'
#Done: pinD='HCD4751166_V1'; pinA='HCA7351165_V1'
#Done: pinD='HCD4871883_V1'; pinA='HCA9088388_V1'
#Done: pinD='HCD5226051_V1'; pinA='HCA8421569_V1'
#done: pinD='HCD5372769_V1'; pinA='HCA6439780_V1'
#done: pinD='HCD5426463_V1'; pinA='HCA6742377_V1'
#done: pinD='HCD5716977_V1'; pinA='HCA7247574_V1'
#done: pinD='HCD5876696_V1'; pinA='HCA9304772_V1'
#done: pinD='HCD3702047_V1'; pinA='HCA6660880_V1'
#done: pinD='HCD5175666_V1'; pinA='HCA9044570_V1'
#Done: pinD='HCD5916985_V1'; pinA='HCA7636686_V1'
grabpair(pinD,pinA,Acurdata,Acurscores,Dcurdata,Dcurscores,specialfolder,specialfoldername)

###################################################
#UCLA
Dcurated=82805124019
Acurated=82807223120
specialfolder=114625306531
specialfoldername='UCLA: https://wustl.app.box.com/folder/114625306531'

#def grabcurated(curated,pin,special):
Acurdata, Acurscores = box2dataframe(fileid=Acurated)
Dcurdata, Dcurscores = box2dataframe(fileid=Dcurated)

#Done: pinA='HCA6749088_V1'; pinD='HCD5458072_V1'
#Done: pinA='HCA7066873_V1'; pinD='HCD5086465_V1'
#Done: pinA='HCA8154168_V1'; pinD='HCD5120439_V1'
#done: pinA='HCA7670989_V1'; pinD='HCD3142540_V1'
#done: pinA='HCA8074170_V1'; pinD='HCD3526962_V1'
#done: pinA='HCA8674902_V1'; pinD='HCD3756474_V1'
#done: pinA='HCA9339892_V1'; pinD='HCD3823059_V1'
#done: pinA='HCA8657295_V1'; pinD='HCD4029347_V1'
#done: pinA='HCA7825487_V1'; pinD='HCD4201030_V1'
#done: pinA='HCA6198582_V1'; pinD='HCD4248763_V1'
#done: pinA='HCA6197580_V1'; pinD='HCD4366971_V1'
#done: pinA='HCA9708190_V1'; pinD='HCD4663371_V1'
#done: pinA='HCA9914193_V1'; pinD='HCD5052448_V1'
#done: pinA='HCA9750290_V1'; pinD='HCD5088368_V1'
#done: pinA='HCA7717484_V1'; pinD='HCD5229562_V1'
#done: pinA='HCA8219877_V1'; pinD='HCD5274365_V1'
#done: pinA='HCA9268996_V1'; pinD='HCD5375472_V1'
#done: pinA='HCA7222558_V1'; pinD='HCD5681681_V1'
#done: pinA='HCA7884605_V1'; pinD='HCD5711866_V1'
#done: pinA='HCA6780082_V1'; pinD='HCD5205952_V1'

grabpair(pinD,pinA,Acurdata,Acurscores,Dcurdata,Dcurscores,specialfolder,specialfoldername)

###################################################
#UMN
Dcurated=82805151056
Acurated=82803665867
specialfolder=114624512771
specialfoldername='UMN: https://wustl.app.box.com/folder/114624512771'

#def grabcurated(curated,pin,special):
Acurdata, Acurscores = box2dataframe(fileid=Acurated)
Dcurdata, Dcurscores = box2dataframe(fileid=Dcurated)

pinA=
pinD=
grabpair(pinD,pinA,Acurdata,Acurscores,Dcurdata,Dcurscores,specialfolder,specialfoldername)


def grabpair(pinD,pinA,Acurdata,Acurscores,Dcurdata,Dcurscores,specialfolder,specialfoldername):
    print('Double Winner: '+pinD+'='+pinA)
    print('...')
    #Pin in curated HCA data location?
    pinpair=[pinA,pinD]
    for pin in pinpair:
        Adata4export=Acurdata.loc[Acurdata.PIN==pin]
        Ascores4export=Acurscores.loc[Acurscores.PIN==pin]
        isemptydata = Adata4export.empty
        isemptyscores=Ascores4export.empty
        if isemptydata and isemptyscores:
            print(pin+ ' not in curated HCA location (nothing to delete there)')
        if not isemptydata:
            print('Note to Petra: Delete ' +pin+ ' from curated HCA location')
            fname = pin + '_fromHCAcurated'
            print('Data/Scores for '+pin+' pulled and placed in '+specialfoldername)
            sendtobox(Ascores4export, Adata4export,fname,specialfolder)
        print('...')
        #Pin in curated HCD data location?
        Ddata4export=Dcurdata.loc[Dcurdata.PIN==pin]
        Dscores4export=Dcurscores.loc[Dcurscores.PIN==pin]
        isemptydatad = Ddata4export.empty
        isemptyscoresd=Dscores4export.empty
        if isemptydatad and isemptyscoresd:
            print(pin+ ' not in curated HCD location (nothing to delete there)')
        if not isemptydatad:
            print('Note to Petra: Delete ' +pin+ ' from curated HCD location')
            fname=pin+'_fromHCDcurated'
            sendtobox(Dscores4export, Ddata4export,fname,specialfolder)
            print('Data/Scores for '+pin+' pulled and placed in '+specialfoldername)
        print('...')



def sendtobox(scoresfound,datafound,fname,specialfolder):
    scoresfound.to_csv(box_temp+'/'+fname+'_scores_'+snapshotdate+'.csv',index=False)
    box.upload_file(box_temp+'/'+fname+'_scores_'+snapshotdate+'.csv',specialfolder)
    datafound.to_csv(box_temp+'/'+fname+'_data_'+snapshotdate+'.csv',index=False)
    box.upload_file(box_temp+'/'+fname+'_data_'+snapshotdate+'.csv',specialfolder)




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



def validpair(pin='HCD0007014_V1'):
    print('Checking files in CORRECTED folder having title with PIN='+pin)
    PINcheckd=data4process.loc[data4process.PIN==pin]
    PINchecks=scores4process.loc[scores4process.PIN==pin]
    box.download_files(PINcheckd.file_id)
    box.download_files(PINchecks.file_id)
    d=catcontents(PINcheckd,box_temp)
    s=catcontents(PINchecks,box_temp)
    if 'PIN' in d.columns:
        if 'PIN' in s.columns:
            d = d.loc[d.PIN.isnull() == False]
            s = s.loc[s.PIN.isnull() == False]
            print('PINS in Data: ')
            print(d.PIN.unique())
            print('PINS in Scores: ')
            print(s.PIN.unique())
            try:
                if d.PIN.unique()==s.PIN.unique():
                    print('Passed Unique PIN test')
                    dlist,slist=findwierdos(d,s)
                    if dlist.empty and slist.empty:
                       d=d.drop_duplicates(subset={'PIN','Inst','ItemID','Position'},keep='first')
                       s=s.drop_duplicates(subset={'PIN','Inst'})
                       print('Passed duplicate Instruments Test')
                       data4process.loc[data4process.PIN == pin,'Fail'] = 0
                       scores4process.loc[scores4process.PIN==pin,'Fail'] = 0
                    else:
                        print('Found Non-Identical Duplications')
                        print(dlist+': in Data')
                        print(slist+': in Scores')
            except:
                print('Status : FAIL')
    else:
        print('variable named PIN not found.  Check for missing header')
        print('Status : FAIL')
    print('##################################################')
    return d,s

