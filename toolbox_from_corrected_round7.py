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

#removelist=pd.read_csv(os.path.join(box_temp,'RemoveFromCurated_perTrello19May2020.csv'))
removelist=pd.read_csv(os.path.join(box_temp,'RemoveFromCurated_perTrello27May2020.csv'))

#validpair(pin='HCD0007014_V1')

#get list of filenames
##################################################################################################
WashuD=84801037257
curated=82804015457

wudfiles, wudfolders=foldercontents(WashuD)
#wudfiles2, wudfolders2=folderlistcontents(wudfolders.foldername,wudfolders.folder_id)
#wudfiles=pd.concat([wudfiles,wudfiles2],axis=0,sort=True)
data4process=wudfiles.loc[(wudfiles.filename.str.contains('aw_')==True) | (wudfiles.filename.str.contains('Raw')==True)]
scores4process=wudfiles.loc[wudfiles.filename.str.contains('cored')==True]
data4process['PIN']=data4process.filename.str[:13]
scores4process['PIN']=scores4process.filename.str[:13]
data4process['Fail']=1
scores4process['Fail']=1

#run the validator for each pair of files in the Corrected data - write log to a file
orig_stdout = sys.stdout
f = open(os.path.join(box_temp,'WashUDevelopment_QC_Corrected_'+snapshotdate+'.txt'),'w')
sys.stdout = f
for p in data4process.PIN:
    checkd, checks=validpair(p)
sys.stdout = orig_stdout
f.close()

data4process.groupby('Fail').count()

#box.download_files(data4process.file_id)
#box.download_files(scores4process.file_id)

#subset to files that passed basic QC for next round
wdatainit=catcontents(data4process.loc[data4process.Fail==0],box_temp)
wscoreinit=catcontents(scores4process.loc[scores4process.Fail==0],box_temp)
wdatainit.PIN=wdatainit.PIN.str.strip()
wscoreinit.PIN=wscoreinit.PIN.str.strip()

#dlist and slist shoult be empty
dlist,slist=findwierdos(wdatainit,wscoreinit)
if dlist.empty and slist.empty:
    wdatainit=wdatainit.drop_duplicates(subset={'PIN','Inst','ItemID','Position'},keep='first')
    wscoreinit=wscoreinit.drop_duplicates(subset={'PIN','Inst'})
    wdatainit = wdatainit.loc[wdatainit.PIN.isnull() == False]
    wscoreinit = wscoreinit.loc[wscoreinit.PIN.isnull() == False]
else:
    print('Found Non-Identical Duplications')
    print(dlist)
    print(slist)

w=findpairs(wdatainit,wscoreinit) #this is the list of ids in both scored and raw data
len(wdatainit.PIN.unique())
len(wscoreinit.PIN.unique())

#of those that passed, check to see if they already in curated.  If so, confirm that RA wanted to replace
#data otherwise, send back to site for confirmation
curdata,curscores=box2dataframe(fileid=curated) #301 in each now
droplist=[]

orig_stdout = sys.stdout
f = open(os.path.join(box_temp,'WashUDevelopment_QC_Corrected_'+snapshotdate+'.txt'),'a')
sys.stdout = f
for i in curscores.loc[curscores.PIN.isin(w)].PIN.unique():
    print('Checking files in CURATED folder for PIN='+i + ' against Trello list membership')
    if i in list(removelist.PIN):
        print('Status : PASS')
    else:
        print(i + ' Is not in List to Remove from Curated: please review Trello card and move to approp list')
        droplist=droplist+[i]
        data4process.loc[data4process.PIN==i,'Fail']=1
        scores4process.loc[scores4process.PIN==i,'Fail']=1
        print('Status : FAIL')
    print('##################################################')

print('Summary of Failures')
print(data4process.loc[data4process.Fail==1])
print(scores4process.loc[scores4process.Fail==1])
print('##################################################')
sys.stdout = orig_stdout
f.close()
droplist
data4process.groupby('Fail').count()
scores4process.groupby('Fail').count()

wdatainit.loc[~(wdatainit.PIN.isin(droplist))].to_csv(box_temp+'/wudPASSED_corrected_data'+snapshotdate+'.csv')
wscoreinit.loc[~(wscoreinit.PIN.isin(droplist))].to_csv(box_temp+'/wudPASSED_corrected_scores'+snapshotdate+'.csv')
box.upload_file(box_temp+'/wudPASSED_corrected_data'+snapshotdate+'.csv',WashuD)
box.upload_file(box_temp+'/wudPASSED_corrected_scores'+snapshotdate+'.csv',WashuD)

##################################################################################################
WashuA=84799623206
curated=82804729845
wuafiles, wuafolders=foldercontents(WashuA)
data4process=wuafiles.loc[(wuafiles.filename.str.contains('aw_')==True) | (wuafiles.filename.str.contains('Raw')==True)]
scores4process=wuafiles.loc[wuafiles.filename.str.contains('core')==True]
data4process['PIN']=data4process.filename.str[:13]
scores4process['PIN']=scores4process.filename.str[:13]
data4process['Fail']=1
scores4process['Fail']=1

#run the validator for each pair of files in the Corrected data - write log to a file
orig_stdout = sys.stdout
f = open(os.path.join(box_temp,'WashUAging_QC_Corrected_'+snapshotdate+'.txt'),'w')
sys.stdout = f
for p in data4process.PIN:
    checkd, checks=validpair(p)
sys.stdout = orig_stdout
f.close()

data4process.groupby('Fail').count()

wadatainit=catcontents(data4process.loc[data4process.Fail==0],box_temp)
wascoreinit=catcontents(scores4process.loc[scores4process.Fail==0],box_temp)
wadatainit.PIN=wadatainit.PIN.str.strip()
wascoreinit.PIN=wascoreinit.PIN.str.strip()

dlist,slist=findwierdos(wadatainit,wascoreinit)

if dlist.empty and slist.empty:
    wadatainit=wadatainit.drop_duplicates(subset={'PIN','Inst','ItemID','Position'},keep='first')
    wascoreinit=wascoreinit.drop_duplicates(subset={'PIN','Inst'})
    wadatainit = wadatainit.loc[wadatainit.PIN.isnull() == False]
    wascoreinit = wascoreinit.loc[wascoreinit.PIN.isnull() == False]
else:
    print('Found Non-Identical Duplications')
    print(dlist)
    print(slist)

w=findpairs(wadatainit,wascoreinit) #this is the list of ids in both scored and raw data
len(wascoreinit.PIN.unique())==len(wadatainit.PIN.unique())
len(wascoreinit.PIN.unique())

#of those that passed, check to see if they already in curated.  If so, confirm that RA wanted to replace
#data otherwise, send back to site for confirmation
curdata,curscores=box2dataframe(fileid=curated)
droplist=[]

orig_stdout = sys.stdout
f = open(os.path.join(box_temp,'WashUAging_QC_Corrected_'+snapshotdate+'.txt'),'a')
sys.stdout = f
for i in curscores.loc[curscores.PIN.isin(w)].PIN.unique():
    print('Checking files in CURATED folder for PIN='+i + ' against Trello list membership')
    if i in list(removelist.PIN):
        print('Status : PASS')
    else:
        print(i + ' Is not in List to Remove from Curated: please review Trello card and move to approp list')
        droplist=droplist+[i]
        data4process.loc[data4process.PIN==i,'Fail']=1
        scores4process.loc[scores4process.PIN==i,'Fail']=1
        print('Status : FAIL')
    print('##################################################')

print('Summary of Failures')
print(data4process.loc[data4process.Fail==1])
print(scores4process.loc[scores4process.Fail==1])
print('##################################################')
sys.stdout = orig_stdout
f.close()
droplist
data4process.groupby('Fail').count()
scores4process.groupby('Fail').count()


wadatainit.loc[~(wadatainit.PIN.isin(droplist))].to_csv(box_temp+'/wuaPASSED_corrected_data'+snapshotdate+'.csv')
wascoreinit.loc[~(wascoreinit.PIN.isin(droplist))].to_csv(box_temp+'/wuaPASSED_corrected_scores'+snapshotdate+'.csv')
box.upload_file(box_temp+'/wuaPASSED_corrected_data'+snapshotdate+'.csv',WashuA)
box.upload_file(box_temp+'/wuaPASSED_corrected_scores'+snapshotdate+'.csv',WashuA)


##########################
Harvard=84800505740
harvardfiles, harvardfolders=foldercontents(Harvard)
harvardfoldersnew=harvardfolders.loc[~(harvardfolders.foldername=='incorporated')]

harvardfiles2, harvardfolders2=folderlistcontents(harvardfoldersnew.foldername,harvardfoldersnew.folder_id)
harvardfiles=harvardfiles2.copy()
data4process=harvardfiles.loc[(harvardfiles.filename.str.contains('aw_')==True) | (harvardfiles.filename.str.contains('Raw')==True)]
scores4process=harvardfiles.loc[harvardfiles.filename.str.contains('core')==True]
data4process['PIN']=data4process.filename.str[:13]
scores4process['PIN']=scores4process.filename.str[:13]
data4process['Fail']=1
scores4process['Fail']=1

#run the validator for each pair of files in the Corrected data - write log to a file
orig_stdout = sys.stdout
f = open(os.path.join(box_temp,'Harvard_QC_Corrected_'+snapshotdate+'.txt'),'w')
sys.stdout = f
for p in data4process.PIN:
    checkd, checks=validpair(p)
sys.stdout = orig_stdout
f.close()

data4process.groupby('Fail').count()

#stil nothing new to process at this time
####upload the concatenated files to site directory in box and move other files to incorporated
####hdatainit.to_csv(box_temp+'/harvard_corrected_data'+snapshotdate+'.csv')
####hscoreinit.to_csv(box_temp+'/harvard_corrected_scores'+snapshotdate+'.csv')
####box.upload_file(box_temp+'/harvard_corrected_data'+snapshotdate+'.csv',Harvard)
####box.upload_file(box_temp+'/harvard_corrected_scores'+snapshotdate+'.csv',Harvard)
####all files associated with this snapshotdate moved to incorporated_snapshotdate folder under this
####corrected folder


#########################################
###CANT ADD NEW DATA FROM MGH BECAUSE UPLOADED AS XLS
###again can't upload because uploaded as gsheet.
MGH=84799213727
mghfiles, mghfolders=foldercontents(MGH)

#petra to send request to update file format for HCA6826989_V1 trello card

####data4process=mghfiles.loc[(mghfiles.filename.str.contains('Data')==True) | (mghfiles.filename.str.contains('Raw')==True)]
####scores4process=mghfiles.loc[mghfiles.filename.str.contains('Score')==True]
####box.download_files(data4process.file_id)
####box.download_files(scores4process.file_id)

####mdatainit=catcontents(data4process,box_temp)
####mscoreinit=catcontents(scores4process,box_temp)

####dlist,slist=findwierdos(mdatainit,mscoreinit)

####if dlist.empty and slist.empty:
####    mdatainit=mdatainit.drop_duplicates(subset={'PIN','Inst','ItemID','Position'},keep='first')
####    mscoreinit=mscoreinit.drop_duplicates(subset={'PIN','Inst'})
####else:
####    print('Found Non-Identical Duplications')
####    print(dlist)
####    print(slist)

####m=findpairs(mdatainit,mscoreinit) #this is the list of ids in both scored and raw data

#####upload the concatenated files to site directory in box and move other files to incorporated
####mdatainit.to_csv(box_temp+'/mgh_corrected_data'+snapshotdate+'.csv')
####mscoreinit.to_csv(box_temp+'/mgh_corrected_scores'+snapshotdate+'.csv')
####box.upload_file(box_temp+'/mgh_corrected_data'+snapshotdate+'.csv',MGH)
#box.upload_file(box_temp+'/mgh_corrected_scores'+snapshotdate+'.csv',MGH)
####
#all files associated with this snapshotdate moved to incorporated_snapshotdate folder under this
#corrected folder


##########################################################################################################
#ANY? OF THE UMN FILES UPLOADED TO CORRECTED HAVE HEADERS...SIGH
#no new data this round...all still missing headers

umnD=84799525828
curated=82805151056
umnDfiles, umnDfolders=foldercontents(umnD)

data4process=umnDfiles.loc[(umnDfiles.filename.str.contains('Data')==True) | (umnDfiles.filename.str.contains('Raw')==True)]
scores4process=umnDfiles.loc[umnDfiles.filename.str.contains('core')==True]
data4process['PIN']=data4process.filename.str[:13]
scores4process['PIN']=scores4process.filename.str[:13]
data4process['Fail']=1
scores4process['Fail']=1

#run the validator for each pair of files in the Corrected data - write log to a file
orig_stdout = sys.stdout
f = open(os.path.join(box_temp,'UMN_Development_QC_Corrected_'+snapshotdate+'.txt'),'w')
sys.stdout = f
for p in data4process.PIN:
    checkd, checks=validpair(p)
sys.stdout = orig_stdout
f.close()

data4process.groupby('Fail').count()

#subset to files that passed basic QC for next round
udatainit=catcontents(data4process.loc[data4process.Fail==0],box_temp)
uscoreinit=catcontents(scores4process.loc[scores4process.Fail==0],box_temp)
udatainit.PIN=udatainit.PIN.str.strip()
uscoreinit.PIN=uscoreinit.PIN.str.strip()


#dlist and slist shoult be empty
dlist,slist=findwierdos(udatainit,uscoreinit)

if dlist.empty and slist.empty:
    udatainit=udatainit.drop_duplicates(subset={'PIN','Inst','ItemID','Position'},keep='first')
    uscoreinit=uscoreinit.drop_duplicates(subset={'PIN','Inst'})
    udatainit = udatainit.loc[udatainit.PIN.isnull() == False]
    uscoreinit = uscoreinit.loc[uscoreinit.PIN.isnull() == False]

else:
    print('Found Non-Identical Duplications')
    print(dlist)
    print(slist)

w=findpairs(udatainit,uscoreinit) #this is the list of ids in both scored and raw data
len(uscoreinit.PIN.unique())
len(udatainit.PIN.unique())

#of those that passed, check to see if they already in curated.  If so, confirm that RA wanted to replace
#data otherwise, send back to site for confirmation
curdata,curscores=box2dataframe(fileid=curated) #301 in each now
droplist=[]

orig_stdout = sys.stdout
f = open(os.path.join(box_temp,'UMN_Development_QC_Corrected_'+snapshotdate+'.txt'),'a')
sys.stdout = f
for i in curscores.loc[curscores.PIN.isin(w)].PIN.unique():
    print('Checking files in CURATED folder for PIN='+i + ' against Trello list membership')
    if i in list(removelist.PIN):
        print('Status : PASS')
    else:
        print(i + ' Is not in List to Remove from Curated: please review Trello card and move to approp list')
        droplist=droplist+[i]
        data4process.loc[data4process.PIN==i,'Fail']=1
        scores4process.loc[scores4process.PIN==i,'Fail']=1
        print('Status : FAIL')
    print('##################################################')

print('Summary of Failures')
print(data4process.loc[data4process.Fail==1])
print(scores4process.loc[scores4process.Fail==1])
print('##################################################')
sys.stdout = orig_stdout
f.close()
droplist
data4process.groupby('Fail').count()
scores4process.groupby('Fail').count()


udatainit.loc[~(udatainit.PIN.isin(droplist))].to_csv(box_temp+'/umndPASSED_corrected_data'+snapshotdate+'.csv')
uscoreinit.loc[~(uscoreinit.PIN.isin(droplist))].to_csv(box_temp+'/umndPASSED_corrected_scores'+snapshotdate+'.csv')
box.upload_file(box_temp+'/umndPASSED_corrected_data'+snapshotdate+'.csv',umnD)
box.upload_file(box_temp+'/umndPASSED_corrected_scores'+snapshotdate+'.csv',umnD)


######################################################
umnA=84799599800
curated=82803665867
umnafiles, umnafolders=foldercontents(umnA)
umnafiles2, umnafolders2=folderlistcontents(umnafolders.loc[~(umnafolders.foldername=='incorporated')].foldername,umnafolders.loc[~(umnafolders.foldername=='incorporated')].folder_id)
umnafiles=pd.concat([umnafiles,umnafiles2],axis=0,sort=True)

data4process=umnafiles.loc[umnafiles.filename.str.contains('Raw')==True]
scores4process=umnafiles.loc[umnafiles.filename.str.contains('Score')==True]
data4process['PIN']=data4process.filename.str[:13]
scores4process['PIN']=scores4process.filename.str[:13]
data4process['Fail']=1
scores4process['Fail']=1

#run the validator for each pair of files in the Corrected data - write log to a file
orig_stdout = sys.stdout
f = open(os.path.join(box_temp,'UMN_Aging_QC_Corrected_'+snapshotdate+'.txt'),'w')
sys.stdout = f
for p in data4process.PIN:
    checkd, checks=validpair(p)
sys.stdout = orig_stdout
f.close()

data4process.groupby('Fail').count()

umadatainit=catcontents(data4process.loc[data4process.Fail==0],box_temp)
umascoreinit=catcontents(scores4process.loc[scores4process.Fail==0],box_temp)
umadatainit.PIN=umadatainit.PIN.str.strip()
umascoreinit.PIN=umascoreinit.PIN.str.strip()


#dlist and slist shoult be empty
dlist,slist=findwierdos(umadatainit,umascoreinit)
if dlist.empty and slist.empty:
    umadatainit=umadatainit.drop_duplicates(subset={'PIN','Inst','ItemID','Position'},keep='first')
    umascoreinit=umascoreinit.drop_duplicates(subset={'PIN','Inst'})
    umadatainit = umadatainit.loc[umadatainit.PIN.isnull() == False]
    umascoreinit = umascoreinit.loc[umascoreinit.PIN.isnull() == False]

else:
    print('Found Non-Identical Duplications')
    print(dlist)
    print(slist)


w=findpairs(umadatainit,umascoreinit) #this is the list of ids in both scored and raw data
len(umadatainit.PIN.unique())
len(umascoreinit.PIN.unique())

#of those that passed, check to see if they already in curated.  If so, confirm that RA wanted to replace
#data otherwise, send back to site for confirmation
curdata,curscores=box2dataframe(fileid=curated)
droplist=[]

orig_stdout = sys.stdout
f = open(os.path.join(box_temp,'UMN_Aging_QC_Corrected_'+snapshotdate+'.txt'),'a')
sys.stdout = f
for i in curscores.loc[curscores.PIN.isin(w)].PIN.unique():
    print('Checking files in CURATED folder for PIN='+i + ' against Trello list membership')
    if i in list(removelist.PIN):
        print('Status : PASS')
    else:
        print(i + ' Is not in List to Remove from Curated: please review Trello card and move to approp list')
        droplist=droplist+[i]
        data4process.loc[data4process.PIN==i,'Fail']=1
        scores4process.loc[scores4process.PIN==i,'Fail']=1
        print('Status : FAIL')
    print('##################################################')

print('Summary of Failures')
print(data4process.loc[data4process.Fail==1])
print(scores4process.loc[scores4process.Fail==1])
print('##################################################')
sys.stdout = orig_stdout
f.close()
droplist
data4process.groupby('Fail').count()
scores4process.groupby('Fail').count()


umadatainit.loc[~(umadatainit.PIN.isin(droplist))].to_csv(box_temp+'/umnaPASSED_corrected_data'+snapshotdate+'.csv')
umascoreinit.loc[~(umascoreinit.PIN.isin(droplist))].to_csv(box_temp+'/umnaPASSED_corrected_scores'+snapshotdate+'.csv')
box.upload_file(box_temp+'/umnaPASSED_corrected_data'+snapshotdate+'.csv',umnA)
box.upload_file(box_temp+'/umnaPASSED_corrected_scores'+snapshotdate+'.csv',umnA)

######################################################
uclaA=84799075673
curated=82807223120
uclaAfiles, uclaAfolders=foldercontents(uclaA)
data4process=uclaAfiles.loc[uclaAfiles.filename.str.contains('Raw')==True]
scores4process=uclaAfiles.loc[uclaAfiles.filename.str.contains('Score')==True]

data4process['PIN']=data4process.filename.str[:13]
scores4process['PIN']=scores4process.filename.str[:13]
data4process['Fail']=1
scores4process['Fail']=1

orig_stdout = sys.stdout
f = open(os.path.join(box_temp,'UCLA_Aging_QC_Corrected_'+snapshotdate+'.txt'),'w')
sys.stdout = f
print('Checking that both Scores and Raw data uploaded for given PIN=')

droplist=[]
for p in data4process.PIN.unique():
    if p in scores4process.PIN.unique():
        pass
    else:
        print(p+' Missing scores file')
        print('Status: FAIL')
        droplist=droplist+[p]

droplist=[]
for p in scores4process.PIN.unique():
    if p in data4process.PIN.unique():
        pass
    else:
        print(p+' Missing Raw/Data file')
        print('Status: FAIL')
        droplist=droplist+[p]
print('##################################################')

data4process=data4process.loc[~(data4process.PIN.isin(droplist))]
scores4process=scores4process.loc[~(scores4process.PIN.isin(droplist))]

#run the validator for each pair of files in the Corrected data - write log to a file
for p in data4process.PIN:
    checkd, checks=validpair(p)
sys.stdout = orig_stdout
f.close()

data4process.groupby('Fail').count()

#subset to files that passed basic QC for next round
uadatainit=catcontents(data4process.loc[data4process.Fail==0],box_temp)
uascoreinit=catcontents(scores4process.loc[scores4process.Fail==0],box_temp)
uadatainit['PIN']=uadatainit.PIN.str.strip()
uascoreinit['PIN']=uascoreinit.PIN.str.strip()

#dlist and slist shoult be empty
dlist,slist=findwierdos(uadatainit,uascoreinit)

if dlist.empty and slist.empty:
    uadatainit=uadatainit.drop_duplicates(subset={'PIN','Inst','ItemID','Position'},keep='first')
    uascoreinit=uascoreinit.drop_duplicates(subset={'PIN','Inst'})
    uadatainit = uadatainit.loc[uadatainit.PIN.isnull() == False]
    uascoreinit = uascoreinit.loc[uascoreinit.PIN.isnull() == False]

else:
    print('Found Non-Identical Duplications')
    print(dlist)
    print(slist)


w=findpairs(uadatainit,uascoreinit) #this is the list of ids in both scored and raw data
#keep the ones that have no nan pins
len(uadatainit.PIN.unique())
len(uascoreinit.PIN.unique())

#of those that passed, check to see if they already in curated.  If so, confirm that RA wanted to replace
#data otherwise, send back to site for confirmation
curdata,curscores=box2dataframe(fileid=curated) #301 in each now
droplist=[]

orig_stdout = sys.stdout
f = open(os.path.join(box_temp,'UCLA_Aging_QC_Corrected_'+snapshotdate+'.txt'),'a')
sys.stdout = f
for i in curscores.loc[curscores.PIN.isin(w)].PIN.unique():
    print('Checking files in CURATED folder for PIN='+i + ' against Trello list membership')
    if i in list(removelist.PIN):
        print('Status : PASS')
    else:
        print(i + ' Is not in List to Remove from Curated: please review Trello card and move to approp list')
        droplist=droplist+[i]
        data4process.loc[data4process.PIN==i,'Fail']=1
        scores4process.loc[scores4process.PIN==i,'Fail']=1
        print('Status : FAIL')
    print('##################################################')

print('Summary of Failures')
print(data4process.loc[data4process.Fail==1])
print(scores4process.loc[scores4process.Fail==1])
print('##################################################')
sys.stdout = orig_stdout
f.close()
droplist
data4process.groupby('Fail').count()
scores4process.groupby('Fail').count()


uadatainit.loc[~(uadatainit.PIN.isin(droplist))].to_csv(box_temp+'/uclaaPASSED_corrected_data'+snapshotdate+'.csv')
uascoreinit.loc[~(uascoreinit.PIN.isin(droplist))].to_csv(box_temp+'/uclaaPASSED_corrected_scores'+snapshotdate+'.csv')
box.upload_file(box_temp+'/uclaaPASSED_corrected_data'+snapshotdate+'.csv',uclaA)
box.upload_file(box_temp+'/uclaaPASSED_corrected_scores'+snapshotdate+'.csv',uclaA)

######################################################
uclaD=84800272537
curated=82805124019
uclaDfiles, uclaDfolders=foldercontents(uclaD)
data4process=uclaDfiles.loc[uclaDfiles.filename.str.contains('Raw')==True]
scores4process=uclaDfiles.loc[uclaDfiles.filename.str.contains('Score')==True]

data4process['PIN']=data4process.filename.str[:13]
scores4process['PIN']=scores4process.filename.str[:13]
data4process['Fail']=1
scores4process['Fail']=1


#run the validator for each pair of files in the Corrected data - write log to a file
orig_stdout = sys.stdout
f = open(os.path.join(box_temp,'UCLA_Development_QC_Corrected_'+snapshotdate+'.txt'),'w')
sys.stdout = f
for p in data4process.PIN:
    checkd, checks=validpair(p)
sys.stdout = orig_stdout
f.close()

data4process.groupby('Fail').count()

#subset to files that passed basic QC for next round
uddatainit=catcontents(data4process.loc[data4process.Fail==0],box_temp)
udscoreinit=catcontents(scores4process.loc[scores4process.Fail==0],box_temp)
uddatainit['PIN']=uddatainit.PIN.str.strip()
udscoreinit['PIN']=udscoreinit.PIN.str.strip()


#dlist and slist shoult be empty
dlist,slist=findwierdos(uddatainit,udscoreinit)
if dlist.empty and slist.empty:
    uddatainit=uddatainit.drop_duplicates(subset={'PIN','Inst','ItemID','Position'},keep='first')
    udscoreinit=udscoreinit.drop_duplicates(subset={'PIN','Inst'})
    uddatainit = uddatainit.loc[uddatainit.PIN.isnull() == False]
    udscoreinit = udscoreinit.loc[udscoreinit.PIN.isnull() == False]
else:
    print('Found Non-Identical Duplications')
    print(dlist)
    print(slist)

w=findpairs(uddatainit,udscoreinit) #this is the list of ids in both scored and raw data
len(uddatainit.PIN.unique())
len(udscoreinit.PIN.unique())

#of those that passed, check to see if they already in curated.  If so, confirm that RA wanted to replace
#data otherwise, send back to site for confirmation
curdata,curscores=box2dataframe(fileid=curated)
droplist=[]

orig_stdout = sys.stdout
f = open(os.path.join(box_temp,'UCLA_Development_QC_Corrected_'+snapshotdate+'.txt'),'a')
sys.stdout = f
for i in curscores.loc[curscores.PIN.isin(w)].PIN.unique():
    print('Checking files in CURATED folder for PIN='+i + ' against Trello list membership')
    if i in list(removelist.PIN):
        print('Status : PASS')
    else:
        print(i + ' Is not in List to Remove from Curated: please review Trello card and move to approp list')
        droplist=droplist+[i]
        data4process.loc[data4process.PIN==i,'Fail']=1
        scores4process.loc[scores4process.PIN==i,'Fail']=1
        print('Status : FAIL')
    print('##################################################')

print('Summary of Failures')
print(data4process.loc[data4process.Fail==1])
print(scores4process.loc[scores4process.Fail==1])
print('##################################################')
sys.stdout = orig_stdout
f.close()
droplist
data4process.groupby('Fail').count()
scores4process.groupby('Fail').count()


uddatainit.loc[~(uddatainit.PIN.isin(droplist))].to_csv(box_temp+'/ucladPASSED_corrected_data'+snapshotdate+'.csv')
udscoreinit.loc[~(udscoreinit.PIN.isin(droplist))].to_csv(box_temp+'/ucladPASSED_corrected_scores'+snapshotdate+'.csv')
box.upload_file(box_temp+'/ucladPASSED_corrected_data'+snapshotdate+'.csv',uclaD)
box.upload_file(box_temp+'/ucladPASSED_corrected_scores'+snapshotdate+'.csv',uclaD)


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

