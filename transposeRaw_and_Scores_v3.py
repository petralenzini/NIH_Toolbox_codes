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
snapshotdate = datetime.datetime.today().strftime('%m_%d_%Y')
#catfromdate=max of last run--'2019-06-17'
redcapconfigfile="/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/PycharmToolbox/.boxApp/redcapconfig.csv"

box_temp='/home/petra/UbWinSharedSpace1/boxtemp'
pathout="/home/petra/UbWinSharedSpace1/redcap2nda_Lifespan2019/NIH_toolbox_crosswalk_docs/HCPD/prepped_structures"
box = LifespanBox(cache=box_temp)

#prep the fields that NDA requires in all of their structures
subjectlist='/home/petra/UbWinSharedSpace1/redcap2nda_Lifespan2019/Dev_pedigrees/UnrelatedHCAHCD_w_STG_Image_and_pseudo_GUID09_27_2019.csv'
subjects=pd.read_csv(subjectlist)[['subjectped','nda_gender', 'nda_guid', 'nda_interview_age', 'nda_interview_date']]
ndar=subjects.loc[subjects.subjectped.str.contains('HCD')].rename(
    columns={'nda_guid':'subjectkey','subjectped':'src_subject_id','nda_interview_age':'interview_age',
             'nda_interview_date':'interview_date','nda_gender':'gender'}).copy()
ndar['interview_date'] = pd.to_datetime(ndar['interview_date']).dt.strftime('%m/%d/%Y')
ndarlist=['subjectkey','src_subject_id','interview_age','interview_date','gender']
#establish the crosswalk
crosswalkfile="/home/petra/UbWinSharedSpace1/redcap2nda_Lifespan2019/NIH_toolbox_crosswalk_docs/HCPD/NIH_Toolbox_crosswalk_HCPD_18Feb2020.csv"
crosswalk=pd.read_csv(crosswalkfile,header=0,low_memory=False)

#import the data
scores=545436452523 #new corrected data from sites (all combined)
raw=545440261404  #new corrected data from sites (all combined)
scordata=Box2dataframe(scores,box_temp)
rawdata=Box2dataframe(raw,box_temp)
#subset to correct visit data...
scordata=scordata.loc[scordata.visit=='V1'].drop(columns=['gender']).copy()
rawdata=rawdata.loc[rawdata.visit=='V1'].drop(columns=['gender']).copy()
#merge with the required fields
scordata=pd.merge(scordata,ndar,how='inner',left_on='subject', right_on='src_subject_id')
rawdata=pd.merge(rawdata,ndar,how='inner',left_on='subject', right_on='src_subject_id')
#this is the list of variables in thes cored data files that you might need..
scorlist=['Age-Corrected Standard Score', 'Age-Corrected Standard Scores Dominant',
 'Age-Corrected Standard Scores Non-Dominant', 'AgeCorrCrystal', 'AgeCorrDCCS', 'AgeCorrEarly',
 'AgeCorrEngRead', 'AgeCorrEngVocab', 'AgeCorrFlanker', 'AgeCorrFluid', 'AgeCorrListSort',
 'AgeCorrPSM', 'AgeCorrPatternComp', 'AgeCorrTotal', 'Assessment Name', 'Computed Score',
 'ComputedDCCS', 'ComputedEngRead', 'ComputedEngVocab', 'ComputedFlanker', 'ComputedPSM',
 'ComputedPatternComp', 'DCCSaccuracy', 'DCCSreactiontime',  'Dominant Score', 'FlankerAccuracy',
 'FlankerReactionTime', 'FullTCrystal', 'FullTDCCS', 'FullTEarly', 'FullTEngRead', 'FullTEngVocab',
 'FullTFlanker', 'FullTFluid', 'FullTListSort', 'FullTPSM', 'FullTPatternComp', 'FullTTotal',
 'Fully-Corrected T-score', 'Fully-Corrected T-scores Dominant', 'Fully-Corrected T-scores Non-Dominant',
 'FullyCorrectedTscore', 'Group', 'Inst', 'InstrumentBreakoff', 'InstrumentRCReason', 'InstrumentRCReasonOther',
 'InstrumentStatus2', 'ItmCnt', 'Language', 'Male', 'National Percentile (age adjusted)',
 'National Percentile (age adjusted) Dominant', 'National Percentile (age adjusted) Non-Dominant',
 'Non-Dominant Score', 'PIN', 'Raw Score Left Ear', 'Raw Score Right Ear', 'RawDCCS',
 'RawFlanker', 'RawListSort', 'RawPSM', 'RawPatternComp', 'RawScore', 'SE', 'Static Visual Acuity Snellen',
 'Static Visual Acuity logMAR', 'TScore', 'Theta', 'ThetaEngRead', 'ThetaEngVocab', 'ThetaPSM', 'Threshold Left Ear',
 'Threshold Right Ear', 'UncorrCrystal', 'UncorrDCCS', 'UncorrEarly', 'UncorrEngRead', 'UncorrEngVocab',
 'UncorrFlanker', 'UncorrFluid', 'UncorrListSort', 'UncorrPSM', 'UncorrPatternComp', 'UncorrTotal',
 'Uncorrected Standard Score', 'Uncorrected Standard Scores Dominant', 'Uncorrected Standard Scores Non-Dominant',
 'UncorrectedStandardScore']
#check that lengths are the same...indicating one to one PIN match between scores and raw
len(rawdata.PIN.unique())
len(scordata.PIN.unique())
#check that shape is same before and after removing duplicates (should not be any)
rawdata.shape
scordata.shape
testraw=rawdata.drop_duplicates(subset={'PIN','Inst','ItemID','Position'},keep='first')
testscore=scordata.drop_duplicates(subset={'PIN','Inst'})
testraw.shape
testscore.shape

#check that your instruments are in both raw data and scores files.
for i in rawdata.Inst.unique():
    if i not in scordata.Inst.unique():
        print(i)

#Within the rawdata structure (for HCP), all but the NIH Toolbox Pain Intensity FF Age 18+ v2.0 Instrument are practices
#So only the Pain Intensity instrument needed special coding attention
#check your data and adjust if needed
#create the NDA structure for this special case
inst_i='NIH Toolbox Pain Intensity FF Age 18+ v2.0'
paindata=rawdata.loc[rawdata.Inst==inst_i][['PIN','subject','Inst','visit','ItemID','Position',
        'subjectkey','src_subject_id','interview_age','interview_date','gender',
        'Response','ResponseTime', 'SE', 'Score', 'TScore','Theta']]
paindata.ItemID = paindata.ItemID.str.lower().str.replace('-','_').str.replace('(','_').str.replace(')','_')
inst = paindata.pivot(index='PIN', columns='ItemID', values='Response').reset_index()
meta = paindata.drop_duplicates(subset=['PIN', 'visit'])
painreshaped = pd.merge(meta, inst, on='PIN', how='inner').drop(columns={'subject','visit','PIN'})
crosswalk_subset=crosswalk.loc[crosswalk['Inst']==inst_i]
crosswalk_subset.reset_index(inplace=True)
cwlist=list(crosswalk_subset['hcp_variable'])
reshapedslim=painreshaped[ndarlist+cwlist]
data2struct(pathout,dout=reshapedslim,crosssub=crosswalk_subset,study='HCPD')

crosswalkfile="/home/petra/UbWinSharedSpace1/redcap2nda_Lifespan2019/NIH_toolbox_crosswalk_docs/HCPD/NIH_Toolbox_crosswalk_HCPD_18Feb2020.csv"
crosswalk=pd.read_csv(crosswalkfile,header=0,low_memory=False)

#inst_i="Psychological Well Being Parent Report Summary (3-7)"
inst_i="NIH Toolbox Anger-Hostility FF Age 18+ v2.0"
#for instruments in both
for i in scordata.Inst.unique():
    if i in rawdata.Inst.unique():
        inst_i=i
        if "Visual Acuity" in inst_i:
            pass  #special case--see below
        elif "Practice" in inst_i:
            print("Note:  Omitting practice instrument, "+inst_i)
        else:
            try:  #this will fail if there are duplicates or if no-one has the data of interest (e.g. idlist too small), or if only V2 instrument
                #print('Processing '+inst_i+'...')
                items=rawdata.loc[rawdata.Inst==inst_i][['PIN','subject','Inst','visit','ItemID','Position',
                   'subjectkey','src_subject_id','interview_age','interview_date','gender',
                   'Response','ResponseTime']]# not these..., 'SE', 'Score', 'TScore','Theta']]
                items.ItemID = items.ItemID.str.lower().str.replace('-','_').str.replace('(','_').str.replace(')','_').str.replace(' ','_')
                inst=items.pivot(index='PIN',columns='ItemID',values='Response').reset_index()
                meta=items.drop_duplicates(subset=['PIN','visit'])
                instreshaped = pd.merge(meta, inst, on='PIN', how='inner').drop(columns={'subject', 'visit','Inst'})
                items2=scordata.loc[scordata.Inst==inst_i][scorlist]
                instreshapedfull=pd.merge(instreshaped,items2,on='PIN',how='inner')
                sendthroughcrosswalk(pathout,instreshapedfull, inst_i, crosswalk)
                #instreshapedfull = instreshapedfull.dropna(how='all', axis=1)
                #filecsv = box_temp + '/ndaformat/' + inst_i + '.csv'
                #dictcsv = box_temp + '/ndaformat/' + 'Dictionary' + inst_i + '.csv'
                #instreshapedfull.to_csv(filecsv, index=False)
                #makedatadict(filecsv, dictcsv,inst_i)
            except:
                print('Couldnt process '+inst_i+'...')

inst_i="Negative Psychosocial Functioning Parent Report Summary (3-7)"
#instruments in score but not raw
for i in scordata.Inst.unique():
    if i not in rawdata.Inst.unique():
        inst_i=i
        if "Practice" in inst_i:
            print("Note:  Omitting practice instrument, "+inst_i)
        elif "Cognition" in inst_i:
            pass #another special case--see below
        else:
            try:
                print('Processing '+inst_i+'...')
                items2 = scordata.loc[scordata.Inst == inst_i][scorlist+ndarlist]
                instreshapedfull=items2
                sendthroughcrosswalk(pathout,instreshapedfull,inst_i,crosswalk)
                # instreshapedfull = instreshapedfull.dropna(how='all', axis=1)
                # filecsv = box_temp + '/ndaformat/' + inst_i + '.csv'
                # dictcsv = box_temp + '/ndaformat/' + 'Dictionary' + inst_i + '.csv'
                # instreshapedfull.to_csv(filecsv, index=False)
                # makedatadict(filecsv, dictcsv,inst_i)
            except:
                print('Couldnt process '+inst_i+'...')


#special case for Cognition Composite scores - going to cogcomp01 structure - mapped before Leo agreed to accept by Instrument name
#keeping in for posterity and to shed light on one type of merge he must do on his end, when it comes to NIH toolbox data
cogcompdata=scordata.loc[scordata.Inst.str.contains('Cognition')==True][['PIN','Language',
    'Assessment Name','Inst',  'Uncorrected Standard Score', 'Age-Corrected Standard Score',
    'National Percentile (age adjusted)', 'Fully-Corrected T-score']+ndarlist]

#initialize prefix
cogcompdata['varprefix']='test'
cogcompdata.loc[cogcompdata.Inst=='Cognition Crystallized Composite v1.1','varprefix']='nih_crystalcogcomp_'
cogcompdata.loc[cogcompdata.Inst=='Cognition Early Childhood Composite v1.1','varprefix']='nih_eccogcomp_'
cogcompdata.loc[cogcompdata.Inst=='Cognition Fluid Composite v1.1','varprefix']='nih_fluidcogcomp_'
cogcompdata.loc[cogcompdata.Inst=='Cognition Total Composite Score v1.1','varprefix']='nih_totalcogcomp_'
#pivot the vars of interest by varprefix and rename
uncorr=cogcompdata.pivot(index='PIN',columns='varprefix',values='Uncorrected Standard Score')
for col in uncorr.columns.values:
    uncorr=uncorr.rename(columns={col:col+"unadjusted"})
ageadj=cogcompdata.pivot(index='PIN',columns='varprefix',values='Age-Corrected Standard Score')
for col in ageadj.columns.values:
    ageadj=ageadj.rename(columns={col:col+"ageadj"})
npage=cogcompdata.pivot(index='PIN',columns='varprefix',values='National Percentile (age adjusted)')
for col in npage.columns.values:
    npage=npage.rename(columns={col:col+"np_ageadj"})
#put them together
cogcompreshape=pd.concat([uncorr,ageadj,npage],axis=1)
meta=cogcompdata[['PIN','Language','Assessment Name']+ndarlist].drop_duplicates(subset={'PIN'})
meta['nih_crystalcogcomp']='Cognition Crystallized Composite v1.1'
meta['nih_eccogcomp']='Cognition Early Childhood Composite v1.1'
meta['nih_fluidcogcomp']='Cognition Fluid Composite v1.1'
meta['nih_totalcogcomp']='Cognition Total Composite Score v1.1'
cogcompreshape=pd.merge(meta,cogcompreshape,on='PIN',how='inner')

inst_i='Cognition Composite Scores'
sendthroughcrosswalk(pathout,cogcompreshape,inst_i,crosswalk)


#special case for instruments with "Visual Acuity" in their titles, which have dup inst/itemid at diff positions
for i in scordata.Inst.unique():
    if i in rawdata.Inst.unique():
        inst_i=i
        if "Visual Acuity" in inst_i:
            print('Processing ' + inst_i + '...')
                #items=testraw.loc[testraw.Inst==inst_i][['PIN','subject','Inst','flagged','parent','site', 'study',
                #   'gender','v1_interview_date', 'visit','ItemID','Position','Response']]
                items=testraw.loc[testraw.Inst.str.contains('Visual Acuity')][['PIN','subject','Inst','flagged','parent','site', 'study',
                   'gender','v1_interview_date', 'visit','ItemID','Position','Response']]
                #initialize firstdup, isdup, tallydup
                items.ItemID = items.ItemID.str.lower()
                items['dup_number']=items.groupby(['PIN','ItemID']).cumcount()+1
                items['ItemID_Dup']=items.ItemID.str.replace('|', '_') + '_P'+items.dup_number.astype(str)
                inst=items.pivot(index='PIN',columns='ItemID_Dup',values='Response')
                meta = items.drop_duplicates(subset=['PIN', 'visit'])[['Inst', 'PIN', 'flagged', 'parent', 'site', 'study',
                                                               'subject', 'v1_interview_date', 'visit']]
                instreshaped = pd.merge(meta, inst, on='PIN', how='inner')
                items2 = testscore.loc[testscore.Inst == inst_i].drop(
                    ['FirstDate4PIN', 'flagged', 'gender', 'v1_interview_date', 'site',
                     'subject', 'raw_cat_date', 'study', 'Column1', 'Column2',
                     'Column3', 'Column4', 'Column5', 'Inst', 'DeviceID', 'source'], axis=1)
                instreshapedfull = pd.merge(instreshaped, items2, on='PIN', how='inner')
                # droppped flagged
                instreshapedfull = instreshapedfull.loc[instreshapedfull.flagged.isnull() == True]
                # drop parents
                instreshapedfull = instreshapedfull.loc[instreshapedfull.parent.isnull() == True]
                # drop if not v1 - this will results in dropping NIH Toolbox Picture Sequence Memory Test Age 8+ Form B v2.1
                instreshapedfull = instreshapedfull.loc[instreshapedfull.visit == 'V1']
                instreshapedfull = instreshapedfull.drop(columns={'App Version', 'iPad Version', 'Firmware Version','DateFinished',
                                                                  'InstrumentBreakoff','InstrumentStatus2','site','study','v1_interview_date','visit'})
                instreshapedfull = instreshapedfull.dropna(how='all', axis=1)
                filecsv = box_temp + '/ndaformat/' + inst_i + '.csv'
                dictcsv = box_temp + '/ndaformat/' + 'Dictionary' + inst_i + '.csv'
                instreshapedfull.to_csv(filecsv, index=False)
                makedatadict(filecsv, dictcsv,inst_i)
        else:
            print('Skipping ' + inst_i + '...')


#now validated all these files by running the following loop on the command line and checking status
#requires that you have downloaded and installed https://github.com/NDAR/nda-tools python package
#per instructions
#for var in pathout/*.csv; do vtcmd $var; done
#note that if any of the folders that leo sent had spaces in their names...these would transate to spaces in the filenames
#that are being validated at this time by way of the $var variable.
#remove spaces in inst_short and regenerate structures to avoid this type of error.
#grep notInteger /home/petra/NDAValidationResults/* > Notintegerwarnings

########################################################################
def sendthroughcrosswalk(pathout,instreshapedfull,inst_i,crosswalk,studystr='HCPD'):
    # replace special charaters in column names
    instreshapedfull.columns = instreshapedfull.columns.str.replace(' ', '_').str.replace('-', '_').str.replace('(','_').str.replace(')', '_')
    crosswalk_subset = crosswalk.loc[crosswalk['Inst'] == inst_i]
    crosswalk_subset.reset_index(inplace=True)
    # crosswalk_subset.loc[crosswalk_subset['hcp_variable_upload'].isnull()==False,'hcp_variable']
    cwlist = list(crosswalk_subset['hcp_variable'])
    before = len(cwlist)
    cwlist = list(set(cwlist) & set(
        instreshapedfull.columns))  # drop the handful of vars in larger instruments that got mapped but that we dont have
    after = len(cwlist)
    if before != after:
        print("WARNING!!! " + inst_i + ": Crosswalk expects " + str(before) + " elements, but only found " + str(after))
    studydata = instreshapedfull[ndarlist + cwlist].copy()
    # execute any specialty codes
    for index, row in crosswalk_subset.iterrows():
        if pd.isna(row['requested_python']):
            pass
        else:
            exec(row['requested_python'])
    uploadlist = list(crosswalk_subset['hcp_variable_upload'])
    uploadlist = list(set(uploadlist) & set(studydata.columns))
    data2struct(pathout,dout=studydata[ndarlist + uploadlist], crosssub=crosswalk_subset, study=studystr)


def data2struct(pathout,dout,crosssub,study='HCPD'):
    strucroot=crosssub['nda_structure'].str.strip().str[:-2][0]
    strucnum=crosssub['nda_structure'].str.strip().str[-2:][0]
    instshort=crosssub['inst_short'].str.strip()[0]
    filePath=os.path.join(pathout,study+'_'+instshort+'_'+strucroot+strucnum+'_'+snapshotdate+'.csv')
    if os.path.exists(filePath):
        os.remove(filePath)
    else:
        pass
        #print("Can not delete the file as it doesn't exists")
    with open(filePath,'a') as f:
        f.write(strucroot+","+str(int(strucnum))+"\n")
        dout.to_csv(f,index=False)


def getredcapfieldsjson(fieldlist, study='hcpdparent '):  # , token=token[0],field=field[0],event=event[0]):
    """
    Downloads requested fields from Redcap databases specified by details in redcapconfig file
    Returns panda dataframe with fields 'study', 'Subject_ID, 'subject', and 'flagged', where 'Subject_ID' is the
    patient id in the database of interest (sometimes called subject_id, parent_id) as well as requested fields.
    subject is this same id stripped of underscores or flags like 'excluded' to make it easier to merge
    flagged contains the extra characters other than the id so you can keep track of who should NOT be uploaded to NDA
    or elsewwhere shared
    """
    auth = pd.read_csv(redcapconfigfile)
    studydata = pd.DataFrame()
    fieldlistlabel = ['fields[' + str(i) + ']' for i in range(5, len(fieldlist) + 5)]
    fieldrow = dict(zip(fieldlistlabel, fieldlist))
    d1 = {'token': auth.loc[auth.study == study, 'token'].values[0], 'content': 'record', 'format': 'json', 'type': 'flat',
          'fields[0]': auth.loc[auth.study == study, 'field'].values[0],
          'fields[1]': auth.loc[auth.study == study, 'interview_date'].values[0],
          'fields[2]': auth.loc[auth.study == study, 'sexatbirth'].values[0],
          'fields[3]': auth.loc[auth.study == study, 'sitenum'].values[0],
          'fields[4]': auth.loc[auth.study == study, 'dobvar'].values[0]}
    d2 = fieldrow
    d3 = {'events[0]': auth.loc[auth.study == study, 'event'].values[0], 'rawOrLabel': 'raw', 'rawOrLabelHeaders': 'raw',
          'exportCheckboxLabel': 'false',
          'exportSurveyFields': 'false', 'exportDataAccessGroups': 'false', 'returnFormat': 'json'}
    data = {**d1, **d2, **d3}
    buf = BytesIO()
    ch = pycurl.Curl()
    ch.setopt(ch.URL, 'https://redcap.wustl.edu/redcap/srvrs/prod_v3_1_0_001/redcap/api/')
    ch.setopt(ch.HTTPPOST, list(data.items()))
    ch.setopt(ch.WRITEDATA, buf)
    ch.perform()
    ch.close()
    htmlString = buf.getvalue().decode('UTF-8')
    buf.close()
    d = json.loads(htmlString)
    #parent_ids = pd.DataFrame(htmlString.splitlines(), columns=['row'])
    #header = parent_ids.iloc[0]
    #headerv2 = header.str.replace(auth.loc[auth.study == study, 'interview_date'].values[0], 'interview_date')
    #headerv3 = headerv2.str.split(',')
    #parent_ids.drop([0], inplace=True)
    #pexpanded = pd.DataFrame(parent_ids.row.str.split(pat='\t').values.tolist(), columns=headerv3.values.tolist()[0])
    pexpanded=pd.DataFrame(d)
    pexpanded = pexpanded.loc[~(pexpanded[auth.loc[auth.study == study, 'field'].values[0]] == '')]  ##
    new = pexpanded[auth.loc[auth.study == study, 'field'].values[0]].str.split("_", 1, expand=True)
    pexpanded['subject'] = new[0].str.strip()
    pexpanded['flagged'] = new[1].str.strip()
    pexpanded['study'] = study  # auth.study[i]
    studydata = pd.concat([studydata, pexpanded], axis=0, sort=True)
    studydata=studydata.rename(columns={auth.loc[auth.study == study, 'interview_date'].values[0]:'interview_date'})
    # Convert age in years to age in months
    # note that dob is hardcoded var name here because all redcap databases use same variable name...sue me
    # interview date, which was originally v1_date for hcpd, has been renamed in line above, headerv2
    try:
        studydata['nb_months'] = (
                12 * (pd.to_datetime(studydata['interview_date']).dt.year - pd.to_datetime(studydata.dob).dt.year) +
                (pd.to_datetime(studydata['interview_date']).dt.month - pd.to_datetime(studydata.dob).dt.month) +
                (pd.to_datetime(studydata['interview_date']).dt.day - pd.to_datetime(studydata.dob).dt.day) / 31)
        studydatasub=studydata.loc[studydata.nb_months.isnull()].copy()
        studydatasuper = studydata.loc[~(studydata.nb_months.isnull())].copy()
        studydatasuper['nb_months'] = studydatasuper['nb_months'].apply(np.floor).astype(int)
        studydatasuper['nb_monthsPHI'] = studydatasuper['nb_months']
        studydatasuper.loc[studydatasuper.nb_months > 1080, 'nb_monthsPHI'] = 1200
        studydata=pd.concat([studydatasub,studydatasuper],sort=True)
        studydata = studydata.drop(columns={'nb_months'}).rename(columns={'nb_monthsPHI': 'interview_age'})
    except:
        pass
    #convert gender to M/F string
    try:
        studydata.gender = studydata.gender.str.replace('1', 'M')
        studydata.gender = studydata.gender.str.replace('2', 'F')
    except:
        print(study+' has no variable named gender')
    return studydata


#def getndarequiredfields(subject,dframe,guidfile=hcalist,studystr='hcpa',structure):
#    """
#    get the pseudo_guid, age, sex, interview_date...merge with dataframe, and prepend structure title
#    :return:
#    """
#    redcapvars = box.getredcapfields([], study=studystr)
#    redcapvars.loc[redcapvars.gender.astype(int)==1,'nda_gender']='M'
#    redcapvars.loc[redcapvars.gender.astype(int)==2,'nda_gender']='F'
#    redcapvars.loc[:,'nda_interview_date']=pd.to_datetime(redcapvars['interview_date']) -pd.offsets.QuarterBegin(startingMonth=1)
#    redcapvars=redcapvars.rename(columns={'interview_age':'nda_interview_age'})
#    redcapvars=redcapvars.loc[redcapvars.flagged.isnull()==True] #these already subset to v1 by redcap event pull
#    hcaids=pd.read_csv(guidfile,header=0)
#    hcaidsupdate=pd.merge(hcaids[['Subject','nda_guid']],redcapvars,left_on='Subject',right_on='subject',how='left')
#    ndadf=pd.merge(hcaidsupdate,dframe,lefton='Subject',righton='subject',how='inner')


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

def makedatadict(filecsv,dictcsv,inst):
    """
    create datadictionary from csvfile
    """
    ksadsraw=pd.read_csv(filecsv,header=0,low_memory=False)
    varvalues=pd.DataFrame(columns=['variable','values_or_example','numunique'])
    varvalues['variable']=ksadsraw.columns
    kcounts=ksadsraw.count().reset_index().rename(columns={'index':'variable',0:'num_nonmissing'})
    varvalues=pd.merge(varvalues,kcounts,on='variable',how='inner')
    summarystats = ksadsraw.describe().transpose().reset_index().rename(columns={'index': 'variable'})
    varvalues=pd.merge(varvalues,summarystats,on='variable',how='left')
    varvalues['min']=varvalues['min'].fillna(-99)
    varvalues['max']=varvalues['max'].fillna(-99)
    varvalues['ValueRange']=varvalues['min'].astype(int).astype(str) + ' :: ' + varvalues['max'].astype(int).astype(str)
    varvalues['min']=varvalues['min'].replace(-99.0,np.nan)
    varvalues['max'] = varvalues['max'].replace(-99.0, np.nan)
    varvalues.loc[varvalues.ValueRange.str.contains('-99')==True,'ValueRange']=' '
    #create a data frame containing summary info of data in the ksadraw, e.g. variablse, their formats, values, ect.
    for var in ksadsraw.columns:
        row=ksadsraw.groupby(var).count().reset_index()[var]
        varvalues.loc[varvalues.variable==var,'numunique']=len(row) #number of unique vars in this column
        varvalues.loc[(varvalues.variable==var) & (varvalues.numunique<=10) &
            (varvalues.num_nonmissing>=10),'values_or_example']=''.join(str(ksadsraw[var].unique().tolist()))
        varvalues.loc[(varvalues.variable==var) & (varvalues.numunique<=10) &
            (varvalues.num_nonmissing<10),'values_or_example']=ksadsraw[var].unique().tolist()[0]
        varvalues.loc[(varvalues.variable==var) & (varvalues.numunique>10),'values_or_example']=ksadsraw[var].unique().tolist()[0]
    varvalues['Instrument Title']=inst
    varvalues.to_csv(dictcsv,index=False)


def redcap2structure(vars,crosswalk,pathstructuresout=box_temp,studystr='hcpa',dframe=None):
    """
    Takes list of vars from the crosswalk, gets the data from Redcap, and puts into structure format after
    merging with NDAR requiredvars.  Outputs a csv structure in NDA format to pathstructureout location
    """
    #varslim=[x for x in fieldlist if str(x) != 'nan']
    #varnan=[x for x in fieldlist if str(x) == 'nan']
    if dframe is not None:
        studydata=dframe
    else:
        studydata=getredcapfieldsjson(fieldlist=vars,study=studystr)
    #get the relevant rows of the crosswalk
    #inner merge works for redcap source..need right merge for box, though, to get extra vars for missing people
    crosswalk_subset=pd.merge(crosswalk,pd.DataFrame(vars,columns=['HCP-D Element']),on='HCP-D Element',how='inner')[['NDA Structure', 'NDA Element', 'HCP-D Element', 'HCP-D Source',
       'CCF action requested',
       'HCP-D Element name in uploaded file',
       'requested_python']]
    #execute transformation codes stored in the crosswalk
    for index,row in crosswalk_subset.iterrows():
        if pd.isna(row['requested_python']):
            pass
        else:
            exec(row['requested_python'])
    #remove fields with empty values HCP-D Element name in uploaded file -- these are empty because NDA doesnt want them
    crosswalk_subset=crosswalk_subset.loc[crosswalk_subset['HCP-D Element name in uploaded file'].isnull()==False]
    listout=['subject','flagged','interview_date','interview_age','gender']+list(crosswalk_subset['HCP-D Element name in uploaded file'])
    #output new variables and subset to those not flagged for withdrawal.
    transformed=studydata[listout].loc[studydata.flagged.isnull()==True].drop(columns={'flagged','interview_date','gender','interview_age'})
    #merge with required fields from vars in intradb staging (guid, etc)
    #not sure whether it makes sense to pull these in here or recalculate on fly from redcap.
    #future issues:  compare this approach (e.g. pull from the file above named 'ndar') vs. what happens in the applycrosswalk.py
    #program for HCD, which regenerates on fly...will require some recodeing below to pull from redcap...
    #might just be easier to pull once...but how will this affect visit numbers?
    ndarsub=ndar[['nda_guid','subjectped','nda_gender','nda_interview_age','nda_interview_date']].rename(
        columns={'nda_guid':'subjectkey','subjectped':'src_subject_id','nda_gender':'gender',
                 'nda_interview_date':'interview_date','nda_interview_age':'interview_age'}).copy()
    dout=pd.merge(ndarsub,transformed,how='left',left_on='src_subject_id',right_on='subject').drop(columns='subject')
    dout['interview_date'] = pd.to_datetime(dout['interview_date']).dt.strftime('%m/%d/%Y')
    #now export
    crosswalk_subset.reset_index(inplace=True)
    strucroot=crosswalk_subset['NDA Structure'].str.strip().str[:-2][0]
    strucnum=crosswalk_subset['NDA Structure'].str.strip().str[-2:][0]
    #finalsubset - i.e. no withdraws
    #subjectkey	src_subject_id	interview_age	interview_date	gender
    filePath=os.path.join(pathstructuresout,'HCPD_'+strucroot+strucnum+'_'+snapshotdate+'.csv')
    if os.path.exists(filePath):
        os.remove(filePath)
    else:
        pass
        #print("Can not delete the file as it doesn't exists")

    with open(filePath,'a') as f:
        f.write(strucroot+","+str(int(strucnum))+"\n")
        dout.to_csv(f,index=False)


