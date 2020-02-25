
#before running this program
#QC corrected folder as in toolbox_from_corrected.py (make round 2)

#then:
#merge stuff stuff from corrected and curated using functions from toolbox_dl_and_append_round3.py
#make sure that anything output to needs_attention folders is newly needing attention, not just a copy of what was there before
#figure out what is still missing - generate a list to put in the needs attention folder


#create records for the SAS rescored folks from UMN and WU - put in corrected


##UMNA is mostly the SAS rescored stuff..s
#for the ids in UMINN Missing iPad Scored Data, need to overwrite scores with new scores
#read the new SAS output
sasfile=box_temp+'/UMINN Missing iPad Scored Data.csv'
sasdata=pd.read_csv(sasfile,header=0,low_memory=False)
sasdata=sasdata.drop(columns=['Assessment_Name', 'Age', 'Male', 'Group', 'edu_yrs',
                      'DCCSaccuracy', 'DCCSreactiontime','FlankerAccuracy',
                      'FlankerReactionTime']).rename(columns={'pin':'PIN'})

dccs=sasdata[['PIN','RawDCCS', 'ComputedDCCS', 'UncorrDCCS', 'AgeCorrDCCS',  'FullTDCCS']]
flanks=sasdata[['PIN','RawFlanker', 'ComputedFlanker', 'UncorrFlanker',
       'AgeCorrFlanker', 'FullTFlanker']]
psm=sasdata[['PIN','RawPSM', 'ThetaPSM', 'ComputedPSM','UncorrPSM', 'AgeCorrPSM', 'FullTPSM']]
pattern=sasdata[['PIN','RawPatternComp',
       'ComputedPatternComp', 'UncorrPatternComp', 'AgeCorrPatternComp',
       'FullTPatternComp']]
listsort=sasdata[['PIN','RawListSort', 'UncorrListSort', 'AgeCorrListSort',
       'FullTListSort']]
oralreading=sasdata[['PIN','ThetaEngRead', 'ComputedEngRead', 'UncorrEngRead',
       'AgeCorrEngRead', 'FullTEngRead']]

picturevocab=[['PIN','ThetaEngVocab', 'ComputedEngVocab',
       'UncorrEngVocab', 'AgeCorrEngVocab', 'FullTEngVocab']]

fluid=[['PIN','UncorrFluid','AgeCorrFluid', 'FullTFluid']]

crystal=[['PIN','UncorrCrystal', 'AgeCorrCrystal','FullTCrystal']]

total=[['PIN','UncorrTotal', 'AgeCorrTotal', 'FullTTotal']]
early=[['PIN','UncorrEarly', 'AgeCorrEarly', 'FullTEarly']]

#pull raw and scores from endpoint and then overwrite with new scores
#local copy of old endpoint data - does not contain missing info...will need to get from backup
endpointtemp='/home/petra/UbWinSharedSpace1/ccf-nda-behavioral/toolbox/endpointmachine/Jun21_2019'
for i in sasdata.PIN.unique():
    print(i)  #saved as a grep list e.g. greplist4rescores in endpointtemp, which doesnt have any of the missing data...



endpointdata=pd.read_csv(endpointtemp+'/AssessmentData.csv',header=0,low_memory=False)
endpointscores=pd.read_csv(endpointtemp+'/AssessmentScores.csv',header=0,low_memory=False)
