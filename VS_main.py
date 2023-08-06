from VS import *	
import sys


#authenticate on your Google Sheets account using a client secrets JSON keyfile for your service account.
gc = gspread.service_account(filename='google_secret.json')

finalDfs=[]


# Demographics file FA2022

shDemosC1 = gc.open_by_url("URL_HERE")
wsDemosC1=shDemosC1.worksheet('WORKSHEET_NAME') 
dfDemosC1=pd.DataFrame(wsDemosC1.get_all_records())

dfDemosC1.rename(columns=rename_columns, inplace=True)	

# Demographics file W2022
shDemosC2 = gc.open_by_url("URL_HERE")
wsDemosC2=shDemosC2.worksheet('WORKSHEET_NAME') 
dfDemosC2=pd.DataFrame(wsDemosC2.get_all_records())
dfDemosC2.rename(columns=rename_columns, inplace=True)

# Demographics file SP2023 
shDemosC3 = gc.open_by_url("URL_HERE")
wsDemosC3=shDemosC3.worksheet('WORKSHEET_NAME') 
dfDemosC3=pd.DataFrame(wsDemosC3.get_all_records())
dfDemosC3.rename(columns=rename_columns, inplace=True)	


assessment=sys.argv[1:]
assessments=createAssessments(gc)

feederCopy=dict(siteCode_to_Feeder)
for key, value in feederCopy.items():
	siteCode_to_Feeder[str(key)]=value



for assessment_type, vs_objs in assessments.items():
	
	for vs_obj in vs_objs:
		if vs_obj.assessment not in assessment and vs_obj.assessment_type not in assessment:
			continue
		vs_obj.df=magicDF(vs_obj.assessment).rename(columns=rename_columns)
		
		
		if (assessment_type in ('STAR', 'iReady', 'SEL','ChrAbs')) and ('FA2022' in vs_obj.terms):
			vs_obj.df=mergeDemos(vs_obj.df, dfDemosC1)
			print("MERGING: ",assessment_type, vs_obj.subjects[0],vs_obj.terms[0])
		if (assessment_type in ('STAR', 'iReady', 'SEL','ChrAbs', 'ESGI')) and ('W2022' in vs_obj.terms):
			vs_obj.df=mergeDemos(vs_obj.df, dfDemosC2)
			print("MERGING: ",assessment_type, vs_obj.subjects[0], vs_obj.terms[0])

		if (assessment_type in ('STAR', 'iReady', 'SEL','ChrAbs', 'ESGI')) and ('SP2023' in vs_obj.terms):
			vs_obj.df=mergeDemos(vs_obj.df, dfDemosC3)
			print("MERGING: ",assessment_type, vs_obj.subjects[0], vs_obj.terms[0])
			
		if assessment_type not in ('SAEBRS'):	
			codifySchoolnames(vs_obj.df)
		
		if assessment_type == 'ChrAbs':
			chronicAbs(vs_obj, 0, finalDfs)

		if assessment_type == 'STAR':
			
			if 'SB' in vs_obj.metrics:
				starSB(vs_obj, finalDfs)
				supesGoals(vs_obj)
			
			if 'SGP' in vs_obj.metrics:		
				starSGP(vs_obj, finalDfs)
				starSGPSubgroups(vs_obj, finalDfs)

			#added for Spanish Reading 	
			if 'DB' in vs_obj.metrics:
				starDB(vs_obj, finalDfs)
		
		if assessment_type == 'iReady':

			if 'ProjProf' in vs_obj.metrics:
				supesGoals(vs_obj)
				iReadyPP(vs_obj, finalDfs)
				
			if 'SpPLMNT' in vs_obj.metrics:
				iReadySpan(vs_obj, finalDfs)
				
			if 'GradeLevel' in vs_obj.metrics:
				supesGoals(vs_obj)
				iReadyGradeLevel(vs_obj, finalDfs)
				
			if 'GRW' in vs_obj.metrics:
				iReadyGrw(vs_obj, finalDfs)
		
		if assessment_type == 'SEL':
			
			if 'DB' in vs_obj.metrics:
				supesGoals(vs_obj)
				starDB(vs_obj, finalDfs)
				
			if 'SGP' in vs_obj.metrics:
				selSGP(vs_obj, finalDfs)

			
			
		if assessment_type == 'ESGI':
			supesGoals(vs_obj)
			ESGI(vs_obj, finalDfs)
			
		
		if assessment_type == 'SuspRte':
			suspRte(vs_obj, finalDfs)
			
		if assessment_type == 'DI':
			disIndx(vs_obj, finalDfs)
			
		if assessment_type == 'SAEBRS':
			saebrsScreener(vs_obj, finalDfs)



createVitalSignsTab(finalDfs)
createGradeLevelTab(gradeLevelTab)
createSupesGoalsTab(supesGoalsTab)

