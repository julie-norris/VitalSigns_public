from VS import *	
import sys


#authenticate on your Google Sheets account using a client secrets JSON keyfile for your service account.
gc = gspread.service_account(filename='google_secret.json')

Final_dfs_list=[]


# Demographics file FA2022
sh_Demos_C1 = gc.open_by_url("URL_HERE")
ws_Demos_C1=sh_Demos_C1.worksheet('WORKSHEET_NAME') 
dfDemos_C1=pd.DataFrame(ws_Demos_C1.get_all_records())

dfDemos_C1.rename(columns=rename_columns, inplace=True)	

# Demographics file W2022
sh_Demos_C2 = gc.open_by_url("URL_HERE")
ws_Demos_C2=sh_Demos_C2.worksheet('WORKSHEET_NAME') 
dfDemos_C2=pd.DataFrame(ws_Demos_C2.get_all_records())
dfDemos_C2.rename(columns=rename_columns, inplace=True)

# Demographics file SP2023 
sh_Demos_C3 = gc.open_by_url("URL_HERE")
ws_Demos_C3=sh_Demos_C3.worksheet('WORKSHEET_NAME') 
dfDemos_C3=pd.DataFrame(ws_Demos_C3.get_all_records())
dfDemos_C3.rename(columns=rename_columns, inplace=True)	


assessment=sys.argv[1:]
assessments=create_assessments(gc)

feederCopy=dict(siteCode_to_Feeder)
for key, value in feederCopy.items():
	siteCode_to_Feeder[str(key)]=value



for assessment_type, vs_objs in assessments.items():
	
	for vs_obj in vs_objs:
		if vs_obj.assessment not in assessment and vs_obj.assessment_type not in assessment:
			continue
		vs_obj.df=magicDF(vs_obj.assessment).rename(columns=rename_columns)
		
		
		if (assessment_type in ('STAR', 'iReady', 'SEL','ChrAbs')) and ('FA2022' in vs_obj.terms):
			vs_obj.df=mergeDemos(vs_obj.df, dfDemos_C1)
			print("MERGING: ",assessment_type, vs_obj.subjects[0],vs_obj.terms[0])
		if (assessment_type in ('STAR', 'iReady', 'SEL','ChrAbs', 'ESGI')) and ('W2022' in vs_obj.terms):
			vs_obj.df=mergeDemos(vs_obj.df, dfDemos_C2)
			print("MERGING: ",assessment_type, vs_obj.subjects[0], vs_obj.terms[0])

		if (assessment_type in ('STAR', 'iReady', 'SEL','ChrAbs', 'ESGI')) and ('SP2023' in vs_obj.terms):
			vs_obj.df=mergeDemos(vs_obj.df, dfDemos_C3)
			print("MERGING: ",assessment_type, vs_obj.subjects[0], vs_obj.terms[0])
			
		if assessment_type not in ('SAEBRS'):	
			codify_schoolnames(vs_obj.df)
		
		if assessment_type == 'ChrAbs':
			ChronicAbs(vs_obj, 0, Final_dfs_list)

		if assessment_type == 'STAR':
			
			if 'SB' in vs_obj.metrics:
				STAR_SB(vs_obj, Final_dfs_list)
				SupesGoals(vs_obj)
			
			if 'SGP' in vs_obj.metrics:		
				STAR_SGP(vs_obj, Final_dfs_list)
				STAR_SGP_Subgroups(vs_obj, Final_dfs_list)

			#added for Spanish Reading 	
			if 'DB' in vs_obj.metrics:
				STAR_DB(vs_obj, Final_dfs_list)
		
		if assessment_type == 'iReady':

			if 'ProjProf' in vs_obj.metrics:
				SupesGoals(vs_obj)
				iReadyPP(vs_obj, Final_dfs_list)
				
			if 'SpPLMNT' in vs_obj.metrics:
				iReadySpan(vs_obj, Final_dfs_list)
				
			if 'GradeLevel' in vs_obj.metrics:
				SupesGoals(vs_obj)
				iReadyGradeLevel(vs_obj, Final_dfs_list)
				
			if 'GRW' in vs_obj.metrics:
				iReady_Grw(vs_obj, Final_dfs_list)
		
		if assessment_type == 'SEL':
			
			if 'DB' in vs_obj.metrics:
				SupesGoals(vs_obj)
				STAR_DB(vs_obj, Final_dfs_list)
				
			if 'SGP' in vs_obj.metrics:
				SEL_SGP(vs_obj, Final_dfs_list)

			
			
		if assessment_type == 'ESGI':
			SupesGoals(vs_obj)
			ESGI(vs_obj, Final_dfs_list)
			
		
		if assessment_type == 'SuspRte':
			SuspRte(vs_obj, Final_dfs_list)
			
		if assessment_type == 'DI':
			DI(vs_obj, Final_dfs_list)
			
		if assessment_type == 'SAEBRS':
			SAEBRS(vs_obj, Final_dfs_list)



createVitalSignsTab(Final_dfs_list)
createGradeLevelTab(GradeLevelTab)
createSupesGoalsTab(SupesGoalsTab)

