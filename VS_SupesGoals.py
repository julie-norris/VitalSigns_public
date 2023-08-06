""" These functions output the data needed specifically for the Superintendent's Goals report. Data is not disaggregated 
by school names"""

def supesGoals(vs_obj):
	idxRename = {'All':'District'} 
	
	#STAR SB calculations - Did Literacy for Supes Goals, then added Math for Assessment Summaries.
	#if (vs_obj.assessment_type == 'STAR') and (vs_obj.metrics[0] == 'SB') and ('math' in vs_obj.subjects):
	if (vs_obj.assessment_type == 'STAR') and (vs_obj.metrics[0] == 'SB') and ('read' in vs_obj.subjects):
		starFilters(vs_obj)
		vs_obj.df=vs_obj.df.sort_values('CompletedDate').groupby('Student_Number').tail(1)
		vs_obj.df=vs_obj.df[vs_obj.df.AssessmentStatus =='Active']

		
		supes=pd.crosstab([vs_obj.df['Grade Level']],[vs_obj.df['StateBenchmarkProficient']],margins=True)
		supes = supes.rename(index=idxRename)
		supes['Percentage Proficient']=(supes['Yes']/supes['All']).mul(100).round(1).astype(str) + '%'
		supes['Percentage Proficient'] = np.where((supes['All']) <= 10,'*',supes['Percentage Proficient'])
		
		#supesRace = supesRace.rename(index=idxRename)
		supesRace=pd.crosstab([vs_obj.df['Grade Level'],vs_obj.df['Race_Ethn']],[vs_obj.df['StateBenchmarkProficient']],margins=True)
		supesRace['Percentage Proficient']=(supesRace['Yes']/supesRace['All']).mul(100).round(1).astype(str) + '%'
		supesRace['Percentage Proficient'] = np.where((supesRace['All']) <= 10,'*',supesRace['Percentage Proficient'])
		supesRace=supesRace.reset_index()

		supesRaceTotal=pd.crosstab([vs_obj.df['Race_Ethn']],[vs_obj.df['StateBenchmarkProficient']],margins=True)
		supesRaceTotal = supesRaceTotal.drop(index=['All'])
		supesRaceTotal['District']=(supesRaceTotal['Yes']/supesRaceTotal['All']).mul(100).round(1).astype(str) + '%'
		supesRaceTotal['District'] = np.where((supesRaceTotal['All']) <= 10,'*',supesRaceTotal['District'])
		
		supes=supes.reset_index().set_index("Grade Level")
		race_dfs=[supesRace, supes,  supesRaceTotal]
		drop_cols=['No','Yes','All','']
		
		
		for col in supesRace.columns:
			if col in drop_cols:
				supesRace=supesRace.drop(columns=col)
		for col in supes.columns:
			if col in drop_cols:
				supes=supes.drop(columns=col)
		for col in supesRaceTotal.columns:
			if col in drop_cols:
				supesRaceTotal=supesRaceTotal.drop(columns=col)
		
		supesRaceTotal=supesRaceTotal.T
		#supesRaceTotal=supesRaceTotal.index.rename('Grade Level')
		
		supes=supes.rename(columns={'Percentage Proficient': "STAR_SG"+vs_obj.subjects[0]+vs_obj.metrics[0]+" ALL_"+vs_obj.terms[0]})
		
		supesRace=supesRace.pivot(index="Grade Level",
									columns="Race_Ethn",
									values='Percentage Proficient')
		supesRace=supesRace.reset_index().set_index("Grade Level")
		
		rename={}
		races={'African_American':'AA', 'American_Indian':'AI','American Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       			'Pac_Islander':'PI', 'White':'W'}
		for col in races.keys():
			rename[col]="STAR_SG"+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+""+vs_obj.terms[0]
		
		supes=supes.rename(columns=rename)
		supesRace=supesRace.rename(columns=rename)
		supesRaceTotal=supesRaceTotal.rename(columns=rename)
		
		supesRace=pd.concat([supesRace,supesRaceTotal])
		supesRace.index.rename('Grade Level', inplace=True)
		supesRace=supesRace.drop(index='All')
		supesRace=pd.concat([supesRace, supes], axis=1)
		supesRace=supesRace.drop(columns=[''])
		#supesRace=supesRace.index.rename("Grade Level")
		supesGoalsTab.append(supesRace)
		
		supeGoalsGrdLvlSubgrps(vs_obj, 0)
		

	#iReady GL calculations - Did Literacy for Supes Goals, then added Math for Assessment Summaries
	#if (vs_obj.assessment_type == 'iReady') and (vs_obj.metrics[0] == 'GradeLevel') and ('Math'in vs_obj.subjects):
	if (vs_obj.assessment_type == 'iReady') and (vs_obj.metrics[0] == 'GradeLevel') and ('Read'in vs_obj.subjects):
		iReadyFilter(vs_obj)
	
		if vs_obj.terms[0] == 'W2022':
			winterWindowFilter(vs_obj)
		
		if vs_obj.terms[0] == 'SP2023':
			springWindowFilter(vs_obj)

		vs_obj.df = vs_obj.df.replace(r'^\s*$', np.nan, regex=True)
		supesiReadyGLCount=pd.crosstab([vs_obj.df['Grade Level']],[vs_obj.df['Overall Relative Placement']], values=vs_obj.df.Student_Number, aggfunc='count',margins=True)
		supesiReadyGL=pd.crosstab([vs_obj.df['Grade Level']],[vs_obj.df['Overall Relative Placement']], values=vs_obj.df.Student_Number, aggfunc='count',margins=True, normalize='index')
		supesiReadyGL=supesiReadyGL.fillna(0)
		supesiReadyGL['On or Above']=supesiReadyGL['Early On Grade Level']+supesiReadyGL['Mid or Above Grade Level']
		supesiReadyGL=supesiReadyGL.fillna(0)
		supesiReadyGL['On or Above']=supesiReadyGL['On or Above'].mul(100).round(1).astype(str)+"%"
		supesiReadyGL['On or Above'] = np.where((supesiReadyGLCount['All'] <= 10) & (supesiReadyGLCount['All'] > 0),'*',supesiReadyGL['On or Above'])
		
		supesiReadyGLCount = supesiReadyGLCount.rename(index=idxRename)
		drop_cols=['1 Grade Level Below',	'2 Grade Levels Below',	'3 or More Grade Levels Below',	'Early On Grade Level',	'Mid or Above Grade Level']
		
		for col in drop_cols:
			if col in supesiReadyGL.columns:
				supesiReadyGL=supesiReadyGL.drop(columns=col)
		
		supesiReadyGL = supesiReadyGL.rename(index=idxRename).rename(columns={'On or Above':"iReady_SG"+vs_obj.subjects[0]+vs_obj.metrics[0]+" ALL_"+vs_obj.terms[0]})
		
		#race
		supesiReadyGLRace=pd.crosstab([vs_obj.df['Grade Level'],vs_obj.df['Race_Ethn']],[vs_obj.df['Overall Relative Placement']], values=vs_obj.df.Student_Number, aggfunc='count',margins=True)
		supesiReadyGLRace=supesiReadyGLRace.fillna(0)
		supesiReadyGLRace['On or Above']=supesiReadyGLRace['Early On Grade Level']+supesiReadyGLRace['Mid or Above Grade Level']
		supesiReadyGLRace['On or Above %']=(supesiReadyGLRace['On or Above']/supesiReadyGLRace['All']).mul(100).round(1).astype(str)+"%"
		supesiReadyGLRace['On or Above %'] = np.where((supesiReadyGLRace['All']) <= 10,'*',supesiReadyGLRace['On or Above %'])
		supesiReadyGLRace = supesiReadyGLRace.rename(index=idxRename).reset_index()
		
		supesiReadyGLRace=supesiReadyGLRace.pivot(index='Grade Level',
										columns='Race_Ethn',
										values='On or Above %')

		rename={}
		races={'African_American':'AA', 'American_Indian':'AI','American Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       			'Pac_Islander':'PI', 'White':'W'}
		
		for col in races.keys():
			rename[col]="iReady_SG"+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+""+vs_obj.terms[0]
		
		supesiReadyGLRace=supesiReadyGLRace.rename(columns=rename)

		supesiReadyGL=pd.concat([supesiReadyGLRace, supesiReadyGL], axis=1)
		supesiReadyGL=supesiReadyGL.drop(columns=[''])

		supesGoalsTab.append(supesiReadyGL)
		
		supeGoalsGrdLvlSubgrps(vs_obj, 0)
		
	elif (vs_obj.assessment_type == 'ESGI') :
		metrics = ['WCCUSD Uppercase Letters (PLF R 3.2)','WCCUSD Lowercase Letters (PLF R 3.2)', 'WCCUSD Number Recognition 0-12 (PLF NS 1.2)']
		for metric in metrics:
			new=vs_obj.df.loc[vs_obj.df['Test Name']== metric]
			
			MetEOYBenchmark=pd.crosstab([new['Test Name'],new['Grade Level_y']],[new['Met EOY Benchmark']],values=vs_obj.df.Student_Number, 
							margins=True, aggfunc='count')
			esgiRace=pd.crosstab([new['Test Name'],new['Grade Level_y'],new['Race_Ethn']],[new['Met EOY Benchmark']],values=vs_obj.df.Student_Number, 
							margins=True, aggfunc='count')
		
			idxRename = {'All':'District'} 
			MetEOYBenchmark = MetEOYBenchmark.rename(index=idxRename)
			esgiRace=esgiRace.rename(index=idxRename)
			MetEOYBenchmark=MetEOYBenchmark.fillna(0)
			esgiRace=esgiRace.fillna(0)

			MetEOYBenchmark['PercentageMetEOYBenchmark']=(MetEOYBenchmark['Y']/MetEOYBenchmark['All']).mul(100).round(1)
			MetEOYBenchmark['PercentageMetEOYBenchmark'] = np.where((MetEOYBenchmark['All']) <= 10,'*',MetEOYBenchmark['PercentageMetEOYBenchmark'])
			MetEOYBenchmark['PercentageMetEOYBenchmark'] = MetEOYBenchmark['PercentageMetEOYBenchmark'].astype(str)+"%"
			MetEOYBenchmark=MetEOYBenchmark.replace(to_replace="*%", value="*")
			MetEOYBenchmark=MetEOYBenchmark.reset_index()
		
			MetEOYBenchmark=MetEOYBenchmark.rename(columns={'PercentageMetEOYBenchmark':metric+" ALL "+vs_obj.terms[0],'Grade Level_y':'Grade Level'})
			MetEOYBenchmark.loc[MetEOYBenchmark.index[-1], 'Grade Level']='District'
			MetEOYBenchmark=MetEOYBenchmark.drop(columns=['Test Name','FALSE','Y','All'])
			esgiRace['PercentageMetEOYBenchmark']=(esgiRace['Y']/esgiRace['All']).mul(100).round(1)
			esgiRace['PercentageMetEOYBenchmark'] = np.where((esgiRace['All']) <= 10,'*',esgiRace['PercentageMetEOYBenchmark'])
			esgiRace['PercentageMetEOYBenchmark'] = esgiRace['PercentageMetEOYBenchmark'].astype(str)+"%"
			esgiRace=esgiRace.replace(to_replace="*%", value="*")
			esgiRace=esgiRace.reset_index()
			esgiRace=esgiRace.pivot(index='Grade Level_y'
									, columns='Race_Ethn'
									, values='PercentageMetEOYBenchmark')
			esgiRace=esgiRace.reset_index().rename(columns={'Grade Level_y':'Grade Level'})	
			esgiRace=esgiRace.set_index('Grade Level').drop(index=[''])
			
			rename={}
			races={'African_American':'AA', 'American_Indian':'AI','American Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       			'Pac_Islander':'PI', 'White':'W'}
		

			for col in races.keys():
				rename[col]=metric+" "+races[col]+" "+vs_obj.terms[0]
		
			esgiRace=esgiRace.rename(columns=rename)	
			esgiRace=esgiRace.reset_index()
			
			supesGoalsTab.append(MetEOYBenchmark)
			supesGoalsTab.append(esgiRace)
			
		supeGoalsGrdLvlSubgrps(vs_obj, 0)
			
	elif vs_obj.assessment_type == 'SEL':
		starFilters(vs_obj)
		
		vs_obj.df=vs_obj.df.sort_values('CompletedDate').groupby('Student_Number').tail(1)
		vs_obj.df=vs_obj.df[vs_obj.df.AssessmentStatus =='Active']

		selDBRace=pd.crosstab([vs_obj.df['Grade Level'], vs_obj.df.Race_Ethn],[vs_obj.df.DistrictBenchmarkCategoryName], values=vs_obj.df.Student_Number, aggfunc='count',margins=True)

		selDBRace['Percent_On_Above']=(selDBRace['At/Above Benchmark']/selDBRace['All']).mul(100).round(1)
		selDBRace['Percent_On_Above'] = np.where((((selDBRace['All']) <= 10) & ((selDBRace['All']) > 0)),'*',selDBRace['Percent_On_Above'])
		selDBRace['Percent_On_Above'] = selDBRace['Percent_On_Above'].astype(str)+"%"
		selDBRace=selDBRace.replace(to_replace="*%", value="*")
		selDBRace=selDBRace.drop(index='All')
	
		selDBRace=selDBRace.reset_index()
		selDBRace=selDBRace.pivot(index='Grade Level'
									, columns='Race_Ethn'
									, values='Percent_On_Above')
		
		rename={}
		races={'African_American':'AA', 'American_Indian':'AI','American Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       			'Pac_Islander':'PI', 'White':'W'}
		for col in races.keys():
			rename[col]="SEL_DB "+races[col]+" "+vs_obj.terms[0]
		
		selDBRace=selDBRace.rename(columns=rename)
		distGrdLvlDB=pd.crosstab([vs_obj.df['Grade Level']],[vs_obj.df['DistrictBenchmarkCategoryName']],margins=True, normalize='index').mul(100).round(1).astype(str)+"%"
		renameCol={'At/Above Benchmark':'District'}
		rename={'Grade Level':'School_Short'}
		
		distGrdLvlDB=distGrdLvlDB.rename(columns=renameCol).drop(columns=['Intervention','On Watch','Urgent Intervention'])
		distGrdLvlDB=distGrdLvlDB.rename(index={'All':"District"})
			
		selDBRace=pd.concat([selDBRace, distGrdLvlDB],axis=1)
		selDBRace=selDBRace.rename(columns={'District':"SEL_DB ALL "+vs_obj.terms[0]})
		
		supesGoalsTab.append(selDBRace)
		supeGoalsGrdLvlSubgrps(vs_obj, 0)


def supeGoalsGrdLvlSubgrps(vs_obj, metric_column_index):
	if (vs_obj.assessment_type == 'iReady') and (vs_obj.terms[0] == 'W2022'):
				vs_obj.df=vs_obj.df[vs_obj.df['Most Recent Diagnostic (Y/N)'] =='Y']
				vs_obj.df=vs_obj.df[vs_obj.df.Enrolled =='Enrolled']
				winterWindowFilter(vs_obj)
				vs_obj.df = vs_obj.df.replace(r'^\s*$', np.nan, regex=True)

	if (vs_obj.assessment_type == 'iReady') and (vs_obj.terms[0] == 'SP2023'):
		iReadyFilter(vs_obj)
		springWindowFilter(vs_obj)
		vs_obj.df = vs_obj.df.replace(r'^\s*$', np.nan, regex=True)

	
	if (vs_obj.assessment_type == 'SEL') and (vs_obj.terms[0] == 'W2022'):
		vs_obj.df=vs_obj.df.loc[vs_obj.df['ScreeningPeriodWindowName'] == 'Winter']
		vs_obj.df=vs_obj.df.sort_values('CompletedDate').groupby('Student_Number').tail(1)
		vs_obj.df=vs_obj.df[vs_obj.df.AssessmentStatus =='Active']		

	if (vs_obj.assessment_type == 'SEL') and (vs_obj.terms[0] == 'SP2023'):
		starFilters(vs_obj)	
	
	
	if (vs_obj.assessment_type == 'ESGI') :
		
		metrics = ['WCCUSD Uppercase Letters (PLF R 3.2)','WCCUSD Lowercase Letters (PLF R 3.2)', 'WCCUSD Number Recognition 0-12 (PLF NS 1.2)']
		for metric in metrics:
			
			vs_obj.df.loc[vs_obj.df['SPED'] =='ESN', 'SPED'] = 'Y'
			vs_obj.df.loc[vs_obj.df['SPED']=='MMSN', 'SPED'] = 'Y'
			vs_obj.df.loc[vs_obj.df['SPED']=='Y', 'SPED'] = 'Y'
			vs_obj.df.loc[vs_obj.df['EL']=='EL', 'EL'] = 'Y'
			vs_obj.df.loc[vs_obj.df['Foster']=='Foster', 'Foster'] = 'Y'

			new=vs_obj.df.loc[vs_obj.df['Test Name']== metric]

			for column in [new.SPED, new.EL]:
				GrdLvlSubgps=pd.crosstab([new['Test Name'],new['Grade Level_y'],column],[new['Met EOY Benchmark']],values=new.Student_Number, 
							margins=True, aggfunc='count')

				GrdLvlSubgps.fillna(0)
			
				GrdLvlSubgps=GrdLvlSubgps.rename(columns = {'Yes':'Y','At/Above Benchmark':'Y', 'Y':'Y','Severe&Chronic':'Y'})
				
				if 'Y' not in GrdLvlSubgps.columns:
					GrdLvlSubgps['Y'] = 0
			
				GrdLvlSubgps['Y %']=((GrdLvlSubgps['Y']/GrdLvlSubgps['All'])*100).round(1)
				GrdLvlSubgps.loc[GrdLvlSubgps['All'] == 0, 'Y %'] = ""
				GrdLvlSubgps.loc[(GrdLvlSubgps['All'] <= 10) & (GrdLvlSubgps['All'] > 0), 'Y %'] = -1
			
				GrdLvlSubgps['Y %']=GrdLvlSubgps['Y %'].astype(str)+"%"
				GrdLvlSubgps['Y %']=GrdLvlSubgps['Y %'].replace(to_replace="-100.0%",value="*").replace(to_replace="nan%",value="").replace(to_replace="-1%", value="*").replace(to_replace="%",value="")
				GrdLvlSubgps=GrdLvlSubgps.reset_index()
				GrdLvlSubgps=GrdLvlSubgps[GrdLvlSubgps[column.name]=='Y']

				GrdLvlSubgps=GrdLvlSubgps[['Grade Level_y', column.name, 'Y %']]
				GrdLvlSubgps=GrdLvlSubgps.rename(columns={'Y %': metric+" "+column.name+" "+vs_obj.terms[0],'Grade Level_y':'Grade Level'})
				
				supesGoalsTab.append(GrdLvlSubgps)

	if ('read' in vs_obj.subjects)  or ('EarlyLit' in vs_obj.subjects) or ('SpEarlyLit' in vs_obj.subjects):
		
		vs_obj.df.loc[vs_obj.df['SPED'] =='ESN', 'SPED'] = 'Y'
		vs_obj.df.loc[vs_obj.df['SPED']=='MMSN', 'SPED'] = 'Y'
		vs_obj.df.loc[vs_obj.df['SPED']=='Y', 'SPED'] = 'Y'
		
		vs_obj.df.loc[vs_obj.df['EL']=='EL', 'EL'] = 'Y'
		vs_obj.df.loc[vs_obj.df['Foster']=='Foster', 'Foster'] = 'Y'
		
		for column in [vs_obj.df.Foster, vs_obj.df.SPED, vs_obj.df.EL]:
			if 'Grade Level' in vs_obj.df.columns:
				GrdLvlSubgps=pd.crosstab([vs_obj.df['Grade Level'],column],[vs_obj.df[vs_obj.columns[metric_column_index]]],values=vs_obj.df.Student_Number, aggfunc='count',margins=True, margins_name='All')
			else:
				GrdLvlSubgps=pd.crosstab([vs_obj.df['Grade_Level_x'],column],[vs_obj.df[vs_obj.columns[metric_column_index]]], values=vs_obj.df.Student_Number, aggfunc='count',margins=True, margins_name='All')
			
			GrdLvlSubgps.fillna(0)
			if (vs_obj.assessment_type == 'STAR') or (vs_obj.assessment_type == 'SEL'):
				starFilters(vs_obj)
			
			GrdLvlSubgps=GrdLvlSubgps.rename(columns = {'Yes':'Y','At/Above Benchmark':'Y', 'Y':'Y','Severe&Chronic':'Y'})
			
			if 'Y' not in GrdLvlSubgps.columns:
				GrdLvlSubgps['Y'] = 0
			
			GrdLvlSubgps['Y %']=((GrdLvlSubgps['Y']/GrdLvlSubgps['All'])*100).round(1)
			GrdLvlSubgps.loc[GrdLvlSubgps['All'] == 0, 'Y %'] = ""
			GrdLvlSubgps.loc[(GrdLvlSubgps['All'] <= 10) & (GrdLvlSubgps['All'] > 0), 'Y %'] = -1
			
			idxRename = {'All':'District'} 
			GrdLvlSubgps = GrdLvlSubgps.rename(index=idxRename)
			
			GrdLvlSubgps['Y %']=GrdLvlSubgps['Y %'].astype(str)+"%"
			GrdLvlSubgps['Y %']=GrdLvlSubgps['Y %'].replace(to_replace="-100.0%",value="*").replace(to_replace="nan%",value="").replace(to_replace="-1%", value="*").replace(to_replace="%",value="")
			GrdLvlSubgps=GrdLvlSubgps.reset_index()
			
			GrdLvlSubgps=GrdLvlSubgps[GrdLvlSubgps[column.name]=='Y']
			
			GrdLvlSubgps=GrdLvlSubgps[['Grade Level', column.name, 'Y %']]
			GrdLvlSubgps=GrdLvlSubgps.rename(columns={'Y %':vs_obj.assessment_type+"_"+vs_obj.subjects[0]+" "+column.name+" "+vs_obj.terms[0],'Grade Level_y':'Grade Level'})
			supesGoalsTab.append(GrdLvlSubgps)


def createSupesGoalsTab(supesGoalsTab):
	SupesGoals_dfs=[df.reset_index().set_index('Grade Level').replace(to_replace="*%",value="*").replace(to_replace=-1,value="*") for df in SupesGoalsTab]
	SupesGoals_concatenated=pd.concat(SupesGoals_dfs, axis=1)
	drop=['index','EL','SPED','']
	for col in drop:
		if col in SupesGoals_concatenated.columns:
			SupesGoals_concatenated=SupesGoals_concatenated.drop(columns=col)
	
	sheet=gc.open_by_url('URL_HERE')
	worksheet = sheet.add_worksheet("Supes Goals Tab"+dt_string, rows = 1000, cols=500)
	set_with_dataframe(worksheet,SupesGoals_concatenated,1,1,include_index=True)