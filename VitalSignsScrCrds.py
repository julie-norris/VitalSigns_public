
from datetime import date
import pandas as pd
import numpy as np
import glob as glob
import openpyxl
from datetime import datetime
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import gspread
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from gspread_pandas import Spread, Client
from Short_Feeder_Dicts import *
gc = gspread.service_account(filename='google_secret.json')


# datetime object containing current date and time
now = datetime.now()

# dd/mm/YY H:M:S
dt_string = now.strftime("%m/%d/%Y %H:%M:%S")

class VitalSignsDataframe(object):
	def __init__(self, assessment,assessment_type, subjects, terms, metrics, columns):
		self.assessment=assessment
		self.df=None
		self.assessment_type=assessment_type
		self.subjects=subjects
		self.terms=terms
		self.metrics=metrics
		self.columns=columns
		if assessment_type == 'SEL':
			self.assessment_column = 'STAR'
		else:
			self.assessment_column = assessment_type
				

assessmentsDFs={}
supesGoalsTab=[]

def magicDF(assessment_type):
	if assessment_type in assessmentsDFs:
		return(assessmentsDFs[assessment_type])
	
	(sheet_id, tab_name) = googleSheets[assessment_type]
	google_sheet = gc.open_by_url("https://docs.google.com/spreadsheets/d/"+sheet_id+"/edit?usp=sharing")
	worksheet=google_sheet.worksheet(tab_name) 
	df=pd.DataFrame(worksheet.get_all_records())

	assessmentsDFs[assessment_type]=df
	return(df)
	
def createAssessments(gc):
	assessments={}

	#SAEBRS W2022
	assessments['SAEBRS']=[VitalSignsDataframe('SAEBRS_C2', 'SAEBRS', ['climate'],['W2022'],['Participation'],['School_Short', 'SAEBRSparticipation ALL_W2022'])]

	#Chronic Abs
	assessments['ChrAbs']=[VitalSignsDataframe('Abs_C1', 'ChrAbs',['absenteeism'],['FA2022'],['Chronic&Severe'],['absCategory','School_Code',
						'siteName','Student_Number', 'studentStatus','grade','gender','totalPercentMissed','daysMissed','totalInstructionalDays',
						'School_Short','Race_Ethn','SPED','FIT','Foster','EL','SED','Grade Level'])
						,VitalSignsDataframe('Abs_C2', 'ChrAbs',['absenteeism'],['W2022'],['Chronic&Severe'],['absCategory','School_Code',
						'siteName','Student_Number', 'studentStatus','School_Short','Race_Ethn','SPED','FIT','Foster','EL','SED','Grade Level'])
						,VitalSignsDataframe('Abs_C3', 'ChrAbs',['absenteeism'],['SP2023'],['Chronic&Severe'],['absCategory','School_Code',
						'siteName','Student_Number', 'studentStatus','School_Short','Race_Ethn','SPED','FIT','Foster','EL','SED','Grade Level'])] 

	#STAR Assessments
	assessments['STAR']=[VitalSignsDataframe('STAR_MC3','STAR',['math'],['SP2023'],['SB'],['StateBenchmarkProficient','StudentGrowthPercentileFallFall','StudentGrowthPercentileFallWinter',
									'CompletedDate','Student_Number','StudentFirstName','StudentLastName','Race_Ethn','School_Short','CurrentGrade','AssessmentStatus','SPED','FIT','Foster'
									,'EL','SED','Grade Level','ScreeningPeriodWindowName','StudentIdentifier','EnrollmentStatus'])
						,VitalSignsDataframe('STAR_RC3', 'STAR',['read'],['SP2023'],['SB'],['StateBenchmarkProficient','StudentGrowthPercentileFallFall','StudentGrowthPercentileFallWinter',
									'CompletedDate','Student_Number','StudentFirstName','StudentLastName','School_Short','Race_Ethn', 'CurrentGrade',
									'AssessmentStatus','SPED','FIT','Foster','EL','SED','Grade Level','ScreeningPeriodWindowName','StudentIdentifier','EnrollmentStatus'])
						,VitalSignsDataframe('STAR_MC3','STAR',['math'],['SP2023'],['SGP'],['StudentGrowthPercentileFallSpring',
									'CompletedDate','Student_Number','Race_Ethn','School_Short','CurrentGrade','AssessmentStatus','SPED','FIT','Foster'
									,'EL','SED','Grade Level','ScreeningPeriodWindowName','StudentIdentifier','EnrollmentStatus'])
						,VitalSignsDataframe('STAR_RC3', 'STAR',['read'],['SP2023'],['SGP'],['StudentGrowthPercentileFallSpring',
									'CompletedDate','Student_Number','School_Short','Race_Ethn', 'CurrentGrade',
									'AssessmentStatus','SPED','FIT','Foster','EL','SED','Grade Level','ScreeningPeriodWindowName','StudentIdentifier','EnrollmentStatus'])
						,VitalSignsDataframe('STAR_SPC3', 'STAR',['SP_read'],['SP2023'],['SGP'],['StudentGrowthPercentileFallSpring',
									'CompletedDate','Student_Number','School_Short','Race_Ethn', 'CurrentGrade',
									'AssessmentStatus','SPED','FIT','Foster','EL','SED','Grade Level','ScreeningPeriodWindowName','StudentIdentifier','EnrollmentStatus'])
						,VitalSignsDataframe('STAR_SPC3', 'STAR',['SP_read'],['SP2023'],['DB'],['DistrictBenchmarkCategoryName',
									'CompletedDate','Student_Number','School_Short','Race_Ethn', 'CurrentGrade',
									'AssessmentStatus','SPED','FIT','Foster','EL','SED','Grade Level','ScreeningPeriodWindowName','StudentIdentifier','EnrollmentStatus'])
						,VitalSignsDataframe('STAR_MC1','STAR',['math'],['FA2022'],['SB'],['StateBenchmarkProficient','StudentGrowthPercentileFallFall',
									'CompletedDate','Student_Number','StudentFirstName','StudentLastName','Race_Ethn','School_Short','CurrentGrade',
									'AssessmentStatus','SPED','FIT','Foster','EL','SED','Grade Level','ScreeningPeriodWindowName','StudentIdentifier','EnrollmentStatus']) 
						,VitalSignsDataframe('STAR_RC1', 'STAR',['read'],['FA2022'],['SB'],['StateBenchmarkProficient','StudentGrowthPercentileFallFall',
									'CompletedDate','Student_Number','StudentFirstName','StudentLastName','School_Short','Race_Ethn', 'CurrentGrade',
									'AssessmentStatus','SPED','FIT','Foster','EL','SED','Grade Level','ScreeningPeriodWindowName','StudentIdentifier','ScaledScore','EnrollmentStatus'])
						,VitalSignsDataframe('STAR_MC1','STAR',['math'],['FA2022'],['SGP'],['StudentGrowthPercentileFallFall',
									'CompletedDate','Student_Number','Race_Ethn','School_Short','CurrentGrade',
									'AssessmentStatus','SPED','FIT','Foster','EL','SED','Grade Level','ScreeningPeriodWindowName','StudentIdentifier','EnrollmentStatus']) 
						 ,VitalSignsDataframe('STAR_RC1', 'STAR',['read'],['FA2022'],['SGP'],['StudentGrowthPercentileFallFall',
									'CompletedDate','Student_Number','School_Short','Race_Ethn', 'CurrentGrade',
									'AssessmentStatus','SPED','FIT','Foster','EL','SED','Grade Level','ScreeningPeriodWindowName','StudentIdentifier','EnrollmentStatus','ScaledScore'])
						,VitalSignsDataframe('STAR_MC2','STAR',['math'],['W2022'],['SB'],['StateBenchmarkProficient','StudentGrowthPercentileFallFall','StudentGrowthPercentileFallWinter',
									'CompletedDate','Student_Number','StudentFirstName','StudentLastName','Race_Ethn','School_Short','CurrentGrade','AssessmentStatus','SPED','FIT','Foster'
									,'EL','SED','Grade Level','ScreeningPeriodWindowName','StudentIdentifier','EnrollmentStatus'])
						,VitalSignsDataframe('STAR_RC2', 'STAR',['read'],['W2022'],['SB'],['StateBenchmarkProficient','StudentGrowthPercentileFallFall','StudentGrowthPercentileFallWinter',
									'CompletedDate','Student_Number','StudentFirstName','StudentLastName','School_Short','Race_Ethn', 'CurrentGrade',
									'AssessmentStatus','SPED','FIT','Foster','EL','SED','Grade Level','ScreeningPeriodWindowName','StudentIdentifier','EnrollmentStatus'])
						,VitalSignsDataframe('STAR_MC2','STAR',['math'],['W2022'],['SGP'],['StudentGrowthPercentileFallWinter',
									'CompletedDate','Student_Number','Race_Ethn','School_Short','CurrentGrade','AssessmentStatus','SPED','FIT','Foster'
									,'EL','SED','Grade Level','ScreeningPeriodWindowName','StudentIdentifier','EnrollmentStatus'])
						,VitalSignsDataframe('STAR_RC2', 'STAR',['read'],['W2022'],['SGP'],['StudentGrowthPercentileFallWinter',
									'CompletedDate','Student_Number','School_Short','Race_Ethn', 'CurrentGrade',
									'AssessmentStatus','SPED','FIT','Foster','EL','SED','Grade Level','ScreeningPeriodWindowName','StudentIdentifier','EnrollmentStatus'])
						]
												


	#iReady Assessments
	assessments['iReady']=[VitalSignsDataframe('iReady_MC3', 'iReady', ['Math','Math'], ['SP2023'],['ProjProf'],['Proficiency if Student Shows No Additional Growth','Overall Relative Placement','Baseline Diagnostic (Y/N)',
							'Most Recent Diagnostic (Y/N)', 'Percent Progress to Annual Typical Growth (%)','Student_Number','Grade Level','School_Short','Enrolled',
							'Race_Ethn','SPED','FIT','Foster','EL','SED','Completion Date']) 
						 	,VitalSignsDataframe('iReady_RC3', 'iReady', ['Read','Read'], ['SP2023'],['ProjProf'],['Proficiency if Student Shows No Additional Growth','Overall Relative Placement','Baseline Diagnostic (Y/N)',
							'Most Recent Diagnostic (Y/N)','Percent Progress to Annual Typical Growth (%)', 'Student_Number','Grade Level','School_Short','Enrolled',
							'Race_Ethn','SPED','FIT','Foster','EL','SED','Completion Date'])
							,VitalSignsDataframe('iReady_MC3', 'iReady', ['Math','Math'], ['SP2023'],['GradeLevel'],['Overall Relative Placement','Baseline Diagnostic (Y/N)',
							'Most Recent Diagnostic (Y/N)', 'Percent Progress to Annual Typical Growth (%)','Student_Number','Grade Level','School_Short','Enrolled',
							'Race_Ethn','SPED','FIT','Foster','EL','SED','Completion Date'])
						 	,VitalSignsDataframe('iReady_RC3', 'iReady', ['Read','Read'], ['SP2023'],['GradeLevel'],['Overall Relative Placement','Baseline Diagnostic (Y/N)',
							'Most Recent Diagnostic (Y/N)','Percent Progress to Annual Typical Growth (%)', 'Student_Number','Grade Level','School_Short','Enrolled',
							'Race_Ethn','SPED','FIT','Foster','EL','SED','Completion Date'])
							,VitalSignsDataframe('iReady_MC3', 'iReady', ['Math','Math'], ['SP2023'],['GRW'],['Percent Progress to Annual Typical Growth (%)','Baseline Diagnostic (Y/N)',
							'Most Recent Diagnostic (Y/N)' ,'Student_Number','Grade Level','School_Short','Enrolled',
							'Race_Ethn','SPED','FIT','Foster','EL','SED','Completion Date']) 
						 	,VitalSignsDataframe('iReady_RC3', 'iReady', ['Read','Read'], ['SP2023'],['GRW'],['Percent Progress to Annual Typical Growth (%)','Baseline Diagnostic (Y/N)',
							'Most Recent Diagnostic (Y/N)','Student_Number','Grade Level','School_Short','Enrolled',
							'Race_Ethn','SPED','FIT','Foster','EL','SED','Completion Date'])
							,VitalSignsDataframe('iReady_SPANC3', 'iReady', ['SPRead','SPRead'], ['SP2023'],['SpPLMNT'],['Overall Spanish Placement','Student_Number','Grade Level','School_Short','Enrolled',
							'Race_Ethn','SPED','FIT','Foster','EL','SED','Completion Date','Window'])
							,VitalSignsDataframe('iReady_MC1', 'iReady', ['Math','Math'], ['FA2022'],['ProjProf'],['Projection if Student Achieves Typical Growth','Overall Relative Placement',
							'Most Recent Diagnostic (Y/N)', 'Student_Number','Grade Level','School_Short','Enrolled',
							'Race_Ethn','SPED','FIT','Foster','EL','SED']) 
						 	,VitalSignsDataframe('iReady_RC1', 'iReady', ['Read','Read'], ['FA2022'],['ProjProf'],['Projection if Student Achieves Typical Growth','Overall Relative Placement',
							'Most Recent Diagnostic (Y/N)', 'Student_Number','Grade Level','School_Short','Enrolled',
							'Race_Ethn','SPED','FIT','Foster','EL','SED'])
							,VitalSignsDataframe('iReady_MC1', 'iReady', ['Math','Math'], ['FA2022'],['GradeLevel'],['Overall Relative Placement',
							'Most Recent Diagnostic (Y/N)', 'Student_Number','Grade Level','School_Short','Enrolled',
							'Race_Ethn','SPED','FIT','Foster','EL','SED']) 
						 	,VitalSignsDataframe('iReady_RC1', 'iReady', ['Read','Read'], ['FA2022'],['GradeLevel'],['Overall Relative Placement',
							'Most Recent Diagnostic (Y/N)', 'Student_Number','Grade Level','School_Short','Enrolled',
							'Race_Ethn','SPED','FIT','Foster','EL','SED'])
							,VitalSignsDataframe('iReady_MC2', 'iReady', ['Math','Math'], ['W2022'],['ProjProf'],['Projection if Student Achieves Typical Growth','Overall Relative Placement','Baseline Diagnostic (Y/N)',
							'Most Recent Diagnostic (Y/N)', 'Percent Progress to Annual Typical Growth (%)','Student_Number','Grade Level','School_Short','Enrolled',
							'Race_Ethn','SPED','FIT','Foster','EL','SED','Completion Date']) 
						 	,VitalSignsDataframe('iReady_RC2', 'iReady', ['Read','Read'], ['W2022'],['ProjProf'],['Projection if Student Achieves Typical Growth','Overall Relative Placement','Baseline Diagnostic (Y/N)',
							'Most Recent Diagnostic (Y/N)','Percent Progress to Annual Typical Growth (%)', 'Student_Number','Grade Level','School_Short','Enrolled',
							'Race_Ethn','SPED','FIT','Foster','EL','SED','Completion Date'])
							,VitalSignsDataframe('iReady_MC2', 'iReady', ['Math','Math'], ['W2022'],['GradeLevel'],['Overall Relative Placement','Baseline Diagnostic (Y/N)',
							'Most Recent Diagnostic (Y/N)', 'Percent Progress to Annual Typical Growth (%)','Student_Number','Grade Level','School_Short','Enrolled',
							'Race_Ethn','SPED','FIT','Foster','EL','SED','Completion Date'])
						 	,VitalSignsDataframe('iReady_RC2', 'iReady', ['Read','Read'], ['W2022'],['GradeLevel'],['Overall Relative Placement','Baseline Diagnostic (Y/N)',
							'Most Recent Diagnostic (Y/N)','Percent Progress to Annual Typical Growth (%)', 'Student_Number','Grade Level','School_Short','Enrolled',
							'Race_Ethn','SPED','FIT','Foster','EL','SED','Completion Date'])
							,VitalSignsDataframe('iReady_MC2', 'iReady', ['Math','Math'], ['W2022'],['GRW'],['Percent Progress to Annual Typical Growth (%)','Baseline Diagnostic (Y/N)',
							'Most Recent Diagnostic (Y/N)' ,'Student_Number','Grade Level','School_Short','Enrolled',
							'Race_Ethn','SPED','FIT','Foster','EL','SED','Completion Date']) 
						 	,VitalSignsDataframe('iReady_RC2', 'iReady', ['Read','Read'], ['W2022'],['GRW'],['Percent Progress to Annual Typical Growth (%)','Baseline Diagnostic (Y/N)',
							'Most Recent Diagnostic (Y/N)','Student_Number','Grade Level','School_Short','Enrolled',
							'Race_Ethn','SPED','FIT','Foster','EL','SED','Completion Date'])]
							

	#STAR Early Literacy
	assessments['SEL']=[VitalSignsDataframe('SEL_C3', 'SEL', ['EarlyLit'], ['SP2023'],['DB'], ['DistrictBenchmarkCategoryName','StudentGrowthPercentileFallSpring','AssessmentStatus','CompletedDate',
							'ScreeningPeriodWindowName','CurrentGrade','Student_Number','Grade Level','School_Short','Race_Ethn','SPED','FIT','Foster',
							'EL','SED','EnrollmentStatus'])
							,VitalSignsDataframe('SEL_C3', 'SEL', ['EarlyLit'], ['SP2023'],['SGP'], ['StudentGrowthPercentileFallSpring','AssessmentStatus','CompletedDate',
							'ScreeningPeriodWindowName','CurrentGrade','Student_Number','Grade Level','School_Short','Race_Ethn','SPED','FIT','Foster',
							'EL','SED','EnrollmentStatus'])
							,VitalSignsDataframe('SpanSEL_C3', 'SEL', ['SPEarlyLit'], ['SP2023'],['DB'], ['DistrictBenchmarkCategoryName','StudentGrowthPercentileFallSpring','AssessmentStatus','CompletedDate',
							'ScreeningPeriodWindowName','CurrentGrade','Student_Number','Grade Level','School_Short','Race_Ethn','SPED','FIT','Foster',
							'EL','SED','EnrollmentStatus'])
							,VitalSignsDataframe('SpanSEL_C3', 'SEL', ['SPEarlyLit'], ['SP2023'],['SGP'], ['StudentGrowthPercentileFallSpring','AssessmentStatus','CompletedDate',
							'ScreeningPeriodWindowName','CurrentGrade','Student_Number','Grade Level','School_Short','Race_Ethn','SPED','FIT','Foster',
							'EL','SED','EnrollmentStatus'])
							,VitalSignsDataframe('SEL_C1', 'SEL', ['EarlyLit'], ['FA2022'],['DB'], ['DistrictBenchmarkCategoryName','AssessmentStatus','CompletedDate',
							'ScreeningPeriodWindowName','Student_Number','Grade Level','School_Short','Race_Ethn','SPED','FIT','Foster',
							'EL','SED','EnrollmentStatus'])
							,VitalSignsDataframe('SEL_C2', 'SEL', ['EarlyLit'], ['W2022'],['DB'], ['DistrictBenchmarkCategoryName','StudentGrowthPercentileFallWinter','AssessmentStatus','CompletedDate',
							'ScreeningPeriodWindowName','CurrentGrade','Student_Number','Grade Level','School_Short','Race_Ethn','SPED','FIT','Foster',
							'EL','SED','EnrollmentStatus'])
							,VitalSignsDataframe('SEL_C2', 'SEL', ['EarlyLit'], ['W2022'],['SGP'], ['StudentGrowthPercentileFallWinter','AssessmentStatus','CompletedDate',
							'ScreeningPeriodWindowName','CurrentGrade','Student_Number','Grade Level','School_Short','Race_Ethn','SPED','FIT','Foster',
							'EL','SED','EnrollmentStatus'])]
	#ESGI assessments	
	assessments['ESGI']=[VitalSignsDataframe('ESGI_C3', 'ESGI', ['UppCaseLet3'],['SP2023'],['WCCUSD Uppercase Letters (PLF R 3.2)'],['Met EOY Benchmark','School Name','Test Date','Student_Number','Test Name',
								'Correct Answers','Grade Level_y','Grade Level_x', 'Race_Ethn','SPED','FIT','Foster','EL','SED','School_Short','Grade Level']),
							VitalSignsDataframe('ESGI_C1', 'ESGI', ['UppCaseLet3'],['FA2022'],['WCCUSD Uppercase Letters (PLF R 3.2)'],['Met EOY Benchmark','School Name','Test Date','Student_Number','Test Name',
								'Correct Answers','Grade Level_y','Grade Level_x', 'Race_Ethn','SPED','FIT','Foster','EL','SED','School_Short','Grade Level']),
							 VitalSignsDataframe('ESGI_C1', 'ESGI', ['NumRec3'],['FA2022'],['WCCUSD Number Recognition 0-12 (PLF NS 1.2)'],['Met EOY Benchmark','School Name','Test Date','Student_Number','Test Name',
								'Correct Answers','Grade Level_y','Grade Level_x', 'Race_Ethn','SPED','FIT','Foster','EL','SED','School_Short','Grade Level']),
							 VitalSignsDataframe('ESGI_C2', 'ESGI', ['UppCaseLet3'],['W2022'],['WCCUSD Uppercase Letters (PLF R 3.2)'],['Met EOY Benchmark','School Name','Test Date','Student_Number','Test Name',
								'Correct Answers','Grade Level_y','Grade Level_x', 'Race_Ethn','SPED','FIT','Foster','EL','SED','School_Short','Grade Level']),
							 VitalSignsDataframe('ESGI_C2', 'ESGI', ['LwrCaseLetr'],['W2022'],['WCCUSD Lowercase Letters (PLF R 3.2)'],['Met EOY Benchmark','School Name','Test Date','Student_Number','Test Name',
								'Correct Answers','Grade Level_y','Grade Level_x', 'Race_Ethn','SPED','FIT','Foster','EL','SED','School_Short','Grade Level']),
							 VitalSignsDataframe('ESGI_C2', 'ESGI', ['NumRec3'],['W2022'],['WCCUSD Number Recognition 0-12 (PLF NS 1.2)'],['Met EOY Benchmark','School Name','Test Date','Student_Number','Test Name',
								'Correct Answers','Grade Level_y','Grade Level_x', 'Race_Ethn_x','SPED','FIT','Foster','EL','SED','School_Short', 'Race_Ethn','Grade Level'])]
		
	#Suspension Rate						 
	assessments['SuspRte']=[VitalSignsDataframe('SuspRte_C1', 'SuspRte', ['SuspRte'], 'FA2022',['Susp'], ['School_Short','School Rate','American Indian','African American','Asian',
													'Filipino','Hispanic','Pacific Islander','White','Two Or More','Missing Or Decline','English Learner','Students With Disabilities','Homeless',
													'Ai E','Aa E','As E','Fi E','Hi E','Pi E','Wh E','Mt E','Md E','El E','Swd E','Fit E'])
							, VitalSignsDataframe('SuspRte_C2', 'SuspRte', ['SuspRte'],'W2022',['Susp'], ['School_Short','School Rate','American Indian','African American','Asian',
													'Filipino','Hispanic','Pacific Islander','White','Two Or More','Missing Or Decline','English Learner','Students With Disabilities','Homeless',
													'Ai E','Aa E','As E','Fi E','Hi E','Pi E','Wh E','Mt E','Md E','El E','Swd E','Fit E'])
							, VitalSignsDataframe('SuspRte_C3', 'SuspRte', ['SuspRte'],'SP2023',['Susp'], ['School_Short','School Rate','American Indian','African American','Asian',
													'Filipino','Hispanic','Pacific Islander','White','Two Or More','Missing Or Decline','English Learner','Students With Disabilities','Homeless',
													'Ai E','Aa E','As E','Fi E','Hi E','Pi E','Wh E','Mt E','Md E','El E','Swd E','Fit E','Cumulative K-13 Enrollment'])
							]
	
	#Disproportionality Index	
	assessments['DI']=[VitalSignsDataframe('DI_C1', 'DI', ['DI'],['FA2022'],['DI'],['School_Short','American Ind DI','African American DI','Asian DI',
																					'Filipino DI','Hispanic DI','Pac Isl DI','White DI','Multiple DI','Missing-Decline DI'])
						,VitalSignsDataframe('DI_C2', 'DI', ['DI'],['W2022'],['DI'],['School_Short','American Ind DI','African American DI','Asian DI',
																					'Filipino DI','Hispanic DI','Pac Isl DI','White DI','Multiple DI','Missing-Decline DI'])
						,VitalSignsDataframe('DI_C3', 'DI', ['DI'],['SP2023'],['DI'],['School_Short','American Ind DI','African American DI','Asian DI',
																					'Filipino DI','Hispanic DI','Pac Isl DI','White DI','Multiple DI','Missing-Decline DI'
																					])
						]
			
	return(assessments)						

def winterWindowFilter(vs_obj):
	if 'ScreeningPeriodWindowName' in vs_obj.df.columns:
		vs_obj.df=vs_obj.df.loc[(vs_obj.df['ScreeningPeriodWindowName'] == 'Winter')] 
	elif ('Completion Date' in vs_obj.df.columns) and (vs_obj.terms[0] == 'W2022'):
		vs_obj.df['Completion Date'] = pd.to_datetime(vs_obj.df['Completion Date']) 
		mask = (vs_obj.df['Completion Date'] >= '2023-01-18') & (vs_obj.df['Completion Date'] <= '2023-02-17')
		vs_obj.df=vs_obj.df.loc[mask]

def springWindowFilter(vs_obj):
	if 'ScreeningPeriodWindowName' in vs_obj.df.columns:
		vs_obj.df=vs_obj.df.loc[(vs_obj.df['ScreeningPeriodWindowName'] == 'Spring')]
	elif ('Completion Date' in vs_obj.df.columns) and (vs_obj.terms[0] == 'SP2023'):
		vs_obj.df['Completion Date'] = pd.to_datetime(vs_obj.df['Completion Date']) 
		mask = (vs_obj.df['Completion Date'] >= '2023-05-01') & (vs_obj.df['Completion Date'] <= '2023-06-01')
		vs_obj.df=vs_obj.df.loc[mask]

def iReadyFilter(vs_obj):
	vs_obj.df=vs_obj.df[vs_obj.columns]
	vs_obj.df=vs_obj.df.reset_index()	
	vs_obj.df=vs_obj.df[vs_obj.df['Most Recent Diagnostic (Y/N)'] =='Y']
	vs_obj.df=vs_obj.df[vs_obj.df.Enrolled =='Enrolled']
	vs_obj.df.replace(r'^\s*$', np.nan, regex=True)
	if vs_obj.subjects[0] == 'math':
		vs_obj.df=vs_obj.df[vs_obj.df['Grade Level'].isin(['0.0','0.1','2.0','3.0','4.0','5.0','6.0','7.0','8.0'])]
	if vs_obj.subjects[0] == 'read':
		vs_obj.df=vs_obj.df[vs_obj.df['Grade Level'].isin(['0.0','0.1','2.0','3.0','4.0','5.0','6.0','7.0','8.0'])]

def gradeLevelAddColumns(vs_obj, grdLvl_df):
	idxRename = {'All':'District'} 
	grdLvl_df = grdLvl_df.rename(index=idxRename)
	assessment_name=vs_obj.assessment_type+" "+vs_obj.subjects[0].title()
	measure=vs_obj.metrics[0]
	cycle=vs_obj.terms[0]
	rename={'FA2022':'Fall 2022', 'SP2023':'Spring 2023','W2022':'Winter 2022-23', 'SGP':'Student Growth Percentile (SGP)',
			'SB':'State Benchmark','DB':'District Benchmark','NumRec3':'Number Recognition','UppCaseLet3':'Uppercase Letter Recognition', 'Uppcaselet3':'See Test Name',
			'GradeLevel':'3 Level Placement (Grade Level)','GRW':'Growth','PP':'Projected Proficiency (on SBAC)','Chronic&Severe':'Chronic Absenteeism',
			'SEL':'STAR Early Lit','ChrAbs':'A2A', 'STAR Sp_Read':'STAR Spanish Reading','iReady Spread':'iReady Spanish Reading','SEL Earlylit':'STAR Early Lit',
			'SEL Spearlylit':'STAR Early Lit Spanish'}
	if cycle in rename.keys():
		cycle=rename[cycle]
		
	if measure in rename.keys():
		measure=rename[measure]

	if assessment_name in rename.keys():
		assessment_name=rename[assessment_name]

	if assessment_name == 'STAR' and vs_obj.subjects[0] == 'SP_read':
		assessment_name = 'STAR Reading Spanish'

	if assessment_name == 'SEL' and vs_obj.subjects[0] == 'SPEarlyLit':
		assessment_name == 'STAR Spanish Early Literacy'

	if assessment_name == 'SEL' and vs_obj.subjects[0] == 'EarlyLit':
		assessment_name == 'STAR Early Literacy'

	if assessment_name == 'iReady' and vs_obj.subjects[0] == 'SPRead':
		measure = 'Overall Spanish Placement'

	
	grdLvl_df.insert(0,"Cycle",[cycle]*len(grdLvl_df))
	grdLvl_df.insert(0,"Measure",[measure]*len(grdLvl_df))
	grdLvl_df.insert(0,"Assessment",[assessment_name]*len(grdLvl_df))
	return(grdLvl_df)

def mergeDemos(df_assessment, dfDemos):
	return(pd.merge(df_assessment, dfDemos, how="left", on='Student_Number'))
	
# Codify school names so that all files have same school names
def codifySchoolnames(df_assessment):
	if 'School_Code' in df_assessment.columns:
		df_assessment['School_Short']=df_assessment['School_Code'].map(SchoolId_to_Short)
	else:
		df_assessment['School_Short']=df_assessment['School'].map(Short_names)


def starFilters(vs_obj):
		if vs_obj.terms[0] == 'FA2022':
			vs_obj.df=vs_obj.df.loc[(vs_obj.df['ScreeningPeriodWindowName'] == 'Fall') | (vs_obj.df['ScreeningPeriodWindowName'] == 'Round 1')]
		if vs_obj.terms[0] == 'W2022':
			vs_obj.df=vs_obj.df.loc[vs_obj.df['ScreeningPeriodWindowName'] == 'Winter']
		if vs_obj.terms[0] == 'SP2023' and vs_obj.metrics[0]=='SB':
			vs_obj.df=vs_obj.df.loc[vs_obj.df['ScreeningPeriodWindowName'] == 'Spring']
		if vs_obj.terms[0] == 'SP2023' and vs_obj.metrics[0]=='DB':
			vs_obj.df=vs_obj.df.loc[vs_obj.df['ScreeningPeriodWindowName'] == 'Spring']

		vs_obj.df=vs_obj.df[vs_obj.df.EnrollmentStatus == 'Enrolled']
	
		if vs_obj.subjects[0] == 'math':
			vs_obj.df=vs_obj.df[vs_obj.df['CurrentGrade'].isin([1,2,3,4,5,6,7,8,9,10,11])]
		
		if vs_obj.subjects[0] == 'read' or vs_obj.subjects[0] == 'SP_read':
			vs_obj.df=vs_obj.df[vs_obj.df['CurrentGrade'].isin([2,3,4,5,6,7,8,9,10,11])]

		if vs_obj.subjects[0] == 'EarlyLit' or vs_obj.subjects[0] == 'SpEarlyLit':
			vs_obj.df=vs_obj.df[vs_obj.df['Grade Level'].isin([0,1])]

def newSTARFilter(vs_obj):
		if vs_obj.terms[0] == 'FA2022':
			vs_obj.df=vs_obj.df.loc[(vs_obj.df['ScreeningPeriodWindowName'] == 'Fall') | (vs_obj.df['ScreeningPeriodWindowName'] == 'Round 1')]
		if vs_obj.terms[0] == 'W2022':
			vs_obj.df=vs_obj.df.loc[vs_obj.df['ScreeningPeriodWindowName'] == 'Winter']
		if vs_obj.terms[0] == 'SP2023' and vs_obj.metrics[0]=='SB':
			vs_obj.df=vs_obj.df.loc[vs_obj.df['ScreeningPeriodWindowName'] == 'Spring']
		if vs_obj.terms[0] == 'SP2023' and vs_obj.metrics[0]=='DB':
			vs_obj.df=vs_obj.df.loc[vs_obj.df['ScreeningPeriodWindowName'] == 'Spring']

		vs_obj.df=vs_obj.df[vs_obj.df.EnrollmentStatus == 'Enrolled']
	
		vs_obj.df=vs_obj.df[vs_obj.df['CurrentGrade'].isin([9,10,11])]
		
def chronicAbs(vs_obj, metric_column_index,finalDfs):
	vs_obj.df=vs_obj.df[vs_obj.df['studentStatus'] =='Active']
	rslt=pd.crosstab([vs_obj.df.School_Short],[vs_obj.df['absCategory']], 
						values=vs_obj.df.Student_Number, aggfunc='count',margins=True,normalize='index').mul(100).round(1)
	idxRename = {'All':'District'} 
	rslt = rslt.rename(index=idxRename)
	rslt=rslt.rename(columns={'CHRONIC':'Chronic','SEVERE CHRONIC':'Severe'})
	rslt['Chronic&Severe']=rslt.Chronic + rslt.Severe
	rslt['Chronic&Severe']=rslt['Chronic&Severe'].round(1).astype(str)+"%"

	rslt=rslt.rename(columns = {'Chronic&Severe': vs_obj.assessment_type+" ALL_"+vs_obj.terms[0]})
	rslt=rslt.reset_index().set_index('School_Short')
	
	#Grade Level
	rslt_GL=pd.crosstab([vs_obj.df.School_Short,vs_obj.df['Grade Level']],vs_obj.df['absCategory'], 
						values=vs_obj.df.Student_Number, aggfunc='count',margins=True,normalize='index').mul(100).round(1)
	idxRename = {'All':'District'} 
	rslt_GL = rslt_GL.rename(index=idxRename)

	rslt_GL=rslt_GL.rename(columns={'CHRONIC':'Chronic','SEVERE CHRONIC':'Severe'})
	rslt_GL['Chronic&Severe']=rslt_GL.Chronic + rslt_GL.Severe
	rslt_GL['Chronic&Severe']=rslt_GL['Chronic&Severe'].round(1).astype(str)+"%"

	rslt_GL=rslt_GL.reset_index()
	dropped=['Chronic', 'Excellent', 'Manageable','Satisfactory', 'Severe','CHRONIC','EXCELLENT','MANAGEABLE','SATISFACTORY','SEVERE']
	for col in rslt_GL.columns:
		if col in dropped:
			rslt_GL=rslt_GL.drop(columns=col)
	rslt_GL=rslt_GL.pivot(index='School_Short',
							columns='Grade Level',
							values='Chronic&Severe')
	rslt_GL=rslt_GL.replace(to_replace="*%",value="*").replace(to_replace="nan%",value="")
	
	grdLvl=gradeLevelAddColumns(vs_obj, rslt_GL)
	gradeLevelTab.append(grdLvl)
	
	
	#Race
	rsltRace=pd.crosstab([vs_obj.df.School_Short, vs_obj.df.Race_Ethn],[vs_obj.df.absCategory], 
						values=vs_obj.df.Student_Number, aggfunc='count',margins=True)
	rsltRace=rsltRace.rename(index=idxRename)

	
	rsltRacePerc=pd.crosstab([vs_obj.df.School_Short, vs_obj.df.Race_Ethn],[vs_obj.df.absCategory], 
						values=vs_obj.df.Student_Number, aggfunc='count',margins=True,normalize='index')
	
	rsltRacePerc=rsltRacePerc.rename(index=idxRename)
	rsltRacePerc=rsltRacePerc.rename(columns={'CHRONIC':'Chronic','SEVERE CHRONIC':'Severe'})				
	rsltRacePerc['Chronic&Severe']=rsltRacePerc.Chronic + rsltRacePerc.Severe
	rsltRace['Chronic&SeverePerc']=rsltRacePerc['Chronic&Severe'].mul(100).round(1)
	rsltRace['Chronic&SeverePerc'] = np.where((rsltRace['All']) <= 10,-1,rsltRace['Chronic&SeverePerc'])

	rsltRace=rsltRace.reset_index()
	rsltRace['Chronic&SeverePerc']=rsltRace['Chronic&SeverePerc'].astype(str)+"%"		
	rsltRace=rsltRace.pivot(index='School_Short',
							columns='Race_Ethn',
							values='Chronic&SeverePerc')
	
	rsltRace=rsltRace.replace(to_replace="-1.0%",value="*").replace(to_replace="nan%",value="")
	
	rename={}
	races={'African_American':'AA', 'American_Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       'Pac_Islander':'PI', 'White':'W'}
	for col in rsltRace.columns:
		if col in races.keys():
			temp_col = vs_obj.assessment_type+" "+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
			
	rsltRace.rename(columns=rename, inplace=True)
	rsltRace=rsltRace.drop(index='District').drop(columns='')
	
	dstRace=pd.crosstab([vs_obj.df.Race_Ethn], [vs_obj.df.absCategory],
						values=vs_obj.df.Student_Number, aggfunc='count',normalize='index')
	dstRace=dstRace.rename(columns={'CHRONIC':'Chronic','SEVERE CHRONIC':'Severe'})				
	dstRace['Chronic&Severe']=dstRace.Chronic + dstRace.Severe
	dstRace['Chronic&Severe']=dstRace['Chronic&Severe'].mul(100).round(1)
	

	dstRace['Chronic&Severe']=dstRace['Chronic&Severe'].astype(str)+"%"
	
	dropped=['Excellent', 'Manageable','Satisfactory','Chronic', 'Severe']
	for col in dstRace.columns:
		if col in dropped:
			dstRace=dstRace.drop(columns=col)

	dstRace=dstRace.T
	i_rename = {'Chronic&Severe':'District'} 
	dstRace = dstRace.rename(index=i_rename)
	
	rename={}
	races={'':'_','absCategory':'School_Short','African_American':'AA', 'American_Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       'Pac_Islander':'PI', 'White':'W'}
	for col in dstRace.columns:
		if col in races.keys():
			temp_col = vs_obj.assessment_type+" "+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
	
	dstRace.index.rename('School_Short', inplace=True)
	dstRace=dstRace.reset_index().set_index('School_Short')
	dstRace.rename(columns=rename, inplace=True)
	dst=dstRace.iloc[-1:, : ]
	
	rsltRace=pd.concat([rsltRace,dst])
	rsltRace=rsltRace.reset_index().set_index('School_Short')
	rslt=rslt[['ChrAbs ALL_SP2023']]
	
	finalDfs.append(rslt)
	finalDfs.append(rsltRace)
	
	subgroups(vs_obj,0,finalDfs)					

def ESGI(vs_obj, finalDfs):
	test_names=['WCCUSD Uppercase Letters (PLF R 3.2)','WCCUSD Number Recognition 0-12 (PLF NS 1.2)','WCCUSD Lowercase Letters (PLF R 3.2)']
	names={'WCCUSD Uppercase Letters (PLF R 3.2)':'UppCaseLet3','WCCUSD Number Recognition 0-12 (PLF NS 1.2)':'NumRec3','WCCUSD Lowercase Letters (PLF R 3.2)':'LowCaseLet'}
	for test_name in test_names:
		
		vs_obj.df.loc[vs_obj.df['Test Name'] == test_name]
		MetEOYBenchmark=pd.crosstab([vs_obj.df.School_Short],[vs_obj.df['Test Name'],vs_obj.df['Met EOY Benchmark']],values=vs_obj.df.Student_Number, 
							margins=True, aggfunc='count')
	
		idxRename = {'All':'District'} 
		MetEOYBenchmark = MetEOYBenchmark.rename(index=idxRename)
		MetEOYBenchmark.columns=['_'.join(col) for col in MetEOYBenchmark.columns.values]
		MetEOYBenchmark=MetEOYBenchmark.fillna(0)
		col_y = test_name +'_Y'
		col_f = test_name +'_FALSE'
		total = MetEOYBenchmark[col_y]+MetEOYBenchmark[col_f]
		MetEOYBenchmark['PercentageMetEOYBenchmark']=(MetEOYBenchmark[col_y]/total).mul(100).round(1)
		MetEOYBenchmark['PercentageMetEOYBenchmark'] = np.where((total) <= 10,'*',MetEOYBenchmark['PercentageMetEOYBenchmark'])
		MetEOYBenchmark['PercentageMetEOYBenchmark'] = MetEOYBenchmark['PercentageMetEOYBenchmark'].astype(str)+"%"
		MetEOYBenchmark=MetEOYBenchmark.replace(to_replace="*%", value="*")
		for k, v in names.items():
			name=names[test_name]
		MetEOYBenchmark=MetEOYBenchmark.reset_index()
		MetEOYBenchmark=MetEOYBenchmark[['School_Short','PercentageMetEOYBenchmark']]
		MetEOYBenchmark=MetEOYBenchmark.rename(columns = {'PercentageMetEOYBenchmark': vs_obj.assessment_type+"_"+name+" ALL_"+vs_obj.terms[0]})
		MetEOYBenchmark=MetEOYBenchmark.set_index('School_Short')
		
		finalDfs.append(MetEOYBenchmark)
		
		#GradeLevel Tab
		new=vs_obj.df.loc[vs_obj.df['Test Name']== test_name]
			
		esgiGrdLvl=pd.crosstab([new['School_Short'],new['Grade Level_y']],[new['Met EOY Benchmark']],values=vs_obj.df.Student_Number, 
							margins=True, aggfunc='count')

		idxRename = {'All':'District'} 
		esgiGrdLvl = esgiGrdLvl.rename(index=idxRename)
		esgiGrdLvl=esgiGrdLvl.fillna(0)
		esgiGrdLvl['PercentageMetEOYBenchmark']=(esgiGrdLvl['Y']/esgiGrdLvl['All']).mul(100).round(1)
		esgiGrdLvl['PercentageMetEOYBenchmark'] = np.where((esgiGrdLvl['All']) <= 10,'*',esgiGrdLvl['PercentageMetEOYBenchmark'])
		esgiGrdLvl['PercentageMetEOYBenchmark'] = esgiGrdLvl['PercentageMetEOYBenchmark'].astype(str)+"%"
		esgiGrdLvl=esgiGrdLvl.replace(to_replace="*%", value="*").reset_index()
		esgiGrdLvl=esgiGrdLvl.rename(columns={'Grade Level_y':'Grade Level', 'Test Name': 'School_Short'})#,'PercentageMetEOYBenchmark': test_name+" "+vs_obj.terms[0]})
		
		drop_cols=['FALSE','Y','All','']
		esgiGrdLvl=esgiGrdLvl.pivot(index='School_Short',
								columns='Grade Level',
								values='PercentageMetEOYBenchmark')
		
		for col in esgiGrdLvl.columns:
			if col in drop_cols:
				esgiGrdLvl=esgiGrdLvl.drop(columns=col)

		idxRename = {'All':'District'} 
		esgiGrdLvl = esgiGrdLvl.rename(index=idxRename)
		assessment_name=vs_obj.assessment_type+" "+vs_obj.subjects[0].title()
		measure=test_name
		cycle=vs_obj.terms[0]
		esgiGrdLvl.insert(0,"Cycle",[cycle]*len(esgiGrdLvl))
		esgiGrdLvl.insert(0,"Measure",[measure]*len(esgiGrdLvl))
		esgiGrdLvl.insert(0,"Assessment",[assessment_name]*len(esgiGrdLvl))
		esgiGrdLvl=esgiGrdLvl.reset_index().set_index('School_Short')
		
		gradeLevelTab.append(esgiGrdLvl)

		#ESGI Race	
		vs_obj.df=vs_obj.df.rename(columns= {'Race_Ethn_y':'Race_Ethn', 'SPED_y':'SPED', 'FIT_y':'FIT',
	       'Foster_y':'Foster', 'EL_y':'EL', 'SED_y':'SED'})

	
		supesRace=pd.crosstab([vs_obj.df['Race_Ethn']],[vs_obj.df['Test Name'],vs_obj.df['Met EOY Benchmark']],margins=True)
		supesRace.columns=['_'.join(col) for col in supesRace.columns.values]
		
		supesRace['District']=(supesRace[test_name+"_Y"]/supesRace['All_']).mul(100).round(1).astype(str) + '%'
		supesRace['District'] = np.where((supesRace['All_']) <= 10,'*',supesRace['District'])
		
		supesRace=supesRace.T
		rename={'All':'ALL'}
		races={'African_American':'AA', 'American Indian':'AI','American_Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
	       'Pac_Islander':'PI', 'White':'W'}
		for col in supesRace.columns:
			if col in races.keys():
				temp_col = vs_obj.assessment_type+"_"+name+" "+races[col]+"_"+vs_obj.terms[0]
				rename[col]=temp_col
				
		supesRace.rename(columns=rename, inplace=True)
		supes=supesRace.iloc[-1:, : ]
		
	
		MetEOYBenchmarkRace=pd.crosstab([vs_obj.df.School_Short, vs_obj.df.Race_Ethn],[vs_obj.df['Test Name'],vs_obj.df['Met EOY Benchmark']],values=vs_obj.df.Student_Number, 
								margins=True, aggfunc='count')
		MetEOYBenchmarkRace.columns=['_'.join(col) for col in MetEOYBenchmarkRace.columns.values]
		col_x = vs_obj.metrics[0]+'_Y'
		col_z = vs_obj.metrics[0]+'_FALSE'
		total = MetEOYBenchmarkRace[col_x]+MetEOYBenchmarkRace[col_z]
		MetEOYBenchmarkRace['PercentageMetEOYBenchmark']=(MetEOYBenchmarkRace[col_x]/total).mul(100).round(1)
		MetEOYBenchmarkRace['PercentageMetEOYBenchmark'] = np.where((total) <= 10,'*',MetEOYBenchmarkRace['PercentageMetEOYBenchmark'])
		MetEOYBenchmarkRace['PercentageMetEOYBenchmark'] = MetEOYBenchmarkRace['PercentageMetEOYBenchmark'].astype(str)+"%"
		
		MetEOYBenchmarkRace.reset_index(inplace=True)
		esgiRace=MetEOYBenchmarkRace.pivot(index='School_Short',
										columns= 'Race_Ethn',
										values='PercentageMetEOYBenchmark')
		
		
		rename={' ':'ALL'}
		races={'African_American':'AA', 'American Indian':'AI','American_Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
	       'Pac_Islander':'PI', 'White':'W'}
		for col in esgiRace.columns:
			if col in races.keys():
				temp_col = vs_obj.assessment_type+"_"+name+" "+races[col]+"_"+vs_obj.terms[0]
				rename[col]=temp_col
				
		esgiRace.rename(columns=rename, inplace=True)
		esgiRace=esgiRace.replace(to_replace="*%",value="*").replace(to_replace="nan%",value="")
		
		esgiRace=pd.concat([esgiRace, supes])
		esgiRace.index.rename('School_Short', inplace=True)
		esgiRace=esgiRace.reset_index().set_index('School_Short')		
		
		drop_cols=['FALSE','Y','All','','ALL']
		
		for col in esgiRace.columns:
			if col in drop_cols:
				esgiRace=esgiRace.drop(columns=col)

		
		finalDfs.append(esgiRace)
		
		vs_obj.df.loc[vs_obj.df['SPED'] =='ESN', 'SPED'] = 'Y'
		vs_obj.df.loc[vs_obj.df['SPED']=='MMSN', 'SPED'] = 'Y'
		
		vs_obj.df['FIT']=vs_obj.df['FIT'].fillna('')
		vs_obj.df.loc[vs_obj.df['FIT'] != '' , 'FIT'] = 'Y'
		vs_obj.df.loc[vs_obj.df['EL']=='EL', 'EL'] = 'Y'
		vs_obj.df.loc[vs_obj.df['Foster']=='Foster', 'Foster'] = 'Y'

		for column in [vs_obj.df.Foster,vs_obj.df.SPED, vs_obj.df.FIT, vs_obj.df.EL]:
		
			subgroupCount=pd.crosstab([vs_obj.df.School_Short,column],[vs_obj.df['Test Name'],vs_obj.df['Met EOY Benchmark']],values=vs_obj.df.Student_Number, 
								margins=True, aggfunc='count')
			subgroupCount.columns=['_'.join(col) for col in subgroupCount.columns.values]
			subgroup_dropped=['WCCUSD Count Up to 10 Objects (PLF NS 1.4) _','WCCUSD Count to 20 (PLF NS 1.1)_',
									 'WCCUSD Letter Sounds (PLF R 3.3)_','WCCUSD Lowercase Letters (PLF R 3.2)_', '_']
			for col in subgroupCount.columns:
				if col in subgroup_dropped:
					subgroupsCount=subgroupCount.drop(columns=col)

			col_y = vs_obj.metrics[0]+'_Y'
			col_f = vs_obj.metrics[0]+'_FALSE'
			subgroupCount[col_y]=subgroupCount[col_y].fillna(0)
			subgroupCount[col_f]=subgroupCount[col_f].fillna(0)
			subgroupCount['total'] = subgroupCount[col_y]+subgroupCount[col_f]
			subgroupCount.reset_index(inplace=True)
			
			if 'SPED' in subgroupCount.columns:
				subgroupCount = subgroupCount[subgroupCount['SPED'] == 'Y']
				subgroupCount.loc['District'] = subgroupCount.iloc[:, :].sum()
				
			elif 'FIT' in subgroupCount.columns:
				subgroupCount = subgroupCount[subgroupCount['FIT'] == 'Y']
				subgroupCount.loc['District'] = subgroupCount.iloc[:, :].sum()

			elif 'EL' in subgroupCount.columns:
				subgroupCount = subgroupCount[subgroupCount['EL'] == 'Y']
				subgroupCount.loc['District'] = subgroupCount.iloc[:, :].sum()

			elif 'Foster' in subgroupCount.columns:
				subgroupCount = subgroupCount[subgroupCount['Foster'] == 'Y']
				subgroupCount.loc['District'] = subgroupCount.iloc[:, :].sum()


			subgroupCount.loc[subgroupCount.index[-1], 'School_Short']='District'
			subgroupCount.loc[subgroupCount.index[-1], column.name]='Y'
			subgroupCount=subgroupCount.set_index('School_Short')
		
			subgroupCount['PercentageMetEOYBenchmark']=(subgroupCount[col_y]/subgroupCount['total']).mul(100).round(1)
			subgroupCount.loc[subgroupCount.total == 0, 'PercentageMetEOYBenchmark'] = ""
			subgroupCount.loc[(subgroupCount['total'] <= 10) & (subgroupCount['total'] > 0), 'PercentageMetEOYBenchmark'] = "*"
			subgroupCount=subgroupCount.reset_index()
			subgroupCount=subgroupCount[['School_Short', column.name, 'PercentageMetEOYBenchmark']]
			subgroupCount['PercentageMetEOYBenchmark']=subgroupCount['PercentageMetEOYBenchmark'].astype(str)+"%"
			subgroupCount=subgroupCount.rename(columns = {'PercentageMetEOYBenchmark': vs_obj.assessment_type+"_"+name+" "+column.name+"_"+vs_obj.terms[0]})
			
			subgroupCount=subgroupCount.set_index('School_Short')
			subgroupCount[column.name] = subgroupCount[column.name].replace(r'^\s*$', np.nan, regex=True)
			final_subgroupCount=subgroupCount[subgroupCount[column.name] =='Y']
			final_subgroupCount=final_subgroupCount.replace(to_replace="*%",value="*").replace(to_replace="%",value="")
			final_subgroupCount=final_subgroupCount.drop(columns=column.name)
			
			finalDfs.append(final_subgroupCount)
		
	
def starSB(vs_obj, finalDfs):
	idxRename = {'All':'District'} 
	vs_obj.df=vs_obj.df[vs_obj.columns]

    #new for 23-24 only calculating 9-11
	newSTARFilter(vs_obj)

	#for previous cycles grade level filters were applied using the starFilters(vs_obj) function
	#starFilters(vs_obj)
	vs_obj.df=vs_obj.df.sort_values('CompletedDate').groupby('Student_Number').tail(1)
	vs_obj.df=vs_obj.df[vs_obj.df.AssessmentStatus =='Active']

	SBcrosstab_ALLCount=pd.crosstab([vs_obj.df.School_Short],[vs_obj.df.StateBenchmarkProficient])
	SBcrosstab_ALL=pd.crosstab([vs_obj.df.School_Short],[vs_obj.df.StateBenchmarkProficient],normalize='index', margins=True,margins_name='District').mul(100).round(1).astype(str)+"%"
	
	rename={}
	races={'Yes':'ALL','African_American':'AA', 'American Indian':'AI','American_Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       'Pac_Islander':'PI', 'White':'W'}
	for col in SBcrosstab_ALL.columns:
		if col in races.keys():
			temp_col = vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
			
	SBcrosstab_ALL.rename(columns=rename, inplace=True)
	SBcrosstab_ALL = SBcrosstab_ALL.rename(index=idxRename)
	finalDfs.append(SBcrosstab_ALL)
	
	SBcrosstab=pd.crosstab([vs_obj.df.School_Short],[vs_obj.df.Race_Ethn,vs_obj.df.StateBenchmarkProficient])
	Sbcrosstab=SBcrosstab.T
	Sbcrosstab.reset_index()
	
	numerator = Sbcrosstab.groupby(["Race_Ethn", "StateBenchmarkProficient"]).sum()
	denominator = Sbcrosstab.groupby("Race_Ethn").sum()
	denominator=denominator.mask((denominator <= 10) & (denominator >= 1), -1)
	
	rslt=numerator.div(denominator, level = 0, axis = 'index').mul(100).round(1)
	rslt=rslt.mask(rslt < 0, "*").astype(str)+"%"
	rslt=rslt.replace(to_replace="*%",value="*").replace(to_replace="-0.0%",value="*").replace(to_replace="nan%",value="")
	rslt=rslt.T
	rslt.columns=['_'.join(col) for col in rslt.columns.values]
	
	supesRace=pd.crosstab([vs_obj.df['Race_Ethn']],[vs_obj.df['StateBenchmarkProficient']],margins=True)
	supesRace['District']=(supesRace['Yes']/supesRace['All']).mul(100).round(1).astype(str) + '%'
	supesRace['District'] = np.where((supesRace['All']) <= 10,'*',supesRace['District'])
	supesRace = supesRace.rename(index=idxRename)
	supesRace= supesRace.drop(columns=['No','Yes','All'])
	supesRace=supesRace.T
	
	rename={}
	races={'African_American':'AA', 'African_American_Yes':'AA','American Indian':'AI','American_Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       'Pac_Islander':'PI', 'White':'W', 'District':'ALL','American Indian_Yes':'AI','American_Indian_Yes':'AI','Asian_Yes':'A', 'Filipino_Yes':'F', 'Hispanic_Yes':'HL', 'Mult_Yes':'Mult',
       'Pac_Islander_Yes':'PI', 'White_Yes':'W'}
	
	for col in supesRace.columns:
		if col in races.keys():
			temp_col = vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
	for col in rslt.columns:
		if col in races.keys():
			temp_col = vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
	
	supesRace.index.rename('School_Short', inplace=True)
	supesRace=supesRace.reset_index().set_index('School_Short')		
	supesRace.rename(columns=rename, inplace=True)
	dropped=['STARmathSB ALL_SP2023','STARreadSB ALL_SP2023']
	for col in dropped:
		if col in supesRace.columns:
			supesRace=supesRace.drop(columns=col)
	rslt.rename(columns=rename, inplace=True)
	rslt = pd.concat([rslt, supesRace])
	finalDfs.append(rslt)
	
	subgroups(vs_obj,0, finalDfs)
	grade_levels(vs_obj, 0, finalDfs)
	
def selSGP(vs_obj, finalDfs):
	vs_obj.df=vs_obj.df[vs_obj.columns]
	
	#not calculated for Fall. No Growth measure exists until Winter
	if (vs_obj.subjects[0] == 'EarlyLit') and ('W2022' in vs_obj.terms):
		vs_obj.df=vs_obj.df[vs_obj.df.AssessmentStatus =='Active']
		starFilters(vs_obj)
		vs_obj.df['StudentGrowthPercentileFallWinter']=pd.to_numeric(vs_obj.df['StudentGrowthPercentileFallWinter'])
		vs_obj.df=vs_obj.df.dropna()	
		vs_obj.df['StudentGrowthPercentileFallWinter'] = vs_obj.df['StudentGrowthPercentileFallWinter'].astype(int)	
		vs_obj.df['Typical and High'] = vs_obj.df['StudentGrowthPercentileFallWinter'].apply(lambda x:1 if x >=35 else 0)
		vs_obj.df['Low'] = vs_obj.df['StudentGrowthPercentileFallWinter'].apply(lambda x:1 if x <35 else 0)
		vs_obj.df['LowHigh']=vs_obj.df['Low']+vs_obj.df['Typical and High']
		CountAll = vs_obj.df.groupby(["School_Short"])["StudentGrowthPercentileFallWinter"].count().reset_index(name="count")

	if (vs_obj.subjects[0] == 'EarlyLit' or vs_obj.subjects[0] == 'SPEarlyLit') and ('SP2023' in vs_obj.terms):
		vs_obj.df=vs_obj.df[vs_obj.df.AssessmentStatus =='Active']
		starFilters(vs_obj)
		vs_obj.df['StudentGrowthPercentileFallSpring']=pd.to_numeric(vs_obj.df['StudentGrowthPercentileFallSpring'])
		vs_obj.df=vs_obj.df.dropna()	
		vs_obj.df['StudentGrowthPercentileFallSpring'] = vs_obj.df['StudentGrowthPercentileFallSpring'].astype(int)	
		vs_obj.df['Typical and High'] = vs_obj.df['StudentGrowthPercentileFallSpring'].apply(lambda x:1 if x >=35 else 0)
		vs_obj.df['Low'] = vs_obj.df['StudentGrowthPercentileFallSpring'].apply(lambda x:1 if x <35 else 0)
		vs_obj.df['LowHigh']=vs_obj.df['Low']+vs_obj.df['Typical and High']
		CountAll = vs_obj.df.groupby(["School_Short"])["StudentGrowthPercentileFallSpring"].count().reset_index(name="count")
		
		#SGP for ALL students
		
		CountTypicalHigh = vs_obj.df[['School_Short','Typical and High']].groupby(['School_Short']).sum()
		rslt=CountAll.merge(CountTypicalHigh, how='inner', on='School_Short')
		rslt=rslt.reset_index(drop=True).set_index('School_Short')
		rslt.loc['District']=rslt.sum()
		rslt['SGPPercentage']= rslt['Typical and High']/rslt['count']
		rslt['SGPPercentage']=rslt['SGPPercentage'].mul(100).round(1).astype(str) + '%'
		
		rslt=rslt.replace(to_replace="-100.0%",value="*")
		rslt=rslt.rename(columns={'SGPPercentage':vs_obj.assessment_column+vs_obj.subjects[0]+vs_obj.metrics[0]+" ALL_"+vs_obj.terms[0]})
		
		finalDfs.append(rslt)

		vs_obj.df.loc[vs_obj.df['SPED'] =='ESN', 'SPED'] = 'Y'
		vs_obj.df.loc[vs_obj.df['SPED']=='MMSN', 'SPED'] = 'Y'
	
		vs_obj.df['FIT']=vs_obj.df['FIT'].fillna('')
		vs_obj.df.loc[vs_obj.df['FIT'] != '' , 'FIT'] = 'Y'
		vs_obj.df.loc[vs_obj.df['EL']=='EL', 'EL'] = 'Y'
		vs_obj.df.loc[vs_obj.df['Foster']=='Foster', 'Foster'] = 'Y'

		for column in [vs_obj.df.Foster,vs_obj.df.SPED, vs_obj.df.FIT, vs_obj.df.EL]:

			tableframe = pd.pivot_table(vs_obj.df, values=['Typical and High','LowHigh'], index=['School_Short', column], aggfunc=np.sum)
			tableframe.reset_index(inplace=True)
			if 'SPED' in tableframe.columns:
				tableframe = tableframe[tableframe['SPED'] == 'Y']
				tableframe.loc['District'] = tableframe.iloc[:, :].sum()
				
			elif 'FIT' in tableframe.columns:
				tableframe = tableframe[tableframe['FIT'] == 'Y']
				tableframe.loc['District'] = tableframe.iloc[:, :].sum()

			elif 'EL' in tableframe.columns:
				tableframe = tableframe[tableframe['EL'] == 'Y']
				tableframe.loc['District'] = tableframe.iloc[:, :].sum()

			elif 'Foster' in tableframe.columns:
				tableframe = tableframe[tableframe['Foster'] == 'Y']
				tableframe.loc['District'] = tableframe.iloc[:, :].sum()

			tableframe['SGPPercentage']= tableframe['Typical and High']/tableframe['LowHigh'] * 100
			tableframe['SGPPercentage'] = tableframe['SGPPercentage'].round(1)
			tableframe['SGPPercentage'] = np.where((tableframe['LowHigh']) <= 10,'-1',tableframe['SGPPercentage'])
			tableframe['SGPPercentage']=tableframe['SGPPercentage'].astype(str)+"%"

			tableframe.reset_index(inplace=True)
			
			tableframe=tableframe.replace(to_replace="-1%", value="*")
			tableframe=tableframe.rename(columns = {'SGPPercentage': vs_obj.assessment_column+vs_obj.subjects[0]+"SGP"+" "+column.name+"_"+vs_obj.terms[0]})
			
			tableframe=tableframe.set_index('School_Short')
			tableframe.rename({tableframe.index[-1]: 'District'}, inplace=True)
			
			finalDfs.append(tableframe)

				
		#Race SEL SGP
		selSGPRace = pd.pivot_table(vs_obj.df, values=['Typical and High','LowHigh'], index=['School_Short', 'Race_Ethn'], aggfunc=np.sum, margins=True)
		selSGPRace['SGPPercentage']= selSGPRace['Typical and High']/selSGPRace['LowHigh'] * 100
		selSGPRace['SGPPercentage'] = selSGPRace['SGPPercentage'].map('{:,.1f}'.format)
		selSGPRace['SGPPercentage'] = np.where((selSGPRace['LowHigh']) <= 10,'-1',selSGPRace['SGPPercentage'])
		selSGPRace.reset_index(inplace=True)
		
		selSGPRace2=selSGPRace.pivot(index='School_Short',
									columns= 'Race_Ethn',
									values='SGPPercentage')
	
		selSGPRace2=selSGPRace2.astype(str) + '%'
		selSGPRace2=selSGPRace2.replace(to_replace="-1%",value="*").replace(to_replace="nan%",value="")
		
		
		#district totals
		distTable=pd.pivot_table(vs_obj.df, values=['Typical and High','LowHigh'], index=['Race_Ethn'], aggfunc=np.sum)
		distTable['District']= distTable['Typical and High']/distTable['LowHigh'] * 100
		distTable['District'] = distTable['District'].map('{:,.1f}'.format)
		distTable['District'] = np.where((distTable['LowHigh']) <= 10,'-1',distTable['District'])
		distTable= distTable.drop(columns=['LowHigh','Typical and High'])
		distTable=distTable.T
	
		rename={}
		races={'African_American':'AA', 'American Indian':'AI','American_Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       'Pac_Islander':'PI', 'White':'W'}
		for col in distTable.columns:
			if col in races.keys():
				temp_col = vs_obj.assessment_column+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+"_"+vs_obj.terms[0]
				rename[col]=temp_col
		distTable.index.rename('School_Short', inplace=True)
		distTable=distTable.reset_index().set_index('School_Short')		
		distTable.rename(columns=rename, inplace=True)
		distTable=distTable.round(1).astype(str) + '%'	
		
		for col in selSGPRace2.columns:
			if col in races.keys():		
				temp_col = vs_obj.assessment_column+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+"_"+vs_obj.terms[0]
				rename[col]=temp_col
		
		selSGPRace2.rename(columns=rename, inplace=True)
		rslt = pd.concat([selSGPRace2, distTable])
		rslt=rslt.replace(to_replace="-1%", value="*").replace(to_replace="-1", value="*")
		finalDfs.append(rslt)

		grade_levels(vs_obj, 0,finalDfs)
								
def starSGP(vs_obj, finalDfs):
	#STAR SGP
	vs_obj.df=vs_obj.df[vs_obj.columns]
	
    #for 23-24 only calculating 9-11
	newSTARFilter(vs_obj)

	#for previous cycles grade level filters were applied using the starFilters(vs_obj) function
	#starFilters(vs_obj)
	if vs_obj.terms[0] == 'FA2022':
		#.dropna() here and then drop again after converting to_numeric()

		vs_obj.df=vs_obj.df.dropna()
		vs_obj.df['StudentGrowthPercentileFallFall']=pd.to_numeric(vs_obj.df['StudentGrowthPercentileFallFall'])
	
		#have to dropna() after coverting to numeric
		vs_obj.df=vs_obj.df.dropna()	
		vs_obj.df['StudentGrowthPercentileFallFall'] = vs_obj.df['StudentGrowthPercentileFallFall'].astype(int)	
		vs_obj.df['Typical and High'] = vs_obj.df['StudentGrowthPercentileFallFall'].apply(lambda x:1 if x >=35 else 0)
	
		#SGP for ALL students
		CountAll = vs_obj.df.groupby(["School_Short"])["StudentGrowthPercentileFallFall"].count().reset_index(name="count")

	elif vs_obj.terms[0] == 'W2022':
		#.dropna() here and then drop again after converting to_numeric()
		vs_obj.df=vs_obj.df.dropna()
		vs_obj.df['StudentGrowthPercentileFallWinter']=pd.to_numeric(vs_obj.df['StudentGrowthPercentileFallWinter'])
	
		#have to dropna() after coverting to numeric
		vs_obj.df=vs_obj.df.dropna()	
		vs_obj.df['StudentGrowthPercentileFallWinter'] = vs_obj.df['StudentGrowthPercentileFallWinter'].astype(int)	
		vs_obj.df['Typical and High'] = vs_obj.df['StudentGrowthPercentileFallWinter'].apply(lambda x:1 if x >=35 else 0)
	
		#SGP for ALL students
		CountAll = vs_obj.df.groupby(["School_Short"])['StudentGrowthPercentileFallWinter'].count().reset_index(name="count")	
	
	elif vs_obj.terms[0] == 'SP2023':
		#.dropna() here and then drop again after converting to_numeric()
		vs_obj.df=vs_obj.df.dropna()
		vs_obj.df['StudentGrowthPercentileFallSpring']=pd.to_numeric(vs_obj.df['StudentGrowthPercentileFallSpring'])
	
		#have to dropna() after coverting to numeric
		vs_obj.df=vs_obj.df.dropna()	
		vs_obj.df['StudentGrowthPercentileFallSpring'] = vs_obj.df['StudentGrowthPercentileFallSpring'].astype(int)	
		vs_obj.df['Typical and High'] = vs_obj.df['StudentGrowthPercentileFallSpring'].apply(lambda x:1 if x >=35 else 0)
	
		#SGP for ALL students
		CountAll = vs_obj.df.groupby(["School_Short"])["StudentGrowthPercentileFallSpring"].count().reset_index(name="count")

	CountTypicalHigh = vs_obj.df[['School_Short','Typical and High']].groupby(['School_Short']).sum()
	rslt=CountAll.merge(CountTypicalHigh, how='inner', on='School_Short')
	rslt=rslt.reset_index(drop=True).set_index('School_Short')
	rslt.loc['District']=rslt.sum()
	rslt['SGPPercentage']= rslt['Typical and High']/rslt['count']
	rslt['SGPPercentage']=rslt['SGPPercentage'].mul(100).round(1).astype(str) + '%'
	

	rename={}
	races={'African_American':'AA', 'African_American_Yes':'AA','American Indian':'AI','American_Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       'Pac_Islander':'PI', 'White':'W', 'District':'ALL','American Indian_Yes':'AI','American_Indian_Yes':'AI','Asian_Yes':'A', 'Filipino_Yes':'F', 'Hispanic_Yes':'HL', 'Mult_Yes':'Mult',
       'Pac_Islander_Yes':'PI', 'White_Yes':'W', 'District':'ALL', 'SGPPercentage':'ALL'}
	for col in rslt.columns:
		if col in races.keys():
			temp_col = vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
	rslt.rename(columns=rename, inplace=True)
	finalDfs.append(rslt)

	grade_levels(vs_obj, 0,finalDfs)
	
	
def starSGPSubgroups(vs_obj,finalDfs):	
	#SGP Race/Ethnicity
	vs_obj.df=vs_obj.df[vs_obj.columns]
	
	#for 23-24 only calculating 9-11
	newSTARFilter(vs_obj)

	#for previous cycles grade level filters were applied using the starFilters(vs_obj) function
	#starFilters(vs_obj)
	vs_obj.df=vs_obj.df.sort_values('CompletedDate').groupby('StudentIdentifier').tail(1)
	
	#.dropna() here and then drop again after converting to_numeric()
	vs_obj.df=vs_obj.df.dropna()
	if vs_obj.terms[0] == 'FA2022':
		vs_obj.df['StudentGrowthPercentileFallFall']=pd.to_numeric(vs_obj.df['StudentGrowthPercentileFallFall'])
		#have to dropna() after coverting to numeric
		vs_obj.df=vs_obj.df.dropna()	
		vs_obj.df['StudentGrowthPercentileFallFall'] = vs_obj.df['StudentGrowthPercentileFallFall'].astype(int)	
		vs_obj.df['Typ_High'] = vs_obj.df['StudentGrowthPercentileFallFall'].apply(lambda x:1 if x >=35 else 0)
		vs_obj.df['Low'] = vs_obj.df['StudentGrowthPercentileFallFall'].apply(lambda x:1 if x <35 else 0)
	
	elif vs_obj.terms[0] == 'W2022':
		vs_obj.df['StudentGrowthPercentileFallWinter']=pd.to_numeric(vs_obj.df['StudentGrowthPercentileFallWinter'])
		#have to dropna() after coverting to numeric
		vs_obj.df=vs_obj.df.dropna()	
		vs_obj.df['StudentGrowthPercentileFallWinter'] = vs_obj.df['StudentGrowthPercentileFallWinter'].astype(int)	
		vs_obj.df['Typ_High'] = vs_obj.df['StudentGrowthPercentileFallWinter'].apply(lambda x:1 if x >=35 else 0)
		vs_obj.df['Low'] = vs_obj.df['StudentGrowthPercentileFallWinter'].apply(lambda x:1 if x <35 else 0)

	elif vs_obj.terms[0] == 'SP2023':
		vs_obj.df['StudentGrowthPercentileFallSpring']=pd.to_numeric(vs_obj.df['StudentGrowthPercentileFallSpring'])
		#have to dropna() after coverting to numeric
		vs_obj.df=vs_obj.df.dropna()	
		vs_obj.df['StudentGrowthPercentileFallSpring'] = vs_obj.df['StudentGrowthPercentileFallSpring'].astype(int)	
		vs_obj.df['Typ_High'] = vs_obj.df['StudentGrowthPercentileFallSpring'].apply(lambda x:1 if x >=35 else 0)
		vs_obj.df['Low'] = vs_obj.df['StudentGrowthPercentileFallSpring'].apply(lambda x:1 if x <35 else 0)


	vs_obj.df['LowHigh']=vs_obj.df['Low']+vs_obj.df['Typ_High']
	SGPCount=pd.crosstab([vs_obj.df.School_Short], [vs_obj.df.Race_Ethn], values=vs_obj.df.StudentIdentifier,aggfunc='count',margins=True)
	SGPTYPHIGH=pd.crosstab([vs_obj.df.School_Short], [vs_obj.df.Race_Ethn, vs_obj.df.Typ_High], values=vs_obj.df.StudentIdentifier, aggfunc='count',margins=True)
	
	tableframe = pd.pivot_table(vs_obj.df, values=['Typ_High','LowHigh'], index=['School_Short','Race_Ethn'], aggfunc=np.sum)
	tableframe['SGPPercentage']= tableframe['Typ_High']/tableframe['LowHigh'] * 100
	tableframe['SGPPercentage'] = tableframe['SGPPercentage'].map('{:,.1f}'.format)
	tableframe['SGPPercentage'] = np.where((tableframe['LowHigh']) <= 10,'-1',tableframe['SGPPercentage'])
	
	tableframe.reset_index(inplace=True)
	tableframe2=tableframe.pivot(index='School_Short',
									columns= 'Race_Ethn',
									values='SGPPercentage')
	
	tableframe2=tableframe2.replace(to_replace=-1,value="*")
	tableframe2=tableframe2.round(1).astype(str) + '%'
	tableframe2=tableframe2.replace(to_replace="*%",value="*").replace(to_replace="nan%",value="")
	
	
	#district totals
	distTable=pd.pivot_table(vs_obj.df, values=['Typ_High','LowHigh'], index=['Race_Ethn'], aggfunc=np.sum)
	distTable['District']= distTable['Typ_High']/distTable['LowHigh'] * 100
	distTable['District'] = distTable['District'].map('{:,.1f}'.format)
	distTable['District'] = np.where((distTable['LowHigh']) <= 10,'-1',distTable['District'])
	
	#distTable = distTable.rename(index=idxRename)
	distTable= distTable.drop(columns=['LowHigh','Typ_High'])
	distTable=distTable.T
	
	rename={}
	races={'African_American':'AA', 'American Indian':'AI','American_Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       'Pac_Islander':'PI', 'White':'W'}
	for col in distTable.columns:
		if col in races.keys():
			temp_col = vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
	distTable.index.rename('School_Short', inplace=True)
	distTable=distTable.reset_index().set_index('School_Short')		
	distTable.rename(columns=rename, inplace=True)
	distTable=distTable.round(1).astype(str) + '%'
	
	rename={}
	races={'African_American':'AA', 'African_American_Yes':'AA','American Indian':'AI','American_Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       'Pac_Islander':'PI', 'White':'W', 'District':'ALL','American Indian_Yes':'AI','American_Indian_Yes':'AI','Asian_Yes':'A', 'Filipino_Yes':'F', 'Hispanic_Yes':'HL', 'Mult_Yes':'Mult',
       'Pac_Islander_Yes':'PI', 'White_Yes':'W', 'District':'ALL', 'SGPPercentage':'ALL'}
	for col in tableframe2.columns:
		if col in races.keys():
			temp_col = vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
	tableframe2.rename(columns=rename, inplace=True)

	rslt = pd.concat([tableframe2, distTable])
	rslt=rslt.replace(to_replace="-1%", value="*").replace(to_replace="-1", value="*")
	finalDfs.append(rslt)
	
	vs_obj.df.loc[vs_obj.df['SPED'] =='ESN', 'SPED'] = 'Y'
	vs_obj.df.loc[vs_obj.df['SPED']=='MMSN', 'SPED'] = 'Y'
	vs_obj.df.loc[vs_obj.df['SPED']=='', 'SPED'] = 'NotSPED'
	
	vs_obj.df['FIT']=vs_obj.df['FIT'].fillna('')
	vs_obj.df.loc[vs_obj.df['FIT'] != '' , 'FIT'] = 'Y'
	vs_obj.df.loc[vs_obj.df['EL']=='EL', 'EL'] = 'Y'
	vs_obj.df.loc[vs_obj.df['Foster']=='Foster', 'Foster'] = 'Y'
	for column in [vs_obj.df.Foster,vs_obj.df.SPED, vs_obj.df.FIT, vs_obj.df.EL]:

		tableframe = pd.pivot_table(vs_obj.df, values=['Typ_High','LowHigh'], index=['School_Short', column], aggfunc=np.sum)
		#tableframe.columns=['_'.join(col) for col in tableframe.columns.values]
		tableframe.reset_index(inplace=True)
		
		if 'SPED' in tableframe.columns:
			tfSubgroup = tableframe[tableframe['SPED'] == 'Y']
			
		if 'FIT' in tableframe.columns:
			tfSubgroup = tableframe[tableframe['FIT'] == 'Y']
			
		if 'EL' in tableframe.columns:
			tfSubgroup = tableframe[tableframe['EL'] == 'Y']

		if 'Foster' in tableframe.columns:
			tfSubgroup = tableframe[tableframe['Foster'] == 'Y']
			
		tfSubgroup=tfSubgroup.set_index('School_Short')
		tfSubgroup.loc['District']=tfSubgroup.sum()
		tfSubgroup.loc[tfSubgroup.index[-1], column.name]=''
		
		tfSubgroup['SGPPercentage']= tfSubgroup['Typ_High']/tfSubgroup['LowHigh'] * 100
		tfSubgroup['SGPPercentage'] = tfSubgroup['SGPPercentage'].round(1)
		tfSubgroup['SGPPercentage'] = np.where((tfSubgroup['LowHigh']) <= 10,'-1',tfSubgroup['SGPPercentage'])
		tfSubgroup['SGPPercentage']=tfSubgroup['SGPPercentage'].astype(str)+"%"
		tfSubgroup=tfSubgroup.rename(columns = {'SGPPercentage': vs_obj.assessment_column+vs_obj.subjects[0]+"SGP"+" "+column.name+"_"+vs_obj.terms[0]})
		tfSubgroup=tfSubgroup.replace(to_replace="-1%", value="*")

		finalDfs.append(tfSubgroup)
		
def iReadyPP(vs_obj,finalDfs):
	#for Reading assessments, there was no need to filter for duplicates when students took a test in Spanish and English because there were no duplicates
	#if vs_obj.terms[0] == 'W2022':
		vs_obj.df=vs_obj.df[vs_obj.columns]
		vs_obj.df=vs_obj.df[vs_obj.df['Most Recent Diagnostic (Y/N)'] =='Y']
		vs_obj.df=vs_obj.df[vs_obj.df.Enrolled =='Enrolled']
		if vs_obj.terms[0] == 'W2022':
			winterWindowFilter(vs_obj)
		
		if vs_obj.terms[0] == 'SP2023':
			iReadyFilter(vs_obj)
			springWindowFilter(vs_obj)

		vs_obj.df = vs_obj.df.replace(r'^\s*$', np.nan, regex=True)

		if vs_obj.terms[0] == 'W2022':
			column = vs_obj.df['Percent Progress to Annual Typical Growth (%)']
		if vs_obj.terms[0] == 'SP2023':
			column = vs_obj.df['Proficiency if Student Shows No Additional Growth']
		
		#Race for District Totals
		ppDistRace=pd.crosstab([vs_obj.df['Race_Ethn']],[column], values=vs_obj.df.Student_Number, aggfunc='count',normalize='index',margins=True)
		ppDistRace=ppDistRace.fillna(0)		
		ppDistRace['ProjProf']=ppDistRace['Level 3']+ppDistRace['Level 4']
		ppDistRace =ppDistRace.drop(['Level 1', 'Level 2', 'Level 3', 'Level 4'], axis=1)
		ppDistRace=ppDistRace.mul(100).round(2).astype(str) + '%'
		ppDistRace=ppDistRace.T
		idxRename={'ProjProf':'District'}
		ppDistRace = ppDistRace.rename(index=idxRename)
		ppDistRace.index.name='School_Short'

		rename={}
		races={'African_American':'AA', 'American_Indian':'AI','American Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
		   			'Pac_Islander':'PI', 'White':'W', 'District':'District', 'All':'ALL'}
		for col in ppDistRace.columns:
			if col in races.keys():		
				temp_col = vs_obj.assessment_type+vs_obj.subjects[0]+'ProjProf '+races[col]+"_"+vs_obj.terms[0]
				rename[col]=temp_col
			
		ppDistRace.rename(columns=rename, inplace=True)
		
		#iReady PP ALL by School	
		rslt=pd.crosstab([vs_obj.df.School_Short],[column], 
							values=vs_obj.df.Student_Number, aggfunc='count',normalize='index',margins=True)

		rsltCount=pd.crosstab([vs_obj.df.School_Short],[column], 
							values=vs_obj.df.Student_Number, aggfunc='count',margins=True)
		rslt.fillna(0)
		rslt['ProjProf']=rslt['Level 3']+rslt['Level 4']
		rslt =rslt.drop(['Level 1', 'Level 2', 'Level 3', 'Level 4'], axis=1)
		rslt=rslt.mul(100).round(2).astype(str) + '%'
		idxRename = {'All':'District'}
		rslt = rslt.rename(index=idxRename)

		rslt['ProjProf'] = np.where((rsltCount['All']) <= 10,'*',rslt['ProjProf'])
		rslt.rename(columns={'ProjProf':vs_obj.assessment_type+vs_obj.subjects[0]+'ProjProf ALL'+"_"+vs_obj.terms[0]}, inplace=True)
		
		#Grade Level iReadyPP
		grdLvl=pd.crosstab([vs_obj.df.School_Short, vs_obj.df['Grade Level']],[column], 
							values=vs_obj.df.Student_Number, aggfunc='count',normalize='index').mul(100).round(1)
		
		
		grdLvlCount=pd.crosstab([vs_obj.df.School_Short, vs_obj.df['Grade Level']],[column], 
							values=vs_obj.df.Student_Number, aggfunc='count')

		grdLvlCount = grdLvlCount.fillna(0)
		grdLvlCount['Total']=grdLvlCount['Level 1']+grdLvlCount['Level 2']+grdLvlCount['Level 3']+grdLvlCount['Level 4']

		grdLvl['ProjProf']=grdLvl['Level 3']+grdLvl['Level 4']
		grdLvl['ProjProf'] = np.where((grdLvlCount['Total']) <= 10,'*',grdLvl['ProjProf'])

		grdLvl =grdLvl.drop(['Level 1', 'Level 2', 'Level 3', 'Level 4'], axis=1)
		grdLvl=grdLvl.reset_index()
		grdLvl = grdLvl.pivot(index='School_Short',
										columns= 'Grade Level',
										values='ProjProf').astype(str)+"%"

		grdLvl=grdLvl.replace(to_replace="nan%",value="").replace(to_replace="*%", value="*")

		grdLvlDist=pd.crosstab([vs_obj.df['Grade Level']],[column], 
							values=vs_obj.df.Student_Number, aggfunc='count',normalize='index').mul(100).round(1)

		grdLvlDistCount=pd.crosstab([vs_obj.df['Grade Level']],[column], 
							values=vs_obj.df.Student_Number, aggfunc='count')

		grdLvlDistCount = grdLvlDistCount.fillna(0)
		grdLvlDistCount['Total']=grdLvlDistCount['Level 1']+grdLvlDistCount['Level 2']+grdLvlDistCount['Level 3']+grdLvlDistCount['Level 4']

		grdLvlDist['ProjProf']=(grdLvlDist['Level 3']+grdLvlDist['Level 4']).astype(str)+"%"
		grdLvlDist['ProjProf'] = np.where((grdLvlDistCount['Total']) <= 10,'*',grdLvlDist['ProjProf'])
		grdLvlDist =grdLvlDist.drop(['Level 1', 'Level 2', 'Level 3', 'Level 4'], axis=1)
		grdLvlDist=grdLvlDist.rename(columns={'ProjProf':'District'})
		grdLvlDist=grdLvlDist.T

		grdLvl=pd.concat([grdLvl, grdLvlDist])
		
		grdLvl=gradeLevelAddColumns(vs_obj, grdLvl)
		gradeLevelTab.append(grdLvl)

		#Race iReady PP		
		rsltRacePPCount=pd.crosstab([vs_obj.df.School_Short, vs_obj.df.Race_Ethn],[column],
							 values=vs_obj.df.Student_Number, aggfunc='count')
		
		rsltRacePPCount=rsltRacePPCount.fillna(0)
		rsltRacePPCount['StuGroupCount']=rsltRacePPCount['Level 1']+rsltRacePPCount['Level 2']+rsltRacePPCount['Level 3']+rsltRacePPCount['Level 4']
		rsltRace=pd.crosstab([vs_obj.df.School_Short, vs_obj.df.Race_Ethn],[column], 
								values=vs_obj.df.Student_Number, aggfunc='count',normalize='index',margins=True)
		rsltRace['ProjProf']=rsltRace['Level 3']+rsltRace['Level 4']
		rsltRace['StuCount']= rsltRacePPCount['StuGroupCount']
		rsltRace.loc[rsltRace['StuCount'] <= 10, 'ProjProf'] = -1
		
		rsltRace =rsltRace.apply(pd.to_numeric)
		rsltRace.reset_index(inplace=True)
		
		rsltRacePP = rsltRace.pivot(index='School_Short',
										columns= 'Race_Ethn',
										values='ProjProf')
		

		rsltRacePP=rsltRacePP.mul(100).round(1).astype(str) + '%'
		rsltRacePP=rsltRacePP.replace(to_replace="-100.0%",value="*").replace(to_replace="nan%",value="")
		
		rename={}
		races={'African_American':'AA', 'American_Indian':'AI','American Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
		   			'Pac_Islander':'PI', 'White':'W'} #'ProjProf':'ALL'

		for col in rsltRacePP.columns:
			if col in races.keys():		
				temp_col = vs_obj.assessment_type+vs_obj.subjects[0]+'ProjProf '+races[col]+"_"+vs_obj.terms[0]
				rename[col]=temp_col
		
		rsltRacePP.rename(columns=rename, inplace=True)
		

		#appends ppDistRace for District total to rsltRaceGL_pivt which has by school totals
				
		frames=[rsltRacePP,rslt]
		rsltRaceFinal=pd.concat(frames,
							    axis=1,
							    join="outer",
							    ignore_index=False,
							    keys=None,
							    levels=None,
							    names=None,
							    verify_integrity=False,
							    copy=True,
							)

		rsltRaceFinal=rsltRaceFinal.reset_index()
		rsltRaceFinal.drop(index=rsltRaceFinal[rsltRaceFinal['School_Short'] == 'District'].index, inplace=True)
		rsltRaceFinal=pd.concat([rsltRaceFinal, ppDistRace])
		rsltRaceFinal['School_Short'] = rsltRaceFinal['School_Short'].fillna('District')
		rsltRaceFinal=rsltRaceFinal.reset_index(drop=True).set_index('School_Short')
		idxRename = {'':'District'}
		rsltRaceFinal = rsltRaceFinal.rename(index=idxRename)
		finalDfs.append(rsltRaceFinal)
		
		subgroups(vs_obj,0, finalDfs)

def iReadyGradeLevel(vs_obj,finalDfs):

	vs_obj.df=vs_obj.df[vs_obj.columns]

	if (vs_obj.terms[0] =='W2022'):
		iReadyFilter(vs_obj)
		winterWindowFilter(vs_obj)
		vs_obj.df= vs_obj.df.replace(r'^\s*$', np.nan, regex=True)
		
	elif (vs_obj.terms[0] == 'FA2022'):
		vs_obj.df=vs_obj.df[vs_obj.columns]
		iReadyFilter(vs_obj)

	elif (vs_obj.terms[0] == 'SP2023'):
		vs_obj.df=vs_obj.df[vs_obj.columns]
		iReadyFilter(vs_obj)
		springWindowFilter(vs_obj)
		
		

	rslt=pd.crosstab([vs_obj.df.School_Short],[vs_obj.df['Overall Relative Placement']], values=vs_obj.df.Student_Number, aggfunc='count', margins=True)

	rslt=rslt.rename(index={'All':'District'})

	rslt=rslt.fillna(0)
	rslt['On or Above']=rslt['Early On Grade Level']+rslt['Mid or Above Grade Level']


	if '3 or More Grade Levels Below' in rslt.columns:
		rslt['Below']=rslt['1 Grade Level Below']+rslt['2 Grade Levels Below']+rslt['3 or More Grade Levels Below']
	else:
		rslt['Below']=rslt['1 Grade Level Below']+rslt['2 Grade Levels Below']
	
	rslt['totalCount']=rslt['On or Above']+rslt['Below']
	
	
	drop_cols=['1 Grade Level Below', '2 Grade Levels Below', '3 or More Grade Levels Below']
	for col in drop_cols:
		if col in rslt.columns:
			rslt =rslt.drop(col, axis=1)

		
	rename={}
	races={'African_American':'AA', 'American_Indian':'AI','American Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
	   			'Pac_Islander':'PI', 'White':'W', 'Percent_On_Above':'ALL'}
	for col in rslt.columns:
		if col in races.keys():		
			temp_col = vs_obj.assessment_type+vs_obj.subjects[0]+'GradeLevel '+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col

	rslt.rename(columns=rename, inplace=True)
	

	rslt['Percent_On_Above']=(rslt['On or Above']/rslt['totalCount']).mul(100).round(1).astype('str')+'%'
	rslt['Percent_On_Above'] = np.where((rslt['All']) <= 10,'*',rslt['Percent_On_Above'])
	rslt=rslt.rename(columns={'Percent_On_Above':vs_obj.assessment_type+vs_obj.subjects[0]+'GradeLevel '+"ALL_"+vs_obj.terms[0]})
	#grade level tabs
	grdLvl=pd.crosstab([vs_obj.df.School_Short, vs_obj.df['Grade Level']],[vs_obj.df['Overall Relative Placement']], 
						values=vs_obj.df.Student_Number, aggfunc='count',normalize='index')


	grdLvlCount=pd.crosstab([vs_obj.df.School_Short, vs_obj.df['Grade Level']],[vs_obj.df['Overall Relative Placement']], 
						values=vs_obj.df.Student_Number, aggfunc='count')

	grdLvlCount=grdLvlCount.fillna(0)
	grdLvl['Percent_On_Above']=grdLvl['Early On Grade Level']+grdLvl['Mid or Above Grade Level']
	drop_cols=['1 Grade Level Below', '2 Grade Levels Below', '3 or More Grade Levels Below']
	for col in drop_cols:
		if col in grdLvl.columns:
			grdLvl =grdLvl.drop(col, axis=1)
	grdLvl=grdLvl.mul(100).round(2).astype(str) + '%'
	grdLvl=grdLvl.reset_index()
	grdLvl =grdLvl.pivot(index='School_Short',
									columns= 'Grade Level',
									values='Percent_On_Above')


	grdLvlDist=pd.crosstab([vs_obj.df['Grade Level']],[vs_obj.df['Overall Relative Placement']], 
						values=vs_obj.df.Student_Number, aggfunc='count',normalize='index')
	grdLvlDistCount=pd.crosstab([vs_obj.df.School_Short, vs_obj.df['Grade Level']],[vs_obj.df['Overall Relative Placement']], 
						values=vs_obj.df.Student_Number, aggfunc='count')

	grdLvlDistCount=grdLvlDistCount.fillna(0)
	grdLvlDist['Percent_On_Above']=grdLvlDist['Early On Grade Level']+grdLvlDist['Mid or Above Grade Level']
	drop_cols=['1 Grade Level Below', '2 Grade Levels Below', '3 or More Grade Levels Below','Early On Grade Level','Mid or Above Grade Level']
	for col in drop_cols:
		if col in grdLvlDist.columns:
			grdLvlDist =grdLvlDist.drop(col, axis=1)
	grdLvlDist=grdLvlDist.mul(100).round(2).astype(str) + '%'
	grdLvlDist=grdLvlDist.rename(columns={'Percent_On_Above':'District'})
	grdLvlDist=grdLvlDist.T
	
	grdLvl=pd.concat([grdLvl, grdLvlDist])
	
	grdLvl=gradeLevelAddColumns(vs_obj, grdLvl)
	gradeLevelTab.append(grdLvl)
	
	#Race for District Totals
	irGLRace=pd.crosstab([vs_obj.df['Race_Ethn']],[vs_obj.df['Overall Relative Placement']], values=vs_obj.df.Student_Number, aggfunc='count',margins=True)
	irGLRace=irGLRace.fillna(0)
	irGLRace['On or Above']=irGLRace['Early On Grade Level']+irGLRace['Mid or Above Grade Level']
	irGLRace['Percent_On_Above']=(irGLRace['On or Above']/irGLRace['All']).mul(100).round(1).astype(str)+"%"
	irGLRace['Percent_On_Above'] = np.where((irGLRace['All']) <= 10,'*',irGLRace['Percent_On_Above'])
	irGLRace=irGLRace.drop(columns=['All','On or Above','Early On Grade Level','Mid or Above Grade Level','1 Grade Level Below','2 Grade Levels Below','3 or More Grade Levels Below'])
	irGLRace=irGLRace.rename(columns={'Percent_On_Above':'District'})
	irGLRace=irGLRace.T
	irGLRace.index.name='School_Short'


	#Race iReady GL vs_obj.df2022
	rsltRaceGLCount=pd.crosstab([vs_obj.df.School_Short, vs_obj.df.Race_Ethn],[vs_obj.df['Overall Relative Placement']], values=vs_obj.df.Student_Number, aggfunc='count', margins=True)
	rsltRaceGL=pd.crosstab([vs_obj.df.School_Short, vs_obj.df.Race_Ethn],[vs_obj.df['Overall Relative Placement']], values=vs_obj.df.Student_Number, aggfunc='count',normalize='index', margins=True).mul(100).round(1)
		
	rsltRaceGL=rsltRaceGL.fillna(0)
	
	if '3 or More Grade Levels Below' in rsltRaceGL.columns:
		rsltRaceGL['Below']=rsltRaceGL['1 Grade Level Below']+rsltRaceGL['2 Grade Levels Below']+rsltRaceGL['3 or More Grade Levels Below']
	else:
		rsltRaceGL['Below']=rsltRaceGL['1 Grade Level Below']+rsltRaceGL['2 Grade Levels Below']
	rsltRaceGL['Percent_On_Above']=rsltRaceGL['Early On Grade Level']+rsltRaceGL['Mid or Above Grade Level']
	#rsltRaceGL['totalCount']=rsltRaceGL['On or Above']+rsltRaceGL['Below']
	rsltRaceGL['totalCount']=rsltRaceGLCount['All']

	rsltRaceGL.loc[rsltRaceGL['totalCount'] <= 10, 'Percent_On_Above'] = -1
	rsltRaceGL=rsltRaceGL.replace(to_replace="-1",value="*").replace(to_replace="nan%",value="")
	
	rsltRaceGL=rsltRaceGL.reset_index()

	rsltRaceGL_pivt = rsltRaceGL.pivot(index='School_Short',
								columns= 'Race_Ethn',
								values='Percent_On_Above').round(1).astype(str)+"%"
	
	rsltRaceGL_pivt=rsltRaceGL_pivt.replace(to_replace="nan%",value="").replace(to_replace="-1.0%",value="*")
	
	rename={}
	races={'African_American':'AA', 'American_Indian':'AI','American Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
	  		'Pac_Islander':'PI', 'White':'W', 'All':'ALL', 'District':'District'}
	for col in rsltRaceGL_pivt.columns:
		if col in races.keys():		
			temp_col = vs_obj.assessment_type+vs_obj.subjects[0]+'GradeLevel '+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
		
	rsltRaceGL_pivt.rename(columns=rename, inplace=True)
	irGLRace.rename(columns=rename, inplace=True)
	
	
	#appends irGLRace for District totatl to rsltRaceGL_pivt which has by school totals
	frames=[rsltRaceGL_pivt, irGLRace]
	rsltRaceFinal=pd.concat(frames,
						    axis=0,
						    join="outer",
						    ignore_index=False,
						    keys=None,
						    levels=None,
						    names=None,
						    verify_integrity=False,
						    copy=True,
						)
	
	
		

	finalDfs.append(rslt)
	finalDfs.append(rsltRaceFinal)
	
	subgroups(vs_obj,0,finalDfs)

def iReadySpan(vs_obj, finalDfs):
	if 'SP2023' in vs_obj.terms[0]:
		vs_obj.df=vs_obj.df[vs_obj.columns]
		vs_obj.df=vs_obj.df.reset_index()	
		vs_obj.df=vs_obj.df[vs_obj.df.Enrolled =='Enrolled']
		vs_obj.df.replace(r'^\s*$', np.nan, regex=True)
		vs_obj.df=vs_obj.df[vs_obj.df['Window'] =='End of Year']
		springWindowFilter(vs_obj)
		
		
		rsltCount=pd.crosstab([vs_obj.df.School_Short],[vs_obj.df["Overall Spanish Placement"]], values=vs_obj.df["Overall Spanish Placement"], aggfunc='count', margins=True, margins_name='All')
		rslt=pd.crosstab([vs_obj.df.School_Short],[vs_obj.df["Overall Spanish Placement"]], values=vs_obj.df["Overall Spanish Placement"], aggfunc='count',normalize='index', margins=True, margins_name='All').mul(100).round(1).astype(str)+"%"
		idxRename = {'All':'District'}
		colRename = {'Met': vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[0]+" ALL_"+vs_obj.terms[0]}
		rslt = rslt.rename(index=idxRename).rename(columns=colRename)
		rslt['iReadySPReadSpPLMNT ALL_SP2023'] = np.where((rsltCount['All']) <= 10,'*',rslt['iReadySPReadSpPLMNT ALL_SP2023'])

		#grade level

		grdLvl=pd.crosstab([vs_obj.df.School_Short, vs_obj.df['Grade Level']],[vs_obj.df["Overall Spanish Placement"]],margins=True)

		grdLvl['Percentage Proficient']=(grdLvl['Met']/grdLvl['All']).mul(100).round(1).astype(str) + '%'
		grdLvl['Percentage Proficient'] = np.where((grdLvl['All']) <= 10,'*',grdLvl['Percentage Proficient'])
		grdLvl=grdLvl.reset_index()
		grdLvl=grdLvl.pivot(index='School_Short',
									columns= 'Grade Level',
									values='Percentage Proficient')

		grdLvl=grdLvl.drop(['All'])

		distGrdLvl=pd.crosstab([vs_obj.df['Grade Level']],[vs_obj.df["Overall Spanish Placement"]], values=vs_obj.df["Overall Spanish Placement"], aggfunc='count',normalize='index', margins=True, margins_name='All').mul(100).round(1).astype(str)+"%"
		
		cols= {'Met': 'District'}
		distGrdLvl = distGrdLvl.rename(columns=cols)
		distGrdLvl=distGrdLvl.drop(columns=['Not Met','Partially Met'])
		distGrdLvl=distGrdLvl.T
		distGrdLvl.index.name='School_Short'

		grdLvl = pd.concat([grdLvl, distGrdLvl])
		grdLvl.index.name='School_Short'
		
	
		grdLvl=gradeLevelAddColumns(vs_obj, grdLvl)
		gradeLevelTab.append(grdLvl)

		finalDfs.append(rslt)


		#subgroups
		vs_obj.df.loc[vs_obj.df['SPED'] =='ESN', 'SPED'] = 'Y'
		vs_obj.df.loc[vs_obj.df['SPED']=='MMSN', 'SPED'] = 'Y'
	
		vs_obj.df['FIT']=vs_obj.df['FIT'].fillna('')
		vs_obj.df.loc[vs_obj.df['FIT'] != '' , 'FIT'] = 'Y'
		vs_obj.df.loc[vs_obj.df['EL']=='EL', 'EL'] = 'Y'
		vs_obj.df.loc[vs_obj.df['Foster']=='Foster', 'Foster'] = 'Y'

		for column in [vs_obj.df.Foster, vs_obj.df.SPED, vs_obj.df.EL]:

			#tableframe = pd.pivot_table(vs_obj.df, values=['Typical and High','LowHigh'], index=['School_Short', column], aggfunc=np.sum)
			subRslt=pd.crosstab([vs_obj.df.School_Short, column],[vs_obj.df["Overall Spanish Placement"]], values=vs_obj.df["Overall Spanish Placement"], aggfunc='count', margins=True, margins_name='All')
			idxRename = {'All':'District'}
			
			
			subRslt.reset_index(inplace=True)
			
			if 'SPED' in subRslt.columns:
				subRslt = subRslt[subRslt['SPED'] == 'Y']
				
			
			elif 'EL' in subRslt.columns:
				subRslt = subRslt[subRslt['EL'] == 'Y']

			elif 'Foster' in subRslt.columns:
				subRslt = subRslt[subRslt['Foster'] == 'Y']

			subRslt.loc['District'] = subRslt.iloc[:, :].sum()

			subRslt['Percentage Proficient']=(subRslt['Met']/subRslt['All']).mul(100).round(1).astype(str) + '%'
			subRslt['Percentage Proficient'] = np.where((subRslt['All']) <= 10,'*',subRslt['Percentage Proficient'])
			subRslt=subRslt.reset_index()
			
		
			colRename = {'Percentage Proficient': vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+column.name+"_"+vs_obj.terms[0]}
			subRslt = subRslt.rename(index=idxRename).rename(columns=colRename)
			
			#subRslt.loc[subRslt.index[-1], column.name]=''
			
			subRslt.loc[subRslt.index[-1], 'School_Short']='District'
			subRslt=subRslt.set_index('School_Short')
			
			finalDfs.append(subRslt)

	
def iReadyGrw(vs_obj,finalDfs):
	vs_obj.df=vs_obj.df[vs_obj.df['Baseline Diagnostic (Y/N)'] =='N']

	if 'W2022' in vs_obj.terms[0]:
		vs_obj.df=vs_obj.df[vs_obj.columns]

		winterWindowFilter(vs_obj)
		vs_obj.df=vs_obj.df[vs_obj.df['Most Recent Diagnostic (Y/N)'] =='Y']
		vs_obj.df=vs_obj.df[vs_obj.df.Enrolled =='Enrolled']
		vs_obj.df = vs_obj.df.replace(r'^\s*$', np.nan, regex=True)
		grw_column = 'Percent Progress to Annual Typical Growth (%)'
		
	if 'SP2023' in vs_obj.terms[0]:
		vs_obj.df=vs_obj.df[vs_obj.columns]
		iReadyFilter(vs_obj)
		springWindowFilter(vs_obj)
		grw_column = 'Percent Progress to Annual Typical Growth (%)'

	#GRW = vs_obj.df.groupby(["School_Short"])["Percent Progress to Annual Typical Growth (%)"].median().reset_index(name="median")

	rslt=pd.crosstab([vs_obj.df.School_Short],vs_obj.df[grw_column], values=vs_obj.df[grw_column], aggfunc='median',margins=True, margins_name='Total').astype(str)+"%"
	rslt=rslt.reset_index().set_index('School_Short')

	idxRename = {'Total':'District'}
	colRename = {'Total': vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[0]+" ALL_"+vs_obj.terms[0]}
	rslt = rslt.rename(index=idxRename).rename(columns=colRename)
	rslt=rslt.filter(['School_Short', 'iReadyReadGRW ALL_SP2023'], axis=1)
	
	grwRaceCount = pd.crosstab([vs_obj.df.School_Short],[vs_obj.df.Race_Ethn], values=vs_obj.df[grw_column], aggfunc='count', margins=True, margins_name='District')
	
	rename={}
	races={'African_American':'AA', 'American_Indian':'AI','American Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
   			'Pac_Islander':'PI', 'White':'W'}
	for col in grwRaceCount.columns:
		if col in races.keys():		
			temp_col = races[col]+"_COUNT"
			rename[col]=temp_col
	
	grwRaceCount.rename(columns=rename, inplace=True)
	grwRace=pd.crosstab([vs_obj.df.School_Short],[vs_obj.df.Race_Ethn], values=vs_obj.df[grw_column], aggfunc='median',margins=True, margins_name='Total')
	
	grwGrdLvl=pd.crosstab([vs_obj.df.School_Short],[vs_obj.df['Grade Level']], values=vs_obj.df[grw_column], aggfunc='median',margins=True, margins_name='Total')

	idxRename = {'Total':'District'}
	grwGrdLvl = grwGrdLvl.rename(index=idxRename)
	grwGrdLvl= grwGrdLvl.astype(str)+"%"
	grwGrdLvl=grwGrdLvl.replace(to_replace="*%",value = "*").replace(to_replace="nan%", value="")
	grwGrdLvl=gradeLevelAddColumns(vs_obj, grwGrdLvl)
	gradeLevelTab.append(grwGrdLvl)
	
	idxRename = {'Total':'District'}
	colRename = {'Total': vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[0]+" ALL_"+vs_obj.terms[0]}
	grwRace = grwRace.rename(index=idxRename).rename(columns=colRename)
	
	grwRace= grwRace.astype(str)+"%"
	grwRace=grwRace.replace(to_replace="*%",value = "*").replace(to_replace="nan%", value="")
	
	rename={}
	races={'African_American':'AA', 'American_Indian':'AI','American Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
   			'Pac_Islander':'PI', 'White':'W'}
	for col in grwRace.columns:
		if col in races.keys():		
			temp_col = vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
	
	grwRace.rename(columns=rename, inplace=True)
	Final=grwRace.merge(grwRaceCount, how='inner', on='School_Short')
	
	cols={'iReadyMathGRW AA_W2022':'AA_COUNT', 'iReadyMathGRW AI_W2022':'AI_COUNT',
   	'iReadyMathGRW A_W2022':'A_COUNT', 'iReadyMathGRW F_W2022':'F_COUNT',
   	'iReadyMathGRW HL_W2022':'HL_COUNT', 'iReadyMathGRW Mult_W2022':'Mult_COUNT',
   	'iReadyMathGRW PI_W2022': 'PI_COUNT', 'iReadyMathGRW W_W2022':'W_COUNT','iReadyReadGrw ALL_W2022':'District',
   	'iReadyReadGRW AA_W2022':'AA_COUNT', 'iReadyReadGRW AI_W2022':'AI_COUNT',
   	'iReadyReadGRW A_W2022':'A_COUNT', 'iReadyReadGRW F_W2022':'F_COUNT',
   	'iReadyReadGRW HL_W2022':'HL_COUNT', 'iReadyReadGRW Mult_W2022':'Mult_COUNT',
   	'iReadyReadGRW PI_W2022':'PI_COUNT', 'iReadyReadGRW W_W2022':'W_COUNT',
   	'iReadyReadGrw ALL_W2022':'District',
   	'iReadyMathGRW AA_SP2023':'AA_COUNT', 'iReadyMathGRW AI_SP2023':'AI_COUNT',
   	'iReadyMathGRW A_SP2023':'A_COUNT', 'iReadyMathGRW F_SP2023':'F_COUNT',
   	'iReadyMathGRW HL_SP2023':'HL_COUNT', 'iReadyMathGRW Mult_SP2023':'Mult_COUNT',
   	'iReadyMathGRW PI_SP2023': 'PI_COUNT', 'iReadyMathGRW W_SP2023':'W_COUNT','iReadyReadGrw ALL_SP2023':'District',
   	'iReadyReadGRW AA_SP2023':'AA_COUNT', 'iReadyReadGRW AI_SP2023':'AI_COUNT',
   	'iReadyReadGRW A_SP2023':'A_COUNT', 'iReadyReadGRW F_SP2023':'F_COUNT',
   	'iReadyReadGRW HL_SP2023':'HL_COUNT', 'iReadyReadGRW Mult_SP2023':'Mult_COUNT',
   	'iReadyReadGRW PI_SP2023':'PI_COUNT', 'iReadyReadGRW W_SP2023':'W_COUNT',
   	'iReadyReadGrw ALL_SP2023':'District'}
   	
	count_cols=[
   	'AA_COUNT', 'AI_COUNT', 'A_COUNT', 'F_COUNT',
   	'HL_COUNT', 'Mult_COUNT', 'PI_COUNT', 'W_COUNT', 'District']
	
	for k,v in cols.items():
		if k in Final.columns:
			Final[k] = np.where((Final[v]) <= 10,'*',Final[k])
	
	Final=Final.reset_index()
	
	finalDfs.append(Final)
	
	#GRW Subgroups
	vs_obj.df.loc[vs_obj.df['SPED'] =='ESN', 'SPED'] = 'Y'
	vs_obj.df.loc[vs_obj.df['SPED']=='MMSN', 'SPED'] = 'Y'

	vs_obj.df['FIT']=vs_obj.df['FIT'].fillna('')
	vs_obj.df.loc[vs_obj.df['FIT'] != '' , 'FIT'] = 'Y'
	vs_obj.df.loc[vs_obj.df['EL']=='EL', 'EL'] = 'Y'
	vs_obj.df.loc[vs_obj.df['Foster']=='Foster', 'Foster'] = 'Y'

	for column in [vs_obj.df.Foster,vs_obj.df.SPED, vs_obj.df.EL, vs_obj.df.FIT]:
		
			subRslt=pd.crosstab([vs_obj.df.School_Short, column],[vs_obj.df['Percent Progress to Annual Typical Growth (%)']], values=vs_obj.df['Percent Progress to Annual Typical Growth (%)'], aggfunc='median', margins=True, margins_name='All')
			subCount=pd.crosstab([vs_obj.df.School_Short, column],[vs_obj.df['Percent Progress to Annual Typical Growth (%)']], values=vs_obj.df['Percent Progress to Annual Typical Growth (%)'], aggfunc='count', margins=True, margins_name='All')
			subRslt=subRslt.reset_index().set_index('School_Short')
			subRslt.loc[subRslt.index[-1], column.name]='Y'
			subCount=subCount.filter(['School_Short', column.name, 'All'], axis=1)

			subCount=subCount.reset_index()
			subCount['School_Short']=subCount['School_Short']+"_"+subCount[column.name]
			
			subCount['School_Short']=subCount['School_Short'].ffill(axis = 0)
			subCount.loc[subCount.index[-1], column.name]='Y'
			subCount=subCount.rename(columns={'All':'CountAll'})
			subRslt=subRslt.filter(['School_Short', column.name, 'All'], axis=1)
			subRslt=subRslt.reset_index()
			subRslt['School_Short']=subRslt['School_Short']+"_"+subRslt[column.name]
		
			subRslt=subRslt.reset_index().set_index('School_Short')
			subCount=subCount.reset_index().set_index('School_Short')
			
			subRslt=subRslt.merge(subCount, how='inner', on='School_Short')
			subRslt['All'] = np.where((subRslt['CountAll']) <= 10,'*',subRslt['All'])
			subRslt['All']=subRslt['All'].astype(str)+"%"
			subRslt=subRslt.filter(['School_Short', column.name, 'All'], axis=1)

			#idxRename = {'All':'District'}
			#subRslt=subRslt.drop(index='All')
			subRslt.reset_index(inplace=True)
			subRslt[column.name]=subRslt['School_Short'].str.contains("_Y")
			subRslt = subRslt[subRslt[column.name] == True]

			#subRslt.reset_index(inplace=True)
			
			subRslt=subRslt.rename(columns = {'All': vs_obj.assessment_type+vs_obj.subjects[0]+"GRW "+column.name+"_"+vs_obj.terms[0]})
			subRslt['School_Short'] =subRslt['School_Short'].replace({'Bayview_Y': 'Bayview', 'Chavez_Y': 'Chavez', 'Dover_Y': 'Dover', 'Grant_Y': 'Grant',
											'Helms_Y':'Helms','Murphy_Y':'Murphy','Obama_Y':'Obama','Ohlone_Y':'Ohlone','Peres_Y':'Peres'
											,'Shannon_Y':'Shannon','Stewart_Y':'Stewart','Valley View_Y':'Valley View','Virtual K-12_Y':'Virtual K-12'})
			subRslt=subRslt.set_index('School_Short')
			dfs=[]
			#DISTRICT TOTAL
			if column.name == 'SPED':	
				spedDistTotal = vs_obj.df[vs_obj.df['SPED'] == 'Y']
				dfs.append(spedDistTotal)
		
			elif column.name == 'EL':
				elDistTotal = vs_obj.df[vs_obj.df['EL'] == 'Y']
				dfs.append(elDistTotal)
		
			elif column.name ==  'FIT':
				fitDistTotal = vs_obj.df[vs_obj.df['FIT'] == 'Y']
				dfs.append(fitDistTotal)

			elif column.name ==  'Foster':
				fosterDistTotal = vs_obj.df[vs_obj.df['Foster'] == 'Y']
				dfs.append(fosterDistTotal)
			
			for df in dfs:
				distTotal=pd.crosstab([column],[df['Percent Progress to Annual Typical Growth (%)']], values=df['Percent Progress to Annual Typical Growth (%)'], aggfunc='median', margins=True, margins_name='All')
				distCount=pd.crosstab([column],[df['Percent Progress to Annual Typical Growth (%)']], values=df['Percent Progress to Annual Typical Growth (%)'], aggfunc='count', margins=True, margins_name='All')
			
				distTotal=distTotal.rename(index={'Y':'District'})
			
				distTotal.index.rename('School_Short', inplace=True)
				distTotal=distTotal.reset_index()		
				distTotal=distTotal[['School_Short','All']]
				distTotal = distTotal[distTotal['School_Short'] == 'District']
				distTotal=distTotal.rename(columns={'All': vs_obj.assessment_type+vs_obj.subjects[0]+"GRW "+column.name+"_"+vs_obj.terms[0]})

				distTotal=distTotal.set_index('School_Short')
				subRslt=pd.concat([subRslt, distTotal], axis=0)
				subRslt=subRslt.replace(to_replace="*%", value="*")
				
				finalDfs.append(subRslt)
	

def starDB(vs_obj,finalDfs):
	vs_obj.df=vs_obj.df[vs_obj.columns]

	starFilters(vs_obj)
	vs_obj.df=vs_obj.df.sort_values('CompletedDate').groupby('Student_Number').tail(1)
	vs_obj.df=vs_obj.df[vs_obj.df.AssessmentStatus =='Active']
	
	rslt=pd.crosstab([vs_obj.df.School_Short],[vs_obj.df.DistrictBenchmarkCategoryName], values=vs_obj.df.Student_Number, aggfunc='count',margins=True, margins_name='District')
	
	rsltPercentage=pd.crosstab([vs_obj.df.School_Short],[vs_obj.df.DistrictBenchmarkCategoryName], values=vs_obj.df.Student_Number, aggfunc='count',margins=True, margins_name='District', normalize='index').mul(100).round(1).astype('str')+'%'
	
	grdLvl=pd.crosstab([vs_obj.df.School_Short, vs_obj.df['Grade Level']],[vs_obj.df['DistrictBenchmarkCategoryName']], 
						values=vs_obj.df.Student_Number, aggfunc='count', margins=True)
	grdLvl.loc[grdLvl['All'] <= 10, 'At/Above Benchmark'] = -1
	grdLvl.reset_index()
	grdLvl['Percent_AtorAbove']=(grdLvl['At/Above Benchmark']/grdLvl['All']).mul(100).round(1)
	grdLvl.loc[grdLvl['Percent_AtorAbove'] < 0, 'Percent_AtorAbove'] = "*"
	
	grdLvl =grdLvl.drop(['Intervention', 'On Watch', 'Urgent Intervention', 'All'], axis=1)
	grdLvl=grdLvl.astype(str) + '%'
	grdLvl=grdLvl.reset_index()

	grdLvl = grdLvl.pivot(index='School_Short',
									columns= 'Grade Level',
									values='Percent_AtorAbove')

	idxRename = {'All':'District'} 
	grdLvl = grdLvl.rename(index=idxRename)
	grdLvl=grdLvl.replace(to_replace="*%",value="*").replace(to_replace="nan%",value="*")
	grdLvl=grdLvl.reset_index().set_index('School_Short')
	grdLvl=grdLvl.drop(['District'])

	#adding distric total for Grade Level tab
	distGrdLvlDB=pd.crosstab([vs_obj.df['Grade Level']],[vs_obj.df['DistrictBenchmarkCategoryName']],margins=True, normalize='index').mul(100).round(1).astype(str)+"%"
	renameCol={'At/Above Benchmark':'District'}
	rename={'Grade Level':'School_Short'}
	distGrdLvlDB=distGrdLvlDB.rename(columns=renameCol).drop(columns=['Intervention','On Watch','Urgent Intervention'])
	
	distGrdLvlDB=distGrdLvlDB.T
	
	distGrdLvlDB.index.name='School_Short'
	
	grdLvl = pd.concat([grdLvl, distGrdLvlDB])
	grdLvl.index.name='School_Short'
	
	grdLvl=gradeLevelAddColumns(vs_obj, grdLvl)
	gradeLevelTab.append(grdLvl)

	rsltRaceCount=pd.crosstab([vs_obj.df.School_Short, vs_obj.df.Race_Ethn],[vs_obj.df.DistrictBenchmarkCategoryName], values=vs_obj.df.Student_Number, aggfunc='count',margins=True, margins_name='All')
	rsltRaceCount=rsltRaceCount.drop(columns=['Intervention','On Watch', 'Urgent Intervention'])
	rsltRaceCount=rsltRaceCount.reset_index()
	
	container=[]
	for label, _df in rsltRaceCount.groupby(['School_Short']):
		row_label=label+'_ALL'
		_df.loc[row_label] = _df[['At/Above Benchmark','All']].sum()
		container.append(_df)

	dfSummary = pd.concat(container)
	
	dfSummary['Percent_On_Above']=dfSummary['At/Above Benchmark']/dfSummary['All']
	dfSummary.loc[dfSummary['All'] <= 10, 'Percent_On_Above'] = -1
	dfSummary['School_Short'] = dfSummary['School_Short'].ffill()
	dfSummary['Race_Ethn']=dfSummary['Race_Ethn'].fillna('ALL')
	rsltRaceCount=dfSummary.copy()

	
	rsltRaceCount=rsltRaceCount.reset_index()
	
	rsltRace = rsltRaceCount.pivot(index='School_Short',
									columns= 'Race_Ethn',
									values='Percent_On_Above').mul(100).round(1).astype('str')+"%"
	rsltRace=rsltRace.replace(to_replace="nan%",value="").replace(to_replace="-100.0%",value="*")
	
	if 'EarlyLit' in vs_obj.subjects[0] and 'FA2022' in vs_obj.terms[0]:
		rsltPercentage.rename(columns=STAREarlyLit_columnheadersFA22, inplace=True)
		rsltRace.rename(columns=STAREarlyLit_columnheadersFA22, inplace=True)
		rsltRace['STAREarlyLitDB ALL_FA2022']=rsltPercentage['STAREarlyLitDB ALL_FA2022']
	
	if 'EarlyLit' in vs_obj.subjects[0] and 'W2022' in vs_obj.terms[0]:
		rsltPercentage.rename(columns=STAREarlyLit_columnheadersW22, inplace=True)
		rsltRace.rename(columns=STAREarlyLit_columnheadersW22, inplace=True)
		rsltRace['STAREarlyLitDB ALL_W2022']=rsltPercentage['STAREarlyLitDB ALL_W2022']

	#district totals
	distRace=pd.crosstab([vs_obj.df.Race_Ethn],[vs_obj.df.DistrictBenchmarkCategoryName], values=vs_obj.df.Student_Number, aggfunc='count',normalize='index',margins=True).mul(100).round(1).astype(str)+"%"
	distRace=distRace.reset_index().set_index('Race_Ethn')
	dropped=['Intervention','On Watch','Urgent Intervention','']
	
	for col in distRace.columns:
		if col in dropped:
			distRace=distRace.drop(columns=col)

	distRace=distRace.T
	distRace=distRace.rename(index={'At/Above Benchmark':'District'})
	distRace=distRace.reset_index()
	distRace=distRace.rename(columns={'DistrictBenchmarkCategoryName':'School_Short'})

	rename={}
	races={'African_American':'AA', 'American_Indian':'AI','American Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       			'Pac_Islander':'PI', 'White':'W', 'All':'ALL','ALL':'ALL'}
	for col in distRace.columns:
		if col in races.keys():		
			temp_col = "STAR"+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
	for col in rsltRace.columns:
		if col in races.keys():		
			temp_col = "STAR"+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
	
	frames=[distRace,rsltRace]
	
	distRace.rename(columns=rename, inplace=True)
	rsltRace.rename(columns=rename, inplace=True)
	rsltRace=rsltRace.reset_index().set_index('School_Short')
	distRace=distRace.reset_index().set_index('School_Short')
	
	rsltRace = pd.concat([rsltRace, distRace])
	finalDfs.append(rsltRace)	
	
	subgroups(vs_obj,0,finalDfs)

def newSubgroups(vs_obj, metric_column_index, subgroupCount, column):
		if vs_obj.metrics[metric_column_index] == 'SGP':
			pass
		else:
			subgroupCount.columns=['_'.join(col) for col in subgroupCount.columns.values]	
			subgroupCount=subgroupCount.rename(columns={'Y_CHRONIC':'Y_Chronic','Y_SEVERE CHRONIC':'Y_Severe'})

		if vs_obj.assessment_type == 'iReady':
			subgroupCount=subgroupCount.fillna(0)
			if ('Y_Mid or Above Grade Level' in subgroupCount.columns) and ('Y_Early On Grade Level' in subgroupCount.columns):		
				subgroupCount['Y']=subgroupCount['Y_Early On Grade Level']+subgroupCount['Y_Mid or Above Grade Level']
			elif 'Y_Early On Grade Level' in subgroupCount.columns:
				subgroupCount['Y']=subgroupCount['Y_Early On Grade Level']
			else:
				subgroupCount['Y']=0

			if 'Y_Level 4' in subgroupCount.columns:
				subgroupCount['Y']=subgroupCount['Y_Level 3']+subgroupCount['Y_Level 4']
			elif 'Y_Level 3' in subgroupCount.columns:
				subgroupCount['Y']=subgroupCount['Y_Level 3']
		
		
		subgroupCount=subgroupCount.fillna(0)

		subgroupCount['Y_SUM'] = subgroupCount.filter(like='Y_').sum(1)
		if 'Y_Chronic' in subgroupCount.columns:
			subgroupCount['Y_Severe&Chronic']=subgroupCount['Y_Chronic']+subgroupCount['Y_Severe']
			
		subgroupCount=subgroupCount.rename(columns = {'Y_Yes':'Y','Y_At/Above Benchmark':'Y', 'Y_Y':'Y','Y_Severe&Chronic':'Y'})
		if 'Y' not in subgroupCount.columns:
			subgroupCount['Y'] = 0
		
		idxRename = {'All':'District'} 
		subgroupCount = subgroupCount.rename(index=idxRename)

		subgroupCount['Y_Percent_On_Above']=((subgroupCount['Y']/subgroupCount['Y_SUM'])*100).round(1)
		subgroupCount.loc[subgroupCount['Y_SUM'] == 0, 'Y_Percent_On_Above'] = ""
		subgroupCount.loc[(subgroupCount['Y_SUM'] <= 10) & (subgroupCount['Y_SUM'] > 0), 'Y_Percent_On_Above'] = -1
		idxRename = {'All':'District'} 
		subgroupCount = subgroupCount.rename(index=idxRename)
		subgroupCount=subgroupCount.reset_index()
		
		subgroupCount['Y_Percent_On_Above']=subgroupCount['Y_Percent_On_Above'].astype(str)+"%"
		subgroupCount['Y_Percent_On_Above']=subgroupCount['Y_Percent_On_Above'].replace(to_replace="-100.0%",value="*").replace(to_replace="nan%",value="").replace(to_replace="-1%", value="*").replace(to_replace="%",value="")
		
		if vs_obj.assessment_type == 'ChrAbs':
			subgroupCount=subgroupCount.rename(columns = {'Y_Percent_On_Above': vs_obj.assessment_type+" "+column.name+ "_"+vs_obj.terms[0]})
			subgroupCount=subgroupCount[['School_Short', vs_obj.assessment_type+" "+column.name+ "_"+vs_obj.terms[0]]]
			subgroupCount=subgroupCount.reset_index(drop=True).set_index('School_Short')
		elif vs_obj.assessment_type == 'iReady':
			#subgroupCount.loc[(subgroupCount['total'] <= 10) & (subgroupCount['total'] > 0), 'Y_Percent_On_Above'] = "*"
			subgroupCount=subgroupCount.rename(columns = {'Y_Percent_On_Above':vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[metric_column_index]+" "+column.name+"_"+vs_obj.terms[0]})	
		else:
			subgroupCount=subgroupCount.rename(columns = {'Y_Percent_On_Above': vs_obj.assessment_column+vs_obj.subjects[metric_column_index]+vs_obj.metrics[metric_column_index]+" "+column.name+"_"+vs_obj.terms[0]})

		subgroupCount=subgroupCount.replace(to_replace="-1%",value="*").replace(to_replace="nan%",value="*")

		return(subgroupCount)
	
def subgroups(vs_obj, metric_column_index, finalDfs):
	
	vs_obj.df=vs_obj.df[vs_obj.columns]
	
	if (vs_obj.assessment_type == 'STAR') or (vs_obj.assessment_type == 'SEL'):
		starFilters(vs_obj)

	if vs_obj.assessment_type == 'ChrAbs':
		vs_obj.df=vs_obj.df[vs_obj.df['studentStatus'] =='Active']

	if vs_obj.assessment_type == 'iReady':
		if vs_obj.terms[0] == 'W2022':
			winterWindowFilter(vs_obj)

		elif vs_obj.terms[0] == 'SP2023':
			springWindowFilter(vs_obj)
			iReadyFilter(vs_obj)

		vs_obj.df[vs_obj.df['Most Recent Diagnostic (Y/N)'] =='Y']
		vs_obj.df[vs_obj.df.Enrolled =='Enrolled']
		vs_obj.df.replace(r'^\s*$', np.nan, regex=True)
		
	vs_obj.df.loc[vs_obj.df['SPED'] =='ESN', 'SPED'] = 'Y'
	vs_obj.df.loc[vs_obj.df['SPED']=='MMSN', 'SPED'] = 'Y'
	
	vs_obj.df['FIT']=vs_obj.df['FIT'].fillna('')
	vs_obj.df.loc[vs_obj.df['FIT'] != '' , 'FIT'] = 'Y'
	vs_obj.df.loc[vs_obj.df['EL']=='EL', 'EL'] = 'Y'
	vs_obj.df.loc[vs_obj.df['Foster']=='Foster', 'Foster'] = 'Y'

	for column in [vs_obj.df.Foster, vs_obj.df.SPED, vs_obj.df.FIT, vs_obj.df.EL]:
	#for column in [vs_obj.df.SPED]:
	
		subgroupCount=pd.crosstab([vs_obj.df.School_Short],[column,vs_obj.df[vs_obj.columns[metric_column_index]]], 
							values=vs_obj.df.Student_Number, aggfunc='count',margins=True, margins_name='All')
		
		grd_lvlCount=pd.crosstab([vs_obj.df['Grade Level']],[column,vs_obj.df[vs_obj.columns[metric_column_index]]], 
							values=vs_obj.df.Student_Number, aggfunc='count',margins=True, margins_name='All')
		
		subgroupFinal=newSubgroups(vs_obj,0, subgroupCount, column)
		subgroupFinal = subgroupFinal.iloc[: , [0,-1]].copy()
		subgroupFinal = subgroupFinal.T.drop_duplicates().T
		
		finalDfs.append(subgroupFinal)

		if vs_obj.assessment_type in ('STAR', 'SEL', 'iReady', 'ESGI'):
			supesTabFinal=newSubgroups(vs_obj,0, grd_lvlCount, column)
			supesTabFinal = supesTabFinal.iloc[: , [0,-1]].copy()
 		
			supesGoalsTab.append(supesTabFinal)
		

gradeLevelTab=[]		
def grade_levels(vs_obj, metric_column_index, finalDfs):
	print("WORKING on GRADE LEVEL TAB FOR: ", vs_obj.assessment_type, vs_obj.subjects[0],vs_obj.terms[0])
	
	vs_obj.df=vs_obj.df[vs_obj.columns]
	if vs_obj.assessment_type == 'STAR' or vs_obj.assessment_type == 'SEL':
		starFilters(vs_obj)
		
	#grade level calculations
	
	if (vs_obj.assessment_type == 'STAR' and vs_obj.metrics[metric_column_index] == 'SGP') or (vs_obj.assessment_type == 'SEL' and vs_obj.metrics[metric_column_index] == 'SGP'):
		if vs_obj.terms[0] == 'FA2022':
			vs_obj.df['Typical and High'] = vs_obj.df['StudentGrowthPercentileFallFall'].apply(lambda x:'Y' if x >=35 else 'N')
			
		elif vs_obj.terms[0] == 'W2022':
			vs_obj.df['Typical and High'] = vs_obj.df['StudentGrowthPercentileFallWinter'].apply(lambda x:'Y' if x >=35 else 'N')
		
		elif vs_obj.terms[0] == 'SP2023':
			
			vs_obj.df['Typical and High'] = vs_obj.df['StudentGrowthPercentileFallSpring'].apply(lambda x:'Y' if x >=35 else 'N')	
		
		grdLvl=pd.crosstab([vs_obj.df.School_Short, vs_obj.df['Grade Level']],[vs_obj.df['Typical and High']],margins=True, normalize='index').mul(100).round(1).astype(str)+"%"
		grdLvlCount=pd.crosstab([vs_obj.df.School_Short, vs_obj.df['Grade Level']],[vs_obj.df['Typical and High']],margins=True)
		grdLvl['Y'] = np.where((grdLvlCount['All']) <= 10,'*',grdLvl['Y'])
		
		grdLvl=grdLvl.reset_index()
		grdLvl=grdLvl.pivot(index='School_Short',
									columns= 'Grade Level',
									values='Y')

		
		distGrdLvlSGP=pd.crosstab([vs_obj.df['Grade Level']],[vs_obj.df['Typical and High']],margins=True, normalize='index').mul(100).round(1).astype(str)+"%"
		renameCol={'Y':'District'}
		rename={'Grade Level':'School_Short'}
		distGrdLvlSGP=distGrdLvlSGP.rename(columns=renameCol).drop(columns='N')
		distGrdLvlSGP=distGrdLvlSGP.reset_index()
		distGrdLvlSGP=distGrdLvlSGP.rename(columns=rename)
		
		distGrdLvlSGP=distGrdLvlSGP.reset_index(drop=True).set_index(['School_Short'])
		distGrdLvlSGP=distGrdLvlSGP.T
		grdLvl=grdLvl.drop(index='All')
		
		grdLvl = pd.concat([grdLvl, distGrdLvlSGP])
		grdLvl.index.name='School_Short'

		
	if vs_obj.assessment_type == 'STAR' and vs_obj.metrics[metric_column_index] == 'SB':
		grdLvl=pd.crosstab([vs_obj.df.School_Short, vs_obj.df['Grade Level']],[vs_obj.df[vs_obj.columns[metric_column_index]]],margins=True)

		grdLvl['Percentage Proficient']=(grdLvl['Yes']/grdLvl['All']).mul(100).round(1).astype(str) + '%'
		grdLvl['Percentage Proficient'] = np.where((grdLvl['All']) <= 10,'*',grdLvl['Percentage Proficient'])
		grdLvl=grdLvl.reset_index()
		grdLvl=grdLvl.pivot(index='School_Short',
									columns= 'Grade Level',
									values='Percentage Proficient')

		distGrdLvlSB=pd.crosstab([vs_obj.df['Grade Level']],[vs_obj.df[vs_obj.columns[metric_column_index]]],margins=True, normalize='index').mul(100).round(1).astype(str)+"%"
		cols={'Yes':'District'}
		_rename={'Grade Level':'School_Short'}
		distGrdLvlSB=distGrdLvlSB.rename(columns=cols).drop(columns='No')
		
		distGrdLvlSB=distGrdLvlSB.reset_index()
		distGrdLvlSB=distGrdLvlSB.rename(columns=_rename)
		
		distGrdLvlSB=distGrdLvlSB.reset_index(drop=True).set_index(['School_Short'])
		distGrdLvlSB=distGrdLvlSB.T

		grdLvl = pd.concat([grdLvl, distGrdLvlSB])
		grdLvl.index.name='School_Short'
		

	grdLvl=gradeLevelAddColumns(vs_obj, grdLvl)
	grdLvl=grdLvl.drop(columns='')
	gradeLevelTab.append(grdLvl)
	
	#finalDfs.append(grdLvl)
	
def disIndx(vs_obj,finalDfs):
	vs_obj.df=vs_obj.df[vs_obj.columns]
	vs_obj.df = vs_obj.df.replace(r'^\s*$', np.nan, regex=True)
	vs_obj.df['African American DI']=vs_obj.df['African American DI'].replace(to_replace="nan",value="")
	DIC1_renameCol={'African American DI':'DI AA_FA2022'}
	DIC2_renameCol={'African American DI':'DI AA_W2022'}
	DIC3_renameCol={'African American DI':'DI AA_SP2023'}
	
	if vs_obj.terms[0] == 'FA2022':
		vs_obj.df.rename(columns=DIC1_renameCol, inplace=True)
	elif vs_obj.terms[0] == 'W2022':
		vs_obj.df.rename(columns=DIC2_renameCol, inplace=True)
	elif vs_obj.terms[0] == 'SP2023':
		vs_obj.df.rename(columns=DIC3_renameCol, inplace=True)
	
	vs_obj.df.reset_index()
	finalDfs.append(vs_obj.df)


def suspRte(vs_obj,finalDfs):
	vs_obj.df=vs_obj.df[vs_obj.columns]

	enroll={'American Indian':'Ai E'
				,'African American':'Aa E'
				,'Asian':'As E'
				,'Filipino':'Fi E'
				,'Hispanic':'Hi E'
				,'Pacific Islander':'Pi E'
				,'White':'Wh E'
				,'Two Or More':'Mt E'
				,'Missing Or Decline':'Md E'
				,'English Learner':'El E'
				,'Students With Disabilities':'Swd E'
				,'Homeless':'Fit E'
				,'School Rate':'Cumulative K-13 Enrollment'}
	
	for k,v in enroll.items():
			vs_obj.df[k] = np.where(((vs_obj.df[v]) <= 10) & ((vs_obj.df[v]) > 0),-1,vs_obj.df[k].astype(str)+"%")
						
	if vs_obj.terms == 'FA2022':
		vs_obj.df.rename(columns=SuspRteC1_renameCol, inplace=True)
		
	if vs_obj.terms == 'W2022':
		vs_obj.df.rename(columns=SuspRteC2_renameCol, inplace=True)
	
	if vs_obj.terms == 'SP2023':					
		vs_obj.df.rename(columns=SuspRteC3_renameCol, inplace=True)
	
	vs_obj.df=vs_obj.df.replace(to_replace="-",value="").replace(to_replace=-1,value="*").replace(to_replace="-%",value="")
	vs_obj.df.reset_index()
	
	finalDfs.append(vs_obj.df)
	
def saebrsScreener(vs_obj,finalDfs):
	vs_obj.df=vs_obj.df.rename(columns={'Percent Complete':'SAEBRSparticipation ALL_FA2022'})
	finalDfs.append(vs_obj.df)
	

def createGradeLevelTab(gradeLevelTab):
	GL_dfs = [df.replace(to_replace="*%",value="*").replace(to_replace=-1,value="*") for df in GradeLevelTab]
	GLs_concatenated=pd.concat(GL_dfs, axis=0)

	drop=['All','Total','',-1,-2]
	for col in drop:
		if col in GLs_concatenated.columns:
			GLs_concatenated=GLs_concatenated.drop(columns=col)
	
	sheet=gc.open_by_url('URL_HERE')
	worksheet = sheet.add_worksheet("Grade Level Tab"+dt_string, rows = 1000, cols=500)
	set_with_dataframe(worksheet,GLs_concatenated,1,1,include_index=True)


def createVitalSignsTab(finalDfs):
	dfs = [df.reset_index().set_index('School_Short').replace(to_replace="*%",value="*").replace(to_replace=-1,value="*") for df in finalDfs]
	concatenated=pd.concat(dfs, axis=1)

	cycle3=concatenated[[c for c in concatenated.columns if c in cylce3_ExpectedColumns]]
	cols=[
		'Gateway',
		'NPS',
		'SHT',
		'Transition',
		'All']

	for col in cols:
		if col in cycle3:
			cycle3.drop(columns=col)


	sheet=gc.open_by_url('URL_HERE')
	worksheet = sheet.add_worksheet("New Goals "+dt_string, rows = 100, cols=1000)
	set_with_dataframe(worksheet,cycle3,1,1,include_index=True)

