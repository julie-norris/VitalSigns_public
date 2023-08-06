
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

def gradeLevelAddColumns(vs_obj, grade_lvl_df):
	idx_rename = {'All':'District'} 
	grade_lvl_df = grade_lvl_df.rename(index=idx_rename)
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

	
	grade_lvl_df.insert(0,"Cycle",[cycle]*len(grade_lvl_df))
	grade_lvl_df.insert(0,"Measure",[measure]*len(grade_lvl_df))
	grade_lvl_df.insert(0,"Assessment",[assessment_name]*len(grade_lvl_df))
	return(grade_lvl_df)

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
	idx_rename = {'All':'District'} 
	rslt = rslt.rename(index=idx_rename)
	rslt=rslt.rename(columns={'CHRONIC':'Chronic','SEVERE CHRONIC':'Severe'})
	rslt['Chronic&Severe']=rslt.Chronic + rslt.Severe
	rslt['Chronic&Severe']=rslt['Chronic&Severe'].round(1).astype(str)+"%"

	rslt=rslt.rename(columns = {'Chronic&Severe': vs_obj.assessment_type+" ALL_"+vs_obj.terms[0]})
	rslt=rslt.reset_index().set_index('School_Short')
	
	#Grade Level
	rslt_GL=pd.crosstab([vs_obj.df.School_Short,vs_obj.df['Grade Level']],vs_obj.df['absCategory'], 
						values=vs_obj.df.Student_Number, aggfunc='count',margins=True,normalize='index').mul(100).round(1)
	idx_rename = {'All':'District'} 
	rslt_GL = rslt_GL.rename(index=idx_rename)

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
	
	grade_lvl=gradeLevelAddColumns(vs_obj, rslt_GL)
	gradeLevelTab.append(grade_lvl)
	
	
	#Race
	rslt_race=pd.crosstab([vs_obj.df.School_Short, vs_obj.df.Race_Ethn],[vs_obj.df.absCategory], 
						values=vs_obj.df.Student_Number, aggfunc='count',margins=True)
	rslt_race=rslt_race.rename(index=idx_rename)

	
	rslt_race_perc=pd.crosstab([vs_obj.df.School_Short, vs_obj.df.Race_Ethn],[vs_obj.df.absCategory], 
						values=vs_obj.df.Student_Number, aggfunc='count',margins=True,normalize='index')
	
	rslt_race_perc=rslt_race_perc.rename(index=idx_rename)
	rslt_race_perc=rslt_race_perc.rename(columns={'CHRONIC':'Chronic','SEVERE CHRONIC':'Severe'})				
	rslt_race_perc['Chronic&Severe']=rslt_race_perc.Chronic + rslt_race_perc.Severe
	rslt_race['Chronic&SeverePerc']=rslt_race_perc['Chronic&Severe'].mul(100).round(1)
	rslt_race['Chronic&SeverePerc'] = np.where((rslt_race['All']) <= 10,-1,rslt_race['Chronic&SeverePerc'])

	rslt_race=rslt_race.reset_index()
	rslt_race['Chronic&SeverePerc']=rslt_race['Chronic&SeverePerc'].astype(str)+"%"		
	rslt_race=rslt_race.pivot(index='School_Short',
							columns='Race_Ethn',
							values='Chronic&SeverePerc')
	
	rslt_race=rslt_race.replace(to_replace="-1.0%",value="*").replace(to_replace="nan%",value="")
	
	rename={}
	races={'African_American':'AA', 'American_Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       'Pac_Islander':'PI', 'White':'W'}
	for col in rslt_race.columns:
		if col in races.keys():
			temp_col = vs_obj.assessment_type+" "+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
			
	rslt_race.rename(columns=rename, inplace=True)
	rslt_race=rslt_race.drop(index='District').drop(columns='')
	
	dst_race=pd.crosstab([vs_obj.df.Race_Ethn], [vs_obj.df.absCategory],
						values=vs_obj.df.Student_Number, aggfunc='count',normalize='index')
	dst_race=dst_race.rename(columns={'CHRONIC':'Chronic','SEVERE CHRONIC':'Severe'})				
	dst_race['Chronic&Severe']=dst_race.Chronic + dst_race.Severe
	dst_race['Chronic&Severe']=dst_race['Chronic&Severe'].mul(100).round(1)
	

	dst_race['Chronic&Severe']=dst_race['Chronic&Severe'].astype(str)+"%"
	
	dropped=['Excellent', 'Manageable','Satisfactory','Chronic', 'Severe']
	for col in dst_race.columns:
		if col in dropped:
			dst_race=dst_race.drop(columns=col)

	dst_race=dst_race.T
	i_rename = {'Chronic&Severe':'District'} 
	dst_race = dst_race.rename(index=i_rename)
	
	rename={}
	races={'':'_','absCategory':'School_Short','African_American':'AA', 'American_Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       'Pac_Islander':'PI', 'White':'W'}
	for col in dst_race.columns:
		if col in races.keys():
			temp_col = vs_obj.assessment_type+" "+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
	
	dst_race.index.rename('School_Short', inplace=True)
	dst_race=dst_race.reset_index().set_index('School_Short')
	dst_race.rename(columns=rename, inplace=True)
	dst=dst_race.iloc[-1:, : ]
	

	rslt_race=pd.concat([rslt_race,dst])
	rslt_race=rslt_race.reset_index().set_index('School_Short')
	rslt=rslt[['ChrAbs ALL_SP2023']]
	
	#rslt_race=pd.concat([rslt, rslt_race])

	finalDfs.append(rslt)
	finalDfs.append(rslt_race)
	
	subgroups(vs_obj,0,finalDfs)					

def ESGI(vs_obj, finalDfs):
	
	test_names=['WCCUSD Uppercase Letters (PLF R 3.2)','WCCUSD Number Recognition 0-12 (PLF NS 1.2)','WCCUSD Lowercase Letters (PLF R 3.2)']
	names={'WCCUSD Uppercase Letters (PLF R 3.2)':'UppCaseLet3','WCCUSD Number Recognition 0-12 (PLF NS 1.2)':'NumRec3','WCCUSD Lowercase Letters (PLF R 3.2)':'LowCaseLet'}
	for test_name in test_names:
		
		vs_obj.df.loc[vs_obj.df['Test Name'] == test_name]
		MetEOYBenchmark=pd.crosstab([vs_obj.df.School_Short],[vs_obj.df['Test Name'],vs_obj.df['Met EOY Benchmark']],values=vs_obj.df.Student_Number, 
							margins=True, aggfunc='count')
	

		idx_rename = {'All':'District'} 
		MetEOYBenchmark = MetEOYBenchmark.rename(index=idx_rename)
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
			
		ESGI_GL=pd.crosstab([new['School_Short'],new['Grade Level_y']],[new['Met EOY Benchmark']],values=vs_obj.df.Student_Number, 
							margins=True, aggfunc='count')

		idx_rename = {'All':'District'} 
		ESGI_GL = ESGI_GL.rename(index=idx_rename)
		ESGI_GL=ESGI_GL.fillna(0)
		ESGI_GL['PercentageMetEOYBenchmark']=(ESGI_GL['Y']/ESGI_GL['All']).mul(100).round(1)
		ESGI_GL['PercentageMetEOYBenchmark'] = np.where((ESGI_GL['All']) <= 10,'*',ESGI_GL['PercentageMetEOYBenchmark'])
		ESGI_GL['PercentageMetEOYBenchmark'] = ESGI_GL['PercentageMetEOYBenchmark'].astype(str)+"%"
		ESGI_GL=ESGI_GL.replace(to_replace="*%", value="*").reset_index()
		

		ESGI_GL=ESGI_GL.rename(columns={'Grade Level_y':'Grade Level', 'Test Name': 'School_Short'})#,'PercentageMetEOYBenchmark': test_name+" "+vs_obj.terms[0]})
		print(ESGI_GL)
		quit()

		drop_cols=['FALSE','Y','All','']
		ESGI_GL=ESGI_GL.pivot(index='School_Short',
								columns='Grade Level',
								values='PercentageMetEOYBenchmark')
		
		for col in ESGI_GL.columns:
			if col in drop_cols:
				ESGI_GL=ESGI_GL.drop(columns=col)


		idx_rename = {'All':'District'} 
		ESGI_GL = ESGI_GL.rename(index=idx_rename)
		assessment_name=vs_obj.assessment_type+" "+vs_obj.subjects[0].title()
		measure=test_name
		cycle=vs_obj.terms[0]
		ESGI_GL.insert(0,"Cycle",[cycle]*len(ESGI_GL))
		ESGI_GL.insert(0,"Measure",[measure]*len(ESGI_GL))
		ESGI_GL.insert(0,"Assessment",[assessment_name]*len(ESGI_GL))

		ESGI_GL=ESGI_GL.reset_index().set_index('School_Short')
		
		gradeLevelTab.append(ESGI_GL)

		#ESGI Race	
		vs_obj.df=vs_obj.df.rename(columns= {'Race_Ethn_y':'Race_Ethn', 'SPED_y':'SPED', 'FIT_y':'FIT',
	       'Foster_y':'Foster', 'EL_y':'EL', 'SED_y':'SED'})

	
		supes_race=pd.crosstab([vs_obj.df['Race_Ethn']],[vs_obj.df['Test Name'],vs_obj.df['Met EOY Benchmark']],margins=True)
		supes_race.columns=['_'.join(col) for col in supes_race.columns.values]
		
		supes_race['District']=(supes_race[test_name+"_Y"]/supes_race['All_']).mul(100).round(1).astype(str) + '%'

		supes_race['District'] = np.where((supes_race['All_']) <= 10,'*',supes_race['District'])
		
		supes_race=supes_race.T
		rename={'All':'ALL'}
		races={'African_American':'AA', 'American Indian':'AI','American_Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
	       'Pac_Islander':'PI', 'White':'W'}
		for col in supes_race.columns:
			if col in races.keys():
				temp_col = vs_obj.assessment_type+"_"+name+" "+races[col]+"_"+vs_obj.terms[0]
				rename[col]=temp_col
				
		supes_race.rename(columns=rename, inplace=True)
		supes=supes_race.iloc[-1:, : ]
		
	
		MetEOYBenchmark_Race=pd.crosstab([vs_obj.df.School_Short, vs_obj.df.Race_Ethn],[vs_obj.df['Test Name'],vs_obj.df['Met EOY Benchmark']],values=vs_obj.df.Student_Number, 
								margins=True, aggfunc='count')
		MetEOYBenchmark_Race.columns=['_'.join(col) for col in MetEOYBenchmark_Race.columns.values]
		col_x = vs_obj.metrics[0]+'_Y'
		col_z = vs_obj.metrics[0]+'_FALSE'
		total = MetEOYBenchmark_Race[col_x]+MetEOYBenchmark_Race[col_z]
		MetEOYBenchmark_Race['PercentageMetEOYBenchmark']=(MetEOYBenchmark_Race[col_x]/total).mul(100).round(1)
		MetEOYBenchmark_Race['PercentageMetEOYBenchmark'] = np.where((total) <= 10,'*',MetEOYBenchmark_Race['PercentageMetEOYBenchmark'])
		MetEOYBenchmark_Race['PercentageMetEOYBenchmark'] = MetEOYBenchmark_Race['PercentageMetEOYBenchmark'].astype(str)+"%"
		
		MetEOYBenchmark_Race.reset_index(inplace=True)
		ESGI_Race=MetEOYBenchmark_Race.pivot(index='School_Short',
										columns= 'Race_Ethn',
										values='PercentageMetEOYBenchmark')
		
		
		rename={' ':'ALL'}
		races={'African_American':'AA', 'American Indian':'AI','American_Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
	       'Pac_Islander':'PI', 'White':'W'}
		for col in ESGI_Race.columns:
			if col in races.keys():
				temp_col = vs_obj.assessment_type+"_"+name+" "+races[col]+"_"+vs_obj.terms[0]
				rename[col]=temp_col
				
		ESGI_Race.rename(columns=rename, inplace=True)
		ESGI_Race=ESGI_Race.replace(to_replace="*%",value="*").replace(to_replace="nan%",value="")
		
		ESGI_Race=pd.concat([ESGI_Race, supes])
		ESGI_Race.index.rename('School_Short', inplace=True)
		ESGI_Race=ESGI_Race.reset_index().set_index('School_Short')		
		
		drop_cols=['FALSE','Y','All','','ALL']
		
		for col in ESGI_Race.columns:
			if col in drop_cols:
				ESGI_Race=ESGI_Race.drop(columns=col)

		
		finalDfs.append(ESGI_Race)
		
		vs_obj.df.loc[vs_obj.df['SPED'] =='ESN', 'SPED'] = 'Y'
		vs_obj.df.loc[vs_obj.df['SPED']=='MMSN', 'SPED'] = 'Y'
		
		vs_obj.df['FIT']=vs_obj.df['FIT'].fillna('')
		vs_obj.df.loc[vs_obj.df['FIT'] != '' , 'FIT'] = 'Y'
		vs_obj.df.loc[vs_obj.df['EL']=='EL', 'EL'] = 'Y'
		vs_obj.df.loc[vs_obj.df['Foster']=='Foster', 'Foster'] = 'Y'

		for column in [vs_obj.df.Foster,vs_obj.df.SPED, vs_obj.df.FIT, vs_obj.df.EL]:
		
			subgroup_count=pd.crosstab([vs_obj.df.School_Short,column],[vs_obj.df['Test Name'],vs_obj.df['Met EOY Benchmark']],values=vs_obj.df.Student_Number, 
								margins=True, aggfunc='count')
			subgroup_count.columns=['_'.join(col) for col in subgroup_count.columns.values]
			subgroup_dropped=['WCCUSD Count Up to 10 Objects (PLF NS 1.4) _','WCCUSD Count to 20 (PLF NS 1.1)_',
									 'WCCUSD Letter Sounds (PLF R 3.3)_','WCCUSD Lowercase Letters (PLF R 3.2)_', '_']
			for col in subgroup_count.columns:
				if col in subgroup_dropped:
					subgroups_count=subgroup_count.drop(columns=col)

			col_y = vs_obj.metrics[0]+'_Y'
			col_f = vs_obj.metrics[0]+'_FALSE'
			subgroup_count[col_y]=subgroup_count[col_y].fillna(0)
			subgroup_count[col_f]=subgroup_count[col_f].fillna(0)
			subgroup_count['total'] = subgroup_count[col_y]+subgroup_count[col_f]
			subgroup_count.reset_index(inplace=True)
			
			if 'SPED' in subgroup_count.columns:
				subgroup_count = subgroup_count[subgroup_count['SPED'] == 'Y']
				subgroup_count.loc['District'] = subgroup_count.iloc[:, :].sum()
				
			elif 'FIT' in subgroup_count.columns:
				subgroup_count = subgroup_count[subgroup_count['FIT'] == 'Y']
				subgroup_count.loc['District'] = subgroup_count.iloc[:, :].sum()

			elif 'EL' in subgroup_count.columns:
				subgroup_count = subgroup_count[subgroup_count['EL'] == 'Y']
				subgroup_count.loc['District'] = subgroup_count.iloc[:, :].sum()

			elif 'Foster' in subgroup_count.columns:
				subgroup_count = subgroup_count[subgroup_count['Foster'] == 'Y']
				subgroup_count.loc['District'] = subgroup_count.iloc[:, :].sum()


			subgroup_count.loc[subgroup_count.index[-1], 'School_Short']='District'
			subgroup_count.loc[subgroup_count.index[-1], column.name]='Y'

			subgroup_count=subgroup_count.set_index('School_Short')
			
			subgroup_count['PercentageMetEOYBenchmark']=(subgroup_count[col_y]/subgroup_count['total']).mul(100).round(1)
			subgroup_count.loc[subgroup_count.total == 0, 'PercentageMetEOYBenchmark'] = ""
			subgroup_count.loc[(subgroup_count['total'] <= 10) & (subgroup_count['total'] > 0), 'PercentageMetEOYBenchmark'] = "*"
			subgroup_count=subgroup_count.reset_index()
			subgroup_count=subgroup_count[['School_Short', column.name, 'PercentageMetEOYBenchmark']]
			subgroup_count['PercentageMetEOYBenchmark']=subgroup_count['PercentageMetEOYBenchmark'].astype(str)+"%"
			subgroup_count=subgroup_count.rename(columns = {'PercentageMetEOYBenchmark': vs_obj.assessment_type+"_"+name+" "+column.name+"_"+vs_obj.terms[0]})
			
			subgroup_count=subgroup_count.set_index('School_Short')
			subgroup_count[column.name] = subgroup_count[column.name].replace(r'^\s*$', np.nan, regex=True)
			final_subgroup_count=subgroup_count[subgroup_count[column.name] =='Y']
			final_subgroup_count=final_subgroup_count.replace(to_replace="*%",value="*").replace(to_replace="%",value="")
			final_subgroup_count=final_subgroup_count.drop(columns=column.name)
			
			finalDfs.append(final_subgroup_count)
		
	
def starSB(vs_obj, finalDfs):
	idx_rename = {'All':'District'} 
	vs_obj.df=vs_obj.df[vs_obj.columns]

    #new for 23-24 only calculating 9-11
	newSTARFilter(vs_obj)

	#for previous cycles grade level filters were applied using the starFilters(vs_obj) function
	#starFilters(vs_obj)

	vs_obj.df=vs_obj.df.sort_values('CompletedDate').groupby('Student_Number').tail(1)
	vs_obj.df=vs_obj.df[vs_obj.df.AssessmentStatus =='Active']

	SBcrosstab_ALL_count=pd.crosstab([vs_obj.df.School_Short],[vs_obj.df.StateBenchmarkProficient])
	SBcrosstab_ALL=pd.crosstab([vs_obj.df.School_Short],[vs_obj.df.StateBenchmarkProficient],normalize='index', margins=True,margins_name='District').mul(100).round(1).astype(str)+"%"
	
	rename={}
	races={'Yes':'ALL','African_American':'AA', 'American Indian':'AI','American_Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       'Pac_Islander':'PI', 'White':'W'}
	for col in SBcrosstab_ALL.columns:
		if col in races.keys():
			temp_col = vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
			
	SBcrosstab_ALL.rename(columns=rename, inplace=True)
	
	SBcrosstab_ALL = SBcrosstab_ALL.rename(index=idx_rename)
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
	

	supes_race=pd.crosstab([vs_obj.df['Race_Ethn']],[vs_obj.df['StateBenchmarkProficient']],margins=True)
	supes_race['District']=(supes_race['Yes']/supes_race['All']).mul(100).round(1).astype(str) + '%'
	supes_race['District'] = np.where((supes_race['All']) <= 10,'*',supes_race['District'])
	supes_race = supes_race.rename(index=idx_rename)
	supes_race= supes_race.drop(columns=['No','Yes','All'])
	supes_race=supes_race.T
	
	rename={}
	races={'African_American':'AA', 'African_American_Yes':'AA','American Indian':'AI','American_Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       'Pac_Islander':'PI', 'White':'W', 'District':'ALL','American Indian_Yes':'AI','American_Indian_Yes':'AI','Asian_Yes':'A', 'Filipino_Yes':'F', 'Hispanic_Yes':'HL', 'Mult_Yes':'Mult',
       'Pac_Islander_Yes':'PI', 'White_Yes':'W'}
	
	for col in supes_race.columns:
		if col in races.keys():
			temp_col = vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
	for col in rslt.columns:
		if col in races.keys():
			temp_col = vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
	
	supes_race.index.rename('School_Short', inplace=True)
	supes_race=supes_race.reset_index().set_index('School_Short')		
	supes_race.rename(columns=rename, inplace=True)
	dropped=['STARmathSB ALL_SP2023','STARreadSB ALL_SP2023']
	for col in dropped:
		if col in supes_race.columns:
			supes_race=supes_race.drop(columns=col)
	rslt.rename(columns=rename, inplace=True)
	

	rslt = pd.concat([rslt, supes_race])
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
		SELSGP_Race = pd.pivot_table(vs_obj.df, values=['Typical and High','LowHigh'], index=['School_Short', 'Race_Ethn'], aggfunc=np.sum, margins=True)
		
		SELSGP_Race['SGPPercentage']= SELSGP_Race['Typical and High']/SELSGP_Race['LowHigh'] * 100
	
		SELSGP_Race['SGPPercentage'] = SELSGP_Race['SGPPercentage'].map('{:,.1f}'.format)
		SELSGP_Race['SGPPercentage'] = np.where((SELSGP_Race['LowHigh']) <= 10,'-1',SELSGP_Race['SGPPercentage'])
	
		SELSGP_Race.reset_index(inplace=True)
		
		SELSGP_Race2=SELSGP_Race.pivot(index='School_Short',
									columns= 'Race_Ethn',
									values='SGPPercentage')
	
	
		SELSGP_Race2=SELSGP_Race2.astype(str) + '%'
		SELSGP_Race2=SELSGP_Race2.replace(to_replace="-1%",value="*").replace(to_replace="nan%",value="")
		
		
		#district totals
		dist_table=pd.pivot_table(vs_obj.df, values=['Typical and High','LowHigh'], index=['Race_Ethn'], aggfunc=np.sum)
		dist_table['District']= dist_table['Typical and High']/dist_table['LowHigh'] * 100

		dist_table['District'] = dist_table['District'].map('{:,.1f}'.format)
		dist_table['District'] = np.where((dist_table['LowHigh']) <= 10,'-1',dist_table['District'])
	
		dist_table= dist_table.drop(columns=['LowHigh','Typical and High'])
		dist_table=dist_table.T
	
		rename={}
		races={'African_American':'AA', 'American Indian':'AI','American_Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       'Pac_Islander':'PI', 'White':'W'}
		for col in dist_table.columns:
			if col in races.keys():
				temp_col = vs_obj.assessment_column+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+"_"+vs_obj.terms[0]
				rename[col]=temp_col
		dist_table.index.rename('School_Short', inplace=True)
		dist_table=dist_table.reset_index().set_index('School_Short')		
		dist_table.rename(columns=rename, inplace=True)
		dist_table=dist_table.round(1).astype(str) + '%'	
		

		for col in SELSGP_Race2.columns:
			if col in races.keys():		
				temp_col = vs_obj.assessment_column+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+"_"+vs_obj.terms[0]
				rename[col]=temp_col
		
		SELSGP_Race2.rename(columns=rename, inplace=True)
		rslt = pd.concat([SELSGP_Race2, dist_table])
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
	dist_table=pd.pivot_table(vs_obj.df, values=['Typ_High','LowHigh'], index=['Race_Ethn'], aggfunc=np.sum)
	dist_table['District']= dist_table['Typ_High']/dist_table['LowHigh'] * 100

	dist_table['District'] = dist_table['District'].map('{:,.1f}'.format)
	dist_table['District'] = np.where((dist_table['LowHigh']) <= 10,'-1',dist_table['District'])
	
	#dist_table = dist_table.rename(index=idx_rename)
	dist_table= dist_table.drop(columns=['LowHigh','Typ_High'])
	dist_table=dist_table.T
	
	rename={}
	races={'African_American':'AA', 'American Indian':'AI','American_Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       'Pac_Islander':'PI', 'White':'W'}
	for col in dist_table.columns:
		if col in races.keys():
			temp_col = vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
	dist_table.index.rename('School_Short', inplace=True)
	dist_table=dist_table.reset_index().set_index('School_Short')		
	dist_table.rename(columns=rename, inplace=True)
	dist_table=dist_table.round(1).astype(str) + '%'
	
	rename={}
	races={'African_American':'AA', 'African_American_Yes':'AA','American Indian':'AI','American_Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       'Pac_Islander':'PI', 'White':'W', 'District':'ALL','American Indian_Yes':'AI','American_Indian_Yes':'AI','Asian_Yes':'A', 'Filipino_Yes':'F', 'Hispanic_Yes':'HL', 'Mult_Yes':'Mult',
       'Pac_Islander_Yes':'PI', 'White_Yes':'W', 'District':'ALL', 'SGPPercentage':'ALL'}
	for col in tableframe2.columns:
		if col in races.keys():
			temp_col = vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
	tableframe2.rename(columns=rename, inplace=True)

	rslt = pd.concat([tableframe2, dist_table])
	
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
			tf_subgroup = tableframe[tableframe['SPED'] == 'Y']
			
		if 'FIT' in tableframe.columns:
			tf_subgroup = tableframe[tableframe['FIT'] == 'Y']
			
		if 'EL' in tableframe.columns:
			tf_subgroup = tableframe[tableframe['EL'] == 'Y']

		if 'Foster' in tableframe.columns:
			tf_subgroup = tableframe[tableframe['Foster'] == 'Y']
			
		
		tf_subgroup=tf_subgroup.set_index('School_Short')
		tf_subgroup.loc['District']=tf_subgroup.sum()
		tf_subgroup.loc[tf_subgroup.index[-1], column.name]=''
		
		tf_subgroup['SGPPercentage']= tf_subgroup['Typ_High']/tf_subgroup['LowHigh'] * 100
		tf_subgroup['SGPPercentage'] = tf_subgroup['SGPPercentage'].round(1)
		tf_subgroup['SGPPercentage'] = np.where((tf_subgroup['LowHigh']) <= 10,'-1',tf_subgroup['SGPPercentage'])
		tf_subgroup['SGPPercentage']=tf_subgroup['SGPPercentage'].astype(str)+"%"
		tf_subgroup=tf_subgroup.rename(columns = {'SGPPercentage': vs_obj.assessment_column+vs_obj.subjects[0]+"SGP"+" "+column.name+"_"+vs_obj.terms[0]})
		tf_subgroup=tf_subgroup.replace(to_replace="-1%", value="*")

		finalDfs.append(tf_subgroup)
		
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
		iR_PP_dist_race=pd.crosstab([vs_obj.df['Race_Ethn']],[column], values=vs_obj.df.Student_Number, aggfunc='count',normalize='index',margins=True)
		iR_PP_dist_race=iR_PP_dist_race.fillna(0)

		
		iR_PP_dist_race['ProjProf']=iR_PP_dist_race['Level 3']+iR_PP_dist_race['Level 4']
		iR_PP_dist_race =iR_PP_dist_race.drop(['Level 1', 'Level 2', 'Level 3', 'Level 4'], axis=1)
		iR_PP_dist_race=iR_PP_dist_race.mul(100).round(2).astype(str) + '%'

		iR_PP_dist_race=iR_PP_dist_race.T
		idx_rename={'ProjProf':'District'}
		iR_PP_dist_race = iR_PP_dist_race.rename(index=idx_rename)
		iR_PP_dist_race.index.name='School_Short'

		rename={}
		races={'African_American':'AA', 'American_Indian':'AI','American Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
		   			'Pac_Islander':'PI', 'White':'W', 'District':'District', 'All':'ALL'}
		for col in iR_PP_dist_race.columns:
			if col in races.keys():		
				temp_col = vs_obj.assessment_type+vs_obj.subjects[0]+'ProjProf '+races[col]+"_"+vs_obj.terms[0]
				rename[col]=temp_col
			
		iR_PP_dist_race.rename(columns=rename, inplace=True)
		

		#iReady PP ALL by School	
		rslt=pd.crosstab([vs_obj.df.School_Short],[column], 
							values=vs_obj.df.Student_Number, aggfunc='count',normalize='index',margins=True)

		rslt_count=pd.crosstab([vs_obj.df.School_Short],[column], 
							values=vs_obj.df.Student_Number, aggfunc='count',margins=True)
		rslt.fillna(0)

		rslt['ProjProf']=rslt['Level 3']+rslt['Level 4']
		rslt =rslt.drop(['Level 1', 'Level 2', 'Level 3', 'Level 4'], axis=1)
		rslt=rslt.mul(100).round(2).astype(str) + '%'
		idx_rename = {'All':'District'}
		rslt = rslt.rename(index=idx_rename)
		

		rslt['ProjProf'] = np.where((rslt_count['All']) <= 10,'*',rslt['ProjProf'])
		rslt.rename(columns={'ProjProf':vs_obj.assessment_type+vs_obj.subjects[0]+'ProjProf ALL'+"_"+vs_obj.terms[0]}, inplace=True)
		
		#Grade Level iReadyPP
		grade_lvl=pd.crosstab([vs_obj.df.School_Short, vs_obj.df['Grade Level']],[column], 
							values=vs_obj.df.Student_Number, aggfunc='count',normalize='index').mul(100).round(1)
		
		
		grade_lvl_count=pd.crosstab([vs_obj.df.School_Short, vs_obj.df['Grade Level']],[column], 
							values=vs_obj.df.Student_Number, aggfunc='count')

		grade_lvl_count = grade_lvl_count.fillna(0)
		grade_lvl_count['Total']=grade_lvl_count['Level 1']+grade_lvl_count['Level 2']+grade_lvl_count['Level 3']+grade_lvl_count['Level 4']

		grade_lvl['ProjProf']=grade_lvl['Level 3']+grade_lvl['Level 4']
		grade_lvl['ProjProf'] = np.where((grade_lvl_count['Total']) <= 10,'*',grade_lvl['ProjProf'])

		grade_lvl =grade_lvl.drop(['Level 1', 'Level 2', 'Level 3', 'Level 4'], axis=1)
		grade_lvl=grade_lvl.reset_index()

		
		grade_lvl = grade_lvl.pivot(index='School_Short',
										columns= 'Grade Level',
										values='ProjProf').astype(str)+"%"


		grade_lvl=grade_lvl.replace(to_replace="nan%",value="").replace(to_replace="*%", value="*")

		grade_lvl_dist=pd.crosstab([vs_obj.df['Grade Level']],[column], 
							values=vs_obj.df.Student_Number, aggfunc='count',normalize='index').mul(100).round(1)

		grade_lvl_dist_count=pd.crosstab([vs_obj.df['Grade Level']],[column], 
							values=vs_obj.df.Student_Number, aggfunc='count')

		grade_lvl_dist_count = grade_lvl_dist_count.fillna(0)
		grade_lvl_dist_count['Total']=grade_lvl_dist_count['Level 1']+grade_lvl_dist_count['Level 2']+grade_lvl_dist_count['Level 3']+grade_lvl_dist_count['Level 4']

		grade_lvl_dist['ProjProf']=(grade_lvl_dist['Level 3']+grade_lvl_dist['Level 4']).astype(str)+"%"
		grade_lvl_dist['ProjProf'] = np.where((grade_lvl_dist_count['Total']) <= 10,'*',grade_lvl_dist['ProjProf'])

		grade_lvl_dist =grade_lvl_dist.drop(['Level 1', 'Level 2', 'Level 3', 'Level 4'], axis=1)
		grade_lvl_dist=grade_lvl_dist.rename(columns={'ProjProf':'District'})
		grade_lvl_dist=grade_lvl_dist.T


		
		grade_lvl=pd.concat([grade_lvl, grade_lvl_dist])
		
		grade_lvl=gradeLevelAddColumns(vs_obj, grade_lvl)
		gradeLevelTab.append(grade_lvl)


		#Race iReady PP
		
		rslt_race_pp_count=pd.crosstab([vs_obj.df.School_Short, vs_obj.df.Race_Ethn],[column],
							 values=vs_obj.df.Student_Number, aggfunc='count')
		

		rslt_race_pp_count=rslt_race_pp_count.fillna(0)
		rslt_race_pp_count['StuGroupCount']=rslt_race_pp_count['Level 1']+rslt_race_pp_count['Level 2']+rslt_race_pp_count['Level 3']+rslt_race_pp_count['Level 4']

		rslt_race=pd.crosstab([vs_obj.df.School_Short, vs_obj.df.Race_Ethn],[column], 
								values=vs_obj.df.Student_Number, aggfunc='count',normalize='index',margins=True)
		rslt_race['ProjProf']=rslt_race['Level 3']+rslt_race['Level 4']
		rslt_race['StuCount']= rslt_race_pp_count['StuGroupCount']
		rslt_race.loc[rslt_race['StuCount'] <= 10, 'ProjProf'] = -1
		
		rslt_race =rslt_race.apply(pd.to_numeric)
		rslt_race.reset_index(inplace=True)
		
		rslt_race_pp = rslt_race.pivot(index='School_Short',
										columns= 'Race_Ethn',
										values='ProjProf')
		

		rslt_race_pp=rslt_race_pp.mul(100).round(1).astype(str) + '%'
		rslt_race_pp=rslt_race_pp.replace(to_replace="-100.0%",value="*").replace(to_replace="nan%",value="")
		
		
		rename={}
		races={'African_American':'AA', 'American_Indian':'AI','American Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
		   			'Pac_Islander':'PI', 'White':'W'} #'ProjProf':'ALL'

		for col in rslt_race_pp.columns:
			if col in races.keys():		
				temp_col = vs_obj.assessment_type+vs_obj.subjects[0]+'ProjProf '+races[col]+"_"+vs_obj.terms[0]
				rename[col]=temp_col
		
		rslt_race_pp.rename(columns=rename, inplace=True)
		

	
		#appends iR_PP_dist_race for District total to rslt_race_GL_pivt which has by school totals
				
		frames=[rslt_race_pp,rslt]
		rslt_race_Final=pd.concat(frames,
							    axis=1,
							    join="outer",
							    ignore_index=False,
							    keys=None,
							    levels=None,
							    names=None,
							    verify_integrity=False,
							    copy=True,
							)

		rslt_race_Final=rslt_race_Final.reset_index()
		rslt_race_Final.drop(index=rslt_race_Final[rslt_race_Final['School_Short'] == 'District'].index, inplace=True)
		
		rslt_race_Final=pd.concat([rslt_race_Final, iR_PP_dist_race])
		rslt_race_Final['School_Short'] = rslt_race_Final['School_Short'].fillna('District')
		rslt_race_Final=rslt_race_Final.reset_index(drop=True).set_index('School_Short')
		idx_rename = {'':'District'}
		rslt_race_Final = rslt_race_Final.rename(index=idx_rename)

		finalDfs.append(rslt_race_Final)
		
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
	
	rslt['total_count']=rslt['On or Above']+rslt['Below']
	
	
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
	

	rslt['Percent_On_Above']=(rslt['On or Above']/rslt['total_count']).mul(100).round(1).astype('str')+'%'
	rslt['Percent_On_Above'] = np.where((rslt['All']) <= 10,'*',rslt['Percent_On_Above'])
	rslt=rslt.rename(columns={'Percent_On_Above':vs_obj.assessment_type+vs_obj.subjects[0]+'GradeLevel '+"ALL_"+vs_obj.terms[0]})
	#grade level tabs
	grade_lvl=pd.crosstab([vs_obj.df.School_Short, vs_obj.df['Grade Level']],[vs_obj.df['Overall Relative Placement']], 
						values=vs_obj.df.Student_Number, aggfunc='count',normalize='index')


	grade_lvl_count=pd.crosstab([vs_obj.df.School_Short, vs_obj.df['Grade Level']],[vs_obj.df['Overall Relative Placement']], 
						values=vs_obj.df.Student_Number, aggfunc='count')

	grade_lvl_count=grade_lvl_count.fillna(0)
	grade_lvl['Percent_On_Above']=grade_lvl['Early On Grade Level']+grade_lvl['Mid or Above Grade Level']
	drop_cols=['1 Grade Level Below', '2 Grade Levels Below', '3 or More Grade Levels Below']
	for col in drop_cols:
		if col in grade_lvl.columns:
			grade_lvl =grade_lvl.drop(col, axis=1)
	grade_lvl=grade_lvl.mul(100).round(2).astype(str) + '%'
	grade_lvl=grade_lvl.reset_index()
	grade_lvl =grade_lvl.pivot(index='School_Short',
									columns= 'Grade Level',
									values='Percent_On_Above')


	grade_lvl_dist=pd.crosstab([vs_obj.df['Grade Level']],[vs_obj.df['Overall Relative Placement']], 
						values=vs_obj.df.Student_Number, aggfunc='count',normalize='index')
	grade_lvl_dist_count=pd.crosstab([vs_obj.df.School_Short, vs_obj.df['Grade Level']],[vs_obj.df['Overall Relative Placement']], 
						values=vs_obj.df.Student_Number, aggfunc='count')

	grade_lvl_dist_count=grade_lvl_dist_count.fillna(0)
	grade_lvl_dist['Percent_On_Above']=grade_lvl_dist['Early On Grade Level']+grade_lvl_dist['Mid or Above Grade Level']
	drop_cols=['1 Grade Level Below', '2 Grade Levels Below', '3 or More Grade Levels Below','Early On Grade Level','Mid or Above Grade Level']
	for col in drop_cols:
		if col in grade_lvl_dist.columns:
			grade_lvl_dist =grade_lvl_dist.drop(col, axis=1)
	grade_lvl_dist=grade_lvl_dist.mul(100).round(2).astype(str) + '%'
	grade_lvl_dist=grade_lvl_dist.rename(columns={'Percent_On_Above':'District'})
	grade_lvl_dist=grade_lvl_dist.T
	
	grade_lvl=pd.concat([grade_lvl, grade_lvl_dist])
	
	grade_lvl=gradeLevelAddColumns(vs_obj, grade_lvl)
	gradeLevelTab.append(grade_lvl)
	
	#Race for District Totals
	iR_GL_race=pd.crosstab([vs_obj.df['Race_Ethn']],[vs_obj.df['Overall Relative Placement']], values=vs_obj.df.Student_Number, aggfunc='count',margins=True)
	iR_GL_race=iR_GL_race.fillna(0)
	iR_GL_race['On or Above']=iR_GL_race['Early On Grade Level']+iR_GL_race['Mid or Above Grade Level']
	iR_GL_race['Percent_On_Above']=(iR_GL_race['On or Above']/iR_GL_race['All']).mul(100).round(1).astype(str)+"%"
	iR_GL_race['Percent_On_Above'] = np.where((iR_GL_race['All']) <= 10,'*',iR_GL_race['Percent_On_Above'])
	iR_GL_race=iR_GL_race.drop(columns=['All','On or Above','Early On Grade Level','Mid or Above Grade Level','1 Grade Level Below','2 Grade Levels Below','3 or More Grade Levels Below'])
	iR_GL_race=iR_GL_race.rename(columns={'Percent_On_Above':'District'})
	iR_GL_race=iR_GL_race.T
	iR_GL_race.index.name='School_Short'


	#Race iReady GL vs_obj.df2022
	rslt_race_GL_Count=pd.crosstab([vs_obj.df.School_Short, vs_obj.df.Race_Ethn],[vs_obj.df['Overall Relative Placement']], values=vs_obj.df.Student_Number, aggfunc='count', margins=True)
	rslt_race_GL=pd.crosstab([vs_obj.df.School_Short, vs_obj.df.Race_Ethn],[vs_obj.df['Overall Relative Placement']], values=vs_obj.df.Student_Number, aggfunc='count',normalize='index', margins=True).mul(100).round(1)
		
	rslt_race_GL=rslt_race_GL.fillna(0)
	
	if '3 or More Grade Levels Below' in rslt_race_GL.columns:
		rslt_race_GL['Below']=rslt_race_GL['1 Grade Level Below']+rslt_race_GL['2 Grade Levels Below']+rslt_race_GL['3 or More Grade Levels Below']
	else:
		rslt_race_GL['Below']=rslt_race_GL['1 Grade Level Below']+rslt_race_GL['2 Grade Levels Below']
	rslt_race_GL['Percent_On_Above']=rslt_race_GL['Early On Grade Level']+rslt_race_GL['Mid or Above Grade Level']
	#rslt_race_GL['total_count']=rslt_race_GL['On or Above']+rslt_race_GL['Below']
	rslt_race_GL['total_count']=rslt_race_GL_Count['All']

	rslt_race_GL.loc[rslt_race_GL['total_count'] <= 10, 'Percent_On_Above'] = -1
	rslt_race_GL=rslt_race_GL.replace(to_replace="-1",value="*").replace(to_replace="nan%",value="")
	
	rslt_race_GL=rslt_race_GL.reset_index()

	rslt_race_GL_pivt = rslt_race_GL.pivot(index='School_Short',
								columns= 'Race_Ethn',
								values='Percent_On_Above').round(1).astype(str)+"%"
	
	rslt_race_GL_pivt=rslt_race_GL_pivt.replace(to_replace="nan%",value="").replace(to_replace="-1.0%",value="*")
	
	rename={}
	races={'African_American':'AA', 'American_Indian':'AI','American Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
	  		'Pac_Islander':'PI', 'White':'W', 'All':'ALL', 'District':'District'}
	for col in rslt_race_GL_pivt.columns:
		if col in races.keys():		
			temp_col = vs_obj.assessment_type+vs_obj.subjects[0]+'GradeLevel '+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
		
	rslt_race_GL_pivt.rename(columns=rename, inplace=True)
	iR_GL_race.rename(columns=rename, inplace=True)
	
	
	#appends iR_GL_race for District totatl to rslt_race_GL_pivt which has by school totals
	frames=[rslt_race_GL_pivt, iR_GL_race]
	rslt_race_Final=pd.concat(frames,
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
	finalDfs.append(rslt_race_Final)
	
	subgroups(vs_obj,0,finalDfs)

def iReadySpan(vs_obj, finalDfs):
	if 'SP2023' in vs_obj.terms[0]:
		vs_obj.df=vs_obj.df[vs_obj.columns]
		vs_obj.df=vs_obj.df.reset_index()	
		vs_obj.df=vs_obj.df[vs_obj.df.Enrolled =='Enrolled']
		vs_obj.df.replace(r'^\s*$', np.nan, regex=True)
		vs_obj.df=vs_obj.df[vs_obj.df['Window'] =='End of Year']
		springWindowFilter(vs_obj)
		
		
		rslt_count=pd.crosstab([vs_obj.df.School_Short],[vs_obj.df["Overall Spanish Placement"]], values=vs_obj.df["Overall Spanish Placement"], aggfunc='count', margins=True, margins_name='All')
		rslt=pd.crosstab([vs_obj.df.School_Short],[vs_obj.df["Overall Spanish Placement"]], values=vs_obj.df["Overall Spanish Placement"], aggfunc='count',normalize='index', margins=True, margins_name='All').mul(100).round(1).astype(str)+"%"
		idx_rename = {'All':'District'}
		col_rename = {'Met': vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[0]+" ALL_"+vs_obj.terms[0]}
		rslt = rslt.rename(index=idx_rename).rename(columns=col_rename)
		rslt['iReadySPReadSpPLMNT ALL_SP2023'] = np.where((rslt_count['All']) <= 10,'*',rslt['iReadySPReadSpPLMNT ALL_SP2023'])

		#grade level

		grade_lvl=pd.crosstab([vs_obj.df.School_Short, vs_obj.df['Grade Level']],[vs_obj.df["Overall Spanish Placement"]],margins=True)

		grade_lvl['Percentage Proficient']=(grade_lvl['Met']/grade_lvl['All']).mul(100).round(1).astype(str) + '%'
		grade_lvl['Percentage Proficient'] = np.where((grade_lvl['All']) <= 10,'*',grade_lvl['Percentage Proficient'])
		grade_lvl=grade_lvl.reset_index()
		grade_lvl=grade_lvl.pivot(index='School_Short',
									columns= 'Grade Level',
									values='Percentage Proficient')

		grade_lvl=grade_lvl.drop(['All'])

		dist_grd_lvl=pd.crosstab([vs_obj.df['Grade Level']],[vs_obj.df["Overall Spanish Placement"]], values=vs_obj.df["Overall Spanish Placement"], aggfunc='count',normalize='index', margins=True, margins_name='All').mul(100).round(1).astype(str)+"%"
		
		cols= {'Met': 'District'}
		dist_grd_lvl = dist_grd_lvl.rename(columns=cols)
		dist_grd_lvl=dist_grd_lvl.drop(columns=['Not Met','Partially Met'])
		dist_grd_lvl=dist_grd_lvl.T
		dist_grd_lvl.index.name='School_Short'

		grade_lvl = pd.concat([grade_lvl, dist_grd_lvl])
		grade_lvl.index.name='School_Short'
		
	
		grade_lvl=gradeLevelAddColumns(vs_obj, grade_lvl)
		gradeLevelTab.append(grade_lvl)

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
			sub_rslt=pd.crosstab([vs_obj.df.School_Short, column],[vs_obj.df["Overall Spanish Placement"]], values=vs_obj.df["Overall Spanish Placement"], aggfunc='count', margins=True, margins_name='All')
			idx_rename = {'All':'District'}
			
			
			sub_rslt.reset_index(inplace=True)
			
			if 'SPED' in sub_rslt.columns:
				sub_rslt = sub_rslt[sub_rslt['SPED'] == 'Y']
				
			
			elif 'EL' in sub_rslt.columns:
				sub_rslt = sub_rslt[sub_rslt['EL'] == 'Y']

			elif 'Foster' in sub_rslt.columns:
				sub_rslt = sub_rslt[sub_rslt['Foster'] == 'Y']

				
		

			sub_rslt.loc['District'] = sub_rslt.iloc[:, :].sum()
			
			sub_rslt['Percentage Proficient']=(sub_rslt['Met']/sub_rslt['All']).mul(100).round(1).astype(str) + '%'
			sub_rslt['Percentage Proficient'] = np.where((sub_rslt['All']) <= 10,'*',sub_rslt['Percentage Proficient'])
			sub_rslt=sub_rslt.reset_index()
			
		
			col_rename = {'Percentage Proficient': vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+column.name+"_"+vs_obj.terms[0]}
			sub_rslt = sub_rslt.rename(index=idx_rename).rename(columns=col_rename)
			
			#sub_rslt.loc[sub_rslt.index[-1], column.name]=''
			
			sub_rslt.loc[sub_rslt.index[-1], 'School_Short']='District'
			sub_rslt=sub_rslt.set_index('School_Short')
			
			finalDfs.append(sub_rslt)

	
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

	
	idx_rename = {'Total':'District'}
	col_rename = {'Total': vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[0]+" ALL_"+vs_obj.terms[0]}
	rslt = rslt.rename(index=idx_rename).rename(columns=col_rename)
	rslt=rslt.filter(['School_Short', 'iReadyReadGRW ALL_SP2023'], axis=1)
	
	GRW_race_count = pd.crosstab([vs_obj.df.School_Short],[vs_obj.df.Race_Ethn], values=vs_obj.df[grw_column], aggfunc='count', margins=True, margins_name='District')
	
	rename={}
	races={'African_American':'AA', 'American_Indian':'AI','American Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
   			'Pac_Islander':'PI', 'White':'W'}
	for col in GRW_race_count.columns:
		if col in races.keys():		
			temp_col = races[col]+"_COUNT"
			rename[col]=temp_col
	
	GRW_race_count.rename(columns=rename, inplace=True)
	GRW_race=pd.crosstab([vs_obj.df.School_Short],[vs_obj.df.Race_Ethn], values=vs_obj.df[grw_column], aggfunc='median',margins=True, margins_name='Total')
	
	GRW_GradeLevel=pd.crosstab([vs_obj.df.School_Short],[vs_obj.df['Grade Level']], values=vs_obj.df[grw_column], aggfunc='median',margins=True, margins_name='Total')

	idx_rename = {'Total':'District'}
	GRW_GradeLevel = GRW_GradeLevel.rename(index=idx_rename)
	

	GRW_GradeLevel= GRW_GradeLevel.astype(str)+"%"
	GRW_GradeLevel=GRW_GradeLevel.replace(to_replace="*%",value = "*").replace(to_replace="nan%", value="")
	GRW_GradeLevel=gradeLevelAddColumns(vs_obj, GRW_GradeLevel)
	gradeLevelTab.append(GRW_GradeLevel)
	
	


	idx_rename = {'Total':'District'}
	col_rename = {'Total': vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[0]+" ALL_"+vs_obj.terms[0]}
	GRW_race = GRW_race.rename(index=idx_rename).rename(columns=col_rename)
	
	GRW_race= GRW_race.astype(str)+"%"
	GRW_race=GRW_race.replace(to_replace="*%",value = "*").replace(to_replace="nan%", value="")
	
	rename={}
	races={'African_American':'AA', 'American_Indian':'AI','American Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
   			'Pac_Islander':'PI', 'White':'W'}
	for col in GRW_race.columns:
		if col in races.keys():		
			temp_col = vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
	
	GRW_race.rename(columns=rename, inplace=True)
	

	Final=GRW_race.merge(GRW_race_count, how='inner', on='School_Short')
	
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
		
			sub_rslt=pd.crosstab([vs_obj.df.School_Short, column],[vs_obj.df['Percent Progress to Annual Typical Growth (%)']], values=vs_obj.df['Percent Progress to Annual Typical Growth (%)'], aggfunc='median', margins=True, margins_name='All')
			sub_count=pd.crosstab([vs_obj.df.School_Short, column],[vs_obj.df['Percent Progress to Annual Typical Growth (%)']], values=vs_obj.df['Percent Progress to Annual Typical Growth (%)'], aggfunc='count', margins=True, margins_name='All')
			sub_rslt=sub_rslt.reset_index().set_index('School_Short')
			sub_rslt.loc[sub_rslt.index[-1], column.name]='Y'
			sub_count=sub_count.filter(['School_Short', column.name, 'All'], axis=1)

			sub_count=sub_count.reset_index()
			sub_count['School_Short']=sub_count['School_Short']+"_"+sub_count[column.name]
			
			sub_count['School_Short']=sub_count['School_Short'].ffill(axis = 0)
			sub_count.loc[sub_count.index[-1], column.name]='Y'
			sub_count=sub_count.rename(columns={'All':'CountAll'})
			sub_rslt=sub_rslt.filter(['School_Short', column.name, 'All'], axis=1)
			sub_rslt=sub_rslt.reset_index()
			sub_rslt['School_Short']=sub_rslt['School_Short']+"_"+sub_rslt[column.name]
		
			sub_rslt=sub_rslt.reset_index().set_index('School_Short')
			sub_count=sub_count.reset_index().set_index('School_Short')
			
			sub_rslt=sub_rslt.merge(sub_count, how='inner', on='School_Short')
			sub_rslt['All'] = np.where((sub_rslt['CountAll']) <= 10,'*',sub_rslt['All'])
			sub_rslt['All']=sub_rslt['All'].astype(str)+"%"
			sub_rslt=sub_rslt.filter(['School_Short', column.name, 'All'], axis=1)

			#idx_rename = {'All':'District'}
			#sub_rslt=sub_rslt.drop(index='All')
			sub_rslt.reset_index(inplace=True)
			sub_rslt[column.name]=sub_rslt['School_Short'].str.contains("_Y")
			sub_rslt = sub_rslt[sub_rslt[column.name] == True]

			#sub_rslt.reset_index(inplace=True)
			
			sub_rslt=sub_rslt.rename(columns = {'All': vs_obj.assessment_type+vs_obj.subjects[0]+"GRW "+column.name+"_"+vs_obj.terms[0]})
			sub_rslt['School_Short'] =sub_rslt['School_Short'].replace({'Bayview_Y': 'Bayview', 'Chavez_Y': 'Chavez', 'Dover_Y': 'Dover', 'Grant_Y': 'Grant',
											'Helms_Y':'Helms','Murphy_Y':'Murphy','Obama_Y':'Obama','Ohlone_Y':'Ohlone','Peres_Y':'Peres'
											,'Shannon_Y':'Shannon','Stewart_Y':'Stewart','Valley View_Y':'Valley View','Virtual K-12_Y':'Virtual K-12'})
			sub_rslt=sub_rslt.set_index('School_Short')
			dfs=[]
			#DISTRICT TOTAL
			if column.name == 'SPED':	#if 'SPED' in vs_obj.df.columns:
				SPED_dist_total = vs_obj.df[vs_obj.df['SPED'] == 'Y']
				dfs.append(SPED_dist_total)
			#if 'EL' in table.columns
			elif column.name == 'EL':
				EL_dist_total = vs_obj.df[vs_obj.df['EL'] == 'Y']
				dfs.append(EL_dist_total)
			#if 'EL' in table.columns:
			elif column.name ==  'FIT':
				FIT_dist_total = vs_obj.df[vs_obj.df['FIT'] == 'Y']
				dfs.append(FIT_dist_total)

			elif column.name ==  'Foster':
				FIT_dist_total = vs_obj.df[vs_obj.df['Foster'] == 'Y']
				dfs.append(FIT_dist_total)
			
			for df in dfs:
				dist_total=pd.crosstab([column],[df['Percent Progress to Annual Typical Growth (%)']], values=df['Percent Progress to Annual Typical Growth (%)'], aggfunc='median', margins=True, margins_name='All')
				dist_count=pd.crosstab([column],[df['Percent Progress to Annual Typical Growth (%)']], values=df['Percent Progress to Annual Typical Growth (%)'], aggfunc='count', margins=True, margins_name='All')
			
				dist_total=dist_total.rename(index={'Y':'District'})
			
				dist_total.index.rename('School_Short', inplace=True)
				dist_total=dist_total.reset_index()
			
			
				dist_total=dist_total[['School_Short','All']]
				dist_total = dist_total[dist_total['School_Short'] == 'District']
				dist_total=dist_total.rename(columns={'All': vs_obj.assessment_type+vs_obj.subjects[0]+"GRW "+column.name+"_"+vs_obj.terms[0]})



				dist_total=dist_total.set_index('School_Short')
				sub_rslt=pd.concat([sub_rslt, dist_total], axis=0)
				sub_rslt=sub_rslt.replace(to_replace="*%", value="*")
				print(sub_rslt)
				finalDfs.append(sub_rslt)
	
	
	

def starDB(vs_obj,finalDfs):
	vs_obj.df=vs_obj.df[vs_obj.columns]

	starFilters(vs_obj)
	vs_obj.df=vs_obj.df.sort_values('CompletedDate').groupby('Student_Number').tail(1)
	vs_obj.df=vs_obj.df[vs_obj.df.AssessmentStatus =='Active']
	
	rslt=pd.crosstab([vs_obj.df.School_Short],[vs_obj.df.DistrictBenchmarkCategoryName], values=vs_obj.df.Student_Number, aggfunc='count',margins=True, margins_name='District')
	
	rslt_percentage=pd.crosstab([vs_obj.df.School_Short],[vs_obj.df.DistrictBenchmarkCategoryName], values=vs_obj.df.Student_Number, aggfunc='count',margins=True, margins_name='District', normalize='index').mul(100).round(1).astype('str')+'%'
	

	grade_lvl=pd.crosstab([vs_obj.df.School_Short, vs_obj.df['Grade Level']],[vs_obj.df['DistrictBenchmarkCategoryName']], 
						values=vs_obj.df.Student_Number, aggfunc='count', margins=True)
	grade_lvl.loc[grade_lvl['All'] <= 10, 'At/Above Benchmark'] = -1
	grade_lvl.reset_index()
	grade_lvl['Percent_AtorAbove']=(grade_lvl['At/Above Benchmark']/grade_lvl['All']).mul(100).round(1)
	grade_lvl.loc[grade_lvl['Percent_AtorAbove'] < 0, 'Percent_AtorAbove'] = "*"
	
	grade_lvl =grade_lvl.drop(['Intervention', 'On Watch', 'Urgent Intervention', 'All'], axis=1)
	grade_lvl=grade_lvl.astype(str) + '%'
	grade_lvl=grade_lvl.reset_index()

	grade_lvl = grade_lvl.pivot(index='School_Short',
									columns= 'Grade Level',
									values='Percent_AtorAbove')


	idx_rename = {'All':'District'} 
	grade_lvl = grade_lvl.rename(index=idx_rename)
	grade_lvl=grade_lvl.replace(to_replace="*%",value="*").replace(to_replace="nan%",value="*")
	grade_lvl=grade_lvl.reset_index().set_index('School_Short')
	grade_lvl=grade_lvl.drop(['District'])


	#adding distric total for Grade Level tab
	dist_grade_lvl_DB=pd.crosstab([vs_obj.df['Grade Level']],[vs_obj.df['DistrictBenchmarkCategoryName']],margins=True, normalize='index').mul(100).round(1).astype(str)+"%"
	rename_col={'At/Above Benchmark':'District'}
	rename={'Grade Level':'School_Short'}
	dist_grade_lvl_DB=dist_grade_lvl_DB.rename(columns=rename_col).drop(columns=['Intervention','On Watch','Urgent Intervention'])
	
	dist_grade_lvl_DB=dist_grade_lvl_DB.T
	
	dist_grade_lvl_DB.index.name='School_Short'
	
	grade_lvl = pd.concat([grade_lvl, dist_grade_lvl_DB])
	grade_lvl.index.name='School_Short'
	
	grade_lvl=gradeLevelAddColumns(vs_obj, grade_lvl)
	gradeLevelTab.append(grade_lvl)
	

	rslt_race_count=pd.crosstab([vs_obj.df.School_Short, vs_obj.df.Race_Ethn],[vs_obj.df.DistrictBenchmarkCategoryName], values=vs_obj.df.Student_Number, aggfunc='count',margins=True, margins_name='All')
	rslt_race_count=rslt_race_count.drop(columns=['Intervention','On Watch', 'Urgent Intervention'])
	rslt_race_count=rslt_race_count.reset_index()
	
	container=[]
	for label, _df in rslt_race_count.groupby(['School_Short']):
		row_label=label+'_ALL'
		_df.loc[row_label] = _df[['At/Above Benchmark','All']].sum()
		container.append(_df)

	df_summary = pd.concat(container)
	
	df_summary['Percent_On_Above']=df_summary['At/Above Benchmark']/df_summary['All']
	df_summary.loc[df_summary['All'] <= 10, 'Percent_On_Above'] = -1
	df_summary['School_Short'] = df_summary['School_Short'].ffill()
	df_summary['Race_Ethn']=df_summary['Race_Ethn'].fillna('ALL')
	rslt_race_count=df_summary.copy()

	
	rslt_race_count=rslt_race_count.reset_index()
	
	rslt_race = rslt_race_count.pivot(index='School_Short',
									columns= 'Race_Ethn',
									values='Percent_On_Above').mul(100).round(1).astype('str')+"%"
	rslt_race=rslt_race.replace(to_replace="nan%",value="").replace(to_replace="-100.0%",value="*")
	
	if 'EarlyLit' in vs_obj.subjects[0] and 'FA2022' in vs_obj.terms[0]:
		rslt_percentage.rename(columns=STAREarlyLit_columnheadersFA22, inplace=True)
		rslt_race.rename(columns=STAREarlyLit_columnheadersFA22, inplace=True)
		rslt_race['STAREarlyLitDB ALL_FA2022']=rslt_percentage['STAREarlyLitDB ALL_FA2022']
	
	if 'EarlyLit' in vs_obj.subjects[0] and 'W2022' in vs_obj.terms[0]:
		rslt_percentage.rename(columns=STAREarlyLit_columnheadersW22, inplace=True)
		rslt_race.rename(columns=STAREarlyLit_columnheadersW22, inplace=True)
		rslt_race['STAREarlyLitDB ALL_W2022']=rslt_percentage['STAREarlyLitDB ALL_W2022']

	#district totals
	dist_race=pd.crosstab([vs_obj.df.Race_Ethn],[vs_obj.df.DistrictBenchmarkCategoryName], values=vs_obj.df.Student_Number, aggfunc='count',normalize='index',margins=True).mul(100).round(1).astype(str)+"%"
	dist_race=dist_race.reset_index().set_index('Race_Ethn')
	dropped=['Intervention','On Watch','Urgent Intervention','']
	
	for col in dist_race.columns:
		if col in dropped:
			dist_race=dist_race.drop(columns=col)

	dist_race=dist_race.T
	dist_race=dist_race.rename(index={'At/Above Benchmark':'District'})
	dist_race=dist_race.reset_index()
	dist_race=dist_race.rename(columns={'DistrictBenchmarkCategoryName':'School_Short'})

	rename={}
	races={'African_American':'AA', 'American_Indian':'AI','American Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       			'Pac_Islander':'PI', 'White':'W', 'All':'ALL','ALL':'ALL'}
	for col in dist_race.columns:
		if col in races.keys():		
			temp_col = "STAR"+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
	for col in rslt_race.columns:
		if col in races.keys():		
			temp_col = "STAR"+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+"_"+vs_obj.terms[0]
			rename[col]=temp_col
	
	frames=[dist_race,rslt_race]
	
	dist_race.rename(columns=rename, inplace=True)
	
	rslt_race.rename(columns=rename, inplace=True)

	rslt_race=rslt_race.reset_index().set_index('School_Short')
	
	dist_race=dist_race.reset_index().set_index('School_Short')
	
	rslt_race = pd.concat([rslt_race, dist_race])
	
	finalDfs.append(rslt_race)	
	
	
	subgroups(vs_obj,0,finalDfs)

def newSubgroups(vs_obj, metric_column_index, subgroup_count, column):
		if vs_obj.metrics[metric_column_index] == 'SGP':
			pass
		else:
			subgroup_count.columns=['_'.join(col) for col in subgroup_count.columns.values]	
			subgroup_count=subgroup_count.rename(columns={'Y_CHRONIC':'Y_Chronic','Y_SEVERE CHRONIC':'Y_Severe'})


		if vs_obj.assessment_type == 'iReady':
			subgroup_count=subgroup_count.fillna(0)
			if ('Y_Mid or Above Grade Level' in subgroup_count.columns) and ('Y_Early On Grade Level' in subgroup_count.columns):		
				subgroup_count['Y']=subgroup_count['Y_Early On Grade Level']+subgroup_count['Y_Mid or Above Grade Level']
			elif 'Y_Early On Grade Level' in subgroup_count.columns:
				subgroup_count['Y']=subgroup_count['Y_Early On Grade Level']
			else:
				subgroup_count['Y']=0

			if 'Y_Level 4' in subgroup_count.columns:
				subgroup_count['Y']=subgroup_count['Y_Level 3']+subgroup_count['Y_Level 4']
			elif 'Y_Level 3' in subgroup_count.columns:
				subgroup_count['Y']=subgroup_count['Y_Level 3']
		
		
		subgroup_count=subgroup_count.fillna(0)

		subgroup_count['Y_SUM'] = subgroup_count.filter(like='Y_').sum(1)
		if 'Y_Chronic' in subgroup_count.columns:
			subgroup_count['Y_Severe&Chronic']=subgroup_count['Y_Chronic']+subgroup_count['Y_Severe']
			
		
		subgroup_count=subgroup_count.rename(columns = {'Y_Yes':'Y','Y_At/Above Benchmark':'Y', 'Y_Y':'Y','Y_Severe&Chronic':'Y'})

		if 'Y' not in subgroup_count.columns:
			subgroup_count['Y'] = 0
		
		idx_rename = {'All':'District'} 
		subgroup_count = subgroup_count.rename(index=idx_rename)

		subgroup_count['Y_Percent_On_Above']=((subgroup_count['Y']/subgroup_count['Y_SUM'])*100).round(1)
		subgroup_count.loc[subgroup_count['Y_SUM'] == 0, 'Y_Percent_On_Above'] = ""
		subgroup_count.loc[(subgroup_count['Y_SUM'] <= 10) & (subgroup_count['Y_SUM'] > 0), 'Y_Percent_On_Above'] = -1
		idx_rename = {'All':'District'} 
		subgroup_count = subgroup_count.rename(index=idx_rename)
		
		subgroup_count=subgroup_count.reset_index()
		
		subgroup_count['Y_Percent_On_Above']=subgroup_count['Y_Percent_On_Above'].astype(str)+"%"
		subgroup_count['Y_Percent_On_Above']=subgroup_count['Y_Percent_On_Above'].replace(to_replace="-100.0%",value="*").replace(to_replace="nan%",value="").replace(to_replace="-1%", value="*").replace(to_replace="%",value="")
		
		if vs_obj.assessment_type == 'ChrAbs':
			subgroup_count=subgroup_count.rename(columns = {'Y_Percent_On_Above': vs_obj.assessment_type+" "+column.name+ "_"+vs_obj.terms[0]})
			subgroup_count=subgroup_count[['School_Short', vs_obj.assessment_type+" "+column.name+ "_"+vs_obj.terms[0]]]
			subgroup_count=subgroup_count.reset_index(drop=True).set_index('School_Short')
			

		elif vs_obj.assessment_type == 'iReady':
			#subgroup_count.loc[(subgroup_count['total'] <= 10) & (subgroup_count['total'] > 0), 'Y_Percent_On_Above'] = "*"
			subgroup_count=subgroup_count.rename(columns = {'Y_Percent_On_Above':vs_obj.assessment_type+vs_obj.subjects[0]+vs_obj.metrics[metric_column_index]+" "+column.name+"_"+vs_obj.terms[0]})
			
		else:
			subgroup_count=subgroup_count.rename(columns = {'Y_Percent_On_Above': vs_obj.assessment_column+vs_obj.subjects[metric_column_index]+vs_obj.metrics[metric_column_index]+" "+column.name+"_"+vs_obj.terms[0]})

		subgroup_count=subgroup_count.replace(to_replace="-1%",value="*").replace(to_replace="nan%",value="*")

		return(subgroup_count)
	
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
	
		subgroup_count=pd.crosstab([vs_obj.df.School_Short],[column,vs_obj.df[vs_obj.columns[metric_column_index]]], 
							values=vs_obj.df.Student_Number, aggfunc='count',margins=True, margins_name='All')
		
		grd_lvl_count=pd.crosstab([vs_obj.df['Grade Level']],[column,vs_obj.df[vs_obj.columns[metric_column_index]]], 
							values=vs_obj.df.Student_Number, aggfunc='count',margins=True, margins_name='All')
		
		subgroupFinal=newSubgroups(vs_obj,0, subgroup_count, column)
		subgroupFinal = subgroupFinal.iloc[: , [0,-1]].copy()
		subgroupFinal = subgroupFinal.T.drop_duplicates().T
		
		finalDfs.append(subgroupFinal)

		if vs_obj.assessment_type in ('STAR', 'SEL', 'iReady', 'ESGI'):
			SupesTabFinal=newSubgroups(vs_obj,0, grd_lvl_count, column)
			SupesTabFinal = SupesTabFinal.iloc[: , [0,-1]].copy()
 		

			supesGoalsTab.append(SupesTabFinal)
		


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
		
		grade_lvl=pd.crosstab([vs_obj.df.School_Short, vs_obj.df['Grade Level']],[vs_obj.df['Typical and High']],margins=True, normalize='index').mul(100).round(1).astype(str)+"%"
		grade_lvl_count=pd.crosstab([vs_obj.df.School_Short, vs_obj.df['Grade Level']],[vs_obj.df['Typical and High']],margins=True)
		grade_lvl['Y'] = np.where((grade_lvl_count['All']) <= 10,'*',grade_lvl['Y'])
		
		grade_lvl=grade_lvl.reset_index()
		grade_lvl=grade_lvl.pivot(index='School_Short',
									columns= 'Grade Level',
									values='Y')

		
		dist_grade_lvl_SGP=pd.crosstab([vs_obj.df['Grade Level']],[vs_obj.df['Typical and High']],margins=True, normalize='index').mul(100).round(1).astype(str)+"%"
		rename_col={'Y':'District'}
		rename={'Grade Level':'School_Short'}
		dist_grade_lvl_SGP=dist_grade_lvl_SGP.rename(columns=rename_col).drop(columns='N')
		dist_grade_lvl_SGP=dist_grade_lvl_SGP.reset_index()
		dist_grade_lvl_SGP=dist_grade_lvl_SGP.rename(columns=rename)
		
		dist_grade_lvl_SGP=dist_grade_lvl_SGP.reset_index(drop=True).set_index(['School_Short'])
		dist_grade_lvl_SGP=dist_grade_lvl_SGP.T
		grade_lvl=grade_lvl.drop(index='All')
		

		grade_lvl = pd.concat([grade_lvl, dist_grade_lvl_SGP])
		grade_lvl.index.name='School_Short'

		
		
	if vs_obj.assessment_type == 'STAR' and vs_obj.metrics[metric_column_index] == 'SB':
		grade_lvl=pd.crosstab([vs_obj.df.School_Short, vs_obj.df['Grade Level']],[vs_obj.df[vs_obj.columns[metric_column_index]]],margins=True)

		grade_lvl['Percentage Proficient']=(grade_lvl['Yes']/grade_lvl['All']).mul(100).round(1).astype(str) + '%'
		grade_lvl['Percentage Proficient'] = np.where((grade_lvl['All']) <= 10,'*',grade_lvl['Percentage Proficient'])
		grade_lvl=grade_lvl.reset_index()
		grade_lvl=grade_lvl.pivot(index='School_Short',
									columns= 'Grade Level',
									values='Percentage Proficient')

		dist_grade_lvl_SB=pd.crosstab([vs_obj.df['Grade Level']],[vs_obj.df[vs_obj.columns[metric_column_index]]],margins=True, normalize='index').mul(100).round(1).astype(str)+"%"
		cols={'Yes':'District'}
		_rename={'Grade Level':'School_Short'}
		dist_grade_lvl_SB=dist_grade_lvl_SB.rename(columns=cols).drop(columns='No')
		
		dist_grade_lvl_SB=dist_grade_lvl_SB.reset_index()
		dist_grade_lvl_SB=dist_grade_lvl_SB.rename(columns=_rename)
		
		dist_grade_lvl_SB=dist_grade_lvl_SB.reset_index(drop=True).set_index(['School_Short'])
		dist_grade_lvl_SB=dist_grade_lvl_SB.T

		grade_lvl = pd.concat([grade_lvl, dist_grade_lvl_SB])
		grade_lvl.index.name='School_Short'
		

	grade_lvl=gradeLevelAddColumns(vs_obj, grade_lvl)
	grade_lvl=grade_lvl.drop(columns='')
	gradeLevelTab.append(grade_lvl)
	
	#finalDfs.append(grade_lvl)
	
def disIndx(vs_obj,finalDfs):
	vs_obj.df=vs_obj.df[vs_obj.columns]
	vs_obj.df = vs_obj.df.replace(r'^\s*$', np.nan, regex=True)
	vs_obj.df['African American DI']=vs_obj.df['African American DI'].replace(to_replace="nan",value="")
	DIC1_rename_col={'African American DI':'DI AA_FA2022'}
	DIC2_rename_col={'African American DI':'DI AA_W2022'}
	DIC3_rename_col={'African American DI':'DI AA_SP2023'}
	
	if vs_obj.terms[0] == 'FA2022':
		vs_obj.df.rename(columns=DIC1_rename_col, inplace=True)
	elif vs_obj.terms[0] == 'W2022':
		vs_obj.df.rename(columns=DIC2_rename_col, inplace=True)
	elif vs_obj.terms[0] == 'SP2023':
		vs_obj.df.rename(columns=DIC3_rename_col, inplace=True)
	
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
		vs_obj.df.rename(columns=SuspRteC1_rename_col, inplace=True)
		
	if vs_obj.terms == 'W2022':
		vs_obj.df.rename(columns=SuspRteC2_rename_col, inplace=True)
	
	if vs_obj.terms == 'SP2023':					
		vs_obj.df.rename(columns=SuspRteC3_rename_col, inplace=True)
	
	vs_obj.df=vs_obj.df.replace(to_replace="-",value="").replace(to_replace=-1,value="*").replace(to_replace="-%",value="")
	vs_obj.df.reset_index()
	
	finalDfs.append(vs_obj.df)
	
def saebrsScreener(vs_obj,finalDfs):
	vs_obj.df=vs_obj.df.rename(columns={'Percent Complete':'SAEBRSparticipation ALL_FA2022'})
	finalDfs.append(vs_obj.df)
	



def supesGoals(vs_obj):
	idx_rename = {'All':'District'} 
	
	#STAR SB calculations - Did Literacy for Supes Goals, then added Math for Assessment Summaries.
	#if (vs_obj.assessment_type == 'STAR') and (vs_obj.metrics[0] == 'SB') and ('math' in vs_obj.subjects):
	if (vs_obj.assessment_type == 'STAR') and (vs_obj.metrics[0] == 'SB') and ('read' in vs_obj.subjects):
		starFilters(vs_obj)
		vs_obj.df=vs_obj.df.sort_values('CompletedDate').groupby('Student_Number').tail(1)
		vs_obj.df=vs_obj.df[vs_obj.df.AssessmentStatus =='Active']

		
		supes=pd.crosstab([vs_obj.df['Grade Level']],[vs_obj.df['StateBenchmarkProficient']],margins=True)
		supes = supes.rename(index=idx_rename)
		supes['Percentage Proficient']=(supes['Yes']/supes['All']).mul(100).round(1).astype(str) + '%'
		supes['Percentage Proficient'] = np.where((supes['All']) <= 10,'*',supes['Percentage Proficient'])
		
		#supes_race = supes_race.rename(index=idx_rename)
		supes_race=pd.crosstab([vs_obj.df['Grade Level'],vs_obj.df['Race_Ethn']],[vs_obj.df['StateBenchmarkProficient']],margins=True)
		supes_race['Percentage Proficient']=(supes_race['Yes']/supes_race['All']).mul(100).round(1).astype(str) + '%'
		supes_race['Percentage Proficient'] = np.where((supes_race['All']) <= 10,'*',supes_race['Percentage Proficient'])
		supes_race=supes_race.reset_index()

		supes_race_total=pd.crosstab([vs_obj.df['Race_Ethn']],[vs_obj.df['StateBenchmarkProficient']],margins=True)
		supes_race_total = supes_race_total.drop(index=['All'])
		supes_race_total['District']=(supes_race_total['Yes']/supes_race_total['All']).mul(100).round(1).astype(str) + '%'
		supes_race_total['District'] = np.where((supes_race_total['All']) <= 10,'*',supes_race_total['District'])
		
		supes=supes.reset_index().set_index("Grade Level")
		race_dfs=[supes_race, supes,  supes_race_total]
		drop_cols=['No','Yes','All','']
		
		
		for col in supes_race.columns:
			if col in drop_cols:
				supes_race=supes_race.drop(columns=col)
		for col in supes.columns:
			if col in drop_cols:
				supes=supes.drop(columns=col)
		for col in supes_race_total.columns:
			if col in drop_cols:
				supes_race_total=supes_race_total.drop(columns=col)
		
		supes_race_total=supes_race_total.T
		#supes_race_total=supes_race_total.index.rename('Grade Level')
		
		supes=supes.rename(columns={'Percentage Proficient': "STAR_SG"+vs_obj.subjects[0]+vs_obj.metrics[0]+" ALL_"+vs_obj.terms[0]})
		
		supes_race=supes_race.pivot(index="Grade Level",
									columns="Race_Ethn",
									values='Percentage Proficient')

		
		supes_race=supes_race.reset_index().set_index("Grade Level")
		
		rename={}
		races={'African_American':'AA', 'American_Indian':'AI','American Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       			'Pac_Islander':'PI', 'White':'W'}
		

		for col in races.keys():
			rename[col]="STAR_SG"+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+""+vs_obj.terms[0]
		


		supes=supes.rename(columns=rename)
		supes_race=supes_race.rename(columns=rename)
		supes_race_total=supes_race_total.rename(columns=rename)
		
		supes_race=pd.concat([supes_race,supes_race_total])
		supes_race.index.rename('Grade Level', inplace=True)
		supes_race=supes_race.drop(index='All')
		supes_race=pd.concat([supes_race, supes], axis=1)
		supes_race=supes_race.drop(columns=[''])
		#supes_race=supes_race.index.rename("Grade Level")
		supesGoalsTab.append(supes_race)
		
	
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
		supesiReadyGL_count=pd.crosstab([vs_obj.df['Grade Level']],[vs_obj.df['Overall Relative Placement']], values=vs_obj.df.Student_Number, aggfunc='count',margins=True)
		supesiReadyGL=pd.crosstab([vs_obj.df['Grade Level']],[vs_obj.df['Overall Relative Placement']], values=vs_obj.df.Student_Number, aggfunc='count',margins=True, normalize='index')
		supesiReadyGL=supesiReadyGL.fillna(0)
		supesiReadyGL['On or Above']=supesiReadyGL['Early On Grade Level']+supesiReadyGL['Mid or Above Grade Level']
		supesiReadyGL=supesiReadyGL.fillna(0)
		supesiReadyGL['On or Above']=supesiReadyGL['On or Above'].mul(100).round(1).astype(str)+"%"
		supesiReadyGL['On or Above'] = np.where((supesiReadyGL_count['All'] <= 10) & (supesiReadyGL_count['All'] > 0),'*',supesiReadyGL['On or Above'])
		
		supesiReadyGL_count = supesiReadyGL_count.rename(index=idx_rename)
		drop_cols=['1 Grade Level Below',	'2 Grade Levels Below',	'3 or More Grade Levels Below',	'Early On Grade Level',	'Mid or Above Grade Level']
		
		for col in drop_cols:
			if col in supesiReadyGL.columns:
				supesiReadyGL=supesiReadyGL.drop(columns=col)
		
		supesiReadyGL = supesiReadyGL.rename(index=idx_rename).rename(columns={'On or Above':"iReady_SG"+vs_obj.subjects[0]+vs_obj.metrics[0]+" ALL_"+vs_obj.terms[0]})
		
		#race
		supesiReadyGL_race=pd.crosstab([vs_obj.df['Grade Level'],vs_obj.df['Race_Ethn']],[vs_obj.df['Overall Relative Placement']], values=vs_obj.df.Student_Number, aggfunc='count',margins=True)
		supesiReadyGL_race=supesiReadyGL_race.fillna(0)
		supesiReadyGL_race['On or Above']=supesiReadyGL_race['Early On Grade Level']+supesiReadyGL_race['Mid or Above Grade Level']
		supesiReadyGL_race['On or Above %']=(supesiReadyGL_race['On or Above']/supesiReadyGL_race['All']).mul(100).round(1).astype(str)+"%"
		supesiReadyGL_race['On or Above %'] = np.where((supesiReadyGL_race['All']) <= 10,'*',supesiReadyGL_race['On or Above %'])
		supesiReadyGL_race = supesiReadyGL_race.rename(index=idx_rename).reset_index()
		
		supesiReadyGL_race=supesiReadyGL_race.pivot(index='Grade Level',
										columns='Race_Ethn',
										values='On or Above %')

		rename={}
		races={'African_American':'AA', 'American_Indian':'AI','American Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       			'Pac_Islander':'PI', 'White':'W'}
		
		for col in races.keys():
			rename[col]="iReady_SG"+vs_obj.subjects[0]+vs_obj.metrics[0]+" "+races[col]+""+vs_obj.terms[0]
		
		supesiReadyGL_race=supesiReadyGL_race.rename(columns=rename)

		supesiReadyGL=pd.concat([supesiReadyGL_race, supesiReadyGL], axis=1)
		supesiReadyGL=supesiReadyGL.drop(columns=[''])

		
		supesGoalsTab.append(supesiReadyGL)
		
		supeGoalsGrdLvlSubgrps(vs_obj, 0)
		
	elif (vs_obj.assessment_type == 'ESGI') :
		metrics = ['WCCUSD Uppercase Letters (PLF R 3.2)','WCCUSD Lowercase Letters (PLF R 3.2)', 'WCCUSD Number Recognition 0-12 (PLF NS 1.2)']
		for metric in metrics:
			new=vs_obj.df.loc[vs_obj.df['Test Name']== metric]
			
			MetEOYBenchmark=pd.crosstab([new['Test Name'],new['Grade Level_y']],[new['Met EOY Benchmark']],values=vs_obj.df.Student_Number, 
							margins=True, aggfunc='count')
			ESGI_race=pd.crosstab([new['Test Name'],new['Grade Level_y'],new['Race_Ethn']],[new['Met EOY Benchmark']],values=vs_obj.df.Student_Number, 
							margins=True, aggfunc='count')
		
			idx_rename = {'All':'District'} 
			MetEOYBenchmark = MetEOYBenchmark.rename(index=idx_rename)
			ESGI_race=ESGI_race.rename(index=idx_rename)
			MetEOYBenchmark=MetEOYBenchmark.fillna(0)
			ESGI_race=ESGI_race.fillna(0)

			MetEOYBenchmark['PercentageMetEOYBenchmark']=(MetEOYBenchmark['Y']/MetEOYBenchmark['All']).mul(100).round(1)
			MetEOYBenchmark['PercentageMetEOYBenchmark'] = np.where((MetEOYBenchmark['All']) <= 10,'*',MetEOYBenchmark['PercentageMetEOYBenchmark'])
			MetEOYBenchmark['PercentageMetEOYBenchmark'] = MetEOYBenchmark['PercentageMetEOYBenchmark'].astype(str)+"%"
			MetEOYBenchmark=MetEOYBenchmark.replace(to_replace="*%", value="*")
			MetEOYBenchmark=MetEOYBenchmark.reset_index()
		
			MetEOYBenchmark=MetEOYBenchmark.rename(columns={'PercentageMetEOYBenchmark':metric+" ALL "+vs_obj.terms[0],'Grade Level_y':'Grade Level'})
			MetEOYBenchmark.loc[MetEOYBenchmark.index[-1], 'Grade Level']='District'
			MetEOYBenchmark=MetEOYBenchmark.drop(columns=['Test Name','FALSE','Y','All'])
			ESGI_race['PercentageMetEOYBenchmark']=(ESGI_race['Y']/ESGI_race['All']).mul(100).round(1)
			ESGI_race['PercentageMetEOYBenchmark'] = np.where((ESGI_race['All']) <= 10,'*',ESGI_race['PercentageMetEOYBenchmark'])
			ESGI_race['PercentageMetEOYBenchmark'] = ESGI_race['PercentageMetEOYBenchmark'].astype(str)+"%"
			ESGI_race=ESGI_race.replace(to_replace="*%", value="*")
			
			ESGI_race=ESGI_race.reset_index()
			
			ESGI_race=ESGI_race.pivot(index='Grade Level_y'
									, columns='Race_Ethn'
									, values='PercentageMetEOYBenchmark')

			
			ESGI_race=ESGI_race.reset_index().rename(columns={'Grade Level_y':'Grade Level'})
			
			ESGI_race=ESGI_race.set_index('Grade Level').drop(index=[''])
			
			rename={}
			races={'African_American':'AA', 'American_Indian':'AI','American Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       			'Pac_Islander':'PI', 'White':'W'}
		

			for col in races.keys():
				rename[col]=metric+" "+races[col]+" "+vs_obj.terms[0]
		


			ESGI_race=ESGI_race.rename(columns=rename)
			
			ESGI_race=ESGI_race.reset_index()
			
			
			supesGoalsTab.append(MetEOYBenchmark)
			supesGoalsTab.append(ESGI_race)
			
		supeGoalsGrdLvlSubgrps(vs_obj, 0)
			
	elif vs_obj.assessment_type == 'SEL':
		starFilters(vs_obj)
		
		vs_obj.df=vs_obj.df.sort_values('CompletedDate').groupby('Student_Number').tail(1)
		vs_obj.df=vs_obj.df[vs_obj.df.AssessmentStatus =='Active']

		SELDB_race=pd.crosstab([vs_obj.df['Grade Level'], vs_obj.df.Race_Ethn],[vs_obj.df.DistrictBenchmarkCategoryName], values=vs_obj.df.Student_Number, aggfunc='count',margins=True)

		SELDB_race['Percent_On_Above']=(SELDB_race['At/Above Benchmark']/SELDB_race['All']).mul(100).round(1)
		SELDB_race['Percent_On_Above'] = np.where((((SELDB_race['All']) <= 10) & ((SELDB_race['All']) > 0)),'*',SELDB_race['Percent_On_Above'])
		SELDB_race['Percent_On_Above'] = SELDB_race['Percent_On_Above'].astype(str)+"%"
		SELDB_race=SELDB_race.replace(to_replace="*%", value="*")
		SELDB_race=SELDB_race.drop(index='All')
	
		SELDB_race=SELDB_race.reset_index()
		SELDB_race=SELDB_race.pivot(index='Grade Level'
									, columns='Race_Ethn'
									, values='Percent_On_Above')

						
		rename={}
		races={'African_American':'AA', 'American_Indian':'AI','American Indian':'AI','Asian':'A', 'Filipino':'F', 'Hispanic':'HL', 'Mult':'Mult',
       			'Pac_Islander':'PI', 'White':'W'}
		

		for col in races.keys():
			rename[col]="SEL_DB "+races[col]+" "+vs_obj.terms[0]
		

		SELDB_race=SELDB_race.rename(columns=rename)
		dist_grade_lvl_DB=pd.crosstab([vs_obj.df['Grade Level']],[vs_obj.df['DistrictBenchmarkCategoryName']],margins=True, normalize='index').mul(100).round(1).astype(str)+"%"
		rename_col={'At/Above Benchmark':'District'}
		rename={'Grade Level':'School_Short'}
		dist_grade_lvl_DB=dist_grade_lvl_DB.rename(columns=rename_col).drop(columns=['Intervention','On Watch','Urgent Intervention'])
		dist_grade_lvl_DB=dist_grade_lvl_DB.rename(index={'All':"District"})
			
		
		SELDB_race=pd.concat([SELDB_race, dist_grade_lvl_DB],axis=1)
		SELDB_race=SELDB_race.rename(columns={'District':"SEL_DB ALL "+vs_obj.terms[0]})
		
		supesGoalsTab.append(SELDB_race)
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
			
			idx_rename = {'All':'District'} 
			GrdLvlSubgps = GrdLvlSubgps.rename(index=idx_rename)
			
			GrdLvlSubgps['Y %']=GrdLvlSubgps['Y %'].astype(str)+"%"
			GrdLvlSubgps['Y %']=GrdLvlSubgps['Y %'].replace(to_replace="-100.0%",value="*").replace(to_replace="nan%",value="").replace(to_replace="-1%", value="*").replace(to_replace="%",value="")
			GrdLvlSubgps=GrdLvlSubgps.reset_index()
			
			GrdLvlSubgps=GrdLvlSubgps[GrdLvlSubgps[column.name]=='Y']
			
			GrdLvlSubgps=GrdLvlSubgps[['Grade Level', column.name, 'Y %']]
			GrdLvlSubgps=GrdLvlSubgps.rename(columns={'Y %':vs_obj.assessment_type+"_"+vs_obj.subjects[0]+" "+column.name+" "+vs_obj.terms[0],'Grade Level_y':'Grade Level'})
			supesGoalsTab.append(GrdLvlSubgps)



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

