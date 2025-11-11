# pip install simple_salesforce
# pip install xlsxwriter

from simple_salesforce import Salesforce
import pandas as pd
import numpy as np
from datetime import datetime as dt
import datetime
import os

startTime = dt.now()
print(startTime)

##----------------------------------------------------------USER DEFINED PARAMETERS----------------------------------------------------------
path = 'Y:\\OLD DATA\\Python KPI Extraction\\AIL 2025\\Nov 25 10112025\\'
#path = 'C:\\Users\\PAGARAX1\\OneDrive - Abbott\\Documents\\SFA scripts archive\KPI AIL\\test2\\'


user = os.getlogin()
if user == "SHINASX":
    path = 'Y:\\Abbworld Data\\OLD DATA\\Python KPI Extraction\\Nov 24 18112024\\'

sopm = datetime.date(2025,11,1) #format --> yyyy-mm-dd
eopm = datetime.date(2025,11,30) #format --> yyyy-mm-dd
companyCode = [1757]

sf = Salesforce(username= 'aditya.pagare@abbott.com', password= 'Abbott@mumbai2512', security_token='')
##-------------------------------------------------------------------------------------------------------------------------------------------

currentDate = dt.now().date()
companyCode = "'" + "','".join(map(str, companyCode)) + "'"

def fetchData(q):
    df = sf.query_all(q)
    df = pd.json_normalize(df['records'])

    df = df.loc[:,~df.columns.str.contains('attributes.type')].reset_index()
    df = df.loc[:,~df.columns.str.contains('attributes.url')].reset_index()

    df = df.drop(['level_0', 'index'], axis=1)
    return df

##########################Fetching Daily Work Summary from Abbworld####################################
query = """SELECT 
USER__R.DIVISION,
TERRITORY_CODE__C,
owner.alias,
USER__R.name,
DATE__C,
DCR_FILED_DATE__C,
ACTIVITY_SELECTION__C,
mtpDay__c,
Day_Duration__c,
ACTIVITY1__r.name,
ACTIVITY2__r.name,
Doctors_Planned__c,
Doctor_Count__c,
Status__c 
FROM DCR__c 
WHERE USER__R.CompanyName IN (""" + str(companyCode) + """) AND Date__c >= """ + str(sopm) + """ AND Date__c <= """ + str(eopm)

dailyWork = fetchData(query)

dailyWork = dailyWork.rename(columns={'User__r.Division':'Division',
                                      'Territory_Code__c':'Territory Code',
                                      'Owner.Alias':'DCR: Owner Alias',
                                      'User__r.Name':'User',
                                      'Date__c':'Date',
                                      'DCR_Filed_Date__c':'Filed Date',
                                      'Activity_Selection__c':'Activity Selection',
                                      'mtpDay__c':'Day',
                                      'Day_Duration__c':'Day Duration',
                                      'Activity1__r.Name':'Activity1',
                                      'Activity2__r.Name':'Activity2',
                                      'Doctors_Planned__c':'Doctors Planned',
                                      'Doctor_Count__c':'Doctor Calls',
                                      'Status__c':'Status'})

dailyWork = dailyWork.drop(['Activity2__r'], axis=1)
dailyWork['Doctors Planned'] = dailyWork['Doctors Planned'].fillna(0)
dailyWork[['DCR: Owner Alias', 'Doctor Calls', 'Doctors Planned']] = dailyWork[['DCR: Owner Alias', 'Doctor Calls', 'Doctors Planned']].astype(int)

dailyWork = dailyWork[['Division', 'User', 'Territory Code', 'DCR: Owner Alias', 'Date', 'Filed Date', 'Activity Selection', 'Day', 'Day Duration', 'Activity1', 'Activity2', 'Doctors Planned', 'Doctor Calls', 'Status']]
dailyWork.to_csv(path + "Daily Work Summary Report - " + format(sopm,'%b %Y') + ".csv", index=False)
print('Daily Work Summary')

##########################Fetching Holiday Master from Abbworld####################################
query = """SELECT 
NAME,
RECORDTYPE.Name,
COMPANY_CODE__C,
DATE__C,
DIVISION__C,
STATE__r.Name,
YEAR__C,
User__r.Alias
FROM Holiday_Master__c 
WHERE COMPANY_CODE__C IN (""" + str(companyCode) + """) AND YEAR__C = '""" + str(sopm.year) + """'"""


holidayMaster = fetchData(query)

holidayMaster = holidayMaster.rename(columns={'Date__c':'Date',
                                              'Year__c':'Year',
                                              'State__r.Name':'State_Name',
                                              'Division__c':'Division',
                                              'Company_Code__c':'Company Code',
                                              'User__r':'User__r.Alias'})
holidayMaster = holidayMaster[['Name', 'RecordType.Name', 'Company Code', 'Date', 'Division', 'State_Name', 'Year', 'User__r.Alias']]
holidayMaster.to_csv(path + "Holiday Master - " + format(sopm,'%b %Y') + ".csv", index=False)
print('Holiday Master')

##########################Fetching Active User from Abbworld####################################
query = """Select 
Territory__c, 
User__r.Name, 
User__r.Alias, 
User__r.HQ__c, 
User__r.Designation__c, 
User__r.Abbott_Designation__c, 
User__r.Division_Name__c, 
User__r.Division, 
User__r.Expense_Designation__c, 
User__r.Start_Date__c, 
User__r.IsActive, 
User__r.Last_Submitted_DCR_Date__c 
FROM Target__c 
WHERE Company_Code__c IN (""" + str(companyCode) + """) AND 
User__r.Designation__c in ('ABM','ZBM','TBM') AND 
User__r.IsActive = TRUE AND 
User__r.Start_Date__c <= """ + str(eopm)

activeUser = fetchData(query)

activeUser = activeUser.rename(columns={'User__r.Division_Name__c':'Division Name',
                                        'Territory__c':'Territory',
                                        'User__r.Name':'Full Name',
                                        'User__r.Designation__c':'Designation',
                                        'User__r.Division':'Division',
                                        'User__r.Abbott_Designation__c':'Abbott Designation',
                                        'User__r.Last_Submitted_DCR_Date__c':'Last Submitted DCR Date',
                                        'User__r.Start_Date__c':'DOJ',
                                        'Profile.Name':'Name',
                                        'User__r.HQ__c':'Territory Headquarter',
                                        'User__r.IsActive':'Active',
                                        'User__r.Alias':'Employee Code',
                                        'User__r.Expense_Designation__c':'Expense Designation'})

activeUser['Last Submitted DCR Date'] = pd.to_datetime(activeUser['Last Submitted DCR Date']).dt.strftime('%d %B %Y')

activeUser = activeUser[['Territory', 'Full Name', 'Employee Code', 'Territory Headquarter', 'Designation', 'Abbott Designation', 'Division Name', 'Division', 'Expense Designation', 'DOJ', 'Active', 'Last Submitted DCR Date']]
activeUser.to_csv(path + "User Details - " + format(sopm,'%b %Y') + ".csv", index=False)
print('Active User')

##########################Fetching data for DCR Junction####################################
query = """SELECT 
DCR__R.USER__R.DIVISION,
DCR__R.USER__R.ALIAS,
DCR__R.DATE__C,
Account__c,
ASSIGNMENT__R.FREQUENCY__C,
ASSIGNMENT__R.SPECIALITY__C,
ASSIGNMENT__R.BRAND1__C,
DCR__R.STATUS__C,
DCR__R.DCR_FILED_DATE__C,
DCR__R.Call_Days__c
 
FROM DCR_Junction__c
WHERE DCR__r.Date__c >= """ + str(sopm) + """ AND 
DCR__r.Date__c <= """ + str(eopm) + """ AND 
DCR__r.USER__R.CompanyName IN (""" + str(companyCode) + """) AND 
DCR__R.STATUS__C = 'Submitted'"""

dcr_data = fetchData(query)

dcr_data = dcr_data.rename(columns={'DCR__r.User__r.Division':'Division',
                                    'DCR__r.User__r.Alias':'Employee Code',
                                    'DCR__r.Date__c':'Date',
                                    'Account__c':'Account',
                                    'Assignment__r.Frequency__c':'Assignment Frequency',
                                    'Assignment__r.Speciality__c':'Specialty By Practice',
                                    'Assignment__r.Brand1__c':'Brand 1',
                                    'DCR__r.Status__c':'DCR Status',
                                    'DCR__r.DCR_Filed_Date__c':'DCR Filed Date'})

dcr_data.rename(columns={'DCR__r.Call_Days__c':'Call Days'}, inplace=True)

dcr_data = dcr_data[~dcr_data['Brand 1'].isnull()]

dcr_data = dcr_data[['Division', 'Employee Code', 'Date', 'DCR Status', 'DCR Filed Date', 'Account', 'Assignment Frequency','Specialty By Practice', 'Brand 1','Call Days']]

dcr_data.to_csv(path + "DCR Junction - " + format(sopm,'%b %Y') + ".csv", index=False)
print('DCR Junction')

##########################Fetching Leave Data from Abbworld####################################
if currentDate.month == 1:
    currentYear = currentDate.year - 1
else:
    currentYear = currentDate.year

query = """SELECT 
Division__c,
User__r.Division_Name__c,
User__r.alias,
User__r.name,
User__r.isActive,
User__r.Start_Date__c,
User__r.CompanyName,
Leave_Balance__r.Year__c,
Applied_On__c,
Leave_Type__c,
From_Date__c,
To_Date__c,
Status__c,
Total_Number_of_Days__c 
FROM Leave_Request__c 
WHERE User__r.CompanyName IN (""" + str(companyCode) + """) AND User__r.isActive = True AND
(CALENDAR_YEAR(From_Date__c) = """ + str(currentYear) + """ OR CALENDAR_YEAR(To_Date__c) = """ + str(currentYear) + """)"""

LeaveDetails = fetchData(query)

LeaveDetails = LeaveDetails.rename(columns={'Division__c':'Division',
                                            'User__r.Division_Name__c':'Division Name',
                                            'User__r.Alias':'Employee Code',
                                            'User__r.Name':'Full Name',
                                            'User__r.IsActive':'Active',
                                            'User__r.Start_Date__c':'Joining Date',
                                            'User__r.CompanyName':'Company Name',
                                            'Leave_Balance__r.Year__c':'Year',
                                            'Applied_On__c':'Applied Date',
                                            'Leave_Type__c':'Leave Type',
                                            'From_Date__c':'From Date',
                                            'To_Date__c':'To Date',
                                            'Status__c':'Status',
                                            'Total_Number_of_Days__c':'Total No. of Days'})

query = """SELECT 
City__r.name,
Company_Code__c,
Name,
Other_States__c,
State__r.name,
Target__r.user__r.alias,
Target__r.user__r.name 
FROM Territory_States__c 
WHERE Company_Code__c IN (""" + str(companyCode) + """)"""

userDetails = fetchData(query)
userDetails = userDetails.drop(['City__r', 'Target__r.User__r'], axis=1)

userDetails = userDetails.rename(columns={'Target__r.User__r.Alias':'Employee Code',
                                          'State__r.Name':'State'})

LeaveDetails = pd.merge(left=LeaveDetails, right=userDetails[['Employee Code','State']].drop_duplicates(), on='Employee Code', how='left')

LeaveDetails['From Date'] =  pd.to_datetime(LeaveDetails['From Date'], format='%Y-%m-%d')
LeaveDetails['To Date'] =  pd.to_datetime(LeaveDetails['To Date'], format='%Y-%m-%d')
LeaveDetails['Total No. of Days'] = LeaveDetails['Total No. of Days'].astype(int)

LeaveDetails = LeaveDetails[['Employee Code', 'Division', 'Division Name', 'Full Name', 'Active', 'Joining Date', 'Company Name', 'Year', 'Applied Date', 'Leave Type', 'From Date', 'To Date', 'Status', 'Total No. of Days', 'State']]
LeaveDetails.to_csv(path + "Leave Details - " + format(sopm,'%b %Y') + ".csv", index=False)
print('Leave Data')

###############################Fetching MTP Data from Abbworld for HCS###########################################
if '1758' in companyCode:
    query = """SELECT 
    MTP_Cycle__r.Target__r.User__r.Alias,
    MTP_Cycle__r.MTP_Junction_Count__c,
    MTP_Cycle__r.Date__c 
    FROM MTP_Junction__c 
    WHERE MTP_Cycle__r.Date__c >= """ + str(sopm) + """ AND MTP_Cycle__r.Date__c <= """ + str(eopm) + """ AND 
    MTP_Cycle__r.Target__r.Company_Code__c = '1758' AND 
    MTP_Cycle__r.status__c = 'Approved' AND 
    MTP_Cycle__r.Target__r.User__r.Designation__c in ('ABM','ZBM','TBM') AND 
    Assignment__r.Customer_Type__c = 'Doctor'"""
    
    MTPData = fetchData(query)
    
    MTPData = MTPData.rename(columns={'MTP_Cycle__r.MTP_Junction_Count__c':'Planned Calls',
                                      'MTP_Cycle__r.Target__r.User__r.Alias':'Employee Code',
                                      'MTP_Cycle__r.Date__c':'Date'})
    
    MTPData_unique = MTPData.drop_duplicates(subset = ['Employee Code', 'Date'], keep = 'last').reset_index(drop = True)
    MTPData_unique.to_csv(path + "MTP Details - " + format(sopm,'%b %Y') + ".csv", index=False)
    
    if len(MTPData) > 0:
        MTPData_summaryCalls = pd.DataFrame(MTPData.groupby(['Employee Code'])['Employee Code'].size())
    
        MTPData_summaryCalls = MTPData_summaryCalls.rename(columns={'Employee Code':'Doctors Planned'})
        MTPData_summaryCalls.reset_index(inplace=True)
        MTPData_summaryCalls['Employee Code'] = MTPData_summaryCalls['Employee Code'].astype(int)
    else:
        MTPData_summaryCalls = pd.DataFrame()

    print('MTP Data')
    
##########################Fetching Assignment Dump from Abbworld####################################
query = """SELECT 
ID,
NAME,
TERRITORY_CODE__C,
TARGET__R.USER__R.ALIAS,
TARGET__R.USER__R.DIVISION,
TARGET__R.COMPANY_CODE__C,
ACCOUNT__C,
BRAND1__C,
EFFECTIVE_DATE__C,
DEACTIVATION_DATE__C,
FREQUENCY__C,
STATUS__C,
TODAY_STATUS__C,
Speciality__c
FROM Assignment__c
WHERE TARGET__R.COMPANY_CODE__C IN (""" + str(companyCode) + """) AND 
Customer_Type__c = 'Doctor' AND 
EFFECTIVE_DATE__C <= """ + str(eopm) + """ AND 
(DEACTIVATION_DATE__C = null OR DEACTIVATION_DATE__C >= """ + str(eopm) + """)"""

Assg_data = fetchData(query)

Assg_data = Assg_data.rename(columns={'Target__r.User__r.Alias':'Employee Code',
                                      'Target__r.User__r.Division':'Division',
                                      'Target__r.Company_Code__c':'Company Code',
                                      'Account__c':'Account',
                                      'Brand1__c':'Brand 1',
                                      'Effective_Date__c':'Effective Date',
                                      'Deactivation_Date__c':'Deactivation Date',
                                      'Frequency__c':'Frequency',
                                      'Status__c':'Status',
                                      'Today_Status__c':'Today''s status',
                                      'Territory_Code__c':'Territory Code',
                                      'Speciality__c':'Specialty By Practice'})

Assg_data = Assg_data[['Id', 'Name', 'Territory Code', 'Employee Code', 'Division', 'Company Code', 'Account', 'Brand 1', 'Effective Date', 'Deactivation Date', 'Frequency', 'Status', 'Today''s status','Specialty By Practice']]
Assg_data.to_csv(path + "Assignment Status - " + format(sopm,'%b %Y') + ".csv", index=False)
print('Assignment Dump')

##Data manipulation and pivot creation on Daily work Summary report
dailyWork = dailyWork[dailyWork['Status'] != 'Saved']

Summary_calls = pd.DataFrame(dailyWork.groupby(['DCR: Owner Alias'])[['Doctor Calls', 'Doctors Planned']].apply(lambda x : x.astype(int).sum())).reset_index()
Summary_calls['DCR: Owner Alias'] = Summary_calls['DCR: Owner Alias'].astype(int)

Summary_calls = Summary_calls.rename(columns={'DCR: Owner Alias':'Employee Code'})

dailyWork.loc[(dailyWork['Activity1'].notnull()) & (dailyWork['Activity2'].isnull()) , 'Day Duration'] = 1.0
dailyWork.loc[(dailyWork['Activity1'].notnull()) & (dailyWork['Activity2'].notnull()), 'Day Duration'] = 0.5

dailyWork_temp = dailyWork[dailyWork['Day Duration'] == 0.5]

dailyWork_temp['Activity1'] = dailyWork_temp['Activity2']
dailyWork_temp['Activity2'] = ''

#dailyWork = dailyWork.append(dailyWork_temp)

dailyWork = pd.concat([dailyWork,dailyWork_temp])

dailyWork = dailyWork.drop(['Activity2','Doctors Planned', 'Doctor Calls', 'Status'], axis=1)

dailyWork_summary = dailyWork.pivot_table(index='DCR: Owner Alias', columns='Activity1', values='Day Duration', aggfunc = sum).fillna(0).reset_index()
dailyWork_summary = dailyWork_summary.rename(columns={'DCR: Owner Alias':'Employee Code'})

query = """SELECT 
Name,
Start_Date__c,
Expiration_Date__c,
Type__c,
Active__c
FROM Activity_Master__c
WHERE Active__c = 'True' OR (Active__c = 'False' AND Expiration_Date__c >= """ + str(sopm) + """ AND Expiration_Date__c <= """ + str(eopm) + """)"""

Activities = fetchData(query)

colList = Activities['Name'].tolist()

for colName in colList:
    if colName not in dailyWork_summary.columns:
        dailyWork_summary[colName] = 0

##Data manipulation and pivot creation on Leave request report
LeaveDetails = LeaveDetails[~LeaveDetails['Leave Type'].isin(['Comp Off', 'Leave Without Pay', 'Unauthorized absence'])].reset_index(drop=True)

#LeaveDetails = LeaveDetails[~LeaveDetails['Leave Type'].isin(['Comp Off', 'Unauthorized absence'])].reset_index(drop=True)
LeaveDetails = LeaveDetails[LeaveDetails['Status'].isin(['Approved', 'HR Applied', 'Manager Applied'])].reset_index(drop=True)


sDate = datetime.datetime.strptime("01-01-2018", "%d-%m-%Y")
eDate = datetime.datetime.strptime("31-12-2025", "%d-%m-%Y")
days = abs((eDate - sDate).days)+1

dateVector = pd.date_range(sDate, periods=days)
monthVector = pd.date_range(sopm, periods=abs((eopm - sopm).days)+1)

holidayMaster['key'] = holidayMaster['Division'] + " - " + holidayMaster['State_Name']
LeaveDetails['key'] = LeaveDetails['Division'] + " - " + LeaveDetails['State']

uniqueCodes = list(holidayMaster['key'].unique())

holidayList = {}
for code in uniqueCodes:
    tempList = holidayMaster.query("key == '" + str(code) + "'")['Date'].tolist()
    tempList = sorted(tempList)
    holidayList[code] = tempList

for i in range(len(LeaveDetails)):
    if int(LeaveDetails['From Date'][i].month) < int(sopm.month) and int(LeaveDetails['To Date'][i].month) < int(sopm.month):
        LeaveDetails['Total No. of Days'][i] = 0
    elif int(LeaveDetails['From Date'][i].month) > int(sopm.month) and int(LeaveDetails['To Date'][i].month) > int(sopm.month):
        LeaveDetails['Total No. of Days'][i] = 0
    else:
        finalVector = dateVector.difference(monthVector) #Vector which has all dates from 1st Jan 2018 to 31st Dec 2025, except the dates of current month
        
        if isinstance(pd.to_datetime(holidayList.get(LeaveDetails['key'][i])), type(None)):
            tempHolidayVector = []
            holidayVector = pd.to_datetime(tempHolidayVector)
        else:
            holidayVector = pd.to_datetime(holidayList.get(LeaveDetails['key'][i]))
        
        finalVector = finalVector.append(holidayVector)#Vector which has all dates from 1st Jan 2018 to 31st Dec 2025 and all holidays in current month including 3rd saturday
        
        fromDate = LeaveDetails['From Date'][i]
        toDate = LeaveDetails['To Date'][i]
        totalDays = abs((toDate - fromDate).days)+1
        possible_days = pd.date_range(fromDate, periods=totalDays)
                
        possible_days = possible_days.difference(finalVector)
        possible_days = possible_days.where(possible_days.weekday < 6)
        
        possible_days_final = pd.date_range(fromDate, periods=0)
        for p in possible_days:
            if not isinstance(p,type(pd.NaT)):
                tempPossibleDay = pd.date_range(p, periods=1)
                possible_days_final = possible_days_final.append(tempPossibleDay)
                
        LeaveDetails.loc[i,'Total No. of Days'] = len(possible_days_final)

leaveType = list(LeaveDetails['Leave Type'].unique())
for c in leaveType:
    LeaveDetails.loc[(LeaveDetails['Leave Type'] == c), c] = LeaveDetails['Total No. of Days']

LeaveDetails[leaveType] = LeaveDetails[leaveType].fillna(0)

Leave_Summary = pd.crosstab(index=LeaveDetails['Employee Code'], 
                            columns=LeaveDetails['Leave Type'], 
                            values=LeaveDetails['Total No. of Days'],
                            aggfunc='sum').fillna(0)

Leave_Summary[leaveType] = Leave_Summary[leaveType].astype(int)
Leave_Summary['Total No. of Days'] = Leave_Summary.sum(axis=1)
Leave_Summary = Leave_Summary.reset_index()

Leave_Summary['Employee Code'] = Leave_Summary['Employee Code'].astype(int)

LeaveDetails.to_csv(path + "KPI Leave Details - " + format(sopm,'%b %Y') + ".csv", index=False)
print('Leave Details')

##Data manipulation and pivot creation on DCR- 1pc,2pc,3pc and 4pc
summary_dcr = dcr_data.groupby(['Employee Code','Account','Assignment Frequency'],as_index=False)['Date'].nunique()

summary_dcr = summary_dcr.rename(columns={'Employee Code':'Employee_Code',
                                          'Assignment Frequency':'Assg_Freq'})

summary_dcr['Assg_Freq'] = pd.to_numeric(summary_dcr['Assg_Freq'])
summary_dcr['Date'] = pd.to_numeric(summary_dcr['Date'])


summary_dcr_total = summary_dcr.groupby(['Employee_Code'],as_index=False)['Account'].count()
summary_dcr_total = summary_dcr_total.rename(columns={'Employee_Code':'Employee Code'})

##### Assg_Freq ####

#Assg_Freq col to be removed from summary_dcr for Total Coverage calculation

summary_dcr_new = dcr_data.groupby(['Employee Code','Account'],as_index=False)['Date'].nunique()

summary_dcr_new = summary_dcr_new.rename(columns={'Employee Code':'Employee_Code'})

summary_dcr_new['Date'] = pd.to_numeric(summary_dcr_new['Date'])


summary_dcr_total_new = summary_dcr_new.groupby(['Employee_Code'],as_index=False)['Account'].count()
summary_dcr_total_new = summary_dcr_total_new.rename(columns={'Employee_Code':'Employee Code'})


####################

dcr_1pc = summary_dcr.query("(Assg_Freq==1) & (Date >= 1)")
dcr_2pc = summary_dcr.query("(Assg_Freq==2) & (Date >= 2)")
dcr_3pc = summary_dcr.query("(Assg_Freq==3) & (Date >= 3)")
dcr_4pc = summary_dcr.query("(Assg_Freq==4) & (Date >= 4)")

dcr_1pc=dcr_1pc.groupby(['Employee_Code'],as_index=False)['Account'].nunique()
dcr_1pc = dcr_1pc.rename(columns={'Employee_Code':'Employee Code'})

dcr_2pc=dcr_2pc.groupby(['Employee_Code'],as_index=False)['Account'].nunique()
dcr_2pc = dcr_2pc.rename(columns={'Employee_Code':'Employee Code'})

#Just simple 2 PC Cov (not Freq Cov)
dcr_2pc_met = summary_dcr.query("(Assg_Freq==2) & (Date >= 1)")
dcr_2pc_met = dcr_2pc_met.groupby(['Employee_Code'],as_index=False)['Account'].nunique()
dcr_2pc_met = dcr_2pc_met.rename(columns={'Employee_Code':'Employee Code'})


dcr_3pc=dcr_3pc.groupby(['Employee_Code'],as_index=False)['Account'].nunique()
dcr_3pc = dcr_3pc.rename(columns={'Employee_Code':'Employee Code'})

dcr_4pc=dcr_4pc.groupby(['Employee_Code'],as_index=False)['Account'].nunique()
dcr_4pc = dcr_4pc.rename(columns={'Employee_Code':'Employee Code'})

##Data manipulation and pivot creation on Assignment data
Assg_data = Assg_data[Assg_data['Employee Code'].notnull()]
summary_assg = pd.crosstab(index=Assg_data['Employee Code'], columns = Assg_data['Frequency'])

#Incase a column isnt present, put a dummy column, with all 0's in it
if '1' not in summary_assg.columns:
  summary_assg['1'] = 0
if '2' not in summary_assg.columns:
  summary_assg['2'] = 0
if '3' not in summary_assg.columns:
  summary_assg['3'] = 0
if '4' not in summary_assg.columns:
  summary_assg['4'] = 0
summary_assg['Total'] =  summary_assg.loc[:,('1', '2','3','4')].sum(axis=1)

summary_assg = summary_assg.reset_index()

##Putting all TBM summaries in 1 data frame
active_user_TBM = activeUser[activeUser['Designation'] == 'TBM']

final_KPI = active_user_TBM[["Division","Employee Code","Full Name","Territory Headquarter","Designation","DOJ","Territory","Last Submitted DCR Date"]]
final_KPI = final_KPI.rename(columns={'Designation':'Abbott Designation'})

final_KPI['Status'] = 'Active'

final_KPI = pd.merge(left=final_KPI, right=dailyWork_summary[['Employee Code','Field Work']].astype(str), on='Employee Code', how='left')
final_KPI = final_KPI.rename(columns={'Field Work':'Call Days'})

if '1758' in companyCode:
    SummaryCallsMerged = pd.concat([Summary_calls,MTPData_summaryCalls],axis=0)
else:
    SummaryCallsMerged = Summary_calls
    
final_KPI = pd.merge(left=final_KPI, right=SummaryCallsMerged[["Employee Code", "Doctors Planned","Doctor Calls"]].astype(str), on="Employee Code", how='left')

final_KPI = final_KPI.rename(columns={'Doctors Planned':'Plan DR Calls'})
final_KPI['Plan DR Calls'] = final_KPI['Plan DR Calls'].fillna(0)

final_KPI = final_KPI.rename(columns={'Doctor Calls':'Actual DR Calls'})

final_KPI['Doctor Call Avg'] = 0.00
final_KPI['Doctor Call Avg'] = round(final_KPI['Actual DR Calls'].astype(float)/final_KPI['Call Days'].astype(float),2)
final_KPI['Doctor Call Avg'] = final_KPI['Doctor Call Avg'].fillna(0.00)

final_KPI = pd.merge(left=final_KPI, right=summary_assg, on="Employee Code", how='left')
final_KPI = final_KPI.rename(columns={'1':'1PC DR Total',
                                      '2':'2PC DR Total',
                                      '3':'3PC DR Total',
                                      '4':'4PC DR Total',
                                      'Total':'Total DR Total'})

# 1/2/3/4 PC Frequency Met
final_KPI = pd.merge(final_KPI,dcr_1pc, on='Employee Code', how='left').fillna(0)
final_KPI = final_KPI.rename(columns={'Account':'1PC Freq Met'})
final_KPI['1PC Freq Met'] = np.where(final_KPI['1PC Freq Met'].astype(float) > final_KPI['1PC DR Total'].astype(float), final_KPI['1PC DR Total'], final_KPI['1PC Freq Met'])

final_KPI = pd.merge(final_KPI,dcr_2pc, on='Employee Code', how = 'left').fillna(0)
final_KPI = final_KPI.rename(columns = {'Account':'2PC Freq Met'})
final_KPI['2PC Freq Met'] = np.where(final_KPI['2PC Freq Met'].astype(float) > final_KPI['2PC DR Total'].astype(float), final_KPI['2PC DR Total'], final_KPI['2PC Freq Met'])

final_KPI = pd.merge(final_KPI,dcr_2pc_met , on='Employee Code', how = 'left').fillna(0)
final_KPI = final_KPI.rename(columns = {'Account':'2PC Cov'})
final_KPI['2PC Cov'] = np.where(final_KPI['2PC Cov'].astype(float) > final_KPI['2PC DR Total'].astype(float), final_KPI['2PC DR Total'], final_KPI['2PC Cov'])


final_KPI = pd.merge(final_KPI,dcr_3pc, on='Employee Code', how = 'left').fillna(0)
final_KPI = final_KPI.rename(columns = {'Account':'3PC Freq Met'})
final_KPI['3PC Freq Met'] = np.where(final_KPI['3PC Freq Met'].astype(float) > final_KPI['3PC DR Total'].astype(float), final_KPI['3PC DR Total'], final_KPI['3PC Freq Met'])

final_KPI = pd.merge(final_KPI,dcr_4pc, on='Employee Code', how = 'left').fillna(0)
final_KPI = final_KPI.rename(columns = {'Account':'4PC Freq Met'})
final_KPI['4PC Freq Met'] = np.where(final_KPI['4PC Freq Met'].astype(float) > final_KPI['4PC DR Total'].astype(float), final_KPI['4PC DR Total'], final_KPI['4PC Freq Met'])

#1/2/3/4 PC Freq Cov
final_KPI['1PC Freq Cov %'] = round((final_KPI['1PC Freq Met']/final_KPI["1PC DR Total"])*100,2)
final_KPI.loc[~np.isfinite(final_KPI['1PC Freq Cov %']), '1PC Freq Cov %'] = np.nan

final_KPI['2PC Freq Cov %'] = round((final_KPI['2PC Freq Met']/final_KPI["2PC DR Total"])*100,2)
final_KPI.loc[~np.isfinite(final_KPI['2PC Freq Cov %']), '2PC Freq Cov %'] = np.nan

final_KPI['2PC Cov %'] = round((final_KPI['2PC Cov']/final_KPI["2PC DR Total"])*100,2)
final_KPI.loc[~np.isfinite(final_KPI['2PC Cov %']), '2PC Cov %'] = np.nan

final_KPI['3PC Freq Cov %'] = round((final_KPI['3PC Freq Met']/final_KPI["3PC DR Total"])*100,2)
final_KPI.loc[~np.isfinite(final_KPI['3PC Freq Cov %']), '3PC Freq Cov %'] = np.nan

final_KPI['4PC Freq Cov %'] = round((final_KPI['4PC Freq Met']/final_KPI["4PC DR Total"])*100,2)
final_KPI.loc[~np.isfinite(final_KPI['4PC Freq Cov %']), '4PC Freq Cov %'] = np.nan

final_KPI = pd.merge(final_KPI,summary_dcr_total, on='Employee Code', how = 'left')
final_KPI = final_KPI.rename(columns = {'Account':'Total DR Visited'})

final_KPI['Total DR Visited'] = np.where(final_KPI['Total DR Visited'].astype(float) > final_KPI['Total DR Total'].astype(float), final_KPI['Total DR Total'], final_KPI['Total DR Visited'])
final_KPI['Total DR Visited'] = final_KPI['Total DR Visited'].fillna(0)
final_KPI['Total DR MIssed'] = final_KPI["Total DR Total"].astype(float) - final_KPI["Total DR Visited"].astype(float)
final_KPI['Total DR Cov %'] = round((final_KPI["Total DR Visited"].astype(float)/final_KPI["Total DR Total"].astype(float))*100,2)
final_KPI['Total DR Cov %'] = final_KPI['Total DR Cov %'].fillna(0)

final_KPI = pd.merge(left=final_KPI, right=Leave_Summary.astype(str), on="Employee Code", how='left')
final_KPI = final_KPI.rename(columns={'Total No. of Days':'Leaves'})

ListForMerging = colList.copy()
ListForMerging.append('Employee Code')

final_KPI = pd.merge(left=final_KPI, right=dailyWork_summary[ListForMerging].astype(str), on="Employee Code", how='left')

cols_to_sum1 = colList.copy()
cols_to_sum1.append('Leaves')

for c in cols_to_sum1:
    final_KPI[c] = final_KPI[c].astype(float)

final_KPI['Call Days'] = final_KPI['Call Days'].astype(float)

final_KPI['Total Days'] = np.where(final_KPI['Leaves'].astype(float) >= 0, final_KPI[cols_to_sum1].sum(axis=1), np.NaN)
final_KPI[leaveType] = final_KPI[leaveType].astype(float)
final_KPI = final_KPI.fillna(0)

# 300925

##Putting all ABM summaries in 1 data frame
active_user_ABM = activeUser[activeUser['Designation'] == 'ABM']

final_KPI_ABM = active_user_ABM[["Division","Employee Code","Full Name", "Territory Headquarter","Designation","DOJ","Territory", "Last Submitted DCR Date"]]
final_KPI_ABM = final_KPI_ABM.rename(columns={'Designation':'Abbott Designation'})

final_KPI_ABM['Status'] = 'Active'

final_KPI_ABM = pd.merge(left=final_KPI_ABM, right=dailyWork_summary[['Employee Code','Field Work']].astype(str), on='Employee Code', how='left')
final_KPI_ABM = final_KPI_ABM.rename(columns={'Field Work':'Call Days'})

final_KPI_ABM = pd.merge(left=final_KPI_ABM, right=SummaryCallsMerged[["Employee Code", "Doctors Planned","Doctor Calls"]].astype(str), on="Employee Code", how='left')

final_KPI_ABM = final_KPI_ABM.rename(columns={'Doctors Planned':'Plan DR Calls'})
final_KPI_ABM['Plan DR Calls'] = final_KPI_ABM['Plan DR Calls'].fillna(0)

final_KPI_ABM = final_KPI_ABM.rename(columns={'Doctor Calls':'Actual DR Calls'})

final_KPI_ABM['Doctor Call Avg'] = 0.00
final_KPI_ABM['Doctor Call Avg'] = round(final_KPI_ABM['Actual DR Calls'].astype(float)/final_KPI_ABM['Call Days'].astype(float),2)
final_KPI_ABM['Doctor Call Avg'] = final_KPI_ABM['Doctor Call Avg'].fillna(0.00)

final_KPI_ABM = pd.merge(left=final_KPI_ABM, right=Leave_Summary.astype(str), on="Employee Code", how='left')
final_KPI_ABM = final_KPI_ABM.rename(columns={'Total No. of Days':'Leaves'})

ListForMerging = colList.copy()
ListForMerging.append('Employee Code')

final_KPI_ABM = pd.merge(left=final_KPI_ABM, right=dailyWork_summary[ListForMerging].astype(str), on="Employee Code", how='left')

for c in cols_to_sum1:
    final_KPI_ABM[c] = final_KPI_ABM[c].astype(float)

final_KPI_ABM['Call Days'] = final_KPI_ABM['Call Days'].astype(float)

final_KPI_ABM['Total Days'] = np.where(final_KPI_ABM['Leaves'].astype(float) >= 0, final_KPI_ABM[cols_to_sum1].sum(axis=1), np.NaN)
final_KPI_ABM = final_KPI_ABM.fillna(0)

##Putting all ZBM summaries in 1 data frame
active_user_ZBM = activeUser[activeUser['Designation'] == 'ZBM']

final_KPI_ZBM = active_user_ZBM[["Division","Employee Code","Full Name", "Territory Headquarter","Designation","DOJ","Territory", "Last Submitted DCR Date"]]
final_KPI_ZBM = final_KPI_ZBM.rename(columns={'Designation':'Abbott Designation'})

final_KPI_ZBM['Status'] = 'Active'

final_KPI_ZBM = pd.merge(left=final_KPI_ZBM, right=dailyWork_summary[['Employee Code','Field Work']].astype(str), on='Employee Code', how='left')
final_KPI_ZBM = final_KPI_ZBM.rename(columns={'Field Work':'Call Days'})

final_KPI_ZBM = pd.merge(left=final_KPI_ZBM, right=SummaryCallsMerged[["Employee Code", "Doctors Planned","Doctor Calls"]].astype(str), on="Employee Code", how='left')

final_KPI_ZBM = final_KPI_ZBM.rename(columns={'Doctors Planned':'Plan DR Calls'})
final_KPI_ZBM['Plan DR Calls'] = final_KPI_ZBM['Plan DR Calls'].fillna(0)

final_KPI_ZBM = final_KPI_ZBM.rename(columns={'Doctor Calls':'Actual DR Calls'})

final_KPI_ZBM['Doctor Call Avg'] = 0.00
final_KPI_ZBM['Doctor Call Avg'] = round(final_KPI_ZBM['Actual DR Calls'].astype(float)/final_KPI_ZBM['Call Days'].astype(float),2)
final_KPI_ZBM['Doctor Call Avg'] = final_KPI_ZBM['Doctor Call Avg'].fillna(0.00)

final_KPI_ZBM = pd.merge(left=final_KPI_ZBM, right=Leave_Summary.astype(str), on="Employee Code", how='left')
final_KPI_ZBM = final_KPI_ZBM.rename(columns={'Total No. of Days':'Leaves'})

ListForMerging = colList.copy()
ListForMerging.append('Employee Code')

final_KPI_ZBM = pd.merge(left=final_KPI_ZBM, right=dailyWork_summary[ListForMerging].astype(str), on="Employee Code", how='left')

for c in cols_to_sum1:
    final_KPI_ZBM[c] = final_KPI_ZBM[c].astype(float)

final_KPI_ZBM['Call Days'] = final_KPI_ZBM['Call Days'].astype(float)

final_KPI_ZBM['Total Days'] = np.where(final_KPI_ZBM['Leaves'].astype(float) >= 0, final_KPI_ZBM[cols_to_sum1].sum(axis=1), np.NaN)
final_KPI_ZBM = final_KPI_ZBM.fillna(0)

print('DCR- 1pc,2pc,3pc and 4pc')
############################################################################################################################################################################

raw_Data_cols = ['Employee Code','Division','Full Name','Territory Headquarter','Abbott Designation','DOJ','Territory','Last Submitted DCR Date','Status',
                       'Call Days','Plan DR Calls','Actual DR Calls','Doctor Call Avg','1PC DR Total','1PC Freq Met','1PC Freq Cov %','2PC DR Total','2PC Freq Met',
                       '2PC Freq Cov %','2PC Cov','2PC Cov %','3PC DR Total','3PC Freq Met','3PC Freq Cov %','4PC DR Total','4PC Freq Met','4PC Freq Cov %','Total DR Total',
                       'Total DR Visited','Total DR MIssed','Total DR Cov %','Leaves']

final_KPI_cols = raw_Data_cols + colList + leaveType + ['Total Days']

final_KPI = final_KPI[final_KPI_cols]
final_KPI.to_csv(path + "final_KPI - " + format(sopm,'%b %Y') + ".csv", index=False)


raw_Data_ABM_cols = ['Employee Code','Division','Full Name','Territory Headquarter','Abbott Designation','DOJ','Territory','Last Submitted DCR Date',
                               'Status','Call Days','Plan DR Calls','Actual DR Calls','Doctor Call Avg','Leaves']

final_KPI_ABM_cols = raw_Data_ABM_cols + colList + leaveType + ['Total Days']

final_KPI_ABM = final_KPI_ABM[final_KPI_ABM_cols]
final_KPI_ABM.to_csv(path + "final_KPI_ABM - " + format(sopm,'%b %Y') + ".csv", index=False)

raw_Data_ZBM_cols = ['Employee Code','Division','Full Name','Territory Headquarter','Abbott Designation','DOJ','Territory','Last Submitted DCR Date','Status',
                               'Call Days','Plan DR Calls','Actual DR Calls','Doctor Call Avg','Leaves']

final_KPI_ZBM_cols = raw_Data_ZBM_cols + colList + leaveType + ['Total Days']

final_KPI_ZBM = final_KPI_ZBM[final_KPI_ZBM_cols]
final_KPI_ZBM.to_csv(path + "final_KPI_ZBM - " + format(sopm,'%b %Y') + ".csv", index=False)

uniqueDivisions = final_KPI["Division"].unique().tolist()

for d in uniqueDivisions:
    dfKPI = final_KPI[final_KPI["Division"] == d]
    dfKPI_ABM = final_KPI_ABM[final_KPI_ABM["Division"] == d]
    dfKPI_ZBM = final_KPI_ZBM[final_KPI_ZBM["Division"] == d]
    
    with pd.ExcelWriter(path + 'KPI (' + str(d) + ') - ' + format(sopm, '%b %Y') + '.xlsx', engine='xlsxwriter') as writer:
        dfKPI.to_excel(writer, sheet_name='final_KPI_TBM', startrow=0, startcol=0, index=False)
        dfKPI_ABM.to_excel(writer, sheet_name='final_KPI_ABM', startrow=0, startcol=0, index=False)
        dfKPI_ZBM.to_excel(writer, sheet_name='final_KPI_ZBM', startrow=0, startcol=0, index=False)

with pd.ExcelWriter(path + 'KPI - ' + format(sopm, '%b %Y') + '.xlsx', engine='xlsxwriter') as writer:
    final_KPI.to_excel(writer, sheet_name='final_KPI_TBM', startrow=0, startcol=0, index=False)
    final_KPI_ABM.to_excel(writer, sheet_name='final_KPI_ABM', startrow=0, startcol=0, index=False)
    final_KPI_ZBM.to_excel(writer, sheet_name='final_KPI_ZBM', startrow=0, startcol=0, index=False)



###################  Adding New Summary #####################

final_KPI = final_KPI.drop_duplicates(subset='Employee Code', keep='last')

final_KPI_updated = final_KPI.copy()

final_KPI_updated = final_KPI_updated[final_KPI_updated['Division'] != 'NP']

user = os.getlogin()

if user == "CHAURNX2":
    path1 = "Y:\\DVL\\DVL extracted using python\\"
    Hierarchy_path = 'Z:\\'
elif user == "mithbix":
    path1 = "W:\\Abbworld Data\\DVL\\DVL extracted using python\\"
    Hierarchy_path = 'Y:\\'
elif user == "jadhaax3":
    path1 =  "Z:\\DVL\\DVL extracted using python\\"
    Hierarchy_path = 'X:\\'
elif user == "SHINASX":
    path1 = "Y:\\Abbworld Data\\DVL\\DVL extracted using python\\"
    Hierarchy_path = 'Z:\\'
else:
    path1 = "Z:\\DVL\\DVL extracted using python\\"
    Hierarchy_path = 'Y:\\'


Hierarchy_AIL = pd.read_excel(os.path.join(Hierarchy_path, 'Comex_AIL.xlsx'))
Hierarchy_APC = pd.read_excel(os.path.join(Hierarchy_path, 'Comex_Apc.xlsx'))
Hierarchy_ASC = pd.read_excel(os.path.join(Hierarchy_path, 'Comex_Asc.xlsx'))
Hierarchy = pd.concat([Hierarchy_AIL, Hierarchy_APC, Hierarchy_ASC])

def fetchData(q):
    df = sf.query_all(q)
    df = pd.json_normalize(df['records'])

    df = df.loc[:,~df.columns.str.contains('attributes.type')].reset_index()
    df = df.loc[:,~df.columns.str.contains('attributes.url')].reset_index()

    df = df.drop(['level_0', 'index'], axis=1)
    return df



query = """SELECT 
City__r.name,
Company_Code__c,
Name,
Other_States__c,
State__r.name,
Target__r.user__r.alias,
Target__r.Parent_Territory__c,
Target__r.user__r.name,
Target__r.user__r.Division,
Target__r.user__r.Division_Name__c,
Target__r.User__r.IsActive
FROM Territory_States__c 
WHERE Target__r.User__r.IsActive = TRUE
"""

userDetails = fetchData(query)
#userDetails = userDetails.drop(['City__r', 'Target__r.User__r'], axis=1)

userDetails = userDetails.rename(columns={'Target__r.User__r.Alias':'Employee Code',
                                          'State__r.Name':'State',
                                         'Target__r.Parent_Territory__c':'Parent Territory',
                                         'Name':'Territory',
                                         'Target__r.User__r.Name':'Employee Name',
                                         'Target__r.User__r.Division':'Division',
                                         'Target__r.User__r.Division_Name__c':'Division Name',
                                         'Target__r.User__r.IsActive':'Active'})

userDetails = userDetails[userDetails['Division Name'] != 'Nepal AIL']

userDetails = userDetails.loc[:,['Division','Territory','Parent Territory','Employee Code','Employee Name','Active','State']]

final_KPI_updated.rename(columns={'Territory':'Territory Code'}, inplace=True)

#ABM ZBM to be added from Hierarchy.columns

final_KPI_updated = final_KPI_updated.merge(Hierarchy[['EHIER_CD', 'PAR_EHIER_CD','PAR_EMPLOYEE_NAME']].drop_duplicates(), left_on='Territory Code', right_on='EHIER_CD', how='left')
final_KPI_updated.drop(columns="EHIER_CD", inplace=True)
final_KPI_updated.rename(columns={"PAR_EHIER_CD":f"Area Code",'PAR_EMPLOYEE_NAME':'ABM Name'}, inplace=True)

#From User Details
final_KPI_updated = final_KPI_updated.merge(userDetails[['Territory', 'Parent Territory']], left_on='Territory Code', right_on = 'Territory', how='left')
final_KPI_updated.drop(columns="Territory", inplace=True)
final_KPI_updated.rename(columns={"Parent Territory":f"Area Code UD"}, inplace=True)
# Update only the null values in 'ABM Name' with values from 'ABM Name_df2'
final_KPI_updated['Area Code'] = final_KPI_updated['Area Code'].fillna(final_KPI_updated['Area Code UD'])
final_KPI_updated.drop(columns="Area Code UD", inplace=True)

final_KPI_updated = final_KPI_updated.merge(Hierarchy[['EHIER_CD', 'PAR_EHIER_CD','PAR_EMPLOYEE_NAME']].drop_duplicates(), left_on='Area Code', right_on='EHIER_CD', how='left')
final_KPI_updated.drop(columns="EHIER_CD", inplace=True)
final_KPI_updated.rename(columns={"PAR_EHIER_CD":f"Zone Code",'PAR_EMPLOYEE_NAME':'ZBM Name'}, inplace=True)
final_KPI_updated.rename(columns={"User: Full Name":'TBM Name'}, inplace=True)

#From User Details
final_KPI_updated = final_KPI_updated.merge(userDetails[['Territory', 'Parent Territory']], left_on='Area Code', right_on = 'Territory', how='left')
final_KPI_updated.drop(columns="Territory", inplace=True)
final_KPI_updated.rename(columns={"Parent Territory":f"Zone Code UD"}, inplace=True)
# Update only the null values in 'ABM Name' with values from 'ABM Name_df2'
final_KPI_updated['Zone Code'] = final_KPI_updated['Zone Code'].fillna(final_KPI_updated['Zone Code UD'])
final_KPI_updated.drop(columns="Zone Code UD", inplace=True)

final_KPI_updated = final_KPI_updated.merge(userDetails[['Territory', 'Employee Name']], left_on='Zone Code', right_on = 'Territory', how='left')
final_KPI_updated.drop(columns="Territory", inplace=True)
final_KPI_updated.rename(columns={"Employee Name":"Zone Name UD"}, inplace=True)
final_KPI_updated['ZBM Name'] = final_KPI_updated['ZBM Name'].fillna(final_KPI_updated['Zone Name UD'])
final_KPI_updated.drop(columns="Zone Name UD", inplace=True)


final_KPI_updated = final_KPI_updated.merge(Hierarchy[['EHIER_CD', 'HQ']].drop_duplicates(), left_on='Zone Code', right_on='EHIER_CD', how='left')
final_KPI_updated.drop(columns="EHIER_CD", inplace=True)
final_KPI_updated.rename(columns={"HQ":'ZBM_HQ'}, inplace=True)

final_KPI_updated = final_KPI_updated.merge(Hierarchy[['EHIER_CD', 'PAR_EHIER_CD','PAR_EMPLOYEE_NAME']].drop_duplicates(), left_on='Zone Code', right_on='EHIER_CD', how='left')
final_KPI_updated.drop(columns="EHIER_CD", inplace=True)
final_KPI_updated.rename(columns={"PAR_EHIER_CD":f"NSM Code",'PAR_EMPLOYEE_NAME':'NSM Name'}, inplace=True)

#From User Details
final_KPI_updated = final_KPI_updated.merge(userDetails[['Territory', 'Parent Territory']], left_on='Zone Code', right_on = 'Territory', how='left')
final_KPI_updated.drop(columns="Territory", inplace=True)
final_KPI_updated.rename(columns={"Parent Territory":f"NSM Code UD"}, inplace=True)
# Update only the null values in 'ABM Name' with values from 'ABM Name_df2'
final_KPI_updated['NSM Code'] = final_KPI_updated['NSM Code'].fillna(final_KPI_updated['NSM Code UD'])
final_KPI_updated.drop(columns="NSM Code UD", inplace=True)

final_KPI_updated = final_KPI_updated.merge(userDetails[['Territory', 'Employee Name']], left_on='NSM Code', right_on = 'Territory', how='left')
final_KPI_updated.drop(columns="Territory", inplace=True)
final_KPI_updated.rename(columns={"Employee Name":"NSM Name UD"}, inplace=True)
final_KPI_updated['NSM Name'] = final_KPI_updated['NSM Name'].fillna(final_KPI_updated['NSM Name UD'])
final_KPI_updated.drop(columns="NSM Name UD", inplace=True)



final_KPI_updated = final_KPI_updated[final_KPI_updated['Division'].isin(uniqueDivisions)]


########################### ABM ZBM Calls ###############

KPI_ABM_ZBM_combined = pd.concat([final_KPI_ABM,final_KPI_ZBM])

dcr_data_new = dcr_data.merge(KPI_ABM_ZBM_combined[['Employee Code','Full Name','Territory']], on='Employee Code', how='left')
dcr_data_new = dcr_data_new.dropna(subset='Territory')



if user == "CHAURNX2":
    focus_path = "Y:\\Focus Coverage using Python\\constant files\\"
else:
    focus_path = "Z:\\Focus Coverage using Python\\constant files\\"

speciality_grouping = pd.read_excel(os.path.join(focus_path, 'Specialities.xlsx'))

dcr_data_new = dcr_data_new.merge(speciality_grouping, left_on='Specialty By Practice', right_on='Specialty', how='left')


#Gennext
#specialty to be included for ABM/ZBM Calls - ORTHOPAEDICIAN
dcr_data_27 = dcr_data_new[dcr_data_new['Division'] == '27']
dcr_data_27_focus = dcr_data_27[dcr_data_27['CORRECT Specialty'].isin(['ORTHOPAEDICIAN'])]


#Neurolife
#specialty to be included for ABM/ZBM Calls - 'NEURO SURGEON', 'NEUROLOGIST', 'ENT'
dcr_data_33 = dcr_data_new[dcr_data_new['Division'] == '33']
dcr_data_33_focus = dcr_data_33[dcr_data_33['CORRECT Specialty'].isin(['NEURO SURGEON','NEUROLOGIST','ENT'])]


dcr_data_27_focus_pivot = dcr_data_27_focus.pivot_table(index=['Employee Code', 'Full Name'],values='Account',aggfunc='count').reset_index()
dcr_data_33_focus_pivot = dcr_data_33_focus.pivot_table(index=['Employee Code', 'Full Name'],values='Account',aggfunc='count').reset_index()

dcr_data_27_focus_pivot.rename(columns={'Account':'HCP Count'}, inplace=True)
dcr_data_33_focus_pivot.rename(columns={'Account':'HCP Count'}, inplace=True)

dcr_data_27_focus_pivot.to_excel(path + f"ABM_ZBM_focused_calls 27" + ".xlsx", index=False)
dcr_data_33_focus_pivot.to_excel(path + f"ABM_ZBM_focused_calls 33" + ".xlsx", index=False)



#############################################################################################################

NSM_summary = final_KPI_updated[['Division','NSM Code','NSM Name']].drop_duplicates().copy()
NSM_summary['Desgn'] = 'NSM'

ZBM_summary = final_KPI_updated[['Division','Zone Code','ZBM Name']].drop_duplicates().copy()
ZBM_summary['Desgn'] = 'ZBM'

Hierarchy_filt = Hierarchy[['EHIER_CD','EMPLOYEE_CODE']].copy()
Hierarchy_filt['EMPLOYEE_CODE'] = Hierarchy_filt['EMPLOYEE_CODE'].astype(str)
    
ZBM_summary = ZBM_summary.merge(Hierarchy_filt, left_on='Zone Code', right_on='EHIER_CD', how='left')    
ZBM_summary.rename(columns={'EMPLOYEE_CODE':'Emp Code'}, inplace=True)
ZBM_summary.drop(columns=['EHIER_CD'], inplace=True)


NSM_summary = NSM_summary.merge(Hierarchy_filt, left_on='NSM Code', right_on='EHIER_CD', how='left')    
NSM_summary.rename(columns={'EMPLOYEE_CODE':'Emp Code'}, inplace=True)
NSM_summary.drop(columns=['EHIER_CD'], inplace=True)

    


#% Coverage For TBMs


def calculate_coverage(df_summary, key_column):

    # Step 1: Precompute totals
    zone_totals = final_KPI_updated.groupby(key_column)[['Total DR Visited', 'Total DR Total']].sum()
    # Step 2: Compute coverage
    zone_totals['% Coverage For TBMs'] = (zone_totals['Total DR Visited'] / zone_totals['Total DR Total'] *100).round(2)
    # Step 3: Merge with the summary DataFrame
    return df_summary.merge(zone_totals['% Coverage For TBMs'], on=key_column, how='left')

# Apply function for ZBM and NSM summaries
ZBM_summary = calculate_coverage(ZBM_summary, 'Zone Code')
NSM_summary = calculate_coverage(NSM_summary, 'NSM Code')


final_KPI_updated['Actual DR Calls'] = final_KPI_updated['Actual DR Calls'].astype(int)


def calculate_call_average(df_summary, key_column):

    # Step 1: Precompute totals
    zone_totals = final_KPI_updated.groupby(key_column)[['Actual DR Calls', 'Call Days']].sum()
    # Step 2: Compute coverage
    zone_totals['Call Average Team For TBMs'] = (zone_totals['Actual DR Calls'] / zone_totals['Call Days'])
    # Step 3: Merge with the summary DataFrame
    return df_summary.merge(zone_totals['Call Average Team For TBMs'], on=key_column, how='left')

# Apply function for ZBM and NSM summaries
ZBM_summary = calculate_call_average(ZBM_summary, 'Zone Code')
NSM_summary = calculate_call_average(NSM_summary, 'NSM Code')

#final_KPI_updated.dtypes[:25]

def calculate_2PC_cov(df_summary, key_column):

    # Step 1: Precompute totals
    zone_totals = final_KPI_updated.groupby(key_column)[['2PC Freq Met', '2PC DR Total']].sum()
    # Step 2: Compute coverage
    zone_totals['2 PC % Frequency For TBMs'] = (zone_totals['2PC Freq Met'] / zone_totals['2PC DR Total']*100).round(2)
    # Step 3: Merge with the summary DataFrame
    return df_summary.merge(zone_totals['2 PC % Frequency For TBMs'], on=key_column, how='left')

def calculate_2PC_cov_simple(df_summary, key_column):

    # Step 1: Precompute totals
    zone_totals = final_KPI_updated.groupby(key_column)[['2PC Cov', '2PC DR Total']].sum()
    # Step 2: Compute coverage
    zone_totals['2 PC % Cov For TBMs'] = (zone_totals['2PC Cov'] / zone_totals['2PC DR Total']*100).round(2)
    # Step 3: Merge with the summary DataFrame
    return df_summary.merge(zone_totals['2 PC % Cov For TBMs'], on=key_column, how='left')



# Apply function for ZBM and NSM summaries
ZBM_summary = calculate_2PC_cov(ZBM_summary, 'Zone Code')
NSM_summary = calculate_2PC_cov(NSM_summary, 'NSM Code')

ZBM_summary = calculate_2PC_cov_simple(ZBM_summary, 'Zone Code')
NSM_summary = calculate_2PC_cov_simple(NSM_summary, 'NSM Code')

final_KPI_ZBM_updated = final_KPI_ZBM.copy()

final_KPI_ZBM_updated = final_KPI_ZBM_updated.drop_duplicates(subset='Employee Code', keep='last')

final_KPI_ZBM_updated = final_KPI_ZBM_updated[final_KPI_ZBM_updated['Division'].isin(uniqueDivisions)]

final_KPI_ZBM_updated = final_KPI_ZBM_updated.merge(Hierarchy[['EHIER_CD', 'PAR_EHIER_CD','PAR_EMPLOYEE_NAME']].drop_duplicates(), left_on='Territory', right_on='EHIER_CD', how='left')
final_KPI_ZBM_updated.drop(columns="EHIER_CD", inplace=True)
final_KPI_ZBM_updated.rename(columns={"PAR_EHIER_CD":f"NSM Code",'PAR_EMPLOYEE_NAME':'NSM Name'}, inplace=True)

#From User Details
final_KPI_ZBM_updated = final_KPI_ZBM_updated.merge(userDetails[['Territory', 'Parent Territory']], left_on='Territory', right_on = 'Territory', how='left')
final_KPI_ZBM_updated.drop(columns="Territory", inplace=True)
final_KPI_ZBM_updated.rename(columns={"Parent Territory":f"NSM Code UD"}, inplace=True)
# Update only the null values in 'ABM Name' with values from 'ABM Name_df2'
final_KPI_ZBM_updated['NSM Code'] = final_KPI_ZBM_updated['NSM Code'].fillna(final_KPI_ZBM_updated['NSM Code UD'])
final_KPI_ZBM_updated.drop(columns="NSM Code UD", inplace=True)



def FW_days_NSM(df_summary,key_column):
    # Step 1: Precompute totals
    zone_totals = final_KPI_ZBM_updated.groupby(key_column)[['Call Days']].sum()
    
    # Step 3: Merge with the summary DataFrame
    return df_summary.merge(zone_totals['Call Days'], on=key_column, how='left')


NSM_summary = FW_days_NSM(NSM_summary, 'NSM Code')
    
ZBM_summary = ZBM_summary.merge(final_KPI_ZBM_updated[['Employee Code','Call Days']].drop_duplicates(), left_on='Emp Code', right_on='Employee Code', how='left')
ZBM_summary.drop(columns='Employee Code', inplace=True)

NSM_summary.rename(columns={'Call Days':'FW days'}, inplace=True)
ZBM_summary.rename(columns={'Call Days':'FW days'}, inplace=True)

final_KPI_ZBM_updated['Actual DR Calls'] = final_KPI_ZBM_updated['Actual DR Calls'].astype(int)


def No_of_CallDays(df_summary,key_column):
    # Step 1: Precompute totals
    zone_totals = final_KPI_ZBM_updated.groupby(key_column)[['Actual DR Calls']].sum()
    
    # Step 3: Merge with the summary DataFrame
    return df_summary.merge(zone_totals['Actual DR Calls'], on=key_column, how='left')

NSM_summary = No_of_CallDays(NSM_summary, 'NSM Code')
    
ZBM_summary = ZBM_summary.merge(final_KPI_ZBM_updated[['Employee Code','Actual DR Calls']].drop_duplicates(), left_on='Emp Code', right_on='Employee Code', how='left')
ZBM_summary.drop(columns='Employee Code', inplace=True)

final_KPI_ZBM_updated.groupby('NSM Code')[['Actual DR Calls']].sum()

NSM_summary.rename(columns={'Actual DR Calls':'No of Calls Made'}, inplace=True)
ZBM_summary.rename(columns={'Actual DR Calls':'No of Calls Made'}, inplace=True)



NSM_summary_melted = NSM_summary.melt(id_vars=['Division','NSM Code','Emp Code','NSM Name','Desgn'], var_name='Metric', value_name='Value')
ZBM_summary_melted = ZBM_summary.melt(id_vars=['Division','Zone Code','Emp Code','ZBM Name','Desgn'], var_name='Metric', value_name='Value')

metric_order = ['% Coverage For TBMs',
 'Call Average Team For TBMs',
 '2 PC % Frequency For TBMs',
 '2 PC % Cov For TBMs',
 'FW days',
 'No of Calls Made']

NSM_summary_melted['Metric'] = pd.Categorical(NSM_summary_melted['Metric'], categories=metric_order, ordered=True)

# Convert the 'Metric' column to a categorical type with the specified order
ZBM_summary_melted['Metric'] = pd.Categorical(ZBM_summary_melted['Metric'], categories=metric_order, ordered=True)



NSM_summary_melted.rename(columns={'NSM Code':'Hierarchy Code','NSM Name':'Emp Name'}, inplace=True)
ZBM_summary_melted.rename(columns={'Zone Code':'Hierarchy Code','ZBM Name':'Emp Name'}, inplace=True)
NSM_summary_melted = NSM_summary_melted.sort_values(by='Metric')

# Sort the DataFrame by 'Hierarchy Code' and 'metric'
ZBM_summary_melted = ZBM_summary_melted.sort_values(by=['Hierarchy Code', 'Metric'])


Final_summary = pd.concat([NSM_summary_melted,ZBM_summary_melted])


Final_summary['Value'] = Final_summary['Value'].round(2)

#Excluding Nepal for NSM ZBM summary
if 'NP' in uniqueDivisions:
    uniqueDivisions.remove('NP')
#df_melted = NSM_summary.melt(id_vars=['NSM Code','NSM Name','Desgn'], var_name='Metric', value_name='Value')

#    path1 = 'C:\\Users\\PAGARAX1\\OneDrive - Abbott\\Documents\\SFA scripts archive\\KPI AIL\\'

#Final_summary[Final_summary['Division'] == d]


for d in uniqueDivisions:
    filt_summary = Final_summary[Final_summary['Division'] == d]
    filt_summary.to_excel(path + f"NSM_ZBM_summary {d}" + ".xlsx", index=False)


######################################## Cleaning #################################
endTime = dt.now()
timeDelta = endTime-startTime
print("Execution Time :" + str(timeDelta))




























