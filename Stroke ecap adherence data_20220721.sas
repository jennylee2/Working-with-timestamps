/*ECAP ADHERENCE DATASET*/
/* Created by Jenny Lee on 2/14/2023 */

libname s "P:\Study Folders\Edmondson_Reach Stroke_2016\Data Management\Sean\SAS\Production";
libname n "P:\Study Folders\Edmondson_Reach Stroke_2016\Data Management\Data Requests\20220721_Kronish_ecaps data\Data";

/*STEP 1: REVIEW ECAP FM DATASET*/
data ecap;
	set s.ecaps;
	if id="49999" or id="50000" then delete;
	run;

proc contents data=ecap order=varnum; run;
/**/
/*proc freq data=ecap;*/
/*table REACH_Consent_eCAPS; run;*/

/*proc import out=ecap_und datafile="P:\Study Folders\Edmondson_Reach Stroke_2016\9. Stroke eCAPs\eCAPs Data Cleaning\eCAP Exports\2019\REACH Stroke 10.21.19 not defined"*/
/*            DBMS=xlsx REPLACE;*/
/*	 getnames=YES;*/
/*run;*/

data ecap(keep= id
eCAP_Date_Consent
REACH_Consent_eCAPS
eCAP_rsn_NoConsent
eCAP_Medication
eCAP_medname
eCAP_UsableData
eCAP_Date_Paid
eCAP_ConfirmationCall
eCAP_date_PtReceived
eCAP_Date_Returned
eCAP_Date_Sent
eCAP_Serial);
set ecap;
run;

proc freq data=ecap;
table REACH_Consent_eCAPS / list missing; run;

/**************** This is number of consented yes ********************/
data ecap_consentyes;
	set ecap;
	where REACH_Consent_eCAPS="Yes" or REACH_Consent_eCAPS="1";
	run;
	*325 obs;
	*349 obs consented yes;

proc freq data=ecap_consentyes;
table eCAP_UsableData / list missing; run;

proc export data=ecap_consentyes 
outfile="P:\Study Folders\Edmondson_Reach Stroke_2016\Data Management\Data Requests\20220721_Kronish_ecaps data\Data\StrokeECAP_consentyes.xlsx" 
dbms=xlsx replace;

run;


/*These are not usable patients- all legit reasons*/
/*proc print data=ecap_consentyes;*/
/*where eCAP_UsableData="eCAP not used" or eCAP_UsableData="No";*/
/*run;*/

/*proc freq data=ecap_consentyes;*/
/*table eCAP_medfreq eCAP_Medication;*/
/*run;*/

/*proc freq data=ecap_consentyes;*/
/*table eCAP_UsableData / list missing; run;*/

data ecap_yes3 (rename=(ID1 = ID));
set ecap_consentyes;
if eCAP_UsableData ne 'Yes' then delete;
ID1 = ID*1;
drop ID;
run;
*169 obs;

proc contents data=ecap_yes3 order=varnum; run;

data n.ecap_fmdata;
set ecap_yes3;
run;

/*proc print data=ecap_yes3;*/
/*var id;*/
/*run;*/

/*Step 2: Import data from file created by JL */
proc import out=dates datafile= "P:\Study Folders\Edmondson_Reach Stroke_2016\Data Management\Data Requests\20220721_Kronish_ecaps data\Data\start_end_dates.xlsx"
			DBMS=xlsx replace;
			getnames=YES;
run;

proc contents data=dates order=varnum; run;

/*proc print data=dates;*/
/*var id Dose_Date_y; run;*/


data dates1 (DROP= Day_num_x Dose_Date_x ID_str Dose_Timestamp Dose_Label_y Dose_Date_y Day_num_y);
set dates;
daynum_start=Day_num_x;
daynum_last=Day_num_y;
start_date=Dose_Date_x;
end_date = mdy(substr(Dose_Date_y,6,2),substr(Dose_Date_y,9,2),substr(Dose_Date_y,1,4));

if id=50230 or id=50078 or id=50671 then delete;
format start_date  Last_Date_50 datetime7.
	end_date mmddyy10.;

run;

proc contents data=dates1 order=varnum; run;
*170 obs;
*168 obs;

/*proc freq data=dates1;*/
/*table id / list missing; run;*/
/**/
/*proc print data=dates1;*/
/*where Dose_Index=1; run;*/

/*reformat dates*/
data dates2 (drop=start_date);
  set dates1; 
  sas1date = datepart(start_date);

  format sas1date mmddyy.;
run;


proc contents data=dates2; run;

/*Step 3: Merge files*/
proc sql;
create table stroke_ecap_all as select distinct A.*,B.*
from ecap_yes3 as A left Join dates2 as B
on a.id=b.id;
quit;
*169 obs;

/*Review with Ian*/
/*Create new last_date var (if only 1 reading, last date is startdate_plus30, if end_date is more than 30 days then use end date, if end_date is less than 30 days, use startdate_plus30)*/
data stroke_ecap_alla (drop=Last_Date_50 sas1date daynum_start daynum_last);
set stroke_ecap_all; 

start_date=sas1date;
startdate_plus30=intnx('day', start_date, 30); 

if dose_index=1 then last_date=startdate_plus30;
	else if end_date >  startdate_plus30 then last_date=end_date;
	else if end_date <= startdate_plus30 then last_date=startdate_plus30;
	else last_date=.;

format  start_date startdate_plus30 last_date MMDDYY10.;
run;

proc contents data=stroke_ecap_all order=varnum; run;

data n.stroke_ecap_all;
set stroke_ecap_all;
run;

/*QA checks*/
/*proc print data=stroke_ecap_alla;*/
/*where eCAP_Date_Returned < end_date;*/
/*run;*/
/**all good;*/
/**/
/*proc print data=stroke_ecap_alla;*/
/*where eCAP_date_PtReceived > start_date;*/
/*run;*/
*all good;

/**/
/*data stroke_ecap_device;*/
/*set stroke_ecap_all;*/
/*if Dose_Label_x="" then delete;*/
/*run;*/
/**169 obs;*/
/**/
/**/
/*proc freq data=stroke_ecap_all;*/
/*table Dose_Label_x / list missing; run;*/

/******************************************************************************************************************************************/

/*STEP 3: IMPORT STROKE ECAPS FILES*/

/*All data is in the files with dates 10.21.19*/

/*undefine*/
proc import out=ecap0 datafile="P:\Study Folders\Edmondson_Reach Stroke_2016\9. Stroke eCAPs\eCAPs Data Cleaning\eCAP Exports\2019\REACH STROKE 10.21.19 not defined"
            DBMS=xls REPLACE;
	sheet="Med-ic Data Export";
	getnames=YES;

run;

/*1 per day*/
proc import out=ecap1 datafile="P:\Study Folders\Edmondson_Reach Stroke_2016\9. Stroke eCAPs\eCAPs Data Cleaning\eCAP Exports\2019\REACH STROKE 10.21.19 1x per day"
            DBMS=xls REPLACE;
	sheet="Med-ic Data Export";
	getnames=YES;

run;

/*2 per day*/
proc import out=ecap2 datafile="P:\Study Folders\Edmondson_Reach Stroke_2016\9. Stroke eCAPs\eCAPs Data Cleaning\eCAP Exports\2019\REACH STROKE 10.21.19 2x per day"
            DBMS=xls REPLACE;
	sheet="Med-ic Data Export";
	getnames=YES;

run;

/* Cleaning individual medication files from medic */
options mprint mlogic symbolgen;
%Macro MAimport;
%do i=0 %to 2;
	data ecap&i (drop= _Patient_ID _ECM_ID study_ID site_ID package_id dose_regimen dose_date compliant package_index dose_index Dose_Timestamp_UTC Dose_Group);
	set ecap&i (rename= (Kit_ID=ECM_ID Subject_ID = Patient_ID));
	retain _Patient_ID _ECM_ID Dose_Label;
	if not missing(Patient_ID) then _Patient_ID = Patient_ID;
	else Patient_ID = _Patient_ID;
	if not missing(ECM_ID) then _ECM_ID = ECM_ID;
	else ECM_ID = _ECM_ID;

/*%if &i=105 %then %do;*/
/*if Patient_ID =2486 or Patient_ID = 2220;*/
/*%end;*/
/**/
/*%if &i=106 %then %do;*/
/*if Patient_ID= 2961 or Patient_ID = 2970 or Patient_ID=2984;*/
/*%end;*/

	run;
	proc sort data=ecap&i; by Patient_ID; run;
%end;
%Mend;
%MAimport 

*merging all medication frequncy files together;
data Stroke (rename=(new_id = ID));
set ecap0 - ecap2; 
new_id=input(Patient_ID, Best8.);
run;

proc sort data=Stroke; by ID Dose_Timestamp; run;
*8973 obs;

proc freq data=stroke;
table id / list missing; run;

/*Converting Date, Time formats*/
data stroke;
set stroke;
CapOpenDate_hms = DHMS(mdy(substr(Dose_Timestamp,6,2),substr(Dose_Timestamp,9,2),substr(Dose_Timestamp,1,4)),substr(Dose_Timestamp,12,2), substr(Dose_Timestamp,15,2), substr(Dose_Timestamp,18,2));
CapOpenDate_hm = DHMS(mdy(substr(Dose_Timestamp,6,2),substr(Dose_Timestamp,9,2),substr(Dose_Timestamp,1,4)),substr(Dose_Timestamp,12,2), substr(Dose_Timestamp,15,2), 0);
CapOpenDay = mdy(substr(Dose_Timestamp,6,2),substr(Dose_Timestamp,9,2),substr(Dose_Timestamp,1,4));
format CapOpenDate_hms datetime. CapOpenDate_hm datetime. CapOpenDay mmddyy10.;
run;

proc contents data=stroke order=varnum; run;

proc sql;
create table stroke1 as
select distinct A.*,B.start_date,B.last_date
from stroke as A Left Join stroke_ecap_alla as B
on A.ID=B.ID;
quit;


*creating list of openings for each pt;
proc transpose data=stroke1 out=stroke_out(drop = _NAME_) prefix=date;
by ID;
var CapOpenDay;
run;

data stroke_out (rename=(ID1 = ID));
retain id1;
set stroke_out;
ID1 = ID*1;
drop ID;
run;

*Merge all medic data to FM data;
proc sql;
create table stroke2 as
select distinct A.*,B.start_date,B.last_date, B.Dose_Label_x
from stroke_out as A right Join stroke_ecap_alla as B
on A.ID=B.ID;
quit;

/*proc print data=stroke_ecap_all;*/
/*where id=50471;*/
/*run;*/



********************************************************** ADHERENCE SCORE CALCULATIONS *****************************************************;

data clean; 
drop i j;
set stroke2;

ARRAY dateopen (211) date1-date211; *if a cap is returned with more data points change number in brackets;
ARRAY opentimes (211) CAP1-CAP211;
ARRAY opendate (211) DAT1-DAT211;

DO j=1 to 211;
   opentimes(j)=0;
   opendate(j)=.;
END;

DO i=1 to 211;
   DO j=1 to 211;
       if dateopen(i)=(start_date - 1 + j) then opentimes(j)=(opentimes(j) + 1); *start at start date and create consecutive date interval;
       if j > last_date - start_date + 1 then opentimes(j) = .; *stop interval at individual's study end date;

	   opendate(j)=(start_date - 1 + j); 
	   if opendate(j) > last_date then opendate(j) = .;
END;
END;

format DAT1-DAT211 mmddyy10.;
run;


data clean211; 
retain id  start_date last_date adheretot leasttot Days;
set clean;

ARRAY opentimes(211) CAP1-CAP211; 
ARRAY adherences(211) ADS1-ADS211;
ARRAY adherencel(211) ADL1-ADL211;

DO j=1 to 211;
   adherences(j)=.; *all columns will start with blank;
   adherencel(j)=.;
END;

if Dose_Label_x="1" or Dose_Label_x ="Once a Day" then DO j=1 to 211; *go to correct frequency for individual;
   if opentimes(j)=0 then adherences(j)=0; *if did not open, mark 0;
   if opentimes(j)=1 then adherences(j)=1; *if opened device open that day, mark score;
   if opentimes(j)>1 then adherences(j)=0; *if opened more than once that day, mark score;
   
   if opentimes(j)=0 then adherencel(j)=0;
   if opentimes(j)=1 then adherencel(j)=1;
   if opentimes(j)>1 then adherencel(j)=1;

END;


if Dose_Label_x='Twice a Day' then do j=1 to 211;
    if opentimes(j)=0 then adherences(j)=0;
	if opentimes(j)=1 then adherences(j)=0.5;
    if opentimes(j)=2 then adherences(j)=1;
    if opentimes(j)>2 then adherences(j)=0;

    if opentimes(j)=0 then adherencel(j)=0;
	if opentimes(j)=1 then adherencel(j)=0.5;
    if opentimes(j)>=2 then adherencel(j)=1;
END;


adheretot = sum (of ADS1-ADS211) /(last_date - start_date + 1); *calculate average sum;
leasttot = sum (of ADL1-ADL211) /(last_date - start_date + 1);
Days = (last_date - start_date + 1);  *time participated in the study not return - sent date;

/*if start_date = last_date then adheretot = 0;*/
/*if start_date = last_date then leasttot = 0;*/
/*if start_date = last_date then days = 1;*/

label Days = "Total days monitored";
label adheretot = "Total strict adherence (as perscribed)";
label leasttot = "Total at least adherence (as perscribed or more)";
run;
/*drop j date51--date211 /*DAT1-DAT50*/


Proc sort data=clean211; by ID; run;

data n.clean211;
set clean211;
run;

/*Univariate stats on days*/
Proc univariate data=clean211;
Id id;
var days;
histogram / normal;
run;


/*data clean2;*/
/*set n.clean2;*/
/*run;*/

proc export data=clean211
outfile="P:\Study Folders\Edmondson_Reach Stroke_2016\Data Management\Data Requests\20220721_Kronish_ecaps data\Data\clean211.sav" 
dbms=spss replace;
FMTLIB=myfmtlib.formats;
run;


*********************************************************** END OF ADHERENCE CODE **********************************;


******************************************************* ADJUST FOR HOSPITALIZATIONS ********************************;
data hosp (rename=(ID1 = ID));
retain id erv_date ERV_DATE_DISCHARGE HOSP_ADJUDSTATUS DATE_NOTIFIED;
set s.hospitalizations_A;
ID1 = ID*1;
if Hosp_AdjudStatus = "All data entered" or Hosp_AdjudStatus ="Received and reviewed the full record";
drop ID;
run;

proc sort data=hosp; by ID erv_date; run;
proc sort data=stroke_ecap_alla; by ID; run;


data hosp_eCaps; 
merge hosp (in=a) stroke_ecap_alla (in=b);
if a and b;
by id; 
keep id erv_date ERV_DATE_DISCHARGE start_date last_date;
run;
*204 obs;

data hosp_eCaps1;
set hosp_eCaps;
where erv_date ge start_date and erv_date le last_date;
run;
*45 obs;

proc freq data=hosp_eCaps1;
table id; run;
*31 pts;

*Hosp date;
proc transpose data=hosp_eCaps1 out= hosp_start (drop=_NAME_) prefix=visit;
by id start_date last_date; 
var erv_date ;
run;
*62 obs;

*Discharge date;
proc transpose data=hosp_eCaps1 out= hosp_eCaps_end (drop=_NAME_) prefix=discharge;
by id start_date last_date; 
var ERV_DATE_DISCHARGE ;
run;

*Table with all visit hosp start dates and end dates by id;
proc sql;
create table merged_hosp_dates as
select a.id,a.start_date,a.last_date,a.visit1,a.visit2,a.visit3,a.visit4,a.visit5,
b.discharge1,b.discharge2,b.discharge3,b.discharge4,b.discharge5
from hosp_start as a Left Join hosp_ecaps_end as b
on a.id=b.id;
quit;
*31 obs;

data merged_hosp_dates1;
set merged_hosp_dates;

format visit1-visit5 discharge1-discharge5 mmddyy10.;
run;

proc sql;
create table clean_withhosp as
select a.*,b.* from clean as a left join merged_hosp_dates1 as b
on a.id=b.id;
quit:

/*Adding hosp days*/
data hosp_eCaps_days; 
drop i j;
set clean_withhosp ;

ARRAY opendate (211) DAT1-DAT211;
ARRAY hospdate (211) HOSP1-HOSP211;
ARRAY visit (5) visit1-visit5;
ARRAY discharge (5) discharge1-discharge5;

DO i=1 to 211;
   hospdate(i)=.; *all columns will start with blank;
END;

DO i=1 to 211;
	do j=1 to 5;
	if opendate(i)=. then hospdate(i)=.;
	else if opendate(i)=visit(j) then hospdate(i)=1;
	else if opendate(i) ne . and opendate(i)> visit(j) and opendate(i) <= discharge(j) then hospdate(i)=1;
END;
END;

hosp_days = sum(of hosp1--hosp211); 

run;


********************************************************** ADHERENCE SCORE CALCULATIONS WITH HOSPITALIZATIONS *****************************************************;

data clean211_withhosp; 
drop j;
retain id  start_date last_date adheretot leasttot Days hosp_days;
set hosp_eCaps_days;

ARRAY opentimes(211) CAP1-CAP211; 
ARRAY adherences(211) ADS1-ADS211;
ARRAY adherencel(211) ADL1-ADL211;
ARRAY hosp (211) HOSP1-HOSP211;

DO j=1 to 211;
   adherences(j)=.; *all columns will start with blank;
   adherencel(j)=.;
END;

if Dose_Label_x="1" or Dose_Label_x ="Once a Day" then DO j=1 to 211; *go to correct frequency for individual;
	if hosp(j)=1 then adherences(j)=.; *marked blank for hospitalization day;
	else if opentimes(j)=0 then adherences(j)=0; *if did not open, mark 0;
	else if opentimes(j)=1 then adherences(j)=1; *if opened device open that day, mark score;
	else if opentimes(j)>1 then adherences(j)=0; *if opened more than once that day, mark score;

	if hosp(j)=1 then adherencel(j)=.; *marked blank for hospitalization day;
	else if opentimes(j)=0 then adherencel(j)=0;
	else if opentimes(j)=1 then adherencel(j)=1;
	else if opentimes(j)>1 then adherencel(j)=1;

END;


if Dose_Label_x='Twice a Day' then do j=1 to 211;
	if hosp(j)=1 then adherences(j)=.;
    else if opentimes(j)=0 then adherences(j)=0;
	else if opentimes(j)=1 then adherences(j)=0.5;
    else if opentimes(j)=2 then adherences(j)=1;
    else if opentimes(j)>2 then adherences(j)=0;

	if hosp(j)=1 then adherencel(j)=.;
    else if opentimes(j)=0 then adherencel(j)=0;
	else if opentimes(j)=1 then adherencel(j)=0.5;
    else if opentimes(j)>=2 then adherencel(j)=1;
END;


adheretot = sum (of ADS1-ADS211) /(last_date - start_date + 1); *calculate average sum;
leasttot = sum (of ADL1-ADL211) /(last_date - start_date + 1);
Days = (last_date - start_date + 1);  *time participated in the study not return - sent date;

/*if start_date = last_date then adheretot = 0;*/
/*if start_date = last_date then leasttot = 0;*/
/*if start_date = last_date then days = 1;*/

label Days = "Total days monitored";
label adheretot = "Total strict adherence (as perscribed)";
label leasttot = "Total at least adherence (as perscribed or more)";
label hosp_days = "Total number of days in hospital";
run;

data n.clean211_withhosp;
set clean211_withhosp;
run;

proc export data=clean211_withhosp
outfile="P:\Study Folders\Edmondson_Reach Stroke_2016\Data Management\Data Requests\20220721_Kronish_ecaps data\Data\clean211_withhosp.sav" 
dbms=spss replace;
FMTLIB=myfmtlib.formats;
run;
