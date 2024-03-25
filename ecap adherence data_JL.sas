/*ECAP ADHERENCE DATASET*/
/*Adapted by Jenny Lee*/
/*February 2023*/


/*STEP 1: Import start_end_dates file (see Python code) */
proc import out=dates datafile= " \start_end_dates.xlsx"
			DBMS=xlsx replace;
			getnames=YES;
run;

proc contents data=dates order=varnum; run;


data dates1 (DROP= Day_num_x Dose_Date_x ID_str Dose_Timestamp Dose_Label_y Dose_Date_y Day_num_y);
set dates;
daynum_start=Day_num_x;
daynum_last=Day_num_y;
start_date=Dose_Date_x;
end_date = mdy(substr(Dose_Date_y,6,2),substr(Dose_Date_y,9,2),substr(Dose_Date_y,1,4));

format start_date  Last_Date_50 datetime7.
	end_date mmddyy10.;

run;

proc contents data=dates1 order=varnum; run;

/*reformat dates*/
data dates2 (drop=start_date);
  set dates1; 
  sas1date = datepart(start_date);

  format sas1date mmddyy.;
run;


proc contents data=dates2; run;

/*DELETION OF ORIGINAL STEPS: I had another table with list of pts who consented to the study. That file is named ecap_yes3. */
proc sql;
create table stroke_ecap_all as select distinct A.*,B.*
from ecap_yes3 as A left Join dates2 as B
on a.id=b.id;
quit;
*169 obs;


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


/******************************************************************************************************************************************/

/*STEP 2: IMPORT ECAPS FILES*/



/*undefine*/
proc import out=ecap0 datafile="FILEPATH"
            DBMS=xls REPLACE;
	sheet="NAME";
	getnames=YES;

run;

/*1 per day*/
proc import out=ecap1 datafile="FILEPATH"
            DBMS=xls REPLACE;
	sheet="NAME";
	getnames=YES;

run;

/*2 per day*/
proc import out=ecap2 datafile="FILEPATH"
            DBMS=xls REPLACE;
	sheet="NAME";
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

********************************************************** ADHERENCE SCORE CALCULATIONS *****************************************************;
/*STEP 3: Adherence score calculations*/

data clean; 
drop i j;
set stroke2;

ARRAY dateopen (211) date1-date211; *if a cap is returned with more data points change number in brackets. This is max number of openings.;
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


proc export data=clean211
outfile="FILEPATH\clean211.sav" 
dbms=spss replace;
FMTLIB=myfmtlib.formats;
run;




******************************************************* ADJUST FOR HOSPITALIZATIONS ********************************;

/*STEP 4: Get hospitalization data.  This could be any days that should be changed to missing if you don't want to count those days.*/

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


/*Adding hospdays with additional array of hosp variables*/
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
/*STEP 5: Adherence score calculations with adjustments*/

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

label Days = "Total days monitored";
label adheretot = "Total strict adherence (as perscribed)";
label leasttot = "Total at least adherence (as perscribed or more)";
label hosp_days = "Total number of days in hospital";
run;

data n.clean211_withhosp;
set clean211_withhosp;
run;

proc export data=clean211_withhosp
outfile="FILEPATH\clean211_withhosp.sav" 
dbms=spss replace;
FMTLIB=myfmtlib.formats;
run;
