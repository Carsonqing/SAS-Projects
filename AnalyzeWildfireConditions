*Read the Data;
%let path=E:\Academic\6714\HW\sasshoot\2017-analytics-shootout-data-package\;
options symbolgen;
options mprint;
/*create macro import to read csv dataset*/
%macro import(datafile);
proc import datafile="&&path.&datafile..csv"
dbms=csv out=&datafile;
GUESSINGROWS = 3000;
run;
%mend;
/*create macro read  to read sas dataset*/
%macro read(datafile);
data &datafile;
set "&&path.&datafile..sas7bdat";
run;
%mend;
%import(LandCover_by_County);
%import(NOAA_Zones_to_Counties);/*have missing value in state_name*/
%import(population_by_county_2001_2009);
%import(population_by_county_2010_2015);
%import(US_weather_stations_counties);
%read(drought_severity);
%read(drought_severity_scoring);/*will not need this scenario dataset for now*/
%read(storm_events);
%read(storm_events_scoring);/*will not need this scenario dataset for now*/
%read(weather_station_data);
%read(weather_station_scoring);/*will not need this scenario dataset for now*/
%read(wildfire_events);
%read(wildfire_narrative);
*check the event id is unique or not;
/*no problem*/
proc sql;
select count(*) as freq from wildfire_events
group by event_id
having  freq >1;
quit;


*add ST_CO_ID to table 1 wildfire_events;
/*change the format for matching*/
data NOAA_Zones_to_Counties_new;
set NOAA_Zones_to_Counties;
State_Name_new = upcase(State_Name);
CZ_FIPS_new = input(CZ_FIPS,8.);/*convert char to num*/
run;

/* ignore temporally for hw 5
proc sql;
create table wildfire as
select a.*, ST_CO_ID, b.ST as state_abbre
from wildfire_events as a left join NOAA_Zones_to_Counties_new as b
on a.STATE = b.State_Name_new and a.CZ_FIPS =b.CZ_FIPS_new;*/
/*466 rows have missing ST_CO_ID and State name, but Sate abbreviated is not missong*/

proc sql;
create table wildfire as
select a.*, ST_CO_ID_new , b.state_ab as state_abbre
from wildfire_events as a left join noaa_final as b
on a.STATE = b.State_Name_new and a.CZ_FIPS =b.CZ_FIPS_new;quit;

%missing(NOAA_Zones_to_Counties_new,ST,char);/*0*/
%missing(NOAA_Zones_to_Counties_new,State_Name_new,char);/*94*/
%missing(NOAA_Zones_to_Counties_new,ST_CO_ID,num);/*94*/

%missing(wildfire,state,char);/*full name is not missing*/
%missing(wildfire,ST_CO_ID,num);/*full name is not missing*/

data wildfire;
set wildfire;
ST_CO_ID = input(ST_CO_ID_new,5.);
run;

proc sql;
create table temp as
select  state,state_abbre,st_co_id,event_id,month_name, month(datepart(BEGIN_DATE_TIME)) as month,intck('dtday',BEGIN_DATE_TIME, END_DATE_TIME) as freq 
from wildfire
where ST_CO_ID NE .;
quit;

proc sql;
create table wildfire_new as
select distinct st_co_id,state,state_abbre,month_name,month, sum(freq) as freq 
from temp
group by month_name,st_co_id;
quit;
/*end of adding ST_CO_ID to wildfire_events*/

*weather;
proc sql;
create table weather as
select st_co_id,month(date) as month,mean(TMAX_F) as tmax_m,mean(TMIN_F) as _tmin_m,mean(PRCP_in) as prcp_m,
       mean(SNWD_in) as snwd_m,mean(SNOW_in) as snow_d
from weather_station_data
group by st_co_id,month;
quit;

proc sql;
create table table1 as
select a.*,tmax_m ,_tmin_m,prcp_m,snwd_m,snow_d
from wildfire_new as a,weather as b
where a.ST_CO_ID=b.ST_CO_ID and b.month = a.month ;
quit;

*drought;
proc sql;
create table temp1 as
select *,month(validStart) as month
from drought_severity;

create table drought_new as
select ST_CO_ID, month,mean(NONE)as none_mean,mean(D0_D4) as D0_mean,mean(D1_D4) as D1_mean ,mean(D2_D4) as D2_mean,
       mean(D3_D4) as D3_mean,mean(D4) as D4_mean
from temp1
group by ST_CO_ID, month ;
quit;

proc sql;
create table table2 as
select a.*, none_mean,D0_mean, D1_mean, D2_mean, D3_mean, D4_mean
from table1 as a,drought_new as b
where  a.ST_CO_ID=b.ST_CO_ID and b.month = a.month ;
quit;
*storm_events;

proc sql;
create table storm as
select a.*, ST_CO_ID_new as ST_CO_ID, b.state_ab as state_abbre
from storm_events as a left join noaa_final as b
on a.STATE = b.State_Name_new and a.CZ_FIPS =b.CZ_FIPS_new;

data storm_new;
set storm;
event_length = intck("dtday",begin_date_time,end_date_time);
month = month(datepart(begin_date_time));
run;
proc sql;
create table temp2 as
select  ST_CO_ID,month,event_type,count(event_type) as count,sum(event_length) as event_month_length
from storm_new
group by ST_CO_ID,month,event_type;
quit;
proc sort data=temp2;
by  ST_CO_ID month;
run;

proc transpose data = temp2
                 out = temp3 ;
id event_type;
by ST_CO_ID month ;
var  count;
run;
data temp3;
set temp3;
ST_CO_ID_new=input(ST_CO_ID,5.);
run;
proc sql;
create table table3 as
select a.*, Cold_Wind_Chill, Dense_Fog,  Drought, Dust_Storm, Excessive_Heat, Extreme_Cold_Wind_Chill, Freezing_Fog, Funnel_Cloud,
Heat, Heavy_Wind, High_Wind, Hurricane__Typhoon_, Lightning, Strong_Wind, Thunderstorm_Wind,  Tornado, tropical_Storm
from table2 as a,temp3 as b
where  a.ST_CO_ID=b.ST_CO_ID_new and b.month = a.month ;
quit;

*model part;
data table3_new;
set table3;
n=.;
run;

proc sql;
update table3_new
set n=
case
when month in (1,3,5,7,8,10,12)
then 31
when month in (4,6,9,11)
then 30
else 29
end;
quit;

data table3_new;
set table3_new;
log_n=log(n);
run;
options symbolgen;
%macro changemiss(dataset,var);
data &dataset;
set &dataset;
if  &var =. then
&var = 0;
%mend;

%changemiss(table3_new,Cold_Wind_Chill);
%changemiss(table3_new,Dense_Fog);
%changemiss(table3_new,Drought);
%changemiss(table3_new,Excessive_Heat);
%changemiss(table3_new,Funnel_Cloud);
%changemiss(table3_new,Heat);
%changemiss(table3_new,Heavy_Wind);
%changemiss(table3_new,High_Wind);
%changemiss(table3_new,Lightning);
%changemiss(table3_new,Strong_Wind);
%changemiss(table3_new,tropical_Storm);
%changemiss(table3_new,Thunderstorm_Wind);
%changemiss(table3_new,Tornado);


proc genmod data=table3_new ;
class state month;
model freq = state month
             tmax_m _tmin_m prcp_m snwd_m snow_d
			 none_mean D0_mean  D1_mean  D2_mean  D3_mean  D4_mean
      Cold_Wind_Chill  Dense_Fog   Drought  Excessive_Heat Funnel_Cloud
             Heat Heavy_Wind High_Wind Lightning Strong_Wind Thunderstorm_Wind  Tornado tropical_Storm
/ dist=poisson offset=log_n;
run;

ods graphics on;
proc genmod data=table3_new plots = (predicted(clm));
class state month;
model  freq = state month
             tmax_m _tmin_m prcp_m snwd_m snow_d
			 none_mean D0_mean  D1_mean  D2_mean  D3_mean  D4_mean
      Cold_Wind_Chill  Dense_Fog   Drought  Excessive_Heat Funnel_Cloud
             Heat Heavy_Wind High_Wind Lightning Strong_Wind Thunderstorm_Wind  Tornado tropical_Storm
/ dist=negbin offset=log_n;
run;
ods graphics off;
*group state;
data table3_grouped;
set table3_new;
if state in ("ALABAMA","ARIZONA","COLORADO","CONNECTICUT","FLORIDA","IDAHO","INDIANA","LOUISIANA","MARYLAND","MINNESOTA","MONTANA","NEW MEXICO",
             "NORTH CAROLINA","SOUTH CAROLINA","VIRGINIA","WYOMING","UTAH","VERMONT") then
state_group = "grouped";
else state_group = state;
run;
*end;
*group month;
proc sql;
create table temp4 as
select month, sum(freq) as sum
from table3_grouped
group by month;
quit;
proc sgplot data = temp4;
title"freq by month";
vbar month/response=sum;
run;
data table3_grouped;
set table3_grouped;
format month_gp $8.;
if month in (12,1,2,3) then
month_gp = "grouped";
else month_gp = put(month,$8.);
run;
*end;

ods graphics on;
proc genmod data=table3_grouped plots = (predicted(clm));
class state_group(param = ref ref = "grouped") month_gp(param = ref ref = "grouped");
model  freq = state_group month_gp
             tmax_m _tmin_m prcp_m snwd_m snow_d
			 none_mean D0_mean  D1_mean  D2_mean  D3_mean  D4_mean
      Cold_Wind_Chill  Dense_Fog   Drought  Excessive_Heat Funnel_Cloud
             Heat Heavy_Wind High_Wind Lightning Strong_Wind Thunderstorm_Wind  Tornado tropical_Storm
/ dist=negbin offset=log_n;
run;
ods graphics off;

*final model after deleting lots of insignificant vars;
ods graphics on;
ods pdf file="E:\Academic\6714\HW\genmodel.pdf";
proc genmod data=table3_grouped plots = (predicted(clm));
class state_group(param = ref ref = "grouped") month_gp(param = ref ref = "grouped");
model  freq = state_group month_gp
             tmax_m _tmin_m snwd_m
		     D2_mean  D4_mean
             Cold_Wind_Chill Funnel_Cloud Heat High_Wind
/ dist=negbin offset=log_n;
run;
ods pdf close;
ods graphics off;
*final dataset;
data final;
set table3_grouped;
keep st_co_id state state_group month_name month month_gp freq 
     tmax_m _tmin_m snwd_m
	 D2_mean  D4_mean
     Cold_Wind_Chill Funnel_Cloud Heat High_Wind
;
run;

proc print data = final (obs=10);run;

*save the final to disk;
libname sasshoot "E:\Academic\6714\HW\sasshoot\2017-analytics-shootout-data-package";
data sasshoot.final;
set final;
run;
