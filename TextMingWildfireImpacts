proc import datafile = "E:\Academic\6714\HW\sasshoot\2017-analytics-shootout-data-package\NOAA_Zones_to_Counties.csv"
dbms = csv  out = noaa;
guessingrows = max;
run;

proc sql;
select * from st_co_lookup
where statecode = "FL" and COUNTYNM = "Seminole";
quit;

proc sql;
select * from Noaa
where ST = "FL" and County = "Seminole";
quit;

proc contents data = Noaa;
run;
data noaa_new(drop= ST County);
set noaa;
state_ab = upcase(strip(ST));
county_name = upcase(strip(County));
st_co_id_new = put(st_co_id,z5.);
cz_fips_new=input(strip(cz_fips),3.);
if st_co_id_new = "    ." then st_co_id_new="";
run;


*extract info from zipcode;
data zip(keep = y x state statecode statename county countynm);
set sashelp.zipcode;
run;

proc contents data = zip;
run;

data ST_CO_lookup;
set zip;
padded = put(county,z3.);
ST_CO_add = cat(state,padded);
run;
proc contents data = ST_CO_lookup;run;
/*The Zw.d format is similar to the w.d format except that Zw.d pads right-aligned output with 0s instead of blanks.*/

*look up the missing value;
*try to look how it looks like;
proc sql;
create table st_co_lookup_new
as select distinct ST_CO_add, statecode, COUNTY
from ST_CO_lookup;
quit;


proc sql;
select  state_ab,state_name,CZ_FIPS_new, COUNTY, st_co_id_new, ST_CO_add
from noaa_new left join st_co_lookup_new
on state_ab=statecode and CZ_FIPS_new = COUNTY
where st_co_id_new is null;
quit;

proc sql;
create table temp2 as
select state_ab,state_name,county_name ,CZ_FIPS_new,st_co_id_new,
(select ST_CO_add from st_co_lookup_new where state_ab=statecode and CZ_FIPS_new = COUNTY
and st_co_id_new is null) as ST_CO_add
from noaa_new;
quit;
*check unique;
proc sql;
select st_co_id_new, count(*) as count
from temp2
group by st_co_id_new
having count >1;
quit;

data noaa_final(drop= state_name);
set temp2;
state_name_new = upcase(state_name);
if CZ_FIPS_new = "" then CZ_FIPS_new = ST_CO_add;
drop ST_CO_add;
run;

*the data still has some problem because the noaa table's st_co_id is not correct for some reason, check the following 
code which gives an example;
proc sql;
select *
from temp2
where st_co_id_new="24047";
quit;

*end of looking up missing id;

data narratives;
set "E:\Academic\6714\HW\sasshoot\2017-analytics-shootout-data-package\wildfire_narrative.sas7bdat";
run;
*new method;
data acre_damage;
set narratives;

 narr_new =  tranwrd(episode_narrative,"-","");
 narr_new_2 = compress(narr_new,",");

 pos2=find(narr_new,'acre','i');
 if 0< pos2 < 21 then num_acres2 = scan(substr(narr_new,1,pos2-1),-1,"");
 else if pos2= 0 then num_acres2 = "";
 else
 num_acres2=scan(substr(narr_new,pos2-20,20),-1,"");

 if event_id = . then delete;
run;

proc sql;
select episode_narrative,  pos2,num_acres2
from acre_damage
where pos2 >0 and num_acres2 = "";
quit;


data test;
set acre_damage;
area = input(num_acres2,comma12.);
drop  narr_new narr_new_2 pos2 num_areas2;
run;
*check where areas have been extraced fully;
proc sql;
select  episode_narrative, pos2,num_acres2,area
from test
where num_acres2  is not null and area is null;
quit;

*check the duplicates;
proc sql;
select EPISODE_ID, count(*) as count,
from test
group by EPISODE_ID
having count >1;
quit;
*event_id is unique no need to deal with duplicates;
proc sql;
select event_id, count(*) as count
from test
group by event_ID
having count >1;
quit;

*combine tables;

proc sql;
create table c1 as
select a.*, b.area
from wildfire as a, test as b
where a.EVENT_ID =b.EVENT_ID;
quit; 


proc sql;
create table c2 as
select  state,state_abbre,st_co_id,event_id,month_name, month(datepart(BEGIN_DATE_TIME)) as month,
        intck('dtday',BEGIN_DATE_TIME, END_DATE_TIME) as freq,area
from c1
where ST_CO_ID NE .;
quit;


proc sql;
create table c3 as
select distinct st_co_id,state,state_abbre,month_name,month, sum(area) as areas_total 
from c2
group by month_name,st_co_id;
quit;
libname sasshoot "E:\Academic\6714\HW\sasshoot\2017-analytics-shootout-data-package";
data final;
set sasshoot.final;
run;
proc sql;
create table c4 as
select a.*, b.areas_total
from final a, c3 as b
where a.ST_CO_ID=b.ST_CO_ID and a.STATE=b.STATE and a.month=b.month;
quit;
ods pdf file="E:\Academic\6714\HW\genmodel.pdf";
title"data set (obs = 20)";
proc print data = c4(obs =15);run;

ods pdf close;

data sasshoot.c4;
set c4;
run;





*trial;
data acre_damage;
set narratives;
 narr_new =  tranwrd(episode_narrative,"-","");

 pos2=find(episode_narrative,'acre','i');
 if 0< pos2 < 21 then num_acres2 = scan(substr(episode_narrative,1,pos2-1),-1,"");
 else if pos2= 0 then num_acres2 = "";
 else
 num_acres2=scan(substr(episode_narrative,pos2-20,20),-1,"");

 test = scan(num_acres2,1,dlm="-");

 pos3=find(episode_narrative,'-acre','i');
 if 0< pos3 < 21 then num_acres3 = scan(substr(episode_narrative,1,pos3-1),-1,"");
 else if pos3= 0 then num_acres3= "";
 else
 num_acres3=scan(substr(episode_narrative,pos3-20,20),-1,"");


 if event_id = . then delete;
run;

proc sql;
select episode_narrative,  pos3,num_acres3
from acre_damage
where pos3 >0 and num_acres3 = "";
quit;
proc sql;
select episode_narrative,  num_acres2, test, num_acres3
from acre_damage
where num_acres2 NE "" and num_acres3 NE "";
quit;

*trial;


data acre_damage;
set narratives;

 narr_new =  tranwrd(episode_narrative,"-","");
 narr_new_2 = compress(narr_new,",");

 pos2=find(narr_new_2,'acre','i');
 if 0< pos2 < 21 then num_acres2 = scan(substr(narr_new_2,1,pos2-1),-1,"");
 else if pos2= 0 then num_acres2 = "";
 else
 num_acres2=scan(substr(narr_new_2,pos2-22,22),-1,"");/*-1 causes some problems*/
 
 if findw(num_acres2,'hundred')>0 or findw(num_acres2,'thousand')>0 then
 temp1 =  scan(substr(narr_new_2,pos2-22,22),-1,"");
 temp2 =  scan(substr(narr_new_2,pos2-22,22),-2,"");/*need to extract to number before hundred or thousand*/
 if event_id = . then delete;
run;
proc sql;
select *
from acre_damage
where temp2 is not null;
quit;

proc sql;
select episode_narrative,  pos2,num_acres2
from acre_damage
where pos2 >0 and num_acres2 = "";
quit;

data test;
set acre_damage;
area = input(num_acres2,comma12.);
run;

proc sql;
select  episode_narrative, pos2,num_acres2,area
from test
where num_acres2  is not null and area is null;
quit;
