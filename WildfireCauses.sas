libname tmp1 "E:\Academic\6714\HW\sasshoot\2017-analytics-shootout-data-package";
data tmp1.narratives;
set tmp1.wildfire_narrative;
run;


data tmp1.test0;
   set tmp1.narratives;
   format cause $8.;
   cause = "unknown";
   if find(EPISODE_NARRATIVE,"arson",'i') ge 1 then cause = "human";
   else if find(EPISODE_NARRATIVE,"lightning",'i') ge 1 then cause = "nature";
run;
proc freq data = tmp1.test0;
tables cause;
where cause = "unknown";
run;

data tmp1.test2;
   set tmp1.narratives;
   format cause $char30.;
   cause = "unknown";
   array nature{5} $ _temporary_  ("lightning","thunderstorm","humidity","warm","dry");
   do i = 1 to dim(nature);
       if find(EPISODE_NARRATIVE,nature{i},'i') ge 1 then cause = "nature";
   end;
   array human{10} $ _temporary_  ("arson","cigarette","human ignition","man-caused","car in flame","practice bomb","explosive targets"
   ,"power line","electrical","moving car");
   do i = 1 to dim(human);
       if find(EPISODE_NARRATIVE,human{i},'i') ge 1 then cause = "human";
   end;
run;
title"freq of cause";
proc freq data= tmp1.test2;
tables cause;
run;
*repeat this for event_narrative;

data tmp1.test3;
   set tmp1.test2;
    length cause2 $30.;
   cause2="unknown";  format cause2 $30.;informat cause2 $30.;
    array human{10} $ _temporary_  ("arson","cigarette","human ignition","man-caused","car in flame","practice bomb","explosive targets"
   ,"power line","electrical","moving car");
   array nature{5} $ _temporary_  ("lightning","thunderstorm","humidity","warm","dry");
 
   do i = 1 to dim(nature);
       if find(event_NARRATIVE,nature{i},'i') ge 1 then cause2 = "nature";
   end;
    do i = 1 to dim(human);
       if find(Event_NARRATIVE,human{i},'i') ge 1 then cause2 = "human";
   end;
run;

data tmp1.test4;
set tmp1.test3;
if cause = "unknown" and cause2 ne "unknown" then cause = cause2;
run;


proc freq data= tmp1.test4;
tables cause;
run;

*combine cause with wildfire data set as dt6_1;
proc sql;
create table tmp1.dt6_1 as
select b.event_id,State,state_abbre,CZ_NAME,st_co_id_new,month(datepart(BEGIN_DATE_TIME)) as month,BEGIN_DATE_TIME,a.cause
from tmp1.test4 as a, tmp1.wildfire as b
where a.event_id = b.event_id and cause ne "unknown" and st_co_id_new ne "";
quit;

data tmp1.dt6_2;
set tmp1.dt6_1;
drop st_co_id_new;
st_co_id = input(st_co_id_new,5.);
run;

*combine dt6_2 with drought severity;
proc sql;
create table tmp1.temp1 as
select *,month(validStart) as month
from tmp1.drought_severity;

create table tmp1.drought_new as
select ST_CO_ID, month,mean(NONE)as none_mean,mean(D0_D4) as D0_mean,mean(D1_D4) as D1_mean ,mean(D2_D4) as D2_mean,
       mean(D3_D4) as D3_mean,mean(D4) as D4_mean
from tmp1.temp1
group by ST_CO_ID, month ;
quit;

proc sql;
create table tmp1.dt6_3 as
select a.*, none_mean,D0_mean, D1_mean, D2_mean, D3_mean, D4_mean
from tmp1.dt6_2 as a left join tmp1.drought_new as b
on  a.ST_CO_ID=b.ST_CO_ID and b.month = a.month ;
quit;

*combine dt6_3with geography;
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
%import(LandCover_by_County);
data tmp1.LandCover_by_County_new;
set LandCover_by_County;
a11=input(_11,percent8.);
a12=input(_12,percent8.);
a21=input(_21,percent8.);
a22=input(_22,percent8.);
a23=input(_23,percent8.);
a24=input(_24,percent8.);
a31=input(_31,percent8.);
a41=input(_41,percent8.);
a42=input(_42,percent8.);
a43=input(_43,percent8.);
a52=input(_52,percent8.);
a71=input(_71,percent8.);
a81=input(_81,percent8.);
a82=input(_82,percent8.);
a90=input(_90,percent8.);
a95=input(_95,percent8.);
run;
proc sql;
create table tmp1.dt6_4 as
select a.*, a11,a12,a21,a22,a23,a24,a31,a41,a42,a43,a52,a71,a81,a82,a90,a95
from tmp1.dt6_3 as a left join LandCover_by_County_new as b
on a.ST_CO_ID = b.ST_CO_ID;
quit;

*combine dt6_4 with population;

*popultaion;
%import(population_by_county_2001_2009);
%import(population_by_county_2010_2015);
data population_2001_2009;
set population_by_county_2001_2009;
temp=compress(County_name,,'p');
County_name_new=scan(temp,1);
a2001=input(_7_1_2001,comma11.);
a2002=input(_7_1_2002,comma11.);
a2003=input(_7_1_2003,comma11.);
a2004=input(_7_1_2004,comma11.);
a2005=input(_7_1_2005,comma11.);
a2006=input(_7_1_2006,comma11.);
a2007=input(_7_1_2007,comma11.);
a2008=input(_7_1_2008,comma11.);
a2009=input(_7_1_2009,comma11.);
run;

data population_2010_2015;
set population_by_county_2010_2015;
county=scan(Geography,1);
state=scan(Geography,-1);
run;

proc sql;
create table population as
select a2001,a2002,a2003,a2004,a2005,a2006,a2007,a2008,a2009,_7_1_2010 as a2010, _7_1_2011 as a2011,_7_1_2012 as a2012,_7_1_2013 as a2013,
       _7_1_2014 as a2014,_7_1_2015 as a2015,b.county,b.state,b.id2 as ST_CO_ID
from population_2001_2009 as a,population_2010_2015 as b
where a.State = b.State and a.County_name_new=b.County;

quit;

proc sort data = population;
by state county ST_CO_ID;
proc transpose data = population NAME=year
                 out = population_t(rename=(col1=population));
	by state county ST_CO_ID;
run;
data tmp1.population_t;
set population_t;
keep state country ST_CO_ID year_new population;
year_new=input(substr(year,2,4),8.);
run;

data tmp1.dt6_4;
set tmp1.dt6_4;
year = year(datepart(BEGIN_DATE_TIME));
run;

proc sql;
create table tmp1.dt6_5 as
select a.*, b.population
from tmp1.dt6_4 as a left join tmp1.population_t as b
on a.st_co_id = b.ST_CO_ID and a.year=b.year_new;
quit;

*combine tmp1.dt6_5 with weather;
proc sql;
create table tmp1.weather as
select st_co_id,month(date) as month,mean(TMAX_F) as tmax_m,mean(TMIN_F) as _tmin_m,mean(PRCP_in) as prcp_m,
       mean(SNWD_in) as snwd_m,mean(SNOW_in) as snow_d
from tmp1.weather_station_data
group by st_co_id,month;
quit;

proc sql;
create table tmp1.dt6_6 as
select a.*,tmax_m ,_tmin_m,prcp_m,snwd_m,snow_d
from tmp1.dt6_5 as a, tmp1.weather as b
where a.ST_CO_ID=b.ST_CO_ID and b.month = a.month ;
quit;

*analysis;
*categorical var:month;
proc sql;
create table temp4 as
select month, cause, count(cause) as freq
from tmp1.dt6_6
group by month, cause;
quit;

axis2  label = ('count of cause');
axis1  label = ('month')  ;
symbol1 i = smooth  value  = dot   c = black;
symbol2 i = smooth  value  = plus   c = red;
symbol3 i = smooth  value  = square   c = blue;
legend1 position = bottom ;

proc gplot data = temp4;
title"cause freq by month";
plot freq*month=cause;
run;


proc freq data = tmp1.dt6_6;
tables month*cause/chisq expected;
title "chisq of cause and month";
run;

data table3_grouped;
set tmp1.dt6_6;
format month_gp $8.;
if month in (12,1,2) then
month_gp = "q4";
else if month in (3,4,5) then
month_gp = "q1";
else month_gp = put(month,$8.);
run;
proc freq data = table3_grouped;
tables month_gp*cause/chisq expected;
title "chisq of cause and month";
run;

*significant after grouping month;

*drought analysis;
proc ttest data= tmp1.dt6_6;
class cause;
var none_mean D0_mean D1_mean D2_mean D3_mean D4_mean ;
run;
data temp;
set tmp1.dt6_6;
d3_ln=log(D3_mean);
d4_ln=log(D4_mean);
drop D3_mean D4_mean;
run;
proc ttest data= temp;
class cause;
var d3_ln d4_ln ;
run;
*land_cover;
ods output "Statistics" = stats
           "T-Tests" = ttests
           "EquivTests" = equivtest;;
proc ttest data= tmp1.dt6_6;
  class cause;
  var a11 ;
  run;
ods output close ;







proc ttest data= tmp1.dt6_6;
class cause;
var a11 a12 a21 a22 a23 a24 a31 a41 a42 a43 a52 a71 a81 a82 a90 a95 ;
run;


*need to write a macro to report the p-value;
ods graphics on;
   proc npar1way data=tmp1.dt6_6 wilcoxon median 
         plots=(wilcoxonboxplot medianplot);
 class cause;
var a11 a12 a21 a22 a23 a24 a31 a41 a42 a43 a52 a71 a81 a82 a90 a95 ;
run;


*population;
data temp;
set tmp1.dt6_6;
population_ln=log(population);
drop population;
run;
proc ttest data= temp;
class cause;
var population_ln ;
run;
*no relation found;
*weather;

proc ttest data= tmp1.dt6_6;
class cause;
var tmax_m  _tmin_m prcp_m snwd_m snow_d
 ;
run;
data dt6_6;
set tmp1.dt6_6;
run;

data temp;
set dt6_6;
prcp_ln=log(prcp_m);
snwd_ln=log(snwd_m+0.001);
snow_ln = log(snow_d+0.001);
drop prcp_m snwd_m snow_d;
run;

proc ttest data= dt6_6;
class cause;
var tmax_m  _tmin_m prcp_m snwd_m snow_d
 ;
run;
   proc npar1way data=dt6_6 wilcoxon median 
         plots=(wilcoxonboxplot medianplot);
 class cause;
var snwd_m snow_d;
run;

data cause_final;
set tmp1.dt6_6;
drop D1_mean D3_mean D2_mean D4_mean a12 a22 a24 a43 a81 a82 year population STATE state_abbre state_abbre CZ_NAME BEGIN_DATE_TIME ;
d1_mean_ln=log(D1_mean);
d3_mean_ln=log(D3_mean);
run;


data cause_final;
set cause_final;
format month_gp $8.;
drop month;
if month in (12,1,2) then
month_gp = "q4";
else if month in (3,4,5) then
month_gp = "q1";
else month_gp = put(month,$8.);
run;
proc print data = cause_final(obs=10);
run;




*plots;

axis2  label = ('count of cause');
axis1  label = ('drought_none_mean')  ;
symbol1 i = smooth  value  = dot   c = black;
symbol2 i = smooth  value  = plus   c = red;
symbol3 i = smooth  value  = square   c = blue;
legend1 position = bottom ;

proc gplot data = cause_final;
title"cause freq by month";
plot freq*mon
