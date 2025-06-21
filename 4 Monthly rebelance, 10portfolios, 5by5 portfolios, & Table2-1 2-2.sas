libname dvd2 'E:\Users\essay2';
OPTIONS USER=DVD2;

libname dvd3 'E:\Users\essay3';

proc import  datafile="F:\users\estidio - 202012.CSV"   out=estidio dbms=CSV  replace;  guessingrows=10000; getnames=YES;run;
proc import  datafile="F:\users\shortable - 202012.CSV"   out=shortable dbms=CSV  replace;  getnames=YES;run;
proc import datafile="F:\users\monthlyambeco - 202012 - 50 - CH4.CSV"   out=ambeco dbms=csv  replace;  run;
proc import  datafile="F:\users\stock202004.CSV"   out=arirawadd0001 dbms=CSV  replace;  guessingrows=10000; getnames=YES;run;
proc import  datafile="F:\users\stock202005-12.CSV"   out=arirawadd0002 dbms=CSV  replace;  guessingrows=10000; getnames=YES;run;
data arirawadd;
set arirawadd0001 arirawadd0002;run;
proc delete data=arirawadd0001 arirawadd0002;run;

proc import datafile="F:\users\monthlyambstock count 8pct 202009.CSV"   out=amballraw00001 dbms=csv  replace;  run;
proc import datafile="F:\users\monthlyambstock count 8pct 202012.CSV"   out=amballraw00002 dbms=csv  replace;  run;
data amballraw00002;set amballraw00002;if date1>164;run;
data amballraw;set amballraw00001 amballraw00002;run;
proc delete data=amballraw00001 amballraw00002;run;


proc import  datafile="F:\users\stockmore202004.CSV"   out=ariraw00001 dbms=CSV  replace;  guessingrows=10000; getnames=YES;run;
proc import  datafile="F:\users\stockmore202005-12.CSV"   out=ariraw00002 dbms=CSV  replace;  guessingrows=10000; getnames=YES;run;/*CSMAR attribute not complete*/
data ariraw;set ariraw00001 ariraw00002;run;
proc delete data=ariraw00001 ariraw00002;run;



%macro importonefile(filename,tblname);

/*proc import  datafile="F:\users\&filename..csv"   out=&tblname. dbms=dlm replace;
delimiter=",";run;*/
data &tblname.;
infile "F:\users\&filename..txt" delimiter = '09'x Missover Dsd lrecl=32767 firstobs=2;
Format Trddt $10.;
Informat Trddt $10.;
Input Stkcd $ Trddt $ Dsmvosd Dsmvtll Dretwd ;
run;

%mend;

%macro importallfile();
%importonefile(TRD_Daly,stkret);

%do i=1 %to 9;
%importonefile(TRD_Daly (&i.),stkret&i.);

proc append data=stkret&i. base=stkret;
RUN;
%end;

%mend;

%importallfile();

data stkret10;
infile "F:\users\TRD_Dalyr (10).txt" delimiter = '09'x Missover Dsd lrecl=32767 firstobs=2;
Format Trddt $10.;
Informat Trddt $10.;
Input Stkcd $ Trddt $ Dsmvosd Dsmvtll Dretwd ;
run;

proc append data=stkret10 base=stkret;
RUN;

data stkret11;
infile "F:\users\TRD_Dalyr (11).txt" delimiter = '09'x Missover Dsd lrecl=32767 firstobs=2;
Format Trddt $10.;
Informat Trddt $10.;
Input Stkcd $ Trddt $ Dsmvosd Dsmvtll Dretwd ;
run;

proc append data=stkret11 base=stkret;
RUN;
proc delete data=stkret1-stkret11;
run;

DATA stkret ;
set stkret;
format date yymmdd10.;
date=input(compress(trddt,'-'),yymmdd10.);
drop Trddt ;
RUN;

data stkret2;
set stkret;
yearmonth=year(date)*100+month(date);
run;


proc sort data=stkret2;by stkcd date ;
run;

proc means data=stkret2  noprint; 
var Dretwd;by STKCD yearmonth ;
output out=TRD_stk_summary_data N=N mean=mean std=std  min=min p25=p25 median=median p75=p75 max=max; 
run;




Proc Import Out=TRD_index1
Datafile="F:\users\IDX_Idxtrd20072020.xlsx"
Dbms=Excel Replace;Sheet="Sheet1";Getnames=Yes;Mixed=No;Scantext=Yes;Usedate=Yes;Scantime=Yes;
Run;

proc means data=TRD_index1 noprint; 
var Idxtrd08;
by month;
output out=TRD_index1_summary_data N=N mean=mean std=std  min=min p25=p25 median=median p75=p75 max=max; 
run;


Proc Import Out=TRD_Co
Datafile="F:\users\TRD_Co - 202012.xlsx"
Dbms=Excel Replace;Sheet="Sheet1";Getnames=Yes;Mixed=No;Scantext=Yes;Usedate=Yes;Scantime=Yes;
Run;

data TRD_Co;
SET TRD_Co;
format DATE yymmdd10.;
DATE=input(compress(Listdt,"-"),yymmdd10.);
date1=(year(DATE)-2007)*12+month(DATE)-1;
if date1>=1;
stkcd2=input(stkcd,BEST12.);
run;

proc import  datafile="F:\SASdata\fivedataalldui - 202012.csv"   out=fivedataall dbms=CSV  replace;  getnames=YES;run;


data dailyms;
set fivedataall;
format timetime yymmdd10.;
miu=fivemiu*countobs;
std=fivestd*sqrt(countobs);
drop _;
timetime=input(compress(ymd," "),yymmdd10.);
date1=(int(ym/100)-2007)*12+mod(ym,100)-1;
if exstock="SH000001" THEN DELETE;
if exstock="SH700001" THEN DELETE;
run;


proc sql;
create table dailymsma as
select dailyms.*,industry,ifs,ifst 
FROM dailyms LEFT OUTER join ariraw on dailyms.date1=ariraw.date1 and dailyms.stkcd=ariraw.code;
quit;

proc sql;/* MONTH*/
create table dailyms1 as
select dailymsma.*,date
FROM dailymsma LEFT OUTER join Trd_co on dailymsma.date1=Trd_co.date1 and dailymsma.stkcd=Trd_co.STKCD2;
quit;

proc sql;
create table dailyms2 as
select dailymsma.*,date
FROM dailymsma LEFT OUTER join Trd_co on dailymsma.TIMEtime=Trd_co.date and dailymsma.stkcd=Trd_co.STKCD2;
quit;

data dailyms2;
SET dailyms2;
*if ym>201003;
IF DATE=.;RUN;


/*
data dailywwms2;
set dailyms2;
if ifs~=. then delete;
run;*/    /*只有几个2018年12月的 不要紧*/

data dailyms2;
set dailyms2;
if (ifs=0 and ifst=0);
run;


data dailyms2;
set dailyms2;
IF (INDUSTRY='银行' or INDUSTRY='非银' or INDUSTRY= '非银行金融') then delete;
run; 

proc sort data=dailyms2 ; 
*by descending miu;
by  miu;
run;

data dailyms2sort2 ; 
set dailyms2 ; 
if _N_<50;
run;

proc sort data=dailyms2 ; 
by descending std;
*by  miu;
run;

data dailyms2sort3 ; 
set dailyms2 ; 
if _N_<50;
run;


proc sort data=dailyms2 ; 
by descending miu;
*by  miu;
run;

data dailyms2sort ; 
set dailyms2 ; 
if _N_<50;
run;



data dailyms2_cat ; 
set dailyms2 ; 
if date1>=39 then rq=1;
if date1<39 then rq=0;
run;

proc sort data=dailyms2_cat ; 
by rq date1 stkcd;
*by  miu;
run;

proc means data=dailyms2_cat noprint; 
var countobs;
by rq;
output out=five_summary_data_n_cat N=N mean=mean std=std  min=min p25=p25 median=median p75=p75 max=max; 
run;

proc means data=dailyms2_cat noprint; 
var miu;
by rq;
output out=five_summary_data_cat N=N mean=mean std=std  min=min p25=p25 median=median p75=p75 max=max; 
run;

proc means data=dailyms2_cat noprint; 
var std;
by rq;
output out=five_summary_data_std_cat N=N mean=mean std=std  min=min p25=p25 median=median p75=p75 max=max; 
run;

data five_summary_L_cat;
set five_summary_data_n_cat five_summary_data_cat five_summary_data_std_cat;
run;

*proc export data=five_summary_data_std outfile="E:\Users\export\five_summary_data_std.csv" dbms=csv  replace;  run; 
proc export data=five_summary_L_cat outfile="E:\Users\export\five_summary_data_LIANG.csv" dbms=csv  replace;  run; 

proc delete data=five_summary_L_cat five_summary_data_n_cat five_summary_data_cat five_summary_data_std_cat dailyms2_cat ;run;





proc means data=dailyms2 noprint; 
var countobs;
output out=five_summary_data_n N=N mean=mean std=std  min=min p25=p25 median=median p75=p75 max=max; 
run;

proc means data=dailyms2 noprint; 
var miu;
output out=five_summary_data N=N mean=mean std=std  min=min p25=p25 median=median p75=p75 max=max; 
run;

proc means data=dailyms2 noprint; 
var std;
output out=five_summary_data_std N=N mean=mean std=std  min=min p25=p25 median=median p75=p75 max=max; 
run;

data five_summary;
set five_summary_data_n five_summary_data five_summary_data_std;
run;

*proc export data=five_summary_data_std outfile="E:\Users\export\five_summary_data_std.csv" dbms=csv  replace;  run; 
proc export data=five_summary outfile="E:\Users\export\five_summary_data_222_one.csv" dbms=csv  replace;  run; 

data _null_; call symput('lagn',5); 
run;
%put &lagn;

data estidioym;
SET estidio;
format trdym2 yymmn6.;
trdym2=input(compress(yearmonth,""),yymmn6.);
date1=(year(trdym2)-2007)*12+month(trdym2)-1;
run;
DATA estidioym;
SET estidioym;
DATE0=DATE1+1;
RUN;

/*
Proc Import Out= CHN_Stkmt_dunderlyings0
Datafile="F:\users\CHN_Stkmt_dunderlyings.xlsx"
Dbms=Excel Replace;Sheet="Sheet1";Getnames=Yes;Mixed=No;Scantext=Yes;Usedate=Yes;Scantime=Yes;
Run;

Proc Import Out= CHN_Stkmt_dunderlyings1
Datafile="F:\users\CHN_Stkmt_dunderlyings1.xlsx"
Dbms=Excel Replace;Sheet="Sheet1";Getnames=Yes;Mixed=No;Scantext=Yes;Usedate=Yes;Scantime=Yes;
Run;

Proc Import Out= CHN_Stkmt_dunderlyings2
Datafile="F:\users\CHN_Stkmt_dunderlyings2.xlsx"
Dbms=Excel Replace;Sheet="Sheet1";Getnames=Yes;Mixed=No;Scantext=Yes;Usedate=Yes;Scantime=Yes;
Run;

Proc Import Out= CHN_Stkmt_dunderlyings3
Datafile="F:\users\CHN_Stkmt_dunderlyings3.xlsx"
Dbms=Excel Replace;Sheet="Sheet1";Getnames=Yes;Mixed=No;Scantext=Yes;Usedate=Yes;Scantime=Yes;
Run;

data CHN_Stkmt_dunderlyings;
set CHN_Stkmt_dunderlyings0;run;

proc append data= CHN_Stkmt_dunderlyings1 base=CHN_Stkmt_dunderlyings;run;
proc append data= CHN_Stkmt_dunderlyings2 base=CHN_Stkmt_dunderlyings;run;
proc append data= CHN_Stkmt_dunderlyings3 base=CHN_Stkmt_dunderlyings;run;


data shortable(keep=Mtdate date yearmonth date1 stock Shortavailable);
set CHN_Stkmt_dunderlyings;
format date yymmdd10.;
date=input(compress(Mtdate,'-'),yymmdd10.);
yearmonth=year(date)*100+month(date);
stock=stockcode;
date1=(year(date)-2007)*12+month(date)-1;
run;

proc sort data=shortable;
  by yearmonth stock date;
run;

data shortable; 
set shortable ; 
by yearmonth stock; 
if last.stock=1 ; 
run;*/

/*proc export data=SHORTABLE outfile="E:\Users\export\shortable - 202012.csv" dbms=csv  replace;  run;*/
proc sql;
create table amball2_gd as
select amballraw.*,industry,ifs,ifst 
FROM amballraw LEFT OUTER join ariraw on amballraw.date1=ariraw.date1 and amballraw.stock=ariraw.code;
quit;

proc sql;/* MONTH*/
create table amball2 as
select amball2_gd.*,Trd_co.date as listdate
FROM amball2_gd LEFT OUTER join Trd_co on amball2_gd.date1=Trd_co.date1 and amball2_gd.stock=Trd_co.STKCD2;
quit;


data amball2;
set amball2;
if listdate=.;
run;

data amball2;
set amball2;
if (ifs=0 and ifst=0);
DROP listdate;
run;


data amball2;
set amball2;
IF (INDUSTRY='银行' or INDUSTRY='非银' or INDUSTRY= '非银行金融') then delete;
run; 



data amball;
set amball2;
IF countobs>10;
run;

data amball;
set amball;
date0=date1+1;
run;
/*删除bottom30*/

/*

data arirawadd_gd(keep=stock date1 Msmvosd Msmvttl);
set arirawadd;
stock=stkcd;
run;
proc sort data=amball; by stock date1;run;
proc sort data=arirawadd_gd; by stock DATE1;run;

data ambsize;
 merge amball(in=in1) arirawadd_gd(in=in2);
 by stock  date1;
 if in1=1;
run;

data amball;
SET ambsize;
IF Msmvosd =. then delete;
RUN;

proc delete data=ambsize arirawadd_gd;run;

proc sort data=amball;
  by date1 stock;
run;

proc univariate data=amball noprint;  
 var Msmvttl; by date1;   
output out=asize307 pctlpts =20 30 pctlpre=per;
run;

data aaa307;merge amball asize307; by date1;
if Msmvttl<=per20 then p3=0;
if  per20<Msmvttl<=per30 then p3=1;
if Msmvttl>per30 then p3=2;
run;

data amball(DROP=P3 per20 per30);
set aaa307;
if p3>=2;
run;

proc delete data=aaa307;
run;
*/

/*bottom end*/

proc sort data=amball;
  by date1 stock;
run;
proc rank data=amball
out=aaa10 groups=10;
  ranks p;
  var ambiguity;
  by date1;
run;


proc rank data=amball
out=aaa4 groups=4;
  ranks p4;
  var ambiguity;
  by date1;
run;
proc rank data=amball
out=aaa5 groups=5;
  ranks p5;
  var ambiguity;
  by date1;

proc univariate data=amball noprint;  
 var ambiguity; by date1;   
output out=aaa3070 pctlpts = 30 70 pctlpre=per;
run;

data aaa3070;merge amball aaa3070; by date1;
if ambiguity<=per30 then p3=0;
if  per30<ambiguity<=per70 then p3=1;
if ambiguity>per70 then p3=2;
run;

data aaa3070;
set aaa3070;
DROP per30 per70;run;


data aaaallp;
merge aaa10(keep=date0 date1 stock p) aaa4(keep=date0 date1 stock p4)  aaa5(keep=date0 date1 stock p5) aaa3070(keep=date0 date1 stock p3);
by date0 date1 stock;run;

data aaa10;
merge aaa10 aaa5(keep=date0 date1 stock p5) ;
by date0 date1 stock;run;


proc sort data=amball;
  by date1;
run;

proc summary data=amball noprint; 
var stock;
by date1;
output out=stockamonthly  n=n ; 
run;

proc means data=stockamonthly noprint; 
var n;
output out=stockmonthlymean mean=stockmonthlymean; 
run;


proc sort data=amball;
  by year stock;
run;

proc means data=amball noprint; 
var ambiguity;
by year stock;
output out=stockannualmean mean=stockannualmean std=std  p25=p25 median=median p75=p75 max=max; 
run;

proc means data=stockannualmean noprint; 
var stockannualmean;
by year;
output out=by_year_summary_data N=N mean=mean std=std  min=min p25=p25 median=median p75=p75 max=max; 
run;


DATA amball_twostage;
set amball;
if date1<=38 then stage=1;
if date1>38 then stage=2;
run;

proc sort data=amball_twostage;
  by stage stock;
run;

proc means data=amball_twostage noprint; 
var ambiguity;
by stage stock;
output out=stockannualmean_stage mean=stockannualmean std=std  p25=p25 median=median p75=p75 max=max; 
run;

data stockannualmean_stage1; 
set stockannualmean_stage; 
if _FREQ_>20;
IF STAGE=1;RUN;
data stockannualmean_stage2; 
set stockannualmean_stage; 
if _FREQ_>40;
IF STAGE=2;RUN;

data stockannualmean_stage; 
set stockannualmean_stage1 stockannualmean_stage2; 
RUN;


proc means data=stockannualmean_stage noprint; 
var stockannualmean;
by stage;
output out=by_year_summary_data_stage N=N mean=mean std=std  min=min p25=p25 median=median p75=p75 max=max; 
run;

proc sort data=amball;
  by ambiguity date1 stock;
run;

proc sort data=amball;
  by  date1 stock;
run;


proc means data=amball noprint; 
var ambiguity;
by date1;
output out=monthly_crosssec_mean mean=monthly_crosssec_mean std=std  min=min p25=p25 median=median p75=p75 max=max; 
run;


proc univariate data=amball noprint; 
var ambiguity;
by date1;
output out=pct95band std=std  mean=mean pctlpre=p  pctlpts=2.5 97.5;
run;

proc means data=monthly_crosssec_mean noprint; 
var monthly_crosssec_mean;
output out=one_line_summary mean=mean std=std min=min  p25=p25 median=median p75=p75 max=max; 
run;


proc means data=monthly_crosssec_mean noprint; 
var _FREQ_;
output out=countstocks mean=mean std=std min=min  p25=p25 median=median p75=p75 max=max; 
run;

data amball_new45(keep=yearmonth stock ambiguity);
SET amball;
yearmonth=year*100+month;
RUN;

data Trd_stk_summary_data2(keep=stock yearmonth std);
set Trd_stk_summary_data;
stock=stkcd+0;
run;

proc SORT data=amball_new45; 
by stock  yearmonth;RUN;

data ambsize_std;
 merge  amball_new45(in=in1) Trd_stk_summary_data2(in=in2);
 by stock  yearmonth;
 if in1=1;
run;

proc SORT data=ambsize_std; 
by yearmonth;RUN;


proc means data=ambsize_std noprint; 
var std;
by yearmonth;
output out=monthly_std_mean mean=monthly_std_mean std=std  min=min p25=p25 median=median p75=p75 max=max; 
run;

proc corr data=ambsize_std; 
  var ambiguity;
  with std;
 ods output PearsonCorr = amb_std_Corr2;
run;



proc corr data=ambsize_std  noprint out=amb_std_Corr; 
  var ambiguity;
  with std;
  by yearmonth;
run;

data amb_std_Corr; 
set amb_std_Corr; 
if  _TYPE_="CORR";
run;

proc SORT data=amb_std_Corr; 
by ambiguity;RUN;


proc means data=amb_std_Corr NOPRINT;
var ambiguity;
output out=amb_std_Corr_tsMEAN mean=meancorr_cs_ts median=med max=max std=std min=min ;
WHERE _TYPE_="CORR";
run;


/*ts-cs 顺序变换*/


proc SORT data=ambsize_std; 
by stock;RUN;

proc corr data=ambsize_std  noprint out=amb_std_Corr; 
  var ambiguity;
  with std;
  by stock;
run;

data amb_std_Corr2; 
set amb_std_Corr; 
if  _TYPE_="CORR";
run;

data amb_std_Corr3; 
set amb_std_Corr; 
if  _TYPE_="N";
RENAME ambiguity=N;
drop _NAME_;
run;

DATA amb_std_Corr;merge amb_std_Corr2 amb_std_Corr3; by stock;
IF N>80;
run;


proc means data=amb_std_Corr NOPRINT;
var ambiguity;
output out=amb_std_Corr_tsMEAN median=med mean=meancorr_ts_cs max=max std=std min=min ;
*WHERE _TYPE_="CORR";
run;








proc export data=monthly_std_mean outfile="E:\Users\export\monthly_VOL_mean.csv" dbms=csv  replace;  run;



proc export data=pct95band outfile="E:\Users\export\pct95band.csv" dbms=csv  replace;  run;
proc export data=stockmonthlymean outfile="E:\Users\export\stockmonthlymean.csv" dbms=csv  replace;  run;
proc export data=by_year_summary_data outfile="E:\Users\export\1_by_year_summary_data1.csv" dbms=csv  replace;  run;
proc export data=by_year_summary_data_stage outfile="E:\Users\export\1_by_year_summary_data_stage.csv" dbms=csv  replace;  run;
proc export data=one_line_summary outfile="E:\Users\export\2_one_line_summary1.csv" dbms=csv  replace;  run;
/*年末*/

data arirawadd3(keep=stock date1 Msmvosd Msmvttl);
set arirawadd;
stock=stkcd;
run;
proc sort data=amball; by stock date1;run;
proc sort data=arirawadd3; by stock DATE1;run;
data ambsize;
 merge amball(in=in1) arirawadd3(in=in2);
 by stock  date1;
 if in1=1;
run;

data aaaambsize;
SET ambsize;
IF Msmvosd =.;
RUN;

proc sort data=ambsize;
  by date1 Msmvosd;
run;
proc rank data=ambsize
out=size5 groups=5;
  ranks s5;
  var Msmvosd;
  by date1;
run;

proc rank data=ambsize
out=size2 groups=2;
  ranks s2;
  var Msmvosd;
  by date1;
run;

proc rank data=size5
out=ranksizeamb groups=5;
  ranks s5_amb5;
  var ambiguity;
  by date1 s5;
run;

data ranksizeamb;
set ranksizeamb;
ranksizeamb=s5*5+s5_amb5;
run;

proc univariate data=size2 noprint;  
 var ambiguity; by date1 s2;   
output out=aasize3070 pctlpts = 30 70 pctlpre=per;
run;

data aasize3070;merge size2 aasize3070 ; by date1;
if ambiguity<=per30 then p3=0;
if  per30<ambiguity<=per70 then p3=1;
if ambiguity>per70 then p3=2;
run;

data aasize3070;
set aasize3070;
DROP per30 per70;run;


data aasize3070;
set aasize3070;
ranksizeamb6=s2*3+p3;
run;

/*
data yearend;
SET ambsize;
if month=12;RUN;

proc sort data=yearend; by year;run;
proc summary data=yearend;
var Msmvosd Msmvttl;
by YEAR;
output out=YEARvol  sum(msmvosd)=vol sum(Msmvttl)=volall;
run;

data YEARvol;
SET YEARvol;
pct=vol/volall;run;

proc export data=YEARvol outfile="E:\Users\export\YEARvol.csv" dbms=csv  replace;  run;
*/

data aaa10ym;
set aaa10;
yearmonth=year*100+month;
run;

/*idio*/
proc sql;
create table zidio as
select aaa10ym.*,_RMSE_,_EDF_
FROM aaa10ym LEFT OUTER join estidio on aaa10ym.yearmonth=estidio.yearmonth and aaa10ym.stock=estidio.stkcd;
quit;

data AAAAAAzidio;
set zidio; IF _RMSE_=.;
RUN;

proc sort data=zidio;
  by date1 stock;
run;
proc rank data=zidio
out=aidio10 groups=10;
  ranks i10;
  var _RMSE_;
  by date1;
run;

proc rank data=zidio
out=aidio5 groups=5;
  ranks i5;
  var _RMSE_;
  by date1;
run;

proc sort data=zidio;
  by date1 p5;
run;

proc rank data=zidio (drop=p)
out=rankambidio groups=5;
  ranks p5_idio5;
  var _RMSE_;
  by date1 p5;
run;

data rankambidio;
set rankambidio;
rankambidio=p5*5+p5_idio5;
run;

proc sort data=aidio5;
  by date1 i5;
run;

proc rank data=aidio5(drop=p)
out=rankidioamb groups=5;
  ranks i5_amb5;
  var ambiguity;
  by date1 i5;
run;

data rankidioamb;
set rankidioamb;
rankidioamb=i5*5+i5_amb5;
run;

proc sort data=rankambidio;by date0 date1 stock;RUN;
proc sort data=rankidioamb;by date0 date1 stock;RUN;
proc sort data=ranksizeamb;by date0 date1 stock;RUN;
proc sort data=aasize3070;by date0 date1 stock;RUN;
proc sort data=aidio5;by date0 date1 stock;RUN;

data xuhao;
merge aidio5(keep=date0 date1 stock i5) aidio10(keep=date0 date1 stock i10) aaa5(keep=date0 date1 stock p5)
rankambidio(keep=date0 date1 stock rankambidio) 
rankidioamb(keep=date0 date1 stock rankidioamb) 
ranksizeamb(keep=date0 date1 stock ranksizeamb)
aasize3070(keep=date0 date1 stock ranksizeamb6)
aaaallp;
by date0 date1 stock;run;

data xuhao;
SET xuhao;
RENAME P=P10;RUN;

/*先横再纵*/
proc sort data=zidio;by yearmonth;RUN;
proc corr data=zidio noprint out=Corr; 
  var ambiguity;
  with _RMSE_;
  by yearmonth;
run;


proc means data=Corr NOPRINT;
var ambiguity;
output out=CorrMEAN1 mean=meancorrts max=max std=std min=min ;
WHERE _TYPE_="CORR";
run;

/*全部*/
proc corr data=zidio ; 
  var ambiguity;
  with _RMSE_;
 ods output PearsonCorr = Corrall;
run;

/*先stock*/
proc sort data=zidio;by stock;RUN;
proc corr data=zidio out=Corr2 noprint ; 
  var ambiguity;
  with _RMSE_;
  by stock; 
run;


data corr2;
SET corr2;WHERE _TYPE_="CORR";
DROP _TYPE_;
run;

proc means data=zidio noprint; 
  var _RMSE_;
  by stock;
  output out=count mean=mean n=n;
run;

data SHAIXUAN;
merge Corr2(in=in1) COUNT(in=in2);
 by stock ;
IF IN1=IN2=1;
IF N>12;
RUN;
proc means data=shaixuan NOPRINT;
var ambiguity;
output out=CorrMEAN2 mean=meancorrcr max=max std=std min=min ;
run;


/*ariraw*/
data ariraw;
set ariraw;
if ifs=0;
run;

data ariraw;
set ariraw;
if ifst=0;
run;

data ariraw;
set ariraw;
IF (INDUSTRY='银行' or INDUSTRY='非银' or INDUSTRY= '非银行金融') then delete;
run; 


data ariraw;
set ariraw;
keep trdmnt Msmvosd mretwd date1 code;
run;

/*加rf*/
proc sql;
create table ariraw001 as
select ariraw.*,RiskPremium,rf,rf3,ret300,ret500,ret800,ret001 
FROM ariraw LEFT OUTER join ambeco on ariraw.date1=ambeco.date1 ;
quit;

/* 加shortable*/
proc sql;
create table ariraw002 as
select ariraw001.*,Shortavailable as sa
FROM ariraw001 LEFT OUTER join shortable on ariraw001.date1=shortable.date1 and ariraw001.code=shortable.stock;
quit;

data ariraw002;
SET ariraw002;
IF SA="Y" THEN shortable=1;
ELSE shortable=0;
DROP SA;
RUN;

data ariraw002;
set ariraw002; 
rif=mretwd*100-rf;
rif3=mretwd*100-rf3;
run;


/*加总市值*/
data arirawadd2(keep=code date1 Msmvttl);
set arirawadd;
rename stkcd=code;
run;

proc sort data=ariraw002; by code date1;run;
proc sort data=arirawadd2; by code date1;run;

data ariraw1guodu;
 merge ariraw002(in=in1) arirawadd2(in=in2);
 by code date1;
 if in1=1;
run;

data ariraw1;
 merge ariraw002(in=in1) arirawadd2(in=in2);
 by code date1;
 if in1=1;
run;

/*完成*/
/*uncamb*/
proc sql;
create table arirawguodu as
select ariraw1guodu.*, ambiguity 
FROM ariraw1guodu LEFT OUTER join amball on ariraw1guodu.date1=amball.date1 and ariraw1guodu.code= amball.stock;
quit;


OPTIONS USER=DVD2;
proc sort data=ariraw1; by code date1;run;

data acopn;
set ariraw1;
date0=date1+1;
run;

data ariraw1;
set ariraw1;
drop msmvosd Msmvttl;
run;


proc sql;
create table acopnewraw as
select ariraw1.*,acopn.msmvosd,Msmvttl
FROM ariraw1 LEFT OUTER join acopn on ariraw1.date1=acopn.date0 and ariraw1.code=acopn.code;
quit;

/*proc sort data=acopnewraw; by code date1;run;*/

data acopnewraw;
set acopnewraw;
if msmvosd=. then delete;
run;

data acopnewraw;
set acopnewraw;
if rif=. then delete;
run;

proc delete data=aaa3070 aaa4 aaa5 ariraw001 ariraw002 aidio10 aidio5;
run;


%macro ordinary();
proc sort data=acopnew10; by p date1;run;
proc summary data=acopnew10;
var mretwd;
by p date1;
output out=aportrawr mean=meanrr ;
run;
proc summary data=acopnew10;weight Msmvosd ;
var  mretwd;
by p date1;
output out=aportrawrvw mean=vwmeanrr;
run;

data araw;
merge aportrawr aportrawrvw ;
by p date1;
rawr=meanrr*100;
rawrvw=vwmeanrr*100;
drop _TYPE_ _FREQ_ meanrr vwmeanrr;
run;


proc summary data=araw;
var rawr rawrvw;
by p;
output out=aportmeanrr mean=meanrr meanrrvw ;
run;

data araw2018;
set araw;
IF date1<=(2018-2007)*12+12-1;
run;


proc summary data=araw2018;
var rawr rawrvw;
by p;
output out=aportmeanrr2018 mean=meanrr meanrrvw ;
run;


/*计算每个组合shortable*/

proc sort data=acopnew10;
by p date1;run;

proc summary data=acopnew10;
var shortable;
by p date1;
output out=shortable10  n=n sum=sum;
run;

data shortable10;
set shortable10;
pct=sum/n;if date1>38;
run;

proc sort data=shortable10;by p date1;run;

proc means data=shortable10  NOPRINT;
var  sum pct;
by p;
output out=aptfshortable2 mean=meansum meanpct max=maxsum maxpct std=stdsum stdpct median=medsum medpct ;
run;

proc sort data=acopnew10;
by p date1;run;
/*计算每个组合size*/

proc summary data=acopnew10;
var Msmvosd Msmvttl;
by p date1;
output out=aport10ptfvol  sum(msmvosd)=ptfvol sum(Msmvttl)=ptfvolall;
run;

proc univariate data=aport10ptfvol  NOPRINT;
var ptfvol ptfvolall;
by p;
output out=aptfvol10
mean=mean10ptfvol mean10ptfvolall;
run;

DATA aptfvol10;
SET aptfvol10;
meantradablebillion=mean10ptfvol/1000000; 
meanoutsbillion=mean10ptfvolall/1000000;
drop mean10ptfvol mean10ptfvolall;
run;

/*组内当期的ambiguity*/
proc sql;
create table acopnew10dqamb as
select acopnew10.*, ambiguity as ambdq
FROM acopnew10 LEFT OUTER join amball on acopnew10.date1=amball.date1 and acopnew10.code= amball.stock;
quit;



/*组内当期的idio*/

data acopnew10idio;
set acopnew10dqamb;run;

proc sql;
create table acopnew10idiodq as
select acopnew10idio.*, _RMSE_ as idio
FROM acopnew10idio LEFT OUTER join estidioym on acopnew10idio.date1=estidioym.date1 and acopnew10idio.code= estidioym.stkcd;
quit;


/*来一个分组、ts平均 上一期、当期amb idio*/
proc sort data=acopnew10idiodq;
by  p date1;run;

proc univariate data=acopnew10idiodq NOPRINT;
var lagamb ambdq lagidio idio;
by  p ;
output out=abetaidiobyp
mean=pmeanlagamb pmeanamb pmeanlagidio  pmeanidio;
run;

/*先by d date1 再*/
proc univariate data=acopnew10idiodq NOPRINT;
var lagamb ambdq lagidio idio;
by  p date1;
output out=abetaidio10
mean=meanlagamb meanamb meanlagidio  meanidio;
run;

proc univariate data=abetaidio10 NOPRINT;
var meanlagamb meanamb meanlagidio  meanidio;
by p;
output out=abetaidio
mean=lagamb amb lagidio  idio;
run;

data abetaidio10_2018;
set abetaidio10;
IF date1<=(2018-2007)*12+12-1;
run;

proc univariate data=abetaidio10_2018 NOPRINT;
var meanlagamb meanamb meanlagidio  meanidio;
by p;
output out=abetaidio_2018
mean=lagamb amb lagidio  idio;
run;


data mergeidio;
MERGE abetaidiobyp abetaidio ;
BY P;
RUN;


data merged;
MERGE aportmeanrr abetaidio(keep=p lagamb) Aptfvol10 aptfshortable2(keep=p meansum meanpct);
BY P;
drop _TYPE_;
RUN;
%MEND;




%macro alphapro(filename,param,n,db);

data ajoined10;
set &filename.;
run;

proc sort data=ajoined10;
by p;
run;
proc summary data=ajoined10;
var meanrif meanrif3 vwmeanrif vwmeanrif3;
BY P;
output out=xindeindex mean=meanrif meanrif3 vwmeanrif vwmeanrif3 ;
run;




ODS OUTPUT ParameterEstimates=Ap10z5;
proc model data=ajoined10  PLOTS=NONE;
	     endo meanrif;
         exog RiskPremium SMB HML RMW CMA;
         instruments _exog_;
         parms a5 b1 b2 b3 bRMW bCMA;
         meanrif=a5 + b1*RiskPremium + b2*SMB + b3*HML + bRMW*RMW + BCMA*CMA ;
         fit meanrif / gmm kernel=(bart,&lagn.,0) vardef=n ;
		 by p ;
         run;
         quit; 
ODS OUTPUT CLOSE;


ODS OUTPUT ParameterEstimates=Avwp10z5;
proc model data=ajoined10  PLOTS=NONE;
	     endo vwmeanrif;
         exog RiskPremium SMB HML RMW CMA;
         instruments _exog_;
         parms avw5 b1 b2 b3 bRMW bCMA;
         vwmeanrif=avw5 + b1*RiskPremium + b2*SMB + b3*HML + bRMW*RMW + BCMA*CMA ;
         fit vwmeanrif / gmm kernel=(bart,&lagn.,0) vardef=n ;
		 by p ;
         run;
         quit; 
ODS OUTPUT CLOSE;

ODS OUTPUT ParameterEstimates=Ap10z4;
proc model data=ajoined10  PLOTS=NONE;
	     endo meanrif;
         exog RiskPremium SMB4 HML4 UMD4;
         instruments _exog_;
         parms a4 b1 b2 b3 bUMD;
         meanrif=a4 + b1*RiskPremium + b2*SMB4 + b3*HML4 + bUMD*UMD4 ;
         fit meanrif / gmm kernel=(bart,&lagn.,0) vardef=n ;
		 by p ;
         run;
         quit; 
ODS OUTPUT CLOSE;


ODS OUTPUT ParameterEstimates=Avwp10z4;
proc model data=ajoined10  PLOTS=NONE ;
	     endo vwmeanrif;
         exog RiskPremium SMB4 HML4 UMD4;
         instruments _exog_;
         parms avw4 b1 b2 b3 bUMD;
         vwmeanrif=avw4 + b1*RiskPremium + b2*SMB4 + b3*HML4 + bUMD*UMD4 ;
         fit vwmeanrif / gmm kernel=(bart,&lagn.,0) vardef=n ;
		 by p ;
         run;
         quit; 
ODS OUTPUT CLOSE;

ODS OUTPUT ParameterEstimates=Ap10z3;
proc model data=ajoined10  PLOTS=NONE;
	     endo meanrif;
         exog RiskPremium SMB4 HML4;
         instruments _exog_;
         parms a3 b1 b2 b3;
         meanrif=a3 + b1*RiskPremium + b2*SMB4 + b3*HML4;
         fit meanrif / gmm kernel=(bart,&lagn.,0) vardef=n ;
		 by p ;
         run;
         quit; 
ODS OUTPUT CLOSE;


ODS OUTPUT ParameterEstimates=Avwp10z3;
proc model data=ajoined10  PLOTS=NONE;
	     endo vwmeanrif;
         exog RiskPremium SMB4 HML4;
         instruments _exog_;
         parms avw3 b1 b2 b3;
         vwmeanrif=avw3 + b1*RiskPremium + b2*SMB4 + b3*HML4;
         fit vwmeanrif / gmm kernel=(bart,&lagn.,0) vardef=n ;
		 by p ;
         run;
         quit; 
ODS OUTPUT CLOSE;

/* rif3 modified FF3*/

ODS OUTPUT ParameterEstimates=Ap10z3mod;
proc model data=ajoined10  PLOTS=NONE;
	     endo meanrif3;
         exog RiskPremium3 SMB3 HML3;
         instruments _exog_;
         parms a3mod b1 b2 b3;
         meanrif3=a3mod + b1*RiskPremium3 + b2*SMB3 + b3*HML3;
         fit meanrif3 / gmm kernel=(bart,&lagn.,0) vardef=n ;
		 by p ;
         run;
         quit; 
ODS OUTPUT CLOSE;


ODS OUTPUT ParameterEstimates=Avwp10z3mod;
proc model data=ajoined10  PLOTS=NONE ;
	     endo vwmeanrif3;
         exog RiskPremium3 SMB3 HML3;
         instruments _exog_;
         parms avw3mod b1 b2 b3;
         vwmeanrif3=avw3mod + b1*RiskPremium3 + b2*SMB3 + b3*HML3;
         fit vwmeanrif3 / gmm kernel=(bart,&lagn.,0) vardef=n ;
		 by p ;
         run;
         quit; 
ODS OUTPUT CLOSE;



ODS OUTPUT ParameterEstimates=ARp10z3mod;
proc model data=ajoined10  PLOTS=NONE;
	     endo meanrif3;
         instruments / intonly;
         parms R3;
         meanrif3=R3;
         fit meanrif3 / gmm kernel=(bart,&lagn.,0) vardef=n ;
		 by p ;
         run;
         quit; 
ODS OUTPUT CLOSE;


ODS OUTPUT ParameterEstimates=AvwRp10z3mod;
proc model data=ajoined10  PLOTS=NONE ;
	     endo vwmeanrif3;
         instruments / intonly;
         parms vwR3;
         vwmeanrif3=vwR3;
         fit vwmeanrif3 / gmm kernel=(bart,&lagn.,0) vardef=n ;
		 by p ;
         run;
         quit; 
ODS OUTPUT CLOSE;




/* rif3 modified CH FF4*/

ODS OUTPUT ParameterEstimates=Ap10z4mod;
proc model data=ajoined10  PLOTS=NONE;
	     endo meanrif3;
         exog RiskPremiumN4 SMBN4 HMLN4 PMON4;
         instruments _exog_;
         parms a4mod b1 b2 b3 b4;
         meanrif3=a4mod + b1*RiskPremiumN4 + b2*SMBN4 + b3*HMLN4 + b4*PMON4;
         fit meanrif3 / gmm kernel=(bart,&lagn.,0) vardef=n ;
		 by p ;
         run;
         quit; 
ODS OUTPUT CLOSE;


ODS OUTPUT ParameterEstimates=Avwp10z4mod;
proc model data=ajoined10  PLOTS=NONE ;
	     endo vwmeanrif3;
         exog RiskPremiumN4 SMBN4 HMLN4 PMON4;
         instruments _exog_;
         parms avw4mod b1 b2 b3 b4;
         vwmeanrif3=avw4mod + b1*RiskPremiumN4 + b2*SMBN4 + b3*HMLN4 + b4*PMON4;
         fit vwmeanrif3 / gmm kernel=(bart,&lagn.,0) vardef=n ;
		 by p ;
         run;
         quit; 
ODS OUTPUT CLOSE;




ODS OUTPUT ParameterEstimates=ARp10;
proc model data=ajoined10  PLOTS=NONE ;
	     endo meanrif;
         instruments / intonly;
         parms R;
         meanrif=R;
         fit meanrif / gmm kernel=(bart,&lagn.,0) vardef=n ;
		 by p ;
         run;
         quit; 
ODS OUTPUT CLOSE;


ODS OUTPUT ParameterEstimates=AvwRp10;
proc model data=ajoined10  PLOTS=NONE;
	     endo vwmeanrif;
         instruments / intonly;
         parms vwR;
         vwmeanrif=vwR;
         fit vwmeanrif / gmm kernel=(bart,&lagn.,0) vardef=n ;
		 by p ;
         run;
         quit; 
ODS OUTPUT CLOSE;


data Apset10;
set ARp10 Ap10z5 Ap10z4 Ap10z3 ARp10z3mod Ap10z3mod AvwRp10 Avwp10z5 Avwp10z4 Avwp10z3 AvwRp10z3mod Avwp10z3mod Ap10z4mod Avwp10z4mod ;;
RUN;

data apset10jian;
set apset10;
y=substr(Parameter,1,1);
if y='a' or y='v' or y='R';
drop EstType;
run;

%if &db="once" %then %do;
data apset10jian2;
set apset10jian;
if p=%eval(&n+1);
run;
proc export data=Apset10jian2 outfile="E:\Users\export\Apsetjian2m8-&file.&i.-&param.-bar&lagn..csv" dbms=csv  replace;  run;
%end;

proc export data=xindeindex outfile="E:\Users\export\Apxindem8-&file.&i.-&param.-bar&lagn..csv" dbms=csv  replace;  run;
proc export data=Apset10 outfile="E:\Users\export\Apsetm8-&file.&i.-&param.-bar&lagn..csv" dbms=csv  replace;  run;
proc export data=Apset10jian outfile="E:\Users\export\Apsetjianm8-&file.&i.-&param.-bar&lagn..csv" dbms=csv  replace;  run;

%mend;

%macro alpha(file,i,param,n,db);
%let np1=%eval(&n.+1);
%let nm1=%eval(&n.-1);

proc sql;
drop table ajoined10;
run;


proc sql;
create table acopnewaddp as
select acopnewraw.*,&PARAM. AS P
FROM acopnewraw LEFT OUTER join xuhao on acopnewraw.date1=xuhao.date0 and acopnewraw.code=xuhao.stock;
quit;

%macro convert(file,i);
%let index0=0;
%let index1=ret001;
%let index2=ret300;
%let index3=ret500;
%let index4=ret800;
data acopnewaddp_index;
set acopnewaddp;
rif=rif+rf-&&index&i.*100;
rif3=rif3+rf3-&&index&i.*100;
run;
%mend;
%convert(&file.,&i.);
/*加lagidio,lagamb*/
proc sql;
create table acopnew10 as
select &file..*,_RMSE_ as lagidio,_EDF_,ambiguity as lagamb
FROM &file.  LEFT OUTER join zidio on &file..date1=zidio.date0 and &file..code= zidio.stock;
quit;

/*subsample*/
/*DATA ACOPNEW10; SET ACOPNEW10;if date1>=40;RUN;*/

proc sort data=acopnew10; by code date1;run;

proc delete data=acopnewaddp ;
run;

data acopnew10;
set acopnew10;
if p=. then delete;
run;


/*rawreturn等*/

%ordinary();



%if &N.=10 %THEN %DO;
data afordifsize1;
SET aport10ptfvol ;
IF P=0 ;
ptfvol1=ptfvol/1000000;
keep date1 ptfvol1;
RUN;

data afordifsize2;
SET aport10ptfvol ;
IF P=9 ;
ptfvol10=ptfvol/1000000;
keep date1 ptfvol10;
RUN;

data afordifsize;
MERGE afordifsize1 afordifsize2;
by date1;
DIF=ptfvol10-ptfvol1;
run;

proc means data=afordifsize NOPRINT;
var DIF ptfvol10 ptfvol1;
OUTPUT OUT=sfe;
run;

ODS OUTPUT ParameterEstimates=aforsizep;
proc model data=afordifsize  PLOTS=NONE;
	     endo dif;
         instruments / intonly;
         parms size;
         dif=size;
         fit dif / gmm kernel=(bart,&lagn.,0) vardef=n ;
         run;
         quit; 
ODS OUTPUT CLOSE;
proc export data=sfe outfile="E:\Users\export\Asize-&file&i.-&param.-bar&lagn..csv" dbms=csv  replace;  run;
proc export data=aforsizep outfile="E:\Users\export\Asizep-&file&i.-&param.-bar&lagn..csv" dbms=csv  replace;  run;
%end;


proc export data=mergeidio outfile="E:\Users\export\idioandamb-&file&i.-&param.-bar&lagn..csv" dbms=csv  replace;  run;
proc export data=merged outfile="E:\Users\export\Aptf10-&file&i.-&param.-bar&lagn..csv" dbms=csv  replace;  run;
proc export data=aportmeanrr2018 outfile="E:\Users\export\Aptf_201810-&file&i.-&param.-bar&lagn..csv" dbms=csv  replace;  run;
proc export data=abetaidio_2018 outfile="E:\Users\export\Aidio_201810-&file&i.-&param.-bar&lagn..csv" dbms=csv  replace;  run;

/*start*/

/*计算每个组合形成期对应的检验期中，每一组的5只股票收益率的组内平均值*/
proc summary data=acopnew10;
var rif rif3;
by p date1;
output out=aport210 mean=meanrif meanrif3;
run;
proc summary data=acopnew10;weight Msmvosd ;
var rif rif3;
by p date1;
output out=aport310 mean=vwmeanrif vwmeanrif3;
run;

data arip10;
merge aport210 aport310;
by p date1;
drop _TYPE_ _FREQ_;
run;



proc sort data=arip10;
by date1 p;run;
proc transpose data=arip10 out=ariptr10 prefix=g;
    by date1;
    id p;
	var meanrif;
run;
proc transpose data=arip10 out=ariptr10vw prefix=g;
    by date1;
    id p;
	var vwmeanrif;
run;
%if &db.="once" %THEN %DO;

data ariptr10;
set ariptr10;
meanrif=g&nm1.-g0;
meanrif3=meanrif;
p=&np1.;
drop g0-g&nm1.;
run;



data ariptr10vw;
set ariptr10vw;
vwmeanrif=g&nm1.-g0;
vwmeanrif3=vwmeanrif;
p=&np1.;
drop g0-g&nm1.;
run;


data aripdif10;
merge ariptr10 ariptr10vw;
by date1 ;
keep p date1 meanrif meanrif3 vwmeanrif vwmeanrif3;
run;
data arip11;
set arip10 aripdif10;run;

%END;

%if &db.="double" %THEN %DO;
data ariptr10;
set ariptr10;
g25=g4-g0;
g26=g9-g5;
g27=g14-g10;
g28=g19-g15;
g29=g24-g20;
run;
data ariptr10vw;
set ariptr10vw;
g25=g4-g0;
g26=g9-g5;
g27=g14-g10;
g28=g19-g15;
g29=g24-g20;
run;


data arip1010;
merge ariptr10(drop=_NAME_) ariptr10vw;run;

data  arip11;
set arip10;
run;

data  arip&param.;
set arip10;
run;

%macro mac();
%do i=25 %to 29;
data tmp1(keep= p date1 meanrif meanrif3 );
set ariptr10;
meanrif3= g&i;
rename  g&i.=meanrif;
p=&i.;
run;

data tmp2(keep= p date1 vwmeanrif vwmeanrif3);
set ariptr10vw;
vwmeanrif3= g&i;
rename  g&i.=vwmeanrif;
p=&i.;
run;

data tmp;
merge tmp1 tmp2;
run;

proc append data=tmp base= arip11;run;
%end;
%mend;
%mac();
%END;


%if &db.="double6" %THEN %DO;
data ariptr10;
set ariptr10;
g6=g2-g0;
g7=g5-g3;
run;
data ariptr10vw;
set ariptr10vw;
g6=g2-g0;
g7=g5-g3;
run;


data arip1010;
merge ariptr10(drop=_NAME_) ariptr10vw;run;

data  arip11;
set arip10;
run;

data  arip&param.;
set arip10;
run;

%macro mac67();
%do i=6 %to 7;
data tmp1(keep= p date1 meanrif meanrif3 );
set ariptr10;
meanrif3= g&i;
rename  g&i.=meanrif;
p=&i.;
run;

data tmp2(keep= p date1 vwmeanrif vwmeanrif3);
set ariptr10vw;
vwmeanrif3= g&i;
rename  g&i.=vwmeanrif;
p=&i.;
run;

data tmp;
merge tmp1 tmp2;
run;

proc append data=tmp base= arip11;run;
%end;
%mend;
%mac67();
%END;

data arip11100;
set arip11;
if date1<2 then delete;
run;


proc sort data=arip11100;
by date1 p;run;
proc sql;
create table areal10 as
select arip11100.*,RiskPremium,SMB, HML, RMW, CMA,SMB4, HML4, UMD4,RiskPremium3,SMB3,HML3,RiskPremiumN4,SMBN4,HMLN4,PMON4,rf,rf3,ret300,ret500,ret800,ret001 
from arip11100 LEFT OUTER join ambeco on arip11100.date1=ambeco.date1;
quit;
data areal10;
set areal10;
if p<150;
run;

proc sort data=Areal10;
by p;
run;
/*

proc summary data=Areal10;
var meanrif meanrif3 vwmeanrif vwmeanrif3;
BY P;
output out=xindeINDEX0 mean=meanri meanri3 vwmeanri vwmeanri3 ;
run;*/

%alphapro(Areal10,&param.,&n.,&db.);
/*proc export data=Apset10jian2 outfile="E:\Users\export\Apset10jian222-m8-&filename.-bar&lagn..csv" dbms=csv  replace;  run;
*/
%mend;
/*%alpha(acopnewaddp,1,p10,10,"once");*/
/*
%alpha(acopnewaddp_index,1,p10,10,"once");
%alpha(acopnewaddp_index,2,p10,10,"once");
%alpha(acopnewaddp_index,3,p10,10,"once");




%alpha(acopnewaddp,1,rankambidio,29,"double");*/

%alpha(acopnewaddp,1,ranksizeamb,29,"double");
%alpha(acopnewaddp,1,rankidioamb,29,"double");





/* 10-8*/
/*
data ariptr10;
set ariptr10;
meanrif=g9-g7;
meanrif3=meanrif;
p=80;
drop g0-g9;
run;



data ariptr10vw;
set ariptr10vw;
vwmeanrif=g9-g7;
vwmeanrif3=vwmeanrif;
p=80;
drop g0-g9;
run;


data aripdif10;
merge ariptr10 ariptr10vw;
by date1 ;
keep p date1 meanrif meanrif3 vwmeanrif vwmeanrif3;
run;


ODS OUTPUT ParameterEstimates=ARp810vw;
proc model data=aripdif10  PLOTS=NONE;
	     endo vwmeanrif3;
         instruments / intonly;
         parms R13;
         vwmeanrif3=R13;
         fit vwmeanrif3 / gmm kernel=(bart,5,0) vardef=n ;
		 by p ;
         run;
         quit; 
ODS OUTPUT CLOSE;

*/

/*risk 比较*/

OPTIONS USER=DVD3;
data acopnew10idio;
SET DVD2.acopnew10idio;
RUN;

data Trd_stk_summary_data2(keep=stock yearmonth std year month date1 date0);
set DVD2.Trd_stk_summary_data;
stock=stkcd+0;
YEAR=INT(yearmonth/100);
month=yearmonth-year*100;
date1=(year-2007)*12+month-1;
date0=date1+1;
run;


data acopnew10idio;
SET DVD2.acopnew10idio;
RUN;


proc sql;
create table acopnew10idiodq as
select acopnew10idio.*, std as std
FROM acopnew10idio LEFT OUTER join trd_stk_summary_data2 on acopnew10idio.date1=trd_stk_summary_data2.date1 and acopnew10idio.code= trd_stk_summary_data2.stock;
quit;

proc sql;
create table acopnew10idiodqlag as
select acopnew10idioDQ.*, trd_stk_summary_data2.std as lagstd
FROM acopnew10idiodq LEFT OUTER join trd_stk_summary_data2 on acopnew10idiodq.date1=trd_stk_summary_data2.date0 and acopnew10idiodq.code= trd_stk_summary_data2.stock;
quit;



/*来一个分组、ts平均 上一期、当期amb idio*/
proc sort data=acopnew10idiodqLAG;
by code date1;run;

proc sort data=acopnew10idiodqLAG;
by p date1;run;

proc univariate data=acopnew10idiodqlag NOPRINT;
var lagamb ambdq lagstd std;
by  p ;
output out=abetaidiobyp
mean=pmeanlagamb pmeanamb pmeanlagidio  pmeanidio;
run;

/*先by d date1 再*/
proc univariate data=acopnew10idiodqlag NOPRINT;
var lagamb ambdq lagstd std;
by  p date1;
output out=abetaidio10
mean=meanlagamb meanamb meanlagidio  meanidio;
run;

proc univariate data=abetaidio10 NOPRINT;
var meanlagamb meanamb meanlagidio  meanidio;
by p;
output out=abetaidio10_std
mean=lagamb amb lagstd  std;
run;

proc export data=abetaidio10_std outfile="E:\Users\export\abeta10_std.csv" dbms=csv  replace;  run;
