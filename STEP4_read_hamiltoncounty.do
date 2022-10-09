************************************************************
*This file will read in and clean the Hamilton County court
*data

************************************************************

clear all
set more off
program drop _all

**Root directory
global root_dir "C:\Users\bscott00\Box\DUIAshlyn\ohio"

**Other directories
global raw_dir "$root_dir/raw_data"
global int_dir "$root_dir/intermediate_data"
global clean_dir "$root_dir/clean_data"

import delimited using "$raw_dir/courts_data/Hamilton_County_Scraped.csv"
save "$int_dir/courts_data/hamilton_county_data.dta", replace

*Drop empty variables
drop v1
drop municipalcasenumber
drop count0

*Rename and label variables
rename court cdi
label var cdi "court division indicator"
rename casenumber cas
label var cas "case number"
rename fileddate fda
label var fda "case file date"
rename casecaption cca
label var cca "case caption"
rename commonpleascasenumber cpn
label var cpn "common pleas case number"
rename casetype ctp
label var ctp "case type"
rename judge jdg
label var jdg "judge"
rename firstname def_fnam
label var def_fnam "defendant first name"
rename lastname def_lnam
label var def_lnam "defendant last name"
rename middlename def_mnam
label var def_mnam "defendant middle name"
rename race def_rac
label var def_rac "defendant race"
rename sex def_sex
label var def_sex "defendant sex"
rename dateofbirth def_dob
label var def_dob "defendant date of birth"
rename age def_age
label var def_age "defendant age (administratively recorded)"

local i = 1
foreach j in a b c d e f g h i j k l m n o p q r s t 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 {
	rename count`j' cnt_`i'
	label var cnt_`i' "count `i'"
	local ++i
}

local i = 1
foreach j in a b c d {
	rename bondamount`j' bam_`i'
	label var bam_`i' "bond amount `i'"
	rename disposition`j' disposition_`i'_full
	label var disposition_`i'_full "disposition `i' date and description"
	local ++i
}

order def_lnam def_fnam def_mnam def_dob def_age def_sex def_rac fda cca cas cpn ctp cdi jdg cnt_1 cnt_2 cnt_3 cnt_4 cnt_5 cnt_6 cnt_7 cnt_8 cnt_9 cnt_10 cnt_11 cnt_12 cnt_13 cnt_14 cnt_15 cnt_16 cnt_17 cnt_18 cnt_19 cnt_20 cnt_21 cnt_22 cnt_23 cnt_24 cnt_25 cnt_26 cnt_27 cnt_28 cnt_29 cnt_30 cnt_31 cnt_32 cnt_33 cnt_34 cnt_35 cnt_36 cnt_37 cnt_38 cnt_39 cnt_40 cnt_41 cnt_42 bam* disposition* notes

forvalues i = 1/42 {
	local i_plus = `i' + 1
	forvalues j = `i_plus'/42 {
		replace cnt_`i' = cnt_`j' if cnt_`i'==""
	}
}

****Clean names****
*Clean middle names
split def_mnam, gen(middlename) parse(",")
foreach p in [ ' ] {
	replace middlename1 = subinstr(middlename1,"`p'","",.)
	replace middlename2 = subinstr(middlename2,"`p'","",.)
}
replace middlename1 = trim(middlename1)
replace middlename2 = trim(middlename2)

drop def_mnam

*Pull out suffixes
gen court_suffix=""

forvalues i=1/2 {
	replace court_suffix="JR" if middlename`i'=="JR"
	replace middlename`i'="" if middlename`i'=="JR"
	replace court_suffix="JR" if middlename`i'=="JR."
	replace middlename`i'="" if middlename`i'=="JR."
	replace court_suffix="SR" if middlename`i'=="SR"
	replace middlename`i'="" if middlename`i'=="SR"
	replace court_suffix="II" if middlename`i'=="II"
	replace middlename`i'="" if middlename`i'=="II"
	replace court_suffix="III" if middlename`i'=="III"
	replace middlename`i'="" if middlename`i'=="III"
	replace court_suffix="IV" if middlename`i'=="IIII"
	replace middlename`i'="" if middlename`i'=="IIII"
	replace court_suffix="IV" if middlename`i'=="IV"
	replace middlename`i'="" if middlename`i'=="IV"
	replace court_suffix="V" if middlename`i'=="V"
	replace middlename`i'="" if middlename`i'=="V"
	replace court_suffix="VI" if middlename`i'=="VI"
	replace middlename`i'="" if middlename`i'=="VI"
	replace court_suffix="JR" if middlename`i'=="JR"
	replace middlename`i'="" if middlename`i'=="JR"
}

gen court_middle_initial = substr(middlename1,1,1)

* prep full_name var
/*
gen split = ustrpos(def_nam, ",") //show substr where the split is
gen last_name_raw = usubstr(def_nam, 1, split-1)
gen first_name_raw = usubstr(def_nam, split+1, .) //create first and last
*/
rename def_lnam last_name_raw
rename def_fnam first_name_raw

gen court_fullname = first_name_raw + " " + court_middle_initial + " " + last_name_raw
replace court_fullname = stritrim(court_fullname)

**Normalize capitilization and punctuation of names
replace last_name_raw=upper(last_name_raw)
replace last_name_raw=subinstr(last_name_raw,"-"," ",.)
replace last_name_raw=subinstr(last_name_raw,".","",.)
replace last_name_raw=subinstr(last_name_raw,"  "," ",.)
replace last_name_raw=subinstr(last_name_raw,"'","",.)
replace last_name_raw=subinstr(last_name_raw,"?"," ",.)
replace last_name_raw=subinstr(last_name_raw,"*"," ",.)
replace last_name_raw=trim(last_name_raw)

replace first_name_raw=upper(first_name_raw)
replace first_name_raw=subinstr(first_name_raw,"-"," ",.)
replace first_name_raw=subinstr(first_name_raw,".","",.)
replace first_name_raw=subinstr(first_name_raw,"  "," ",.)
replace first_name_raw=subinstr(first_name_raw,"'","",.)
replace first_name_raw=trim(first_name_raw)

**Make first name vars (one with first word, one with all words)
split first_name_raw

**Pull out suffixes
replace court_suffix="JR" if first_name_raw2=="JR"
replace first_name_raw2="" if first_name_raw2=="JR"
replace court_suffix="JR" if first_name_raw2=="JR."
replace first_name_raw2="" if first_name_raw2=="JR."
replace court_suffix="SR" if first_name_raw2=="SR"
replace first_name_raw2="" if first_name_raw2=="SR"
replace court_suffix="II" if first_name_raw2=="II"
replace first_name_raw2="" if first_name_raw2=="II"
replace court_suffix="III" if first_name_raw2=="III"
replace first_name_raw2="" if first_name_raw2=="III"
replace court_suffix="IV" if first_name_raw2=="IIII"
replace first_name_raw2="" if first_name_raw2=="IIII"
replace court_suffix="IV" if first_name_raw2=="IV"
replace first_name_raw2="" if first_name_raw2=="IV"
replace court_suffix="V" if first_name_raw2=="V"
replace first_name_raw2="" if first_name_raw2=="V"
replace court_suffix="VI" if first_name_raw2=="VI"
replace first_name_raw2="" if first_name_raw2=="VI"
replace court_suffix="JR" if first_name_raw2=="JR"
replace first_name_raw2="" if first_name_raw2=="JR"

gen first_name=first_name_raw1
gen f_first3 = substr(first_name,1,3)

**The alt_first_name is everything but the suffix
gen court_alt_first_name=first_name_raw1+first_name_raw2 if first_name_raw2!=""

drop first_name_raw*

**Make last name vars
split last_name_raw

**Pull out suffixes
replace court_suffix="JR" if last_name_raw2=="JR"
replace last_name_raw2="" if last_name_raw2=="JR"
replace court_suffix="JR" if last_name_raw2=="JR."
replace last_name_raw2="" if last_name_raw2=="JR."
replace court_suffix="SR" if last_name_raw2=="SR"
replace last_name_raw2="" if last_name_raw2=="SR"
replace court_suffix="II" if last_name_raw2=="II"
replace last_name_raw2="" if last_name_raw2=="II"
replace court_suffix="III" if last_name_raw2=="III"
replace last_name_raw2="" if last_name_raw2=="III"
replace court_suffix="IV" if last_name_raw2=="IIII"
replace last_name_raw2="" if last_name_raw2=="IIII"
replace court_suffix="IV" if last_name_raw2=="IV"
replace last_name_raw2="" if last_name_raw2=="IV"
replace court_suffix="V" if last_name_raw2=="V"
replace last_name_raw2="" if last_name_raw2=="V"
replace court_suffix="VI" if last_name_raw2=="VI"
replace last_name_raw2="" if last_name_raw2=="VI"
replace court_suffix="JR" if last_name_raw2=="JR"
replace last_name_raw2="" if last_name_raw2=="JR"

**Finish last name cleaning (combine last name phrases like "de la" into a single word, but not cases that look like multiple distinct surnames)	
*One-word last names		
gen last_name=last_name_raw1 if last_name_raw2==""
gen court_alt_last_name=""

*Two-word last names
*Take care of compound names
replace last_name=last_name_raw1+last_name_raw2 if (last_name_raw1=="DOS" | last_name_raw1=="D" | last_name_raw1=="DE" | last_name_raw1=="DEL" | last_name_raw1=="DELLA" | last_name_raw1=="DELA" | last_name_raw1=="DA" | last_name_raw1=="VAN" | last_name_raw1=="VON" | last_name_raw1=="VOM" | last_name_raw1=="SAINT" | last_name_raw1=="ST" | last_name_raw1=="SANTA" | last_name_raw1=="A" | last_name_raw1=="AL" | last_name_raw1=="BEN" | last_name_raw1=="DI" | last_name_raw1=="EL" | last_name_raw1=="LA" | last_name_raw1=="LE" | last_name_raw1=="MC" | last_name_raw1=="MAC" | last_name_raw1=="O" | last_name_raw1=="SAN")

*Last names that seem to be 2 different surnames
replace court_alt_last_name=last_name_raw1+last_name_raw2 if last_name==""
replace last_name=last_name_raw2 if last_name==""

gen l_first3 = substr(last_name,1,3)

drop last_name_raw*


****Clean sex vars****
gen court_male = def_sex=="M"
	replace court_male = . if !inlist(def_sex,"M","F")
	
****Indicator for record source****
gen court_source = "HAMILTON COUNTY"

****Create defendant race vars****
gen court_race_black = def_rac=="BLACK - AFRICAN AMERICAN"
gen court_race_white = def_rac=="WHITE"
gen court_race_asian = def_rac=="ASIAN"
gen court_race_nativeam = def_rac=="INDIAN - NATIVE AMER/ESKIMO"
gen court_race_hispanic = def_rac=="HISPANIC"

foreach var in black white asian nativeam hispanic {
	replace court_race_`var' = . if def_rac=="NONE" | def_rac=="UNKNOWN" | def_rac=="Unavailable" | def_rac==""
}

drop def_rac

****Create date vars****
*Case filed date vars
gen court_file_date = date(fda, "MDY")
format court_file_date %td

foreach time in year month day {
	gen court_file_`time' = `time'(court_file_date)
}

*Disposition date vars
forvalues i = 1/4 {
	split disposition_`i'_full, gen(disposition_`i'_split) parse("-") limit(2)
	rename disposition_`i'_full court_disposition_`i'_raw
	rename disposition_`i'_split1 disposition_`i'_date
	rename disposition_`i'_split2 court_disposition_`i'
	
	*Correct for disposition descriptions that include an extra date that belongs to another disposition
	split court_disposition_`i', gen(court_disposition_`i'_new) parse(";") limit(2)
	drop court_disposition_`i'
	rename court_disposition_`i'_new1 court_disposition_`i'
	
	gen court_disposition_date_`i' = date(disposition_`i'_date, "MDY")
	format court_disposition_date_`i' %td
	drop disposition_`i'_date
	
	foreach time in year month day {
		gen court_disposition_`time'_`i' = `time'(court_disposition_date_`i')
	}
}

drop court_disposition_1_new2

*Date of birth vars
gen court_b_date = date(def_dob,"MDY")
format court_b_date %td


foreach time in year month day {
	gen court_b_`time' = `time'(court_b_date)
}

destring def_age, gen(def_age_num) force
drop def_age
rename def_age_num def_age


*** Identify crime types
gen curr_off_lit = cnt_1
forvalues i = 2/42 {
	replace curr_off_lit = curr_off_lit + cnt_`i'
}

*According to Hamilton County Court website, Common Pleas handles felonies and Municipal handles misdemeanors
gen court_curroff_felony = (cdi=="Common Pleas Criminal")
gen court_curroff_misdemeanor = (cdi=="Municipal Criminal/Traffic")

* Current offense DWI vars
gen court_curroff_dwi = regexm(curr_off_lit, "DUI")
	replace court_curroff_dwi = 1 if regexm(curr_off_lit,"OVI-")
	replace court_curroff_dwi = 1 if regexm(curr_off_lit,"OVI -")
	replace court_curroff_dwi = 1 if regexm(curr_off_lit,"OVI 1")
	replace court_curroff_dwi = 1 if regexm(curr_off_lit,"OVI 3")
	replace court_curroff_dwi = 1 if regexm(curr_off_lit,"DWI")
	replace court_curroff_dwi = 1 if regexm(curr_off_lit,"BREATH")
	replace court_curroff_dwi = 1 if regexm(curr_off_lit,"BLOOD")
	replace court_curroff_dwi = 1 if regexm(curr_off_lit,"DRIVING WHILE I")
	replace court_curroff_dwi = 1 if regexm(curr_off_lit,"'100'")
	replace court_curroff_dwi = 1 if regexm(curr_off_lit,"CONTROL OF VEHICLE WHILE UNDER THE INFLUENCE")
gen court_curroff_aggdwi = court_curroff_dwi & (regexm(curr_off_lit,"AGG") | regexm(curr_off_lit,"SBI") | regexm(curr_off_lit,"CHILD") | regexm(curr_off_lit,"OPEN"))
	
	
* Current offense non-DWI vars
gen court_curroff_poss = regexm(curr_off_lit,"POSS")
	replace court_curroff_poss = 0 if regexm(curr_off_lit,"WEAPON")	| regexm(curr_off_lit,"WPN")
gen court_curroff_drugmanufdeliv = regexm(curr_off_lit,"MANUFACTURE OF DRUGS")
	replace court_curroff_drugmanufdeliv = 1 if regexm(curr_off_lit,"MANU OF DRUGS")
	replace court_curroff_drugmanufdeliv = 1 if regexm(curr_off_lit,"SELLING, PURCHASING, DISTRIBUTING, OR DELIVERING DANGEROUS DRUGS")
	replace court_curroff_drugmanufdeliv = 1 if regexm(curr_off_lit,"SELL,DEL,POSS DANGEROUS DRUGS")
gen court_curroff_recklessdriving = regexm(curr_off_lit,"RECKLESS OPERATION")
	replace court_curroff_recklessdriving = 1 if regexm(curr_off_lit,"RECKLESS DRIVING")
gen court_curroff_resistarrest = regexm(curr_off_lit,"RESISTING ARREST")
gen court_curroff_weapon = regexm(curr_off_lit,"WEAPON") | regexm(curr_off_lit,"WPN")
gen court_curroff_hitandrun = regexm(curr_off_lit,"STOP AFTER") | regexm(curr_off_lit,"STOPPING AFTER")
gen court_curroff_licenseinvalid = regexm(curr_off_lit,"REINSTATE LIC")

*Construct criminal history
gen disposition = court_disposition_1
forvalues i = 2/4 {
	replace disposition = disposition + court_disposition_`i'
}
*RA: double check that guilty is coded correctly
*RA: create vars for prior felony convictions, prior felony charges, prior misdemeanor convictions, prior misdemeanor charges
gen court_conviction = regexm(disposition,"CONVICTED") | regexm(disposition,"GUILTY")
	replace court_conviction = 0 if regexm(disposition, "NOT GUILTY")


gen court_nolocontend = regexm(disposition,"NOLO")

gen court_conv_defer_nolo = court_conviction | court_nolocontend
	
gen court_dwiconviction = court_curroff_dwi & court_conviction
gen court_dwinolocontend = court_curroff_dwi & court_nolocontend
gen court_dwiconv_defer_nolo = court_curroff_dwi & court_conv_defer_nolo 

gen def_id = court_fullname + def_dob

sort def_id fda

bys def_id (fda): gen court_priordwis = sum(court_dwiconviction)
	replace court_priordwis = court_priordwis - court_dwiconviction

bys def_id (fda): gen court_priorconvictions = sum(court_conviction)
	replace court_priorconvictions = court_priorconvictions - court_conviction

	
*** Get rid of vars that aren't useful
drop cdi fda ctp court_disposition_1_raw court_disposition_2_raw court_disposition_3_raw court_disposition_4_raw notes curr_off_lit disposition def_id middlename1 middlename2


*** Rename a few vars
rename cas court_case_number
rename cca court_case_caption
rename cpn court_cp_case_number
rename jdg court_judge

forvalues i = 1/42 {
	rename cnt_`i' court_curr_off_`i'
}

forvalues i = 1/4 {
	rename bam_`i' court_bond_amount_`i'
}

	
*** Label vars
label var court_curroff_felony "Current offense is a felony"
label var court_curroff_misdemeanor "Current offense is a misdemeanor"
label var court_curroff_dwi "Current offense is a DWI"
label var court_curroff_aggdwi "Current offense code consistent with aggravated DWI"
label var court_curroff_poss "Current offense is a drug possession charge"
label var court_curroff_drugmanufdeliv "Current offense is a drug manufacturing/delivery charge"
label var court_curroff_recklessdriving "Current offense is a reckless driving charge"
label var court_curroff_resistarrest "Current offense is a resisting arrest charge"
label var court_curroff_weapon "Current offense is a weapons charge"
label var court_curroff_hitandrun "Current offense is a hit and run charge (a.k.a. failure to stop and give information)"
label var court_curroff_licenseinvalid "Current offense is a driving with invalid license charge"
label var court_conviction "Indicator for convicted (including guilty pleas)"
label var court_nolocontend "Indicator for no lo contendere"
label var court_conv_defer_nolo "Indicator for convicted OR deferred adjudication OR no lo contendere"
label var court_dwiconviction "Indicator for convicted (including guilty pleas) & current offense is DWI"
label var court_dwinolocontend "Indicator for no lo contendere & current offense is DWI"
label var court_dwiconv_defer_nolo "Indicator for convicted OR deferred adjudication OR no lo contendere & current offense is DWI"
label var court_priorconvictions "Number of prior convictions"
label var court_priordwis "Number of prior DWI offenses"
label var court_file_year "Court case filing year"
label var court_file_month "Court case filing month"
label var court_file_day "Court case filing day (of month)"
label var court_file_date "Court case filing date"
label var court_disposition_1 "Court case disposition 1"
label var court_disposition_year_1 "Court case disposition 1 year"
label var court_disposition_month_1 "Court case disposition 1 month"
label var court_disposition_day_1 "Court case disposition 1 day (of month)"
label var court_disposition_date_1 "Court case disposition 1 date"
label var court_disposition_2 "Court case disposition 2"
label var court_disposition_year_2 "Court case disposition 2 year"
label var court_disposition_month_2 "Court case disposition 2 month"
label var court_disposition_day_2 "Court case disposition 2 day (of month)"
label var court_disposition_date_2 "Court case disposition 2 date"
label var court_disposition_3 "Court case disposition 3"
label var court_disposition_year_3 "Court case disposition 3 year"
label var court_disposition_month_3 "Court case disposition 3 month"
label var court_disposition_day_3 "Court case disposition 3 day (of month)"
label var court_disposition_date_3 "Court case disposition 3 date"
label var court_disposition_year_4 "Court case disposition 4 year"
label var court_disposition_month_4 "Court case disposition 4 month"
label var court_disposition_day_4 "Court case disposition 4 day (of month)"
label var court_disposition_date_4 "Court case disposition 4 date"
label var court_b_year "Birth year from court record"
label var court_b_month "Birth month from court record"
label var court_b_day "Birth day (of month) from court record"
label var court_b_date "Birth date from court record"
label var court_race_black "Indicator for race recorded as Black in court record"
label var court_race_white "Indicator for race recorded as White in court record"
label var court_race_asian "Indicator for race recorded as Asian in court record"
label var court_race_nativeam "Indicator for race recorded as Native American in court record"
label var court_race_hispanic "Indicator for race recorded as Hispanic in court record"
label var court_fullname "Full name from court records"
label var court_suffix "Suffix from court records"
label var first_name "First word of name"
label var f_first3 "First 3 letters of first name"
label var court_alt_first_name "All first name words, without suffixes or spaces"
label var court_middle_initial "Middle initial from court record (constructed; first letter of second word of name)"
label var last_name "Last listed last name, combining compound name and removing suffixes and spaces (e.g. VONLEHM not VON LEHM)"
label var court_alt_last_name "All last name words, without suffixes or spaces"
label var l_first3 "First 3 letters of last name"
label var court_male "Indicator for male in court record"
label var court_source "Where court record came from"

compress

sort first_name last_name
save "$int_dir/hamilton_county_allcases.dta", replace



*** Dataset with all offenses within plausible timeframe
use "$int_dir/hamilton_county_allcases.dta", clear

*remove records that are out of the range of breath test records
keep if inrange(court_file_year,2009,2018)

save "$int_dir/hamilton_county_breathtestyears.dta", replace



*** Dataset with only likely stop-related offenses
use "$int_dir/hamilton_county_allcases.dta", clear

*remove records that are out of the range of breath test records
keep if inrange(court_file_year,2009,2018)

*keep only likely stop-related offenses
keep if court_curroff_dwi | court_curroff_poss | court_curroff_drugmanufdeliv | court_curroff_recklessdriving | court_curroff_resistarrest | court_curroff_weapon | court_curroff_hitandrun | court_curroff_licenseinvalid


sort first_name last_name
save "$int_dir/hamilton_county_stopoffenses.dta", replace
