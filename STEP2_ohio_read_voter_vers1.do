*****************************************************
/*
INPUT:	 `countyname'.txt, county_1_fips.dta
INTERMEDIARY: voter_file_`countyname'_`A-Z'.dta, voter_file_A.dta, voter_file_`A-Z'.dta
OUTPUT: voter_file_`A-Z'.dta, voter_f_name_freq.dta, voter_l_name_freq.dta
*/

*This file will:
*(1) read in voterByCounty files for cleaning
*(2) merge to add FIPS codes to the dataset
*(3) make frequency tables for first/last names
*****************************************************
clear
capture log close
set more off



**Root directory
*global root_dir "/Volumes/Elements/DUI/texas"
*global root_dir "/Users/ll263/Box/DUI/texas"
*global root_dir C:/Users/andersee/Box/DUI/texas
*global root_dir C:/Users/ashlyn04/Box/DUIAshlyn/ohio
global root_dir "/Users/mukaie/Box/DUIAshlyn/ohio"

global raw_dir "$root_dir/raw_data/voter_data"
global int_dir "$root_dir/intermediate_data"
global clean_dir "$root_dir/clean_data"


** Read in file for each county, clean, sort into files by first letter of first name
foreach var in FAIRFIELD RICHLAND CHAMPAIGN MEIGS CRAWFORD LUCAS BUTLER PREBLE ASHLAND DELAWARE FULTON STARK DARKE ERIE SANDUSKY ADAMS CLARK PUTNAM GREENE LORAIN ROSS HURON MERCER MUSKINGUM VANWERT GUERNSEY PIKE BELMONT DEFIANCE GALLIA CLINTON HENRY ALLEN PORTAGE PAULDING CUYAHOGA HOCKING HAMILTON COLUMBIANA SENECA HOLMES ATHENS COSHOCTON MARION WARREN MORGAN MONROE FAYETTE KNOX TRUMBULL MADISON HARRISON WILLIAMS FRANKLIN BROWN LOGAN NOBLE WASHINGTON OTTAWA WAYNE HANCOCK JACKSON WYANDOT LAKE MIAMI HARDIN UNION MONTGOMERY LICKING SUMMIT SCIOTO HIGHLAND CLERMONT AUGLAIZE ASHTABULA JEFFERSON VINTON LAWRENCE GEAUGA TUSCARAWAS MORROW MEDINA CARROLL PICKAWAY MAHONING WOOD PERRY SHELBY {
clear
	
**Information on Hispanic ethnicity, party, and voting method do not appear to be present in the data so we won't read that in.
import delimited using "$raw_dir/`var'.txt"

//droping voted-in-election data
for var pri* spe* gen*: drop X

//dropping unneccessary variables (aka not in Texas code)
drop residential_secondary_addr mailing_secondary_address voter_status party_affiliation residential_zip_plus4 residential_country residential_postalcode mailing_zip_plus4 mailing_country mailing_postal_code career_center city city_school_district congressional_district edu_service_center_district exempted_vill_school_district library local_school_district state_board_of_education state_representative_district state_senate_district township village ward /*drop county_court_district court_of_appeals municipal_court_district*/ county_id county_number

for var *: tostring X, replace
for var *: replace X = trim(X)

//creating full address variables (residential and mailing)
gen voter_residential_address = residential_address1 + ", " + residential_city + ", " + residential_state + " " + residential_zip

gen voter_mail_address = mailing_address1 + ", " + mailing_city + ", " + mailing_state + " " + mailing_zip if mailing_address1 ~= ""

//dropping individual variables used to make full addresses
drop residential_address1 residential_city residential_state residential_zip mailing_address1 mailing_city mailing_state mailing_zip

//prep birth date vars
gen voter_b_year = real(substr(date_of_birth, 1, 4))
gen voter_b_month = real(substr(date_of_birth, 6, 2))
gen voter_b_day = real(substr(date_of_birth, 9, 2))

gen voter_b_date = date(string(voter_b_month) + "/" + string(voter_b_day) + "/" + string(voter_b_year), "MDY")
format voter_b_date %td

drop date_of_birth

//rename name variables to match texas code
rename first_name first_name_raw
rename last_name last_name_raw
rename middle_name voter_middle_name
rename suffix voter_suffix
rename registration_date voter_reg_date
rename precinct_name voter_precinct


//clean names and constrict name variables for merging with other files
gen voter_fullname = first_name_raw + " " + voter_middle_name + " " + last_name_raw + " " + voter_suffix

//standardize case and remove punctuation + extra spaces
replace last_name_raw=upper(last_name_raw)
replace first_name_raw=upper(first_name_raw)
replace voter_middle_name=upper(voter_middle_name)
replace last_name_raw=subinstr(last_name_raw,"-"," ",.)
replace last_name_raw=subinstr(last_name_raw,".","",.)
replace last_name_raw=subinstr(last_name_raw,"  "," ",.)
replace last_name_raw=subinstr(last_name_raw,"  "," ",.)
replace last_name_raw=subinstr(last_name_raw,"'","",.)
replace last_name_raw=subinstr(last_name_raw,"?"," ",.)
replace last_name_raw=subinstr(last_name_raw,"*"," ",.)
replace last_name_raw=strtrim(last_name_raw)
replace last_name_raw=stritrim(last_name_raw)

replace first_name_raw=subinstr(first_name_raw,"-"," ",.)
replace first_name_raw=subinstr(first_name_raw,".","",.)
replace first_name_raw=subinstr(first_name_raw,"  "," ",.)
replace first_name_raw=subinstr(first_name_raw,"  "," ",.)
replace first_name_raw=subinstr(first_name_raw,"'","",.)
replace first_name_raw = strtrim(first_name_raw)
replace first_name_raw = stritrim(first_name_raw)


//make first name vars (one with first word, one with all words)
split first_name_raw

gen first_name=first_name_raw1
gen voter_alt_first_name=subinstr(first_name_raw," ","",.)

drop first_name_raw*


//make last name vars
split last_name_raw

cap gen last_name_raw2 = ""
cap gen last_name_raw3 = ""
cap gen last_name_raw4 = ""
cap gen last_name_raw5 = ""
cap gen last_name_raw6 = ""


//pull out suffixes
	forval i=2/5 {
		local j=`i'+1
		capture replace voter_suffix="JR" if last_name_raw`i'=="JR" & last_name_raw`j'=="" 
		capture replace last_name_raw`i'="" if last_name_raw`i'=="JR" & last_name_raw`j'==""
		capture replace voter_suffix="SR" if last_name_raw`i'=="SR" & last_name_raw`j'=="" 
		capture replace last_name_raw`i'="" if last_name_raw`i'=="SR" & last_name_raw`j'==""
		capture replace voter_suffix="III" if last_name_raw`i'=="III" & last_name_raw`j'=="" 
		capture replace last_name_raw`i'="" if last_name_raw`i'=="III" & last_name_raw`j'==""
		capture replace voter_suffix="IV" if last_name_raw`i'=="IV" & last_name_raw`j'=="" 
		capture replace last_name_raw`i'="" if last_name_raw`i'=="IV" & last_name_raw`j'==""
		capture replace voter_suffix="V" if last_name_raw`i'=="V" & last_name_raw`j'=="" 
		capture replace last_name_raw`i'="" if last_name_raw`i'=="V" & last_name_raw`j'==""
		capture replace voter_suffix="VI" if last_name_raw`i'=="VI" & last_name_raw`j'=="" 
		capture replace last_name_raw`i'="" if last_name_raw`i'=="VI" & last_name_raw`j'==""
		capture replace voter_suffix="JR" if last_name_raw`i'=="II" & last_name_raw`j'=="" 
		capture replace last_name_raw`i'="" if last_name_raw`i'=="II" & last_name_raw`j'==""
		}
		
//replace II with JR in suffix var
replace voter_suffix = "JR" if voter_suffix == "II"


**Finish last name cleaning (combine last name phrases like "de la" into a single word, but not cases that look like multiple distinct surnames)
*One-word last names
gen last_name=last_name_raw1 if last_name_raw2==""	
gen voter_alt_last_name = ""

*Two-word last names
*Take care of compound names
replace last_name=last_name_raw1+last_name_raw2 if last_name_raw3=="" & (last_name_raw1=="DOS" | last_name_raw1=="D" | last_name_raw1=="DE" | last_name_raw1=="DEL" | last_name_raw1=="DELLA" | last_name_raw1=="DELA" | last_name_raw1=="DA" | last_name_raw1=="VAN" | last_name_raw1=="VON" | last_name_raw1=="VOM" | last_name_raw1=="SAINT" | last_name_raw1=="ST" | last_name_raw1=="SANTA" | last_name_raw1=="A" | last_name_raw1=="AL" | last_name_raw1=="BEN" | last_name_raw1=="DI" | last_name_raw1=="EL" | last_name_raw1=="LA" | last_name_raw1=="LE" | last_name_raw1=="MC" | last_name_raw1=="MAC" | last_name_raw1=="O" | last_name_raw1=="SAN")

*Last names that seem to be 2 different surnames
replace voter_alt_last_name=last_name_raw1+last_name_raw2 if last_name_raw3=="" & last_name==""
replace last_name=last_name_raw2 if last_name_raw3=="" & last_name==""



*Three-word last names
*Take care of compound names
replace last_name=last_name_raw1+last_name_raw2+last_name_raw3 if last_name_raw4=="" & (last_name_raw1+last_name_raw2=="DELA" | last_name_raw1+last_name_raw2=="DELOS" | last_name_raw1+last_name_raw2=="DELAS"  | last_name_raw1+last_name_raw2=="VANDE" | last_name_raw1+last_name_raw2=="VANDER" | last_name_raw1+last_name_raw2=="VANDEN")

*Three-word names that seem a compound of one two word name and another 1 word name
replace voter_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3 if last_name=="" & last_name_raw4=="" & (last_name_raw1=="DOS" | last_name_raw1=="D" | last_name_raw1=="DE" | last_name_raw1=="DEL" | last_name_raw1=="DELLA" | last_name_raw1=="DELA" | last_name_raw1=="DA" | last_name_raw1=="VAN" | last_name_raw1=="VON" | last_name_raw1=="VOM" | last_name_raw1=="SAINT" | last_name_raw1=="ST" | last_name_raw1=="SANTA" | last_name_raw1=="A" | last_name_raw1=="AL" | last_name_raw1=="BEN" | last_name_raw1=="DI" | last_name_raw1=="EL" | last_name_raw1=="LA" | last_name_raw1=="LE" | last_name_raw1=="MC" | last_name_raw1=="MAC" | last_name_raw1=="O" | last_name_raw1=="SAN")

replace last_name=last_name_raw3 if last_name=="" & last_name_raw4=="" & (last_name_raw1=="DOS" | last_name_raw1=="D" | last_name_raw1=="DE" | last_name_raw1=="DEL" | last_name_raw1=="DELLA" | last_name_raw1=="DELA" | last_name_raw1=="DA" | last_name_raw1=="VAN" | last_name_raw1=="VON" | last_name_raw1=="VOM" | last_name_raw1=="SAINT" | last_name_raw1=="ST" | last_name_raw1=="SANTA" | last_name_raw1=="A" | last_name_raw1=="AL" | last_name_raw1=="BEN" | last_name_raw1=="DI" | last_name_raw1=="EL" | last_name_raw1=="LA" | last_name_raw1=="LE" | last_name_raw1=="MC" | last_name_raw1=="MAC" | last_name_raw1=="O" | last_name_raw1=="SAN")


*Three-word names that seem a compound of 1 one-word name and another two-word name
replace voter_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3 if last_name=="" & last_name_raw4=="" & (last_name_raw2=="DOS" | last_name_raw2=="D" | last_name_raw2=="DE" | last_name_raw2=="DEL" | last_name_raw2=="DELLA" | last_name_raw2=="DELA" | last_name_raw2=="DA" | last_name_raw2=="VAN" | last_name_raw2=="VON" | last_name_raw2=="VOM" | last_name_raw2=="SAINT" | last_name_raw2=="ST" | last_name_raw2=="SANTA" | last_name_raw2=="A" | last_name_raw2=="AL" | last_name_raw2=="BEN" | last_name_raw2=="DI" | last_name_raw2=="EL" | last_name_raw2=="LA" | last_name_raw2=="LE" | last_name_raw2=="MC" | last_name_raw2=="MAC" | last_name_raw2=="O" | last_name_raw2=="SAN")

replace last_name=last_name_raw2+last_name_raw3 if last_name=="" & last_name_raw4=="" & (last_name_raw2=="DOS" | last_name_raw2=="D" | last_name_raw2=="DE" | last_name_raw2=="DEL" | last_name_raw2=="DELLA" | last_name_raw2=="DELA" | last_name_raw2=="DA" | last_name_raw2=="VAN" | last_name_raw2=="VON" | last_name_raw2=="VOM" | last_name_raw2=="SAINT" | last_name_raw2=="ST" | last_name_raw2=="SANTA" | last_name_raw2=="A" | last_name_raw2=="AL" | last_name_raw2=="BEN" | last_name_raw2=="DI" | last_name_raw2=="EL" | last_name_raw2=="LA" | last_name_raw2=="LE" | last_name_raw2=="MC" | last_name_raw2=="MAC" | last_name_raw2=="O" | last_name_raw2=="SAN")


*The rest of the three-word names
replace voter_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3 if last_name=="" & last_name_raw4==""
replace last_name=last_name_raw3 if last_name=="" & last_name_raw4==""


*Four-word names
*Names that include de los or van de
replace voter_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3+last_name_raw4 if last_name=="" & last_name_raw5=="" & (last_name_raw1+last_name_raw2=="DELA" | last_name_raw1+last_name_raw2=="DELOS" | last_name_raw1+last_name_raw2=="DELAS"  | last_name_raw1+last_name_raw2=="VANDE" | last_name_raw1+last_name_raw2=="VANDER" | last_name_raw1+last_name_raw2=="VANDEN")

replace last_name=last_name_raw4 if last_name=="" & last_name_raw5=="" & (last_name_raw1+last_name_raw2=="DELA" | last_name_raw1+last_name_raw2=="DELOS" | last_name_raw1+last_name_raw2=="DELAS"  | last_name_raw1+last_name_raw2=="VANDE" | last_name_raw1+last_name_raw2=="VANDER" | last_name_raw1+last_name_raw2=="VANDEN")


replace voter_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3+last_name_raw4 if last_name=="" & last_name_raw5=="" & (last_name_raw2+last_name_raw3=="DELA" | last_name_raw2+last_name_raw3=="DELOS" | last_name_raw2+last_name_raw3=="DELAS"  | last_name_raw2+last_name_raw3=="VANDE" | last_name_raw2+last_name_raw3=="VANDER" | last_name_raw2+last_name_raw3=="VANDEN")

replace last_name=last_name_raw2+last_name_raw3+last_name_raw4 if last_name=="" & last_name_raw5=="" & (last_name_raw2+last_name_raw3=="DELA" | last_name_raw2+last_name_raw3=="DELOS" | last_name_raw2+last_name_raw3=="DELAS"  | last_name_raw2+last_name_raw3=="VANDE" | last_name_raw2+last_name_raw3=="VANDER" | last_name_raw2+last_name_raw3=="VANDEN")

*Names that end with a two-word name like de santis
replace voter_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3+last_name_raw4 if last_name=="" & last_name_raw5=="" & (last_name_raw3=="DOS" | last_name_raw3=="D" | last_name_raw3=="DE" | last_name_raw3=="DEL" | last_name_raw3=="DELLA" | last_name_raw3=="DELA" | last_name_raw3=="DA" | last_name_raw3=="VAN" | last_name_raw3=="VON" | last_name_raw3=="VOM" | last_name_raw3=="SAINT" | last_name_raw3=="ST" | last_name_raw3=="SANTA" | last_name_raw3=="A" | last_name_raw3=="AL" | last_name_raw3=="BEN" | last_name_raw3=="DI" | last_name_raw3=="EL" | last_name_raw3=="LA" | last_name_raw3=="LE" | last_name_raw3=="MC" | last_name_raw3=="MAC" | last_name_raw3=="O" | last_name_raw3=="SAN")

replace last_name=last_name_raw3+last_name_raw4 if last_name=="" & last_name_raw5=="" & (last_name_raw3=="DOS" | last_name_raw3=="D" | last_name_raw3=="DE" | last_name_raw3=="DEL" | last_name_raw3=="DELLA" | last_name_raw3=="DELA" | last_name_raw3=="DA" | last_name_raw3=="VAN" | last_name_raw3=="VON" | last_name_raw3=="VOM" | last_name_raw3=="SAINT" | last_name_raw3=="ST" | last_name_raw3=="SANTA" | last_name_raw3=="A" | last_name_raw3=="AL" | last_name_raw3=="BEN" | last_name_raw3=="DI" | last_name_raw3=="EL" | last_name_raw3=="LA" | last_name_raw3=="LE" | last_name_raw3=="MC" | last_name_raw3=="MAC" | last_name_raw3=="O" | last_name_raw3=="SAN")


*The rest of the 4 word names
replace voter_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3+last_name_raw4 if last_name=="" & last_name_raw5==""
replace last_name=last_name_raw4 if last_name=="" & last_name_raw5==""


*Five-word names
*We're just going to work on the last parts of the names
replace voter_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3+last_name_raw4+last_name_raw5 if last_name=="" & (last_name_raw3+last_name_raw4=="DELA" | last_name_raw3+last_name_raw4=="DELOS" | last_name_raw3+last_name_raw4=="DELAS"  | last_name_raw3+last_name_raw4=="VANDE" | last_name_raw3+last_name_raw4=="VANDER" | last_name_raw3+last_name_raw4=="VANDEN")

replace last_name=last_name_raw3+last_name_raw4+last_name_raw5 if last_name=="" & (last_name_raw3+last_name_raw4=="DELA" | last_name_raw3+last_name_raw4=="DELOS" | last_name_raw3+last_name_raw4=="DELAS"  | last_name_raw3+last_name_raw4=="VANDE" | last_name_raw3+last_name_raw4=="VANDER" | last_name_raw3+last_name_raw4=="VANDEN")

*Names that end with a 2 word name like de santis
replace voter_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3+last_name_raw4+last_name_raw5 if last_name=="" & (last_name_raw4=="DOS" | last_name_raw4=="D" | last_name_raw4=="DE" | last_name_raw4=="DEL" | last_name_raw4=="DELLA" | last_name_raw4=="DELA" | last_name_raw4=="DA" | last_name_raw4=="VAN" | last_name_raw4=="VON" | last_name_raw4=="VOM" | last_name_raw4=="SAINT" | last_name_raw4=="ST" | last_name_raw4=="SANTA" | last_name_raw4=="A" | last_name_raw4=="AL" | last_name_raw4=="BEN" | last_name_raw4=="DI" | last_name_raw4=="EL" | last_name_raw4=="LA" | last_name_raw4=="LE" | last_name_raw4=="MC" | last_name_raw4=="MAC" | last_name_raw4=="O" | last_name_raw4=="SAN")

replace last_name=last_name_raw4+last_name_raw5 if last_name=="" & (last_name_raw4=="DOS" | last_name_raw4=="D" | last_name_raw4=="DE" | last_name_raw4=="DEL" | last_name_raw4=="DELLA" | last_name_raw4=="DELA" | last_name_raw4=="DA" | last_name_raw4=="VAN" | last_name_raw4=="VON" | last_name_raw4=="VOM" | last_name_raw4=="SAINT" | last_name_raw4=="ST" | last_name_raw4=="SANTA" | last_name_raw4=="A" | last_name_raw4=="AL" | last_name_raw4=="BEN" | last_name_raw4=="DI" | last_name_raw4=="EL" | last_name_raw4=="LA" | last_name_raw4=="LE" | last_name_raw4=="MC" | last_name_raw4=="MAC" | last_name_raw4=="O" | last_name_raw4=="SAN")


*The rest of the five-word names
replace voter_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3+last_name_raw4+last_name_raw5 if last_name=="" & last_name_raw6==""
replace last_name=last_name_raw5 if last_name=="" & last_name_raw6==""


*Six-word last names
replace voter_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3+last_name_raw4+last_name_raw5+last_name_raw6 if last_name_raw6~=""


drop last_name_raw*

//br voter_fullname first_name voter_alt_first_name last_name voter_alt_last_name if last_name_raw3 ~=""

* Make middle initial var
gen voter_middle_initial=substr(voter_middle_name,1,1)

//merging in FIPS/county data set to get FIPS codes
gen county_1 = "`var'"
merge m:1 county_1 using "$int_dir/county_1_fips"
drop if _m==2
drop _m 
rename county_1 voter_county_name
rename fips_1 voter_county_fips

//compress because for some reason fips and county name are wayy longer than necessary in the data browser
compress

* Label vars
label var sos_voterid "Voter file ID number" 
label var voter_middle_name "Voter file middle name (administratively recorded, not constructed)"
label var voter_suffix "Voter file suffix"
label var voter_reg_date "Voter registration date"
label var voter_precinct "Voter file precinct"
label var precinct_code "Precinct alphanumeric code from file" //not in texas 
label var voter_residential_address "Residential address from voter file"
label var voter_mail_address "Mailing address from voter file"
label var voter_b_year "Birth year from voter file"
label var voter_b_month "Birth month from voter file"
label var voter_b_day "Birth day (of month) from voter file"
label var voter_b_date "Birth date from voter file"
label var first_name "First word of name"
label var voter_alt_first_name "All first name words, without suffixes or spaces, from voter file"
label var last_name "Last listed last name, combining compound name and removing suffixes and spaces (e.g. VONLEHM not VON LEHM)"
label var voter_alt_last_name "All last name words, without suffixes or spaces, from voter file"
label var voter_middle_initial "Middle initial from voter file (first letter of administratively recorded middle name)"
label var voter_county_fips "County FIPS associated with county name from voter file"
label var voter_county_name "County name from voter file"


foreach var2 in A B C D E F G H I J K L M N O P Q R S T U V W X Y Z	{
		
		preserve
		gen temp = substr(first_name,1,1)
		keep if temp=="`var2'"
		drop temp
		save "$int_dir/voter_file_`var'_`var2'", replace 
		restore
}
}
exit


**Now we stitch back together all of the county files starting with the same first initial
foreach var2 in A B C D E F G H I J K L M N O P Q R S T U V W X Y Z {

clear

foreach var in FAIRFIELD RICHLAND CHAMPAIGN MEIGS CRAWFORD LUCAS BUTLER PREBLE ASHLAND DELAWARE FULTON STARK DARKE ERIE SANDUSKY ADAMS CLARK PUTNAM GREENE LORAIN ROSS HURON MERCER MUSKINGUM VANWERT GUERNSEY PIKE BELMONT DEFIANCE GALLIA CLINTON HENRY ALLEN PORTAGE PAULDING CUYAHOGA HOCKING HAMILTON COLUMBIANA SENECA HOLMES ATHENS COSHOCTON MARION WARREN MORGAN MONROE FAYETTE KNOX TRUMBULL MADISON HARRISON WILLIAMS FRANKLIN BROWN LOGAN NOBLE WASHINGTON OTTAWA WAYNE HANCOCK JACKSON WYANDOT LAKE MIAMI HARDIN UNION MONTGOMERY LICKING SUMMIT SCIOTO HIGHLAND CLERMONT AUGLAIZE ASHTABULA JEFFERSON VINTON LAWRENCE GEAUGA TUSCARAWAS MORROW MEDINA CARROLL PICKAWAY MAHONING WOOD PERRY SHELBY	{
	capture append using "$int_dir/voter_file_`var'_`var2'.dta"

}

compress
save "$int_dir/voter_file_`var2'.dta", replace

}


*** Create a database of name frequency based on voter files to help make predicted match quality measures later

use first_name last_name using "$int_dir/voter_file_A.dta", clear
save tmpdat, replace


foreach y in B C D E F G H I J K L M N O P Q R S T U V W X Y Z {

use first_name last_name using "$int_dir/voter_file_`y'.dta"
append using tmpdat
save tmpdat, replace
}

**Now let's create frequency tables of first and last names.

use tmpdat, clear
gen f_name_freq=1
collapse (sum) f_name_freq, by(first_name)
save "$int_dir/voter_f_name_freq.dta", replace


use tmpdat, clear
gen l_name_freq=1
collapse (sum) l_name_freq, by(last_name)
save "$int_dir/voter_l_name_freq.dta", replace