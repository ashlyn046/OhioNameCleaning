*************************************************************************************************
/*
INPUT: department_county_data.xlsx, OSP_county_clean.dta, county_`1-3'_fips.dta
INTERMEDIARY: department_county_data2.dta
OUTPUT: arrestingagency_final.dta , testingagency_final.dta
*/

*This file will :
*(1) read in the cleaned department/county info from excel and put it into intermediate data
*(2) merge the clean dept/county data from the excel with OSP dept/county data
*************************************************************************************************
clear all
set more off
program drop _all

**Root directory
*global root_dir "/Users/ashlyn04/Box/DUIAshlyn/ohio"
global root_dir "/Users/mukaie/Box/DUIAshlyn/ohio"

global raw_dir "$root_dir/raw_data"
global int_dir "$root_dir/intermediate_data"
global clean_dir "$root_dir/clean_data"


************************************
*PHASE 1 : Import & finish cleaning
************************************
import excel using "$raw_dir/department_county/department_county_data.xlsx"

//renaming variables
rename A arrestingagency
rename B county

**converting to caps
replace arrestingagency = upper(arrestingagency)

**cleaning county variable
split county, parse("_")
replace county = upper(county1)
drop county1 county2

**dropping apostrphes from arrestingagency
replace arrestingagency = subinstr(arrestingagency, "'", "", .)

**renaming county var to match OSP data format
rename county county_1

**combine and drop duplicates
sort arrestingagency
duplicates list arrestingagency, sepby(arrestingagency)

gen agency_count=1

gen id = _n
replace agency_count=2 if id==32
replace agency_count=2 if id==44
replace agency_count=2 if id==71
replace agency_count=2 if id==128
replace agency_count=2 if id==132
replace agency_count=3 if id==133
replace agency_count=2 if id==183
replace agency_count=2 if id==233
replace agency_count=2 if id==271
replace agency_count=2 if id==302
replace agency_count=2 if id==304
replace agency_count=2 if id==375
replace agency_count=2 if id==397
replace agency_count=2 if id==423
replace agency_count=2 if id==467
drop id

//format is now able to handle the reshape command
reshape wide county_1, i(arrestingagency) j(agency_count)
rename county_11 county_1
rename county_12 county_2
rename county_13 county_3

save "$int_dir/department_county_data2", replace


************************************
*PHASE 2 : Append dept/county files
************************************
**Combine department_county_data2.dta with OSP_county_clean.dta (from SCRAPE_ppost_county.do)
append using "$int_dir/OSP_county_clean"

**Normalize names we may have missed
replace arrestingagency = subinstr(arrestingagency, "&AMP;", "AND", .)

***********************************
*PHASE 3: Merging in FIPS Codes
***********************************
forval i = 1/3{
	merge m:1 county_`i' using "$int_dir/county_`i'_fips"
	drop if _m == 2
	drop _m
}

compress

//we really don't need this variable
drop post_num

//save final "arrestingagency" file
save "$int_dir/arrestingagency_final", replace

//save final "testingagency" file
rename arrestingagency testingagency
save "$int_dir/testingagency_final", replace

