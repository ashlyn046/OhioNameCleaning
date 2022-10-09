************************************************************
/*
INPUT: merged_data.do

INTERMEDIARY: OhioBreathalyzer.dta, ohio_breath_tests_uniqueincident.dta

OUTPUT: ohio_breath_tests_uniqueincident_`A-Z'
*/

*This file will read in merged data and do final cleaning
************************************************************

clear all
set more off
program drop _all


**Root directory
global root_dir "/Users/ashlyn04/Box/DUIAshlyn/ohio"
*global root_dir "/Users/mukaie/Box/DUIAshlyn/ohio"
*global root_dir C:\Users\andersee\Box\DUIAshlyn\ohio
*global root_dir "C:\Users\jtd44\Box\DUIAshlyn\ohio"
*global root_dir "/Users/ll263/Library/CloudStorage/Box-Box/DUIAshlyn/ohio"

global raw_dir "$root_dir/raw_data"
global int_dir "$root_dir/intermediate_data"
global clean_dir "$root_dir/clean_data"



**importing cleaned data from step1_names do file
use "$int_dir/merged_data", clear


//renaming fips variables
rename fips_1 dui_cnty_fips_1
rename fips_2 dui_cnty_fips_2
rename fips_3 dui_cnty_fips_3

//generating first and last county vars
sort dui_incident_id dui_test_time
bys dui_incident_id: gen double dui_first_result = result[1]
bys dui_incident_id: gen dui_first_test = _n==1
bys dui_incident_id: gen dui_first_cnty1 = county_1[1]
bys dui_incident_id: gen dui_first_cnty2 = county_2[1]
bys dui_incident_id: gen dui_first_cnty3 = county_3[1]
bys dui_incident_id: gen dui_first_cnty_fips_1 = dui_cnty_fips_1[1]
bys dui_incident_id: gen dui_first_cnty_fips_2 = dui_cnty_fips_2[1]
bys dui_incident_id: gen dui_first_cnty_fips_3 = dui_cnty_fips_3[1]
//bys dui_incident_id: gen first_dui_county_name = dui_county_name[1]

bys dui_incident_id: gen double dui_last_result = result[_N]
bys dui_incident_id: gen dui_last_test = _n==_N
bys dui_incident_id: gen dui_last_cnty1 = county_1[_N]
bys dui_incident_id: gen dui_last_cnty2 = county_2[_N]
bys dui_incident_id: gen dui_last_cnty3 = county_3[_N]
bys dui_incident_id: gen dui_last_cnty_fips_1 = dui_cnty_fips_1[_N]
bys dui_incident_id: gen dui_last_cnty_fips_2 = dui_cnty_fips_2[_N]
bys dui_incident_id: gen dui_last_cnty_fips_3 = dui_cnty_fips_3[_N]
//bys dui_incident_id: gen last_dui_county_name = dui_county_name[_N]


compress

save "$int_dir/breath_test_data/OhioBreathalyzer", replace



** Make a version of the data with one obs per incident
use "$int_dir/breath_test_data/OhioBreathalyzer.dta", clear

//generating variables for testing officer names
split testingofficer, parse(", ") gen(dui_opr)

rename dui_opr1 dui_oprl
rename dui_opr2 dui_oprf

//renaming officer's certification number
rename odhcertification dui_ocert

//rename other variables
rename testingagency dui_oagency
rename arrestingagency dui_aagency
rename arrestingofficer dui_aofficer

//fixing a problematic typo
replace dui_aagency = "OSHP MT GILEAD" if dui_aagency == "OSHP MY GILEAD"
replace dui_oagency = "OSHP MT GILEAD" if dui_oagency == "OSHP MY GILEAD"



*************************************
//GEN PRIMARY COUNTY FOR THOSE MERGED BY CITY
//Generating first and last "dui_primary_county" and "primary county fips" variables (we found which county had the largest portion of each city, and will use this county)
gen dui_first_primary_county = dui_first_cnty1
gen dui_first_primary_county_fips = dui_first_cnty_fips_1
gen dui_last_primary_county = dui_last_cnty1
gen dui_last_primary_county_fips = dui_last_cnty_fips_1

//the counties in Burkettsville were incorrectly recorded from the scrape, so here we fix this problem
if (city == "BURKETTSVILLE"){
	replace dui_first_cnty2 == "MERCER"
	replace dui_last_cnty2 == "MERCER"
	replace dui_first_cnty_fips_1 == "107"
	replace dui_last_cnty_fips_1 == "107"
}

//changing first/last county and first/last county fips for those whose primary county should be county 2
//ONLY FOR THOSE THAT MERGED IN THE FIRST MERGE (the merge on city)
foreach var in "VERONA" "CRESTON" "CARLISLE" "SCOTT" "RIDGEWAY" "WILSON" "CLAYTON" "NEW HOLLAND" "HUBER HEIGHTS" "UNION" "TALLMADGE" "KETTERING" "LYNCHBURG" "SPRINGBORO" "MAGNOLIA" "ADENA" "MOGADORE" "ROSEVILLE" "SHARONVILLE" "UTICA" "CLIFTON" "WESTERVILLE" "FAIRVIEW" "CANAL WINCHESTER" "BUCKEYE LAKE" "VERMILION" "REYNOLDSBURG" "BELLEVUE" "DUBLIN" "LOVELAND" "FOSTORIA" "BURKETTSVILLE"{
	replace dui_first_primary_county = dui_first_cnty2 if city == "`var'" & mergedin == 1
	replace dui_first_primary_county_fips = dui_first_cnty_fips_2 if city == "`var'" & mergedin == 1
	replace dui_last_primary_county = dui_last_cnty2 if city == "`var'" & mergedin == 1
	replace dui_last_primary_county_fips = dui_last_cnty_fips_2 if city == "`var'" & mergedin == 1
}

foreach var in "COLUMBUS" "MINERVA" "BALTIC" {
	replace dui_first_primary_county = dui_first_cnty3 if city == "`var'" & mergedin == 1
	replace dui_first_primary_county_fips = dui_first_cnty_fips_3 if city == "`var'" & mergedin == 1
	replace dui_last_primary_county = dui_last_cnty3 if city == "`var'" & mergedin == 1
	replace dui_last_primary_county_fips = dui_last_cnty_fips_3 if city == "`var'" & mergedin == 1
}
*************************************


*************************************
//GEN PRIMARY COUNTY FOR THOSE MERGED BY ARRESTING/TESTING AGENCY

//changing first/last county and first/last county fips for those whose primary county should be county 2
//ONLY FOR THOSE THAT MERGED IN THE FIRST MERGE (the merge on city)
foreach var in "SAINT CLAIRSVILLE HIGHWAY PATROL" "TOLEDO HIGHWAY PATROL" "CHILLICOTHE HIGHWAY PATROL" "NEW PHILADELPHIA HIGHWAY PATROL" "STEUBENVILLE HIGHWAY PATROL" "JACKSON HIGHWAY PATROL" "GEORGETOWN HIGHWAY PATROL" "DEFIANCE HIGHWAY PATROL" "MARYSVILLE HIGHWAY PATROL" "GALLIPOLIS HIGHWAY PATROL" "CAMBRIDGE HIGHWAY PATROL" "FINDLAY HIGHWAY PATROL" "BOWLING GREEN HIGHWAY PATROL" "MARIETTA HIGHWAY PATROL" "LIMA HIGHWAY PATROL" "WOOSTER HIGHWAY PATROL" "CARLISLE POLICE DEPARTMENT" "BLUFFTON POLICE DEPARTMENT" "OSHP MT GILEAD" "RITTMAN POLICE DEPARTMENT"{
	replace dui_first_primary_county = dui_first_cnty2 if ((dui_aagency == "`var'" & mergedin == 2) | (dui_oagency == "`var'" & mergedin == 3))
	replace dui_first_primary_county_fips = dui_first_cnty_fips_2 if ((dui_aagency == "`var'" & mergedin == 2) | (dui_oagency == "`var'" & mergedin == 3))
	replace dui_last_primary_county = dui_last_cnty2 if ((dui_aagency == "`var'" & mergedin == 2) | (dui_oagency == "`var'" & mergedin == 3))
	replace dui_last_primary_county_fips = dui_last_cnty_fips_2 if ((dui_aagency == "`var'" & mergedin == 2) | (dui_oagency == "`var'" & mergedin == 3))
}

foreach var in "DUBLIN POLICE DEPARTMENT" "DAYTON HIGHWAY PATROL"{
	replace dui_first_primary_county = dui_first_cnty3 if ((dui_aagency == "`var'" & mergedin == 2) | (dui_oagency == "`var'" & mergedin == 3))
	replace dui_first_primary_county_fips = dui_first_cnty_fips_3 if ((dui_aagency == "`var'" & mergedin == 2) | (dui_oagency == "`var'" & mergedin == 3))
	replace dui_last_primary_county = dui_last_cnty3 if ((dui_aagency == "`var'" & mergedin == 2) | (dui_oagency == "`var'" & mergedin == 3))
	replace dui_last_primary_county_fips = dui_last_cnty_fips_3 if ((dui_aagency == "`var'" & mergedin == 2) | (dui_oagency == "`var'" & mergedin == 3))
}
*************************************


keep dui_test_date dui_test_time dui_sex dui_male dui_age dui_test_year dui_test_month dui_test_day dui_bdate_max dui_bdate_min dui_incident_id dui_highest_result dui_lowest_result dui_first_result dui_last_result dui_first_cnty1 dui_first_cnty2 dui_first_cnty3 dui_last_cnty1 dui_last_cnty2 dui_last_cnty3 dui_fullname dui_middle_initial dui_suffix first_name dui_alt_first_name name_last_word last_name dui_alt_last_name f_* l_* dui_oprl dui_oprf dui_ocert dui_oagency dui_aofficer dui_aagency recorded_race race_black race_hispanic race_white f_likely_race f_probability_american_indian f_probability_asian f_probability_black f_probability_hispanic f_probability_white f_probability_2race l_likely_race l_probability_american_indian l_probability_asian l_probability_black l_probability_hispanic l_probability_white l_probability_2race rounded_lowest_result index above_limit interact dui_first_cnty_fips_1 dui_first_cnty_fips_2 dui_first_cnty_fips_3 dui_last_cnty_fips_1 dui_last_cnty_fips_2 dui_last_cnty_fips_3 result_raw refused dui_first_primary_county dui_last_primary_county dui_first_primary_county_fips dui_last_primary_county_fips

duplicates tag dui_incident_id, gen(dup)

sort dui_incident_id dui_test_time

bys dui_incident_id: drop if dup>0 & _n!=_N
//21,825 observations deleted. When we do this, we're keeping the officer information for the last recorded test.

drop dup


* Label vars
label var dui_test_date "Date of breath test"
label var dui_test_time "Time of breath test"
label var dui_middle_initial "Middle initial of breath tested person (constructed, not administratively recorded)"
label var dui_age "Age of breath tested person (administratively recorded, not constructed)"
label var dui_oprl "Last name of breath test operator"
label var dui_oprf "First initial of breath test operator"
label var dui_ocert "(?) Certification number of breath test operator"
label var dui_oagency "Agency of breath test operator"
label var dui_aofficer "Name of arresting officer"
label var dui_aagency "Agency of arresting officer"
label var dui_test_year "Year of breath test"
label var dui_test_month "Month of breath test"
label var dui_test_day "Day (of month) of breath test"
label var dui_male "Indicator for breath tested person being male"
label var dui_sex "Sex of breath tested person (administratively recorded)"
label var dui_bdate_max "Latest possible birthdate given age at test and date of test"
label var dui_bdate_min "Earliest possible birthdate given age at test and date of test"
label var dui_incident_id "Unique indentifier for breath test encounter"
label var dui_highest_result "Highest recorded BrAC result (where result is the min of two samples taken within a test)"
label var dui_lowest_result "Lowest recorded BrAC result"
//label var dui_highest_test "Highest recorded reading from any test"
label var dui_first_result "First recorded result (where result is the min of two samples taken within a test)"
label var dui_last_result "Last recorded result (where result is the min of two samples taken within a test)"
label var dui_first_cnty1 "First county associated with first recorded result"
label var dui_first_cnty2 "Second county associated with first recorded result (blank if N/A)" //not in texas
label var dui_first_cnty3 "Third county associated with first recorded result (blank if N/A)" //not in texas
//label var dui_first_cnty_name "County name associated with first recorded result"
label var dui_last_cnty1 "First county associated with last recorded result"
label var dui_last_cnty2 "Second county associated with last recorded result (blank if N/A)" //not in texas
label var dui_last_cnty3 "Third county associated with last recorded result (blank if N/A)" //not in texas
label var dui_first_cnty_fips_1 "County FIPS associated with first recorded result for county 1"
label var dui_first_cnty_fips_2 "County FIPS associated with first recorded result for county 2"
label var dui_first_cnty_fips_3 "County FIPS associated with first recorded result for county 3"
label var dui_last_cnty_fips_1 "County FIPS associated with last recorded result for county 1"
label var dui_last_cnty_fips_2 "County FIPS associated with last recorded result for county 2"
label var dui_last_cnty_fips_3 "County FIPS associated with last recorded result for county 3"
label var dui_first_primary_county "County primarily associated with first recorded result"
label var dui_last_primary_county "County primarily associated with last recorded result"
label var dui_first_primary_county_fips "County FIPS primarily associated with first recorded result"
label var dui_last_primary_county_fips "County FIPS primarily associated with last recorded result"
label var dui_fullname "Full name from breath test record"
label var dui_suffix "Suffix from breath test record"
label var first_name "First word of name"
label var dui_alt_first_name "All first name words, without suffixes or spaces"
label var f_first3 "First 3 letters of first name"
label var name_last_word "Last word of name"
label var last_name "Last listed last name, combining compound name and removing suffixes and spaces (e.g. VONLEHM not VON LEHM)"
label var dui_alt_last_name "All last name words, without suffixes or spaces"
label var l_first3 "First 3 letters of last name"
label var rounded_lowest_result "Rounded down BrAC"
label var index "BrAC, re-centered so .08 = 0"
label var above_limit "Indicator for BrAC >= .08"
label var interact "Interaction: index*above_limit"

//race re:what's just listed in the data (none, white, black, hispanic)
label var recorded_race "Race of tested person (administratively recorded, not constructed)"
label var race_black "Indicator for being Black (administratively recorded)"
label var race_hispanic "Indicator for being Hispanic (administratively recorded)"
label var race_white "Indicator for being White (administratively recorded)"

//constructed race probabilities:
label var f_likely_race "Highest probability race, based on first_name"
label var f_probability_american_indian "Share of people with same first_name who are American Indian"
label var f_probability_asian "Share of people with same first_name who are Asian"
label var f_probability_black "Share of people with same first_name who are Black"
label var f_probability_hispanic "Share of people with same first_name who are Hispanic"
label var f_probability_white "Share of people with same first_name who are White"
label var f_probability_2race "Share of people with same first_name who are multi-racial"
label var l_likely_race "Highest probability race, based on last_name"
label var l_probability_american_indian "Share of people with same last_name who are American Indian"
label var l_probability_asian "Share of people with same last_name who are Asian"
label var l_probability_black "Share of people with same last_name who are Black"
label var l_probability_hispanic "Share of people with same last_name who are Hispanic"
label var l_probability_white "Share of people with same last_name who are White"
label var l_probability_2race "Share of people with same last_name who are multi-racial"

//labeling result variables
label var result_raw "Original results with missing codes"
label var refused "A dummy for if they refused the breath test"

save "$int_dir/ohio_breath_tests_uniqueincident", replace



** Now make first-initial datasets for breath tests
use "$int_dir/ohio_breath_tests_uniqueincident.dta", clear

foreach var in A B C D E F G H I J K L M N O P Q R S T U V W X Y Z	{
	
	preserve
	gen temp = substr(first_name,1,1)
	keep if temp=="`var'"
	drop temp
	save $int_dir/ohio_breath_tests_uniqueincident_`var', replace
	restore
	
}
