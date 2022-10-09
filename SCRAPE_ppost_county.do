****************************************************************
/*
INPUT: N/A (data scraped from https://wiki.radioreference.com/index.php/Ohio_State_Highway_Patrol_(OSP)_(OH))

INTERMEDIARY: patrol_post_depts.dta, ppost_county_`y'.txt, ppost_county.dta 

OUTPUT: OSP_county_clean.dta
*/

*This file will read in and clean patrol_post/county info
****************************************************************

**Root directory
global root_dir "/Users/ashlyn04/Box/DUIAshlyn/ohio"
*global root_dir "/Users/mukaie/Box/DUIAshlyn/ohio"

global raw_dir "$root_dir/raw_data"
global int_dir "$root_dir/intermediate_data"
global clean_dir "$root_dir/clean_data"

**********************
*PHASE 1 : Webscrape
**********************
//installing webscrape package 
net install readhtml, from(https://ssc.wisc.edu/sscc/stata/)

//reading in patrolpost-county info from website
readhtmltable https://wiki.radioreference.com/index.php/Ohio_State_Highway_Patrol_(OSP)_(OH)


******************************************************************
*PHASE 2 : Clean

**part 1: reduce all scrapeed data to just offices and counties
**part 2: reformat to make dataset mergeable
**part 3: match format of other department/county dataset
******************************************************************
**Cleaning part 1 (all the scraped data)
//drop useless variables
drop t1c1 t1c2 t2c3 t3c3 t4c3 t5c3 t6c3 t7c3 t8c3 t9c3 t10c3 t11c3 t12c3 t13c1 t13c2

//rename vars
rename t2c1 county_1
rename t2c2 post_1
rename t3c1 county_2
rename t3c2 post_2
rename t4c1 county_3
rename t4c2 post_3
rename t5c1 county_4
rename t5c2 post_4
rename t6c1 county_5
rename t6c2 post_5
rename t7c1 county_6
rename t7c2 post_6
rename t8c1 county_7
rename t8c2 post_7
rename t9c1 county_8
rename t9c2 post_8
rename t10c1 county_9
rename t10c2 post_9
rename t11c1 county_10
rename t11c2 post_10
rename t12c1 county_11
rename t12c2 post_11

//remove leading/lagging spaces
forvalues y=1/11 {
replace county_`y' = strtrim(county_`y')
replace post_`y' = strtrim(post_`y')
}

//drop first observation (just a repeat of variable names)
gen id = _n
drop if id == 1
drop id

//save to raw data directory to allow further manipulation
save "$raw_dir/department_county/patrol_post_depts", replace


**Cleaning part 2.1 (reformatting to make dataset mergeable-- separate areas 1-11)
//isolate each post/dept set to its own .txt file
forvalues y=1/11 {
	
use "$raw_dir/department_county/patrol_post_depts.dta", clear
keep county_`y' post_`y'

//rename to prep for appending in part 2.2 (we want two variables: county, post)
rename county_`y' county
rename post_`y' post

//export each area txt file for part 2.2
export delimited "$int_dir/county_dept_cleaning/ppost_county_`y'.txt", replace
}
clear


**Cleaning part 2.2 (reformatting to make dataset mergeable-- combine all ppost_county_`y'.txt files into single dataset)
//make a tempfile to rebuild the dataset
tempfile master
save master, replace empty

//read in & append each ppost_county_`y'.txt file to the one before it 
forvalues y=1/11 {
clear

import delimited using "$int_dir/county_dept_cleaning/ppost_county_`y'.txt", clear

append using master, force
save master, replace
}
//save recombined data to intermediate directory for further cleaning
save "$int_dir/county_dept_cleaning/ppost_county", replace


**Cleaning part 3 (matching observation format of other dept/county dataset)
use "$int_dir/county_dept_cleaning/ppost_county.dta", clear

//get rid of empty observations & variable names that became observations
drop if v2 =="post"
drop if v1 ==""

//reducing v2 (post) observations to just the patrol office name, making post number var
gen starts_with_patrolled = strpos(v2, "patrolled")
split v2 if starts_with_patrolled == 1, parse("patrolled by ")
drop v21
replace v2 = v22 if starts_with_patrolled == 1
drop v22 starts_with_patrolled

gen contains_dash = 1 if strpos(v2, "-")
split v2 if contains_dash == 1, parse("-")
replace v2=v22 if contains_dash == 1
rename v21 post_num
drop v22 contains_dash

gen end_parentheses = 1 if substr(reverse(v2), 1, 1) == ")"
split v2 if end_parentheses == 1, parse(" (")
replace v2 = v21 if !(strpos(v21, "Post")) & end_parentheses == 1
drop v22
split v21 if end_parentheses == 1
replace post_num = v212 if post_num=="" & v212 !=""
replace v2 = v213 if v213!=""
drop end_parentheses v21*

gen starts_with_post = strpos(v2, "Post ")
split v2 if starts_with_post == 1
replace post_num = v22 if v22 != ""
replace v2 = v23 if v23 != "" & v24 == ""
replace v2 = v23 + " " + v24 if v23 != "" & v24 != ""
rename v2 arrestingagency
drop v2* starts_with_post

split post_num, gen(numbers)
replace post_num = numbers2 if numbers2 != ""
drop numbers*

//cleaning county var : capitalize, drop "county" in name
rename v1 county_
split county_, parse(" ")
replace county_1 = county_1 + " " + county_2 if county_3 != ""
replace county_1 = upper(county_1)
drop county_ county_2 county_3

//cleaning post var : capitalize agencies 
replace arrestingagency = upper(arrestingagency)

//name matching between datasets
replace county_1 = "VANWERT" if county_1 == "VAN WERT"
replace arrestingagency = "VANWERT" if arrestingagency == "VAN WERT"
replace arrestingagency = "OHIO STATE PATROL ACADEMY" if post_num == "96"
replace arrestingagency = "CINCINNATI" if arrestingagency == "CINCINNATI (EASTERN"
replace arrestingagency = "SAINT CLAIRSVILLE" if arrestingagency == "ST. CLAIRSVILLE"
replace arrestingagency = "MT GILEAD" if arrestingagency == "MT. GILEAD"

//some posts patrol multiple counties: create multiple county vars, combine observations
sort post_num
duplicates list arrestingagency, sepby(arrestingagency)

gen agency_count=1

gen id = _n
replace agency_count=2 if id==4
replace agency_count=3 if id==5
replace agency_count=2 if id==8
replace agency_count=2 if id==11
replace agency_count=2 if id==13
replace agency_count=2 if id==16
replace agency_count=2 if id==18
replace agency_count=2 if id==21
replace agency_count=2 if id==23
replace agency_count=2 if id==27
replace agency_count=2 if id==30
replace agency_count=2 if id==34
replace agency_count=2 if id==36
replace agency_count=2 if id==41
replace agency_count=2 if id==44
replace agency_count=2 if id==49
replace agency_count=2 if id==51
replace agency_count=3 if id==52
replace agency_count=2 if id==54
replace agency_count=2 if id==56
replace agency_count=2 if id==58
replace agency_count=2 if id==62
replace agency_count=2 if id==65
replace agency_count=2 if id==67
replace agency_count=2 if id==70
replace agency_count=2 if id==73
replace agency_count=2 if id==75
replace agency_count=2 if id==77
replace agency_count=3 if id==78
replace agency_count=2 if id==80
replace agency_count=2 if id==83
replace agency_count=2 if id==85
replace agency_count=2 if id==87
drop id

/*
//i can't get this loop to work rip (we don't need it bc of lines 179-214 work instead, but this would be much more efficient)
forvalues i=1/95 {
	local j=`i'-1
	local k=`i'-2

	//replace agency_count[`i'] = agency_count[`j'] + 1 if arrestingagency[`j']==arrestingagency[`i']
	replace agency_count[`i'] = 2 if arrestingagency[`j']==arrestingagency[`i']
	replace agency_count[`i'] = 3 if arrestingagency[`k']==arrestingagency[`i']
	
//error: weights not allowed? r(101)
}
*/

//format is now able to handle the reshape command
reshape wide county_1, i(arrestingagency) j(agency_count)
rename county_11 county_1
rename county_12 county_2
rename county_13 county_3

//adding the counties that were mentioned in comments with the initial scrape
replace county_2 = "HAMILTON" if post_num == "13"
replace county_2 = "FRANKLIN" if post_num == "21" | post_num == "49" | post_num == "23" | post_num == "45"
replace county_3 = "FRANKLIN" if post_num == "65"

//normalizing patrol post names 
replace arrestingagency = "ZANESVILLE HIGHWAY PATROL" if arrestingagency == "ZANESVILLE"
replace arrestingagency = "XENIA HIGHWAY PATROL" if arrestingagency == "XENIA"
replace arrestingagency = "WOOSTER HIGHWAY PATROL" if arrestingagency == "WOOSTER"
replace arrestingagency = "WILMINGTON HIGHWAY PATROL" if arrestingagency == "WILMINGTON"
replace arrestingagency = "WEST JEFFERSON HIGHWAY PATROL" if arrestingagency == "WEST JEFFERSON"
replace arrestingagency = "WARREN HIGHWAY PATROL" if arrestingagency == "WARREN"
replace arrestingagency = "WAPAKONETA HIGHWAY PATROL" if arrestingagency == "WAPAKONETA"
replace arrestingagency = "VANWERT HIGHWAY PATROL" if arrestingagency == "VANWERT"
replace arrestingagency = "TOLEDO HIGHWAY PATROL" if arrestingagency == "TOLEDO"
replace arrestingagency = "STEUBENVILLE HIGHWAY PATROL" if arrestingagency == "STEUBENVILLE"
replace arrestingagency = "SPRINGFIELD HIGHWAY PATROL" if arrestingagency == "SPRINGFIELD"
replace arrestingagency = "SANDUSKY HIGHWAY PATROL" if arrestingagency == "SANDUSKY"
replace arrestingagency = "SAINT CLAIRSVILLE HIGHWAY PATROL" if arrestingagency == "SAINT CLAIRSVILLE"
replace arrestingagency = "RAVENNA HIGHWAY PATROL" if arrestingagency == "RAVENNA"
replace arrestingagency = "PORTSMOUTH HIGHWAY PATROL" if arrestingagency == "PORTSMOUTH"
replace arrestingagency = "PIQUA HIGHWAY PATROL" if arrestingagency == "PIQUA"
replace arrestingagency = "NORWALK HIGHWAY PATROL" if arrestingagency == "NORWALK"
replace arrestingagency = "NEW PHILADELPHIA HIGHWAY PATROL" if arrestingagency == "NEW PHILADELPHIA"
replace arrestingagency = "OSHP MY GILEAD" if arrestingagency == "MT GILEAD"
replace arrestingagency = "MEDINA HIGHWAY PATROL" if arrestingagency == "MEDINA"
replace arrestingagency = "MARYSVILLE HIGHWAY PATROL" if arrestingagency == "MARYSVILLE"
replace arrestingagency = "MARION HIGHWAY PATROL" if arrestingagency == "MARION"
replace arrestingagency = "MARIETTA HIGHWAY PATROL" if arrestingagency == "MARIETTA"
replace arrestingagency = "MANSFIELD HIGHWAY PATROL" if arrestingagency == "MANSFIELD"
replace arrestingagency = "LISBON HIGHWAY PATROL" if arrestingagency == "LISBON"
replace arrestingagency = "LIMA HIGHWAY PATROL" if arrestingagency == "LIMA"
replace arrestingagency = "LEBANON HIGHWAY PATROL" if arrestingagency == "LEBANON"
replace arrestingagency = "LANCASTER HIGHWAY PATROL" if arrestingagency == "LANCASTER"
replace arrestingagency = "JACKSON HIGHWAY PATROL" if arrestingagency == "JACKSON"
replace arrestingagency = "IRONTON HIGHWAY PATROL" if arrestingagency == "IRONTON"
replace arrestingagency = "HAMILTON HIGHWAY PATROL" if arrestingagency == "HAMILTON"
replace arrestingagency = "GRANVILLE HIGHWAY PATROL" if arrestingagency == "GRANVILLE"
replace arrestingagency = "GEORGETOWN HIGHWAY PATROL" if arrestingagency == "GEORGETOWN"
replace arrestingagency = "GALLIPOLIS HIGHWAY PATROL" if arrestingagency == "GALLIPOLIS"
replace arrestingagency = "FREMONT HIGHWAY PATROL" if arrestingagency == "FREMONT"
replace arrestingagency = "FINDLAY HIGHWAY PATROL" if arrestingagency == "FINDLAY"
replace arrestingagency = "ELYRIA HIGHWAY PATROL" if arrestingagency == "ELYRIA"
replace arrestingagency = "DELAWARE HIGHWAY PATROL" if arrestingagency == "DELAWARE"
replace arrestingagency = "DEFIANCE HIGHWAY PATROL" if arrestingagency == "DEFIANCE"
replace arrestingagency = "DAYTON HIGHWAY PATROL" if arrestingagency == "DAYTON"
replace arrestingagency = "HIRAM HIGHWAY PATROL" if post_num == "91"
replace arrestingagency = "MILAN HIGHWAY PATROL" if post_num == "90"
replace arrestingagency = "SWANTON HIGHWAY PATROL" if post_num == "89"
replace arrestingagency = "ASHLAND HIGHWAY PATROL" if arrestingagency == "ASHLAND"
replace arrestingagency = "ASHTABULA HIGHWAY PATROL" if arrestingagency == "ASHTABULA"
replace arrestingagency = "ATHENS HIGHWAY PATROL" if arrestingagency == "ATHENS"
replace arrestingagency = "BATAVIA HIGHWAY PATROL" if arrestingagency == "BATAVIA"
replace arrestingagency = "BOWLING GREEN HIGHWAY PATROL" if arrestingagency == "BOWLING GREEN"
replace arrestingagency = "BUCYRUS HIGHWAY PATROL" if arrestingagency == "BUCYRUS"
replace arrestingagency = "CAMBRIDGE HIGHWAY PATROL" if arrestingagency == "CAMBRIDGE"
replace arrestingagency = "CANFIELD HIGHWAY PATROL" if arrestingagency == "CANFIELD"
replace arrestingagency = "CANTON HIGHWAY PATROL" if arrestingagency == "CANTON"
replace arrestingagency = "CHARDON HIGHWAY PATROL" if arrestingagency == "CHARDON"
replace arrestingagency = "CHILLICOTHE HIGHWAY PATROL" if arrestingagency == "CHILLICOTHE"
replace arrestingagency = "CINCINNATI HIGHWAY PATROL" if arrestingagency == "CINCINNATI"
replace arrestingagency = "CIRCLEVILLE HIGHWAY PATROL" if arrestingagency == "CIRCLEVILLE"
replace arrestingagency = "COLUMBUS HIGHWAY PATROL" if arrestingagency == "COLUMBUS"

//save clean dataset to int. directory in order to merge with other dept/county dataset
save "$int_dir/OSP_county_clean", replace