**********************************************************************
/*
INPUT: county_`i'_fips.dta
INTERMEDIARY: county_city.dta
OUTPUT: city_final.dta
*/
*This file will read in city/county data and merge it with FIPS codes
**********************************************************************

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

//reading in county-fips info from website
readhtmltable https://en.wikipedia.org/wiki/List_of_municipalities_in_Ohio
compress


**********************
*PHASE 2 : Cleaning
**********************
//dropping unneeded columns from the table
keep t1c1 t1c4

//dropping first observation which should have been the variable name
drop if strpos(t1c1, "Name")

//renaming variables
rename t1c1 city
rename t1c4 county_1

//we used the code char(10) to replace line breaks which were at the end of each variable
replace county_1 = subinstr(county_1, char(10), "", .)
replace city = subinstr(city, char(10), "", .)

//cleaning the county and city variables
replace county_1 = upper(county_1)
replace city = upper(city)

//generating additional county variables
gen county_2 = ""
gen county_3 = ""

//fixing formatting
replace county_3 = city[_n+2] if county_1[_n+2] == "" & county_1[_n+1] == ""
replace county_2 = city[_n+1] if county_1[_n+1]==""

//dropping counties that were read as cities
drop if county_1 == ""

duplicates drop

//there are multiple towns named CENTERVILLE (2) and OAKWOOD (3), so we drop them from the data
drop if city == "CENTERVILLE"
drop if city == "OAKWOOD"

// save to intermediate directory
save "$int_dir/county_city", replace


**************************
*PHASE 3 : Add FIPS codes 
**************************
//merge in the FIPS codes from SCRAPE_county_fips.do
forval i = 1/3{
	merge m:1 county_`i' using "$int_dir/county_`i'_fips"
	drop if _m == 2
	drop _m
}

compress

//save final "city" file
save "$int_dir/city_final", replace