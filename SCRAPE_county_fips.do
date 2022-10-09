*************************************************
/*
INPUT: N/A
INTERMEDIARY: N/A
OUTPUT: county_1_fips.dta, county_2_fips.dta, county_3_fips.dta
*/

*This file will read in county/fips code info
*************************************************

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
readhtmltable https://en.wikipedia.org/wiki/List_of_counties_in_Ohio

**********************
*PHASE 2 : Cleaning
**********************

//taking random strings of characters out of observations
replace t1c1 = subinstr(t1c1, "&#32;", " ", .)

//dropping unneeded columns from the table
forval i = 3/9{
	drop t1c`i' 
}
forval i = 1/2{
	drop t2c`i' 
}
forval i = 1/2{
	drop t3c`i' 
}
forval i = 1/3{
	drop t4c`i' 
}

//dropping first observation which should have been the variable name
drop if strpos(t1c2, "FIPS")

//renaming variables
rename t1c1 county_1
rename t1c2 fips_1

//we used the code char(10) to replace line breaks which were at the end of each variable
replace county_1 = subinstr(county_1, char(10), "", .)
replace fips_1 = subinstr(fips_1, char(10), "", .)

//cleaning the county variable
replace county_1 = subinstr(county_1, " County", "", .)
replace county_1 = upper(county_1)

// save to intermediate directory
save "$int_dir/county_1_fips", replace

//county_2
rename county_1 county_2
rename fips_1 fips_2
// save to intermediate directory
save "$int_dir/county_2_fips", replace

//county_3
rename county_2 county_3
rename fips_2 fips_3
// save to intermediate directory
save "$int_dir/county_3_fips", replace


