************************************************************
/*
INPUT: step1_names.do, city_final.dta, arrestingagency_final.dta, testingagency_final.dta

INTERMEDIARY: city_merge.dta, arrestingagency_merge.dta, testingagency_merge.dta

OUTPUT: merged_data.dta
*/

*This file will read in cleaned data from step1_names.do and merge it by city, arrestingagency, and testing agency in a ladder process. Then it will finish the data cleaning, labeling, and save the completed file
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
use "$int_dir/step1_names.dta", clear

*********
*PHASE 1
*********
//merging by city and saving merged values to an external set so that we can attempt to merge the remaining values by arresting or testing agency

//generating a variable to determine which merge step each observation was matched in (this varaiable will hold a 0 if the observation was not matched in any merge, a 1 if it was matched in the first merge, etc.)
gen mergedin = 0

*****
*MERGE 1
*****
//merge on city
merge m:1 city using "$int_dir/city_final"
//120,370 observations matched
//23,740 not matched (from master)
preserve

//saving matched varibales as a separate file to append with later
drop if _m!=3
replace mergedin = 1 if _m == 3
drop _m
save "$int_dir/city_merge.dta", replace
//returning to original data and dropping all amtched observations
restore
drop if _m == 3
drop _m


*****
*MERGE 2
*****
//merging unmatched observations on arrestingagency
merge m:1 arrestingagency using "$int_dir/arrestingagency_final", update
//17,020 matched
//6,793 not matched (from master)
preserve

//saving matched varibales as a separate file to append with later
drop if _m!=3
replace mergedin = 2 if _m == 3
drop _m
save "$int_dir/arrestingagency_merge.dta", replace

//returning to original data and dropping all matched observations
restore
drop if _m == 3
drop _m


*****
*MERGE 3
*****
//merging unmatched observations on testingagency
merge m:1 testingagency using "$int_dir/testingagency_final", update
//1,129 matched
//5,888 not matched (from master)
preserve

//saving matched varibales as a separate file to append with later
drop if _m!=3
replace mergedin = 3 if _m == 3
drop _m
save "$int_dir/testingagency_merge.dta", replace

//returning to original data and dropping all matched observations
restore
drop if _m == 3
drop _m

//some were empty observations
drop if fullname == ""
//5,591 unmatched observations from master file

//Appending matches from each step
append using "$int_dir/city_merge.dta" "$int_dir/arrestingagency_merge.dta" "$int_dir/testingagency_merge.dta"

save "$int_dir/merged_data", replace