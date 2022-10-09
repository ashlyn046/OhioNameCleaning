*******************************************************************
/*
INPUT:	ohio_breath_tests_uniqueincident_`A-Z'.dta, voter_file_`A-Z'.dta
INTERMEDIARY: dui_voter_merged_'A-Z'.dta
OUTPUT: dui_voter_merged_all.dta
*/
*This file will merge data from the Ohio DUI file and voter files
*******************************************************************

clear all
set more off
program drop _all



**Root directory
*global root_dir "/Volumes/Elements/DUI/ohio"
*global root_dir "/Users/ll263/Box/DUI/ohio"
*global root_dir C:/Users/andersee/Box/DUI/ohio
global root_dir C:/Users/ashlyn04/Box/DUIAshlyn/ohio
*global root_dir "/Users/mukaie/Box/DUIAshlyn/ohio"

global raw_dir "$root_dir/raw_data/breath_test_data"
global int_dir "$root_dir/intermediate_data"
global clean_dir "$root_dir/clean_data"


	
**Now we'll merge to the voter files

foreach y in A B C D E F G H I J K L M N O P Q R S T U V W X Y Z {
	
	**ROUND 1
	
	display "Round 1"
	use "$int_dir/ohio_breath_tests_uniqueincident_`y'.dta", clear
	
	* PHASE 1
	mmerge first_name last_name using "$int_dir/voter_file_`y'.dta"

	*Work with observations that merged
	keep if _merge==3
	drop _merge
	
		
	
	* PHASE 2
	**Get rid of matches where the age at stop and voter birthdate aren't compatible
	keep if voter_b_date < dui_bdate_max & voter_b_date > dui_bdate_min
	

	
	**Compare middle initials
	gen dui_voter_mi_match = dui_middle_initial==voter_middle_initial
	gen dui_voter_mi_none = dui_middle_initial=="" & voter_middle_initial==""
	gen mi_mismatch = dui_middle_initial!=voter_middle_initial & dui_middle_initial!="" & voter_middle_initial!=""
	
	
	
	* PHASE 3
	*Drop if the middle initials are mismatched (neither missing & they aren't the same)
	drop if mi_mismatch
	drop mi_mismatch
	
	
	**At this point we want to count how many people satisfy the criteria for each DUI incident.
	**We may use this to further prune observations when we perform the eventual analysis.
	
	duplicates tag dui_incident_id, gen(dui_voter_dup_initial)
	
	* PHASE 4
	*Drop obs with a missing middle initial in the voter file when there exists another potential merge with matching middle initial
	egen dui_voter_num_mi_match = sum(dui_voter_mi_match), by(dui_incident_id)
	drop if dui_voter_mi_match==0 & dui_voter_num_mi_match>0
	
	
	* PHASE 5
	*Drop obs where there is no middle initial in the DUI record, there is one in the voter file, and there exists another potential merge from the voter file with no middle initial
	egen dui_voter_num_mi_none = sum(dui_voter_mi_none), by(dui_incident_id)
	drop if dui_voter_mi_none==0 & dui_voter_num_mi_none>0
	
	
	
	* PHASE 6
	**Compare alternative first and last names (which include more words from the raw names) 
	gen dui_voter_alt_first_name_match=dui_alt_first_name==voter_alt_first_name & dui_alt_first_name~=""
	gen dui_voter_alt_last_name_match=dui_alt_last_name==voter_alt_last_name & dui_alt_last_name~=""
	
	*Drop people with an alternative name match with some record but don't have an alternative name match with the current record.
	egen num_alt_first_match=sum(dui_voter_alt_first_name_match), by(dui_incident_id)
	egen num_alt_last_match=sum(dui_voter_alt_last_name_match), by(dui_incident_id)
		
	drop if dui_voter_alt_first_name_match==0 & num_alt_first_match>0
	drop if dui_voter_alt_last_name_match==0 & num_alt_last_match>0
	
	
	
	* PHASE 7
	**Compare suffixes
	gen dui_voter_suffix_match = dui_suffix==voter_suffix & dui_suffix!=""
	
	egen dui_voter_num_suffix_match = sum(dui_voter_suffix_match), by(dui_incident_id)
	drop if dui_voter_suffix_match==0 & dui_voter_num_suffix_match>0
	
	* PHASE 8
	** Compare counties
	//we have multiple counties because of the department merging in step one, so this code is a little different from the Texas code
	gen dui_voter_county_match = 0
	replace dui_voter_county_match = 1 if (dui_last_primary_county_fips ==voter_county_fips) 

	
	*Drop observations within each incident that don't match the county if there is another observation in that incident that matches the county
	egen dui_voter_num_county_match=sum(dui_voter_county_match), by(dui_incident_id)
	drop if dui_voter_county_match==0 & dui_voter_num_county_match>0
	
	drop num*
	
	
	* PHASE 9
	**We have some multiple matches--some of these are people who presumably moved because
	**the birthdates are exactly the same.  Others are people with extremely common names.
	
	
	**Let's get rid of duplicates where the birthdate is exactly the same and it looks plausible that 
	**the person just moved and hence was registered to vote in 2 places.  Note that there are
	**a couple of cases where the duplicates appear to be 2 people registered in different places
	**with middle names that are just slightly different spellings.
	duplicates drop dui_incident_id first_name last_name voter_middle_initial voter_b_date, force
	
	**Inventory the obs we have left
	duplicates tag dui_incident_id, gen(dui_voter_dup)
	
	
	* PHASE 10
	**Add back in incidents that didn't merge
	append using "$int_dir/ohio_breath_tests_uniqueincident_`y'.dta"
	gen dui_voter_no_match = sos_voterid==""
	bys dui_incident_id: egen temp = min(dui_voter_no_match)
	drop if dui_voter_no_match==1 & temp==0
	drop temp
	
	save "$int_dir/dui_voter_merged_`y'.dta", replace
	
		
}	
	
	
*** Append all first-initial datasets together
clear

foreach y in A B C D E F G H I J K L M N O P Q R S T U V W X Y Z {
    
	append using "$int_dir/dui_voter_merged_`y'.dta"
	
}

compress
save "$int_dir/dui_voter_merged_all", replace
