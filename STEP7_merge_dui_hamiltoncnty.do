*************************************************************************************
/*
INPUT: ohio_breath_tests_uniqueincident.dta, hamilton_county_breathtestyears.dta

INTERMEDIARY: ohio_breath_tests_uniqueincident_hc.dta, dui_hc_round1.dta, dui_hc_round2.dta, tmpdat.dta, dui_hc_round3.dta, dui_hc_round4.dta, dui_hc_round5.dta, dui_hc_round6.dta

OUTPUT: dui_hc_merged.dta
*/

*This do-file cleans court records from criminal cases in the Hamilton County District Court.
*************************************************************************************
/*
v1: Use "RECORD LAYOUT - 2020-12-08.xlsx" to read in data and name variables

ROUND 4: Merge on first 3 of first name + likely birth year (or age at filing + age at breath test)
*/


**Root directory--This is all that needs to be changed across users
global root_dir C:/Users/ashlyn04/Box/DUIAshlyn/ohio
*global root_dir "/Users/mukaie/Box/DUIAshlyn/ohio"


**Note that the directory pir_sos_20210118 may need to be uncompressed for the file to run
global raw_dir "$root_dir/raw_data/courts_data"
global int_dir "$root_dir/intermediate_data"
global clean_dir "$root_dir/clean_data"


clear all
set more off



	use "$int_dir/ohio_breath_tests_uniqueincident" if dui_last_primary_county_fips=="061", clear

	save "$int_dir/ohio_breath_tests_uniqueincident_hc", replace
	
	*** ROUND 1 MERGE
	display "Round 1"
	
	* STEP 1
	mmerge first_name last_name using "$int_dir/hamilton_county_breathtestyears.dta"
	keep if _merge==3
	drop _merge
	
	
	* STEP 1.2 (no analogue for this step in voter file merge)
	**Get rid of matches where filing date is before stop date or more than 2 days after stop date (matches after that appear low quality)
	drop if court_file_date-dui_test_date<0 | court_file_date-dui_test_date>2
		
	
	* STEP 2
	**Get rid of matches where the age at stop and voter birthdate aren't compatible
	keep if court_b_date < dui_bdate_max & court_b_date > dui_bdate_min
	
	
	**Compare middle initials
	gen mi_match = dui_middle_initial==court_middle_initial
	gen mi_none = dui_middle_initial=="" & court_middle_initial==""
	gen mi_mismatch = dui_middle_initial!=court_middle_initial & dui_middle_initial!="" & court_middle_initial!=""
	
	
	* STEP 3
	*Drop if the middle initials are mismatched (neither missing & they aren't the same)
	drop if mi_mismatch
	
	//unique dui_incident_id
	//di _N
	
	
	* STEP 4
	*Drop obs with a missing middle initial in the voter file when there exists another potential merge with matching middle initial
	egen num_mi_match = sum(mi_match), by(dui_incident_id)
	drop if mi_match==0 & num_mi_match>0
	
	//di _N
	
	
	* STEP 5
	*Drop obs where there is no middle initial in the DUI record, there is one in the voter file, and there exists another potential merge from the voter file with no middle initial
	egen num_mi_none = sum(mi_none), by(dui_incident_id)
	drop if mi_none==0 & num_mi_none>0
	
	//di _N
	
	
	* STEP 6
	**Compare alternative first and last names (which include more words from the raw names) 
	gen alt_first_name_match=dui_alt_first_name==court_alt_first_name & dui_alt_first_name~=""
	gen alt_last_name_match=dui_alt_last_name==court_alt_last_name & dui_alt_last_name~=""
	
	**Drop people who have an alternative name match with some record but don't have an alternative name match with the current record.
	egen num_alt_first_match=sum(alt_first_name_match), by(dui_incident_id)
	egen num_alt_last_match=sum(alt_last_name_match), by(dui_incident_id)
		
	drop if alt_first_name_match==0 & num_alt_first_match>0
	drop if alt_last_name_match==0 & num_alt_last_match>0
	
	
	* STEP 7
	**Compare suffixes
	gen suffix_match = dui_suffix==court_suffix & dui_suffix!=""
	
	egen num_suffix_match = sum(suffix_match), by(dui_incident_id)
	drop if suffix_match==0 & num_suffix_match>0
	
	

	***Looking at observations that matched to multiple crimes, it appears that these are multiple offenses for the same breath test incident. We'll keep them all in for now.
	egen num_off_filed=sum(1), by(dui_incident_id)
	
	
	
	***Save the incidents we've matched so far
	gen hc_round1match = 1
	
	save "$int_dir/dui_hc_round1", replace
	
	
	
	**Now we'll merge them back into the original DUI file so we get back all of the observations that didn't merge
	use "$int_dir/dui_hc_round1", clear
	
	mmerge dui_incident_id using "$int_dir/ohio_breath_tests_uniqueincident_hc"
	tab _merge
	
	keep if _m==2
	drop _m
	drop court_case_number-num_off_filed
	
	
	*** ROUND 2 MERGE
	display "Round 2"
	
	*** Some people with composite last names in the DUI data (e.g. CHAVARRIA-SANTOS, merge on CHAVARRIA, not SANTOS)--this
	*** merges these folks.

	
	**Now I figure out the length of variables so I can pull the first part of the alt_last_name off to merge it
	gen length_lname=length(last_name)	
	gen length_alname=length(dui_alt_last_name)

	
	**I will merge on the basis of the first part of the alt last name.  I change the names of some variables
	**to do this
	rename last_name dui_last_name
	
	
	**I create new last name with first part of compound last name
	gen last_name=substr(dui_alt_last_name,1,length_alname-length_lname) if length_alname>0
	
	**We'll only merge people with non-missing values of the edited last names
	
	keep if last_name~=""
	
	
	**Now merge to the hamilton county data.
	mmerge first_name last_name using "$int_dir/hamilton_county_breathtestyears.dta"
	
	
	
	keep if _merge==3
	drop _merge length_*
	
	**Now do the other restrictions from the round 1 merges
		
	**Get rid of matches where filing date is before stop date or more than 2 days after stop date (matches after that appear low quality)
	drop if court_file_date-dui_test_date<0 | court_file_date-dui_test_date>2
		
	**Get rid of matches where the age at stop and voter birthdate aren't compatible
	keep if court_b_date < dui_bdate_max & court_b_date > dui_bdate_min
	
	
	**Compare middle initials
	gen mi_match = dui_middle_initial==court_middle_initial
	gen mi_none = dui_middle_initial=="" & court_middle_initial==""
	gen mi_mismatch = dui_middle_initial!=court_middle_initial & dui_middle_initial!="" & court_middle_initial!=""
	
	
	*Drop if the middle initials are mismatched (neither missing & they aren't the same)
	drop if mi_mismatch
	
	//unique dui_incident_id
	//di _N
	
	
	*Drop obs with a missing middle initial in the voter file when there exists another potential merge with matching middle initial
	egen num_mi_match = sum(mi_match), by(dui_incident_id)
	drop if mi_match==0 & num_mi_match>0
	
	//di _N
	
	
	*Drop obs where there is no middle initial in the DUI record, there is one in the voter file, and there exists another potential merge from the voter file with no middle initial
	egen num_mi_none = sum(mi_none), by(dui_incident_id)
	drop if mi_none==0 & num_mi_none>0
	
	//di _N
	
		
	**Compare alternative first and last names (which include more words from the raw names) 
	gen alt_first_name_match=dui_alt_first_name==court_alt_first_name & dui_alt_first_name~=""
	gen alt_last_name_match=dui_alt_last_name==court_alt_last_name & dui_alt_last_name~=""
	
	**Drop people who have an alternative name match with some record but don't have an alternative name match with the current record.
	egen num_alt_first_match=sum(alt_first_name_match), by(dui_incident_id)
	egen num_alt_last_match=sum(alt_last_name_match), by(dui_incident_id)
		
	drop if alt_first_name_match==0 & num_alt_first_match>0
	drop if alt_last_name_match==0 & num_alt_last_match>0
	
	
	**Compare suffixes
	gen suffix_match = dui_suffix==court_suffix & dui_suffix!=""
	
	egen num_suffix_match = sum(suffix_match), by(dui_incident_id)
	drop if suffix_match==0 & num_suffix_match>0
	
	
	
	
	egen num_off_filed=sum(1), by(dui_incident_id)

	
	***Save the incidents we've matched so far
	gen hc_round2match = 1
		
	save "$int_dir/dui_hc_round2", replace
	
	
	
	
	*** ROUND 3 MERGE
	
	display "Round 3"
	
	*** Some people with composite last names in the courts data (e.g. CHAVARRIA-SANTOS, merge on CHAVARRIA, not SANTOS)--this
	*** merges these folks.  We need to start with the hamilton county data first for this merge.

	use "$int_dir/hamilton_county_breathtestyears.dta"
	
	
	**Now I figure out the length of variables so I can pull the first part of the alt_last_name off to merge it
	gen length_lname=length(last_name)	
	gen length_alname=length(court_alt_last_name)

	
	**I will merge on the basis of the first part of the alt last name.  I change the names of some variables
	**to do this
	rename last_name court_last_name
	
	
	**I create new last name with first part of compound last name
	gen last_name=substr(court_alt_last_name,1,length_alname-length_lname) if length_alname>0
	
	**We'll only merge people with non-missing values of the edited last names
	
	keep if last_name~=""
	
	save "$int_dir/tmpdat.dta", replace
	
	
	
	**Now call up the dui data that we're looking to merges
	use "$int_dir/dui_hc_round2"
	append using "$int_dir/dui_hc_round1"
	**Now we'll merge them back into the original DUI file so we get back all of the observations that didn't merge
	
	mmerge dui_incident_id using "$int_dir/ohio_breath_tests_uniqueincident_hc"
	tab _merge
	
	keep if _m==2
	drop _m
	drop court_case_number-num_off_filed
	
	
	
	**Now merge to the hamilton county data.
	mmerge first_name last_name using "$int_dir/tmpdat.dta"
	
		
	keep if _merge==3
	drop _merge length_*
	
	**Now do the other restrictions from the round 1 merges
		
	**Get rid of matches where filing date is before stop date or more than 2 days after stop date (matches after that appear low quality)
	drop if court_file_date-dui_test_date<0 | court_file_date-dui_test_date>2
		
	**Get rid of matches where the age at stop and voter birthdate aren't compatible
	keep if court_b_date < dui_bdate_max & court_b_date > dui_bdate_min
	
	
	**Compare middle initials
	gen mi_match = dui_middle_initial==court_middle_initial
	gen mi_none = dui_middle_initial=="" & court_middle_initial==""
	gen mi_mismatch = dui_middle_initial!=court_middle_initial & dui_middle_initial!="" & court_middle_initial!=""
	
	
	*Drop if the middle initials are mismatched (neither missing & they aren't the same)
	drop if mi_mismatch
	
	//unique dui_incident_id
	//di _N
	
	//exit
	*Drop obs with a missing middle initial in the voter file when there exists another potential merge with matching middle initial
	
	///PROBLEM!!!!!!
	//all our observations are gone
	egen num_mi_match = sum(mi_match), by(dui_incident_id)
	drop if mi_match==0 & num_mi_match>0
	
	//di _N
	
	
	*Drop obs where there is no middle initial in the DUI record, there is one in the voter file, and there exists another potential merge from the voter file with no middle initial
	egen num_mi_none = sum(mi_none), by(dui_incident_id)
	drop if mi_none==0 & num_mi_none>0
	
	//di _N
	
		
	**Compare alternative first and last names (which include more words from the raw names) 
	gen alt_first_name_match=dui_alt_first_name==court_alt_first_name & dui_alt_first_name~=""
	gen alt_last_name_match=dui_alt_last_name==court_alt_last_name & dui_alt_last_name~=""
	
	**Drop people who have an alternative name match with some record but don't have an alternative name match with the current record.
	egen num_alt_first_match=sum(alt_first_name_match), by(dui_incident_id)
	egen num_alt_last_match=sum(alt_last_name_match), by(dui_incident_id)
		
	drop if alt_first_name_match==0 & num_alt_first_match>0
	drop if alt_last_name_match==0 & num_alt_last_match>0
	
	
	**Compare suffixes
	gen suffix_match = dui_suffix==court_suffix & dui_suffix!=""
	
	egen num_suffix_match = sum(suffix_match), by(dui_incident_id)
	drop if suffix_match==0 & num_suffix_match>0
	
	
	
	
	egen num_off_filed=sum(1), by(dui_incident_id)

	
	***Save the incidents we've matched so far
	gen hc_round3match = 1
		
	save "$int_dir/dui_hc_round3", replace
	
	
		
	append using "$int_dir/dui_hc_round1" "$int_dir/dui_hc_round2"
	**Now we'll merge them back into the original DUI file so we get back all of the observations that didn't merge
	
	mmerge dui_incident_id using "$int_dir/ohio_breath_tests_uniqueincident_hc"
	tab _merge
	
	keep if _m==2
	drop _m
	drop court_case_number-num_off_filed
	
	
		
	
	*** ROUND 4 MERGE
	
	display "Round 4"
	
	mmerge f_first3 l_first3 using "$int_dir/hamilton_county_breathtestyears.dta"
	
	keep if _merge==3
	drop _merge
	
	**Get rid of matches where filing date is before stop date or more than 2 days after stop date (matches after that appear low quality)
	drop if court_file_date-dui_test_date<0 | court_file_date-dui_test_date>2
		
	**Compare birth years. Drop obs where they are incompatible, but there is another match with a compatible birth year
	gen b_year_match = court_b_date < dui_bdate_max & court_b_date > dui_bdate_min
	bys dui_incident_id: egen max_b_year_match = max(b_year_match)
	bys dui_incident_id: egen min_b_year_match = min(b_year_match)
	
	drop if b_year_match==0 & max_b_year_match==1


	**Compare names using Jaro-Winkler distance
	jarowinkler court_fullname dui_fullname
	
	
	
	sort jarowinkler
	
		* It looks to me like almost all are good matches. On the lower end of the jarowinkler score, the matches are clearly bad. First good match from my visual inspection is where jarowinkler==.782 (DANIEL S KOKOSKIE, b_year_match==1)
		* From that point up to about where jarowinkler==.866, it seems to me that if we should keep everyone with b_year_match==1 and drop if b_year_match==0. For jarowinkler>.866, it seems like we should basically keep everyone.
		* Let's use round-number thresholds that match across different merge rounds.
		//drop if jarowinkler<.782
		//drop if inrange(jarowinkler,.782,.866) & b_year_match==0
	keep if jarowinkler > .9 | (jarowinkler>=.8 & b_year_match==1)
		
	egen num_off_filed=sum(1), by(dui_incident_id)
	
	drop *b_year_match jarowinkler
	
	gen hc_round4match = 1
	
	save "$int_dir/dui_hc_round4", replace
	append using "$int_dir/dui_hc_round3" "$int_dir/dui_hc_round2" "$int_dir/dui_hc_round1"
	
	mmerge dui_incident_id using "$int_dir/ohio_breath_tests_uniqueincident_hc"
	tab _merge
	
	keep if _m==2
	drop _m
	drop court_case_number-num_off_filed
	
	
	*** ROUND 5 MERGE
	
	display "Round 5"
	
	mmerge first_name using "$int_dir/hamilton_county_breathtestyears.dta"
	
	keep if _merge==3
	drop _merge
	
	**Get rid of matches where filing date is before stop date or more than 2 days after stop date (matches after that appear low quality)
	drop if court_file_date-dui_test_date<0 | court_file_date-dui_test_date>2
		
	**Compare birth years. Drop obs where they are incompatible, but there is another match with a compatible birth year
	gen b_year_match = court_b_date < dui_bdate_max & court_b_date > dui_bdate_min
	bys dui_incident_id: egen max_b_year_match = max(b_year_match)
	bys dui_incident_id: egen min_b_year_match = min(b_year_match)
	
	drop if b_year_match==0 & max_b_year_match==1
	

	**Compare names using Jaro-Winkler distance
	jarowinkler court_fullname dui_fullname
	
	
		* It looks to me like all the matches with jarowinkler >=.95 are good. Matches with jarowinkler in something like [.877,.95) and compatible birth years also look good.
		
		
	* Figure out how many words and initials match (excluding first word and first initial)
	gen dui_namewordcount = wordcount(dui_fullname)
	gen court_namewordcount = wordcount(court_fullname)
	
	split dui_fullname, p(" ")
	split court_fullname, p(" ")
	
	forval i = 1/6	{
		
		gen dui_initial`i' = substr(dui_fullname`i',1,1)
		gen court_initial`i' = substr(court_fullname`i',1,1)
		
		* Don't want to include matching initials or JRs in word match count
		replace dui_fullname`i' = "" if length(dui_fullname`i')==1 | dui_fullname`i'=="JR"
		replace court_fullname`i' = "" if length(court_fullname`i')==1 | court_fullname`i'=="JR"
		
	}
	
	
	gen n_word_match = 0
	gen n_initial_match = 0
	
	forval i = 2/6	{
		forval j = 2/6	{
		
			replace n_word_match = n_word_match + 1 if dui_fullname`i'==court_fullname`j' & dui_fullname`i'!="" & court_fullname`j'!=""
			replace n_initial_match = n_initial_match + 1 if dui_initial`i'==court_initial`j' & dui_initial`i'!="" & court_initial`j'!=""
			
		}
	}
	
		* People with at least one matching word besides first name word seem to be the same person as well
	//keep if jarowinkler>=.95 | (jarowinkler>=.877 & b_year_match==1) | n_word_match>0
	keep if jarowinkler>=.95 | (jarowinkler>=.9 & b_year_match==1) | n_word_match>0
	
	egen num_off_filed=sum(1), by(dui_incident_id)
	
	drop *b_year_match jarowinkler dui_namewordcount-n_initial_match
	
	gen hc_round5match = 1
	
	save "$int_dir/dui_hc_round5", replace
	append using  "$int_dir/dui_hc_round4" "$int_dir/dui_hc_round3" "$int_dir/dui_hc_round2" "$int_dir/dui_hc_round1"
	
	
	
	mmerge dui_incident_id using "$int_dir/ohio_breath_tests_uniqueincident_hc"
	tab _merge
	
	keep if _m==2
	drop _m
	drop court_case_number-num_off_filed
	
	
	
	*** ROUND 6 MERGE
	
	display "Round 6"
	
	//gen def_age = cage //
	
	//mmerge l_first3 def_age using "$int_dir/hamilton_county_breathtestyears.dta"
	//mmerge l_first3 using "$int_dir/hamilton_county_breathtestyears.dta"
	mmerge last_name using "$int_dir/hamilton_county_breathtestyears.dta"
	
	keep if _merge==3
	drop _merge
	
	**Get rid of matches where filing date is before stop date or more than 2 days after stop date (matches after that appear low quality)
	drop if court_file_date-dui_test_date<0 | court_file_date-dui_test_date>2
		
	**Compare birth years. Drop obs where they are incompatible, but there is another match with a compatible birth year
	gen b_year_match = court_b_date < dui_bdate_max & court_b_date > dui_bdate_min
	bys dui_incident_id: egen max_b_year_match = max(b_year_match)
	bys dui_incident_id: egen min_b_year_match = min(b_year_match)
	
	drop if b_year_match==0 & max_b_year_match==1
	

	**Compare names using Jaro-Winkler distance
	jarowinkler court_fullname dui_fullname
	
	keep if jarowinkler >= .95 | (jarowinkler >= .85 & b_year_match==1)
	
	
	
	egen num_off_filed=sum(1), by(dui_incident_id)
	
	drop *b_year_match jarowinkler
	
	gen hc_round6match = 1
	
	save "$int_dir/dui_hc_round6", replace
	append using "$int_dir/dui_hc_round1" "$int_dir/dui_hc_round2" "$int_dir/dui_hc_round3" "$int_dir/dui_hc_round4" "$int_dir/dui_hc_round5"
	
	mmerge dui_incident_id using "$int_dir/ohio_breath_tests_uniqueincident_hc"
	replace num_off_filed=0 if _merge~=3
	assert num_off_filed~=.
	drop _merge
	
	
	
	** Cleaning up variables for analysis
	foreach var in curroff_dwi curroff_1stdwi curroff_2nddwi curroff_3rddwi curroff_aggdwi comoff_dwi comoff_1stdwi comoff_2nddwi comoff_3rddwi comoff_aggdwi curroff_poss comoff_poss curroff_drugmanufdeliv comoff_drugmanufdeliv curroff_recklessdriving comoff_recklessdriving curroff_resistarrest comoff_resistarrest curroff_weapon comoff_weapon curroff_hitandrun comoff_hitandrun curroff_evadearrest comoff_evadearrest curroff_licenseinvalid comoff_licenseinvalid conviction deferredadjud nolocontend conv_defer_nolo dwiconviction dwideferredadju dwinolocontend dwiconv_defer_nolo	{
	    
		replace court_`var' = 0 if court_`var'==.
		
	}

foreach var in sentence_incarcmonths sentence_probation sentence_fine	{
    
	replace court_`var' = 0 if court_`var'==.
	gen has_court_`var' = court_`var' > 0
	
}

gen court_nondwiconviction = court_curroff_dwi==0 & court_conviction==1


	** Addressing incidents with multiple associated charges
	duplicates tag dui_incident_id, gen(n_charges)
	replace n_charges = n_charges + 1 if court_case_number!=.
	gen any_charges = n_charges>0
	
	foreach var in curroff_dwi comoff_dwi dwiconviction conviction deferredadjud nolocontend conv_defer_nolo nondwiconviction	{
	    
		bys dui_incident_id: egen n_court_`var' = sum(court_`var')
		bys dui_incident_id: gen any_court_`var' = n_court_`var' > 0
	
	}
	
	* Check what happens with sentences when there are convictions on multiple charges. The sentences are almost always listed as identical, which makes me think they are NOT additive.
	foreach var in sentence_incarc sentence_probation sentence_fine	{
	    
		bys dui_incident_id: egen max_court_`var' = max(court_`var')
		bys dui_incident_id: egen max_has_court_`var' = max(has_court_`var')
		
	}
	

** Construct the predicted race variables that we'll use (taking advantage of race info in court records)
gen f_prob_ms=f_probability_black==.
   replace f_probability_black=0 if f_probability_black==.

gen l_prob_ms=l_probability_black==.
   replace l_probability_black=0 if l_probability_black==.

gen f2=f_probability_black^2
   
gen l2=l_probability_black^2
   
gen int1=f_probability_black*l_probability_black
gen int2=f_probability_black*l_prob_ms
gen int3=l_probability_black*f_prob_ms

logit court_race_black f_probability_black l_probability_black f_prob_ms l_prob_ms int1 int2 int3
predict p_black

sum p_black dui_lowest_result


gen likely_black = p_black>.5 & f_likely_race!="hispanic" & l_likely_race!="hispanic"
gen likely_hispanic = f_likely_race=="hispanic" & l_likely_race=="hispanic"
gen likely_white = p_black<.2 & !likely_hispanic & f_likely_race=="white" & l_likely_race=="white"


label var court_nondwiconviction "Indicator for non-DWI conviction"
label var likely_black "High predicted probability Black (using names and court-recorded race information)"
label var likely_hispanic "High predicted probability Hispanic (using names)"
label var likely_white "High predicted probability White (using names and court-recorded race information)"


save "$clean_dir/dui_hc_merged.dta", replace


exit

/*

*** ASSESS QUALITY OF MATCH
* Take random sample of matched obs to audit match quality
use "$clean_dir/dui_hc_merged.dta", clear

// keep only matched obs
keep if court_case_number!=.

// create random sample
set seed 12345

gen rand = runiform()
sort rand

//browse dui_fullname dui_b_year_likely dui_b_year_unlikely court_fullname court_b_date com_off_lit lowest_result if _n<=200




* Take random sample of unmatched obs with high BrAC and look for matches by hand
use "$clean_dir/dui_hc_merged.dta", clear
keep if cas==. & inrange(lowest_result,.09,.13)

gen rand = runiform()
sort rand

browse dui_fullname dui_test_date dui_b_year_likely dui_b_year_unlikely if _n<=200

/*
*** Example code to look for match by hand:

use "$int_dir/hamilton_county_breathtestyears.dta", clear

* Suppose we have
	* dui_fullname == "EMILY C LESLIE"
	* dui_test_date == 12/4/2021

browse hc_fullname def_dob com_off_lit if regexm(first_name,"EMI") & filing_month==12 & inlist(filing_day,4,5) & filing_year==2021

browse hc_fullname def_dob com_off_lit if regexm(first_name,"LES") & filing_month==12 & inlist(filing_day,4,5) & filing_year==2021


*/


*** Look at share per round across BrAC spectrum
use "$clean_dir/dui_hc_merged.dta", clear

keep if hc_round1match==1 | hc_round2match==1 | hc_round3match==1 | hc_round4match==1 | hc_round5match==1 | hc_round6match==1

forval i = 1/6	{
    
	replace hc_round`i'match = 0 if hc_round`i'match==.
}



gen double rounded_lowest_result = floor(lowest_result*100)/100

forval i = 1/6	{
    
	bys lowest_result: egen mean_round`i' = mean(hc_round`i'match)
	bys rounded_lowest_result: egen rmean_round`i' = mean(hc_round`i'match)
}

line rmean_round1 rounded_lowest_result if inrange(lowest_result,.03,.13), xline(.08) || ///
	line rmean_round2 rounded_lowest_result if inrange(lowest_result,.03,.13) || ///
	line rmean_round3 rounded_lowest_result if inrange(lowest_result,.03,.13) || ///
	line rmean_round4 rounded_lowest_result if inrange(lowest_result,.03,.13) || ///
	line rmean_round5 rounded_lowest_result if inrange(lowest_result,.03,.13) || ///
	line rmean_round6 rounded_lowest_result if inrange(lowest_result,.03,.13)

scatter mean_round1 lowest_result if inrange(lowest_result,.03,.13), msize(tiny) xline(.08) || ///
	scatter mean_round2 lowest_result if inrange(lowest_result,.03,.13), msize(tiny) || ///
	scatter mean_round3 lowest_result if inrange(lowest_result,.03,.13), msize(tiny) || ///
	scatter mean_round4 lowest_result if inrange(lowest_result,.03,.13), msize(tiny) || ///
	scatter mean_round5 lowest_result if inrange(lowest_result,.03,.13), msize(tiny) || ///
	scatter mean_round6 lowest_result if inrange(lowest_result,.03,.13), msize(tiny)
