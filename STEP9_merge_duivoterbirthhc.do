
**Root directory--This is all that needs to be changed across users
*global root_dir "/Users/ll263/Library/CloudStorage/Box-Box/DUI/texas"
global root_dir C:/Users/andersee/Box/DUI/texas


**Note that the directory pir_sos_20210118 may need to be uncompressed for the file to run
global raw_dir "$root_dir/raw_data/courts_data"
global int_dir "$root_dir/intermediate_data"
global clean_dir "$root_dir/clean_data"



clear all
set more off




**Merge the combined DUI/voter and DUI/birth index datasets.  We need to
**merge based on all of the different variables we want to keep so that we don't
**have missing values in the merged file.



use "$int_dir/dui_voter_merged_all"
gen b_date=voter_b_date
replace b_date=99999 if voter_b_date==.
sort dui_test_date dui_test_time dui_test_time_zone dui_middle_initial dui_male dui_age dui_oprl dui_oprf dui_oprm dui_ocert dui_oagency dui_aofficer dui_aagency dui_predict dui_test_year dui_test_month dui_test_day dui_incident_id dui_highest_result dui_lowest_result dui_first_result dui_first_cnty dui_first_cnty_fips dui_first_cnty_name dui_last_result dui_last_cnty dui_last_cnty_fips dui_last_cnty_name dui_fullname dui_suffix first_name dui_alt_first_name dui_all_tests_invalid dui_highest_test dui_highest_vresult dui_lowest_vresult f_first3 name_last_word last_name dui_alt_last_name l_first3 f_likely_race f_probability_american_indian f_probability_asian f_probability_black f_probability_hispanic f_probability_white f_probability_2race l_likely_race l_probability_american_indian l_probability_asian l_probability_black l_probability_hispanic l_probability_white l_probability_2race b_date


save "$int_dir/tmpdat", replace

use "$int_dir/dui_b_index_merged_all"
gen b_date=b_index_b_date
replace b_date=99999 if b_index_b_date==.

sort dui_test_date dui_test_time dui_test_time_zone dui_middle_initial dui_male dui_age dui_oprl dui_oprf dui_oprm dui_ocert dui_oagency dui_aofficer dui_aagency dui_predict dui_test_year dui_test_month dui_test_day dui_incident_id dui_highest_result dui_lowest_result dui_first_result dui_first_cnty dui_first_cnty_fips dui_first_cnty_name dui_last_result dui_last_cnty dui_last_cnty_fips dui_last_cnty_name dui_fullname dui_suffix first_name dui_alt_first_name dui_all_tests_invalid dui_highest_test dui_highest_vresult dui_lowest_vresult f_first3 name_last_word last_name dui_alt_last_name l_first3 f_likely_race f_probability_american_indian f_probability_asian f_probability_black f_probability_hispanic f_probability_white f_probability_2race l_likely_race l_probability_american_indian l_probability_asian l_probability_black l_probability_hispanic l_probability_white l_probability_2race b_date

merge dui_test_date dui_test_time dui_test_time_zone dui_middle_initial dui_male dui_age dui_oprl dui_oprf dui_oprm dui_ocert dui_oagency dui_aofficer dui_aagency dui_predict dui_test_year dui_test_month dui_test_day dui_incident_id dui_highest_result dui_lowest_result dui_first_result dui_first_cnty dui_first_cnty_fips dui_first_cnty_name dui_last_result dui_last_cnty dui_last_cnty_fips dui_last_cnty_name dui_fullname dui_suffix first_name dui_alt_first_name dui_all_tests_invalid dui_highest_test dui_highest_vresult dui_lowest_vresult f_first3 name_last_word last_name dui_alt_last_name l_first3 f_likely_race f_probability_american_indian f_probability_asian f_probability_black f_probability_hispanic f_probability_white f_probability_2race l_likely_race l_probability_american_indian l_probability_asian l_probability_black l_probability_hispanic l_probability_white l_probability_2race b_date using "$int_dir/tmpdat"
tab _merge

drop b_date

***Every dui_incident_id is represented

	
** STEP 3: Merge in name frequency.
mmerge  first_name using "$int_dir/voter_f_name_freq.dta"
drop if _merge==2
drop _merge

mmerge last_name using "$int_dir/voter_l_name_freq.dta"
drop if _merge==2
drop _merge

replace f_name_freq=1 if f_name_freq==.
replace l_name_freq=1 if l_name_freq==.


**We'll use log frequency and polynomials in log frequency.
gen ln_f_freq=ln(f_name_freq)
gen ln_l_freq=ln(l_name_freq)

gen f2_freq=ln_f_freq^2
gen l2_freq=ln_l_freq^2
gen fl_freq=ln_f_freq*ln_l_freq


* STEP 4: Merge Harris County obs with the DUI + Harris County Courts data to see whether voter file or birth index is providing higher quality matches.  We're keeping all of the DUI data because we're going to want predicted probabilities for everyone.

sort dui_incident_id
save "$int_dir/tmpdat", replace

**I'll adjust the harris county data so there's only one observation per individuals

use "$clean_dir/dui_hc_merged.dta", clear

**There are 4 individuals with multiple birthdates that cannot be disambiguated
**in the Harris county data.  These all have birthdates inconsistent with
**the DUI data, so dropping one observation from each pair won't affect
**the model.

duplicates drop dui_incident_id, force

keep dui_incident_id court_* likely_black likely_white likely_hispanic

sort dui_incident_id

mmerge dui_incident_id using "$int_dir/tmpdat"
tab _merge
drop _merge
drop if dui_incident_id==.


generate b_date=voter_b_date
 replace b_date=b_index_b_date if b_date==.
 format b_date %td
 label var b_date "Birthdate"
 
 gen b_date_source = ""
 replace b_date_source = "VOTER FILE AND BIRTH INDEX" if b_date==voter_b_date & b_date==b_index_b_date & b_date!=.
 replace b_date_source = "VOTER FILE ONLY" if b_date==voter_b_date & b_date!=. & b_date_source==""
 replace b_date_source = "BIRTH INDEX ONLY" if b_date==b_index_b_date & b_date!=. & b_date_source==""
 replace b_date_source = "NO BIRTHDATE MATCH" if b_date_source==""
 label var b_date_source "Source of birthdate information"

**There are a small number of ids who have multiple different individuals with the same
**birthday.  These are going to be people with common names who we will drop later anyway
**because they are unlikely to be a correct match.
egen num=sum(1), by(dui_incident_id b_date)
egen num2=max(num), by(dui_incident_id)
drop if num2>1
drop num*


unique dui_incident_id
**Dropped 1289 incidents with common names as a consequence




**STEP 3: Creating variables that will help us predict the probability of a good match

**Create variables that indicate whether there is voter or birth index information
**for the possible match.
gen voter_yes=voter_b_date~=.
gen b_index_yes=b_index_b_date~=.
gen either_yes=b_index_yes==1 | voter_yes==1

**Calculate the total number of candidate matches.
egen num_voter_matches=sum(voter_yes), by(dui_incident_id)
egen num_b_index_matches=sum(b_index_yes), by(dui_incident_id)
egen num_either_matches=sum(either_yes), by(dui_incident_id)


** Get rid of some extra obs
drop if num_either_matches > 0 & voter_b_date==. & b_index_b_date==.

/*
**********************************************************
**Look at frequency of matches by individual and probability of being in voter file at threshold
preserve
duplicates drop dui_incident_id, force
tab num_either_matches
**26 percent of individuals have no matches in either databse
**51 percent have one match

gen above_limit = dui_lowest_result>=.08
gen above_limitxresult = above_limit*dui_lowest_result

gen index = dui_lowest_result - .08
gen interact = above_limit*index

gen in_voter_file = num_voter_matches>0

reg in_voter_file above_limit index interact if inrange(dui_lowest_result,.03,.13) & !inrange(dui_lowest_result,.07,.079), robust
reg in_voter_file above_limit index interact if inrange(dui_lowest_result,.03,.13), robust

restore
************************************************************
*/

unique dui_incident_id if court_file_year~=.
**Number of unique values of dui_incident_id is  42443
**Number of records is  80956

unique dui_incident_id if court_file_year~=. & num_either_matches>0
**Number of unique values of dui_incident_id is  29526
**Number of records is  68039



**Create variables that indicate that either the Harris County birth date matches with the voter
**record, birth index record, or both.
/* This version of the code counts obs with a missing HC bdate as possible matches
gen voter_hc_b_date_match=voter_b_date==court_b_date if last_dui_county_fips==201
gen b_index_hc_b_date_match=b_index_b_date==court_b_date if last_dui_county_fips==201
gen either_b_date_match=b_index_hc_b_date_match==1 | voter_hc_b_date_match==1 if last_dui_county_fips==201
*/
gen voter_hc_b_date_match=voter_b_date==court_b_date if dui_last_cnty_fips==201 & court_b_date!=.
gen b_index_hc_b_date_match=b_index_b_date==court_b_date if dui_last_cnty_fips==201 & court_b_date!=.
gen either_b_date_match=b_index_hc_b_date_match==1 | voter_hc_b_date_match==1 if dui_last_cnty_fips==201

** Notes
**Number of incidents merging to Harris County court records
unique dui_incident_id if court_file_year~=.
**Number of unique values of dui_incident_id is  42443
**Number of records is  80956

**Number of incidents with voter or birth record birthdays merging to Harris County court records
unique dui_incident_id if court_file_year~=. & num_either_matches>0
**Number of unique values of dui_incident_id is  29526
**Number of records is  68039

**Number of incidents with a single birthday from voter or birth record data merging to Harris County court records
unique dui_incident_id if court_file_year~=. & num_either_matches==1
**Number of unique values of dui_incident_id is  19739
**Number of records is  19739



**Assess the quality of the matches using the Harris County subsample.


**These say if there is more than one match in the voter file, birth file, or the two
**combined it is unlikely to be a good match
tab num_voter_matches voter_hc_b_date_match if voter_yes==1 & dui_last_cnty_fips==201
tab num_b_index_matches b_index_hc_b_date_match if b_index_yes==1 & dui_last_cnty_fips==201
tab num_either_matches either_b_date_match if either_yes==1 & dui_last_cnty_fips==201


**If there is only one voter file match but another birth index match, it is again
**unlikely to be correct and vice versa.
sum either_b_date_match if voter_yes==1 & num_either_matches>1 & num_voter_matches==1 & dui_last_cnty_fips==201
sum either_b_date_match if b_index_yes==1 & num_either_matches>1 & num_b_index_matches==1 & dui_last_cnty_fips==201


**************
**WRITE CODE TO MAKE SURE NUM_EITHER_MATCH>1 ISN'T CHANGING AT THE CUTOFF
** Make vars for regressions
preserve

bys dui_incident_id: gen insample = _n==1

gen multiple_matches = num_either_matches>1

**We see a significant decline in the propensity of multiple matches at the cutoff
**with the donut RD strategy
* RD on indicator for multiple matches
reg multiple_matches above_limit index interact if inrange(dui_lowest_vresult,.07,.09) & insample==1, robust

	* With donut hole
reg multiple_matches above_limit index interact if inrange(dui_lowest_vresult,.06,.09) & !inrange(dui_lowest_vresult,.07,.079) & insample==1, robust

**This is all driven by the fact that people with low blood alcohol content have
**common names.  Once you control for this, the effect goes away.
reg multiple_matches above_limit index interact ln_f_freq ln_l_freq f2_freq l2_freq fl_freq if inrange(dui_lowest_result,.03,.13) & !inrange(dui_lowest_result,.07,.079), robust

**The effect also goes away if you focus on first and last names that are in the bottom 75 percent of name frequecy
reg multiple_matches above_limit index interact if inrange(dui_lowest_result,.03,.13) & !inrange(dui_lowest_result,.07,.079) & l_name_freq<67644 & f_name_freq<103737, robust


**On the sharp RD, there's no effect
reg multiple_matches above_limit index interact if inrange(dui_lowest_result,.03,.13), robust

reg multiple_matches above_limit index interact ln_f_freq ln_l_freq f2_freq l2_freq fl_freq  if inrange(dui_lowest_result,.03,.13), robust


restore
***************



**Let's drop if there is more than one match of either sort.
drop if num_either_matches>1
count

**I want to use variables indicating whether there is a middle initial and suffix match
**between the DUI and other datasets
replace dui_voter_mi_match=0 if dui_voter_mi_match==.
replace dui_voter_mi_none=0 if dui_voter_mi_none==.
replace dui_voter_suffix_match=0 if dui_voter_suffix_match==.

replace dui_b_index_mimatch=0 if dui_b_index_mimatch==.
replace dui_b_index_minone=0 if dui_b_index_minone==.
replace dui_b_index_suffix_match=0 if dui_b_index_suffix_match==.


**I create variables that indicate an observation is only in the voter or birth
**index files.  The omitted category is that they're in both.
gen voter_only=voter_yes==1 & b_index_yes==0
gen b_index_only=voter_yes==0 & b_index_yes==1


**Estimate the probability that the match is correct as a function of name frequency
**and other merge criteria.

** The first model is for when we have either a voter or birth record match
logit either_b_date_match ln_* f2_* l2_* fl_* likely_hispanic dui_voter_mi_match dui_voter_mi_none dui_voter_suffix_match  dui_b_index_mimatch dui_b_index_minone dui_b_index_suffix_match voter_only b_index_only if num_either_matches==1 & either_yes==1 & dui_last_cnty_fips==201
predict prob1 if num_either_matches==1

label variable prob1 "Probability of correct match, model 2 (using voter and birth record info)"

**Look at the frequency of correct matches for this Harris sample and everyone
sum prob1 if num_either_matches==1 & either_yes==1 & dui_last_cnty_fips==201, d
sum prob1 if num_either_matches==1, d

**Now we estimate a model that works whether or not we have the birth and voter
**information.

**We might want to bring in the likely Hispanic variables etc but we would need to bring it in for non Harris county matches.
//logit either_b_date_match ln_* f2_* l2_* fl_* if num_either_matches==1 & last_dui_county_fips==201
logit either_b_date_match ln_* f2_* l2_* fl_* likely_hispanic if num_either_matches==1 & either_yes==1 & dui_last_cnty_fips==201
predict prob2
label variable prob2 "Probability of correct match, model 2 (using only name info)"

**Look at the frequency of correct matches for this group.  Group with birthday or voter info.
//sum prob2 if num_either_matches==1 & last_dui_county_fips==201, d
sum prob2 if num_either_matches==1 & either_yes & dui_last_cnty_fips==201, d
sum prob2 if num_either_matches==1, d


**Group without voter or birthday info.
sum prob2 if num_either_matches==0 & dui_last_cnty_fips==201, d
sum prob2 if num_either_matches==0, d



**Generate a final probability of a correct match which is prob1 for the group with
**voter or birthdate info and prob2 if there is no birthdate or voter info.

gen final_prob=prob1 if num_either_matches==1 & either_yes==1
replace final_prob=prob2 if final_prob==.

label var final_prob "Preferred probability of correct match (= prob1 for obs with bindex or voter match, =prob2 else)"

count
sum final_prob, d

save $clean_dir/allmerged, replace



*** Eliminate vars that we won't send to the THECB
use $clean_dir/allmerged, clear

drop court_last_name court_fullname court_suffix court_alt_first_name court_middle_initial court_alt_last_name court_b_year court_b_month court_b_day court_b_date court_def_spn court_def_stnum court_def_stnam court_def_cty court_def_tx court_def_zip court_case_number court_aty_nam court_aty_spn court_aty_coc court_aty_coc_lit court_comp_nam court_off_rpt_num

drop voter_id voter_middle_name voter_former_last_name voter_suffix voter_b_year voter_b_month voter_b_day voter_b_date voter_fullname voter_alt_first_name voter_alt_last_name voter_middle_initial voter_perm_address voter_mail_address voter_sex voter_reg_date voter_male

drop b_index_b_date b_index_fullname b_index_suffix b_index_name_middle_words b_index_middle_initial b_index_b_year b_index_b_month b_index_b_day b_index_b_date

drop f_name_freq l_name_freq ln_f_freq ln_l_freq f2_freq l2_freq fl_freq

drop dui_alt_first_name f_first3 dui_alt_last_name l_first3 name_last_word dui_oprl dui_oprf dui_oprm dui_ocert dui_aofficer

drop dui_b_index_mimatch dui_b_index_num_mimatch dui_b_index_suffix_match dui_b_index_num_suffix_match dui_b_index_no_match dui_voter_mi_match dui_voter_num_mi_match dui_voter_alt_first_name_match dui_voter_alt_last_name_match dui_voter_suffix_match dui_voter_num_suffix_match dui_voter_county_match dui_voter_num_county_match dui_voter_no_match num_voter_matches num_b_index_matches num_either_matches voter_hc_b_date_match b_index_hc_b_date_match either_b_date_match dui_b_index_minone dui_b_index_dup_initial dui_b_index_num_minone dui_b_index_dup dui_voter_mi_none dui_voter_dup_initial dui_voter_num_mi_none dui_voter_dup voter_yes b_index_yes either_yes voter_only b_index_only

label var dui_fullname "[PII for merge] Full name from breath test record"
label var dui_suffix "[PII for merge] Suffix from breath test record"
label var first_name "[PII for merge] First word of name"
label var last_name "[PII for merge] Last listed last name, combining compound name and removing suffixes and spaces"
label var dui_age "[PII for merge] Age of breath tested person (administratively recorded, not constructed)"
label var b_date "[PII for merge] birthdate"

* To use R for .dta > .sas7bdat conversation, need data in version 5-12 format
//save $clean_dir/data_for_thecb, replace
saveold $clean_dir/data_for_thecb_dta12, v(12) replace

preserve
describe, replace clear
list
export excel using $int_dir\thecb_vars.xlsx, replace first(var)
restore
