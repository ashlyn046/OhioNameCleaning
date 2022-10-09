# Ohio-Name-Cleaning
**This is a WORKING README; Last update: Wed. June 29, 2022

View more readable version (the working copy) on google drive at:
https://docs.google.com/document/d/1XBpE-7S0rdP6RpD_vGI1ZmiILcG_qRb2Om8kUtpxiAc/edit?usp=sharing

Cleaning and scraping data from multiple sources for the Ohio portion of the data. 

This repository includes 11* main .do files (Stata files) that accomplish the cleaning and scraping tasks. This process occurs over the course of 11** steps, mirroring what was done for the Texas data. These steps (and their associated .do files) are as follows: 

STEP 1: Clean DUI data
Step 1 cleans the raw Ohio breathalyzer data. The purpose of this step is to set up the primary dataset for Ohio that will be used in the regression and analysis for this research project. This step is composed of 7 .do files, listed below in the order that they should be run. 
SCRAPE_ppost_county.do
SCRAPE_county_fips.do
dept_county_data_final_cleaning.do
SCRAPE_city_county.do
step1_names.do
step1_merge.do
step1_final_cleaning.do

STEP 2: Clean Voter data
Step 2 cleans the raw Ohio voter data. The purpose of this step is to prepare the data that we will use to verify the identities of the individuals in the data from Step 1. More specifically, this step is preparing the voter data for the merge to the main data set in Step 5. This step is composed of 1 .do file, listed below. 
STEP2_ ohio_read_voter_vers1.do

STEP 3: Read Birth Index 
We skip Step 3 in the Ohio data because there is no Ohio birth index data to mimic what was used in Step 3 of the Texas code.

STEP 4: Read Hamilton County
Step 4 cleans the county court data for Hamilton County. The purpose of this step is _____. This step is composed of 1 .do file, listed below.
STEP4_read_hamiltoncounty.do

STEP 5: Merge DUI and Voter datasets
Step 5 merges the Ohio breathalyzer data (from Step 1) and the Ohio voter data (from Step 2). The purpose of this step is to identify which DUI observations have a clear match in the voter data, which will allow us to put more weight on the matched observations when running regressions. This step is composed of 1 .do file, listed below.
STEP5_ohio_merge_dui_voter.do

STEP 6: Merge DUI and Birth Indexes
We skip Step 6 in the Ohio data because there is no Ohio birth index data to mimic what was used in Step 6 of the Texas code.

STEP 7: Merge DUI and Hamilton County
Step 7 merges the primary dataset from Step 1 with the cleaned county court data from Step 4. The purpose of this step is to identify the legal effects of a person having blood alcohol content above the cutoff. This will help us to more holistically identify the treatment bundle for individuals just above the RD cutoff (e.g. whether they get incarcerated). This step is composed of 1 .do file, listed below. 
STEP7_merge_dui_hamiltoncnty.do

STEP 8: Merge DUI and Convictions 
Step 8 aims to (step explanation goes here). The purpose of this step is _____. This step is composed of 5 .do files, listed below in the order that they should be run. 
STEP8.do***

STEP 9: Merge DUI and Voter HC
Step 9 aims to (step explanation goes here). The purpose of this step is _____. This step is composed of 5 .do files, listed below in the order that they should be run. 
STEP9.do***

STEP 10: RD Validity 
Step 10 aims to (step explanation goes here). The purpose of this step is _____. This step is composed of 5 .do files, listed below in the order that they should be run. 
STEP10.do***

STEP 11: Hamilton County First Stage
Step 11 aims to (step explanation goes here). The purpose of this step is _____. This step is composed of 5 .do files, listed below in the order that they should be run. 
STEP11.do***

Each of these files will be explained in detail in the sections that follow (as well as their input, intermediary, and output files). Please note that the pathing of each file’s save location is based on the Box setup for DUIAshlyn>ohio and then divided into our three directories: raw_data, intermediate_data, clean_data.

STEP 1: Clean DUI data
SCRAPE_ppost_county.do
This file reads in patrol post (OSP or  “Ohio State Patrol”, sometimes called OSHP or “Ohio State Highway Patrol”) names and their associated counties, scraped from https://wiki.radioreference.com/index.php/Ohio_State_Highway_Patrol_(OSP)_(OH). Ultimately, this will allow us to add a county variable to the Ohio breathalyzer file. We accomplish this by merging patrol post/county data from the output of this .do with data from Ohio Breathalyzer file. We merge on the department name (see step1_merge.do for more explanation). Note that this file supplements the highway patrol information that was not available on the other department website that was scraped into “department_county_data.xlsx”.
The file is broken into two phases: webscraping (Phase 1) and data cleaning (Phase 2). The data webscrape is done in Stata with a package from the Social Science Computing Cooperative (SSCC). Further information about this package can be found at https://sscc.wisc.edu/sscc/pubs/readhtml.htm. The data cleaning is as it sounds. A specific breakdown of how we cleaned the data can be found in the file itself. Most notably, we saved the intermediate, manually-cleaned data under “patrol_post_depts.dta”, which can be found in the raw directory under the department_county folder. The reformation we performed above made the scraped dataset compatible for merging with our other county/department clean .dta file later on in dept_county_data_final_cleaning.do, resulting in the output of this file: OSP_county_clean.dta. 
INPUT:	 N/A
INTERMEDIARY: 
patrol_post_depts.dta
	>$raw_dir/department_county/patrol_post_depts.dta	
ppost_county_`y’.txt
	>$int_dir/county_dept_cleaning/ppost_county_`y'.txt
ppost_county.dta 
	>$int_dir/county_dept_cleaning/ppost_county.dta
OUTPUT: 
 OSP_county_clean.dta
	>$int_dir/OSP_county_clean.dta

SCRAPE_county_fips.do
This file reads in county and their associated FIPS codes for each county in Ohio scraped from https://en.wikipedia.org/wiki/List_of_counties_in_Ohio. This data is supplemental, giving us the option to use either county name or county FIPS code later down the line in data analysis. FIPS codes tend to be preferable over county names because it’s easier to avoid typos or mismatched naming conventions.
The file is broken into two phases: webscraping (Phase 1) and data cleaning (Phase 2). The data webscrape is done in Stata with a package from SSCC. Further information about this package can be found at https://sscc.wisc.edu/sscc/pubs/readhtml.htm. The data cleaning is as it sounds. Specifics about how we cleaned the data can be found in the file itself. Most notably, we condensed the data to two columns: county name and associated FIPS code. We create three separate county/FIPS files because some cities have up to three counties associated with it; the same is true for the number of counties associated with a given police department or patrol post. Having three county/FIPS files allows us to have county and FIPS variables 1-3, thus enabling us to merge the city/county data and department/county data with the county/FIPS data on each county variable while keeping each county’s FIPS code intact in dept_county_final_cleaning.do and SCRAPE_city_county.do. More information about this will be provided under those files’ explanation.
INPUT: N/A
INTERMEDIARY: N/A
OUTPUT:
county_1_fips
>$int_dir/county_1_fips.dta
county_2_fips
>$int_dir/county_2_fips.dta
county_3_fips
>$int_dir/county_3_fips.dta

dept_county_data_final_cleaning.do
This file reads in the three (technically 5) cleaned datasets from the two previous .do files (and an auxiliary excel file), finishing the cleaning process in order to combine them into two final department/county/FIPS datasets that can be merged with the Ohio breathalyzer file in the Step 1 .do file: arrestingagency_final.dta and testingagency_final.dta.
This file is broken into three phases: importing and cleaning the department/county excel file (Phase 1), appending the patrol post/county file (Phase 2), and merging this with the FIPS/county file (Phase 3). Importing and cleaning the excel file– which contains police department names and their associated counties for non-patrol post agencies and was scraped from https://www.ocjs.ohio.gov/ohiocollaborative/cert-le.html into this excel file department_county_data.xlsx– is mainly for further normalizing the variables across the three input files, also combining observations for departments with more than one county. Appending the patrol post/county file simply combines the newly clean regular department/county data to the state patrol/county data. Merging the complete department/county data to the FIPS/county data results in the final two department/county/FIPS code datasets: arrestingagency_final.dta and testingagency_final.dta. These are the datasets that will be used to add the county and FIPS code variables to the breathalyzer file later on (see the Step 1 merge file explanation for details). Specifics for each phase can be found in the file itself. 
INPUT:	 
department_county_data.xlsx
	>$raw_dir/department_county/department_county_data.xlsx
OSP_county_clean.dta
	>$int_dir/OSP_county_clean.dta
county_`1-3’_fips.dta
>$int_dir/county_`i'_fips.dta
INTERMEDIARY:
department_county_data2.dta
	>$int_dir/department_county_data2.dta
OUTPUT: 
arrestingagency_final.dta
>$int_dir/arrestingagency_final.dta
testingagency_final.dta
	>$int_dir/testingagency_final.dta

SCRAPE_city_county.do
This file reads in city (municipality) names and their associated counties, police department names and their associated counties, scraped from https://en.wikipedia.org/wiki/List_of_municipalities_in_Ohio. Ultimately, this will allow us to add a county variable to the Ohio Breathalyzer file. We accomplish this by merging the city/county data from the output of this .do file with data from Ohio breathalyzer file. We merge on the city name (see step1_merge.do for more explanation). 
The file is broken into three phases: the webscrape (Phase 1), data cleaning (Phase 2), and merging (Phase 3). The data webscrape is done in Stata with a package from SSCC. Further information about this package can be found at https://sscc.wisc.edu/sscc/pubs/readhtml.htm. The data cleaning is as it sounds. Specifics about how we cleaned the data can be found in the file itself. Most notably, we divide the data into four variables: city, county_1, county_2, county_3. This accounts for the fact that some cities have up to three counties associated with them. Merging the complete city/county data to the FIPS/county data results in the final city/county/FIPS code dataset: city_final.dta. This is the dataset that will be used to add the county and FIPS code variables to the breathalyzer file later on (see the Step 1 merge file explanation for details). Specifics for each phase can be found in the file itself.
Note: we drop observations for the cities “Centerville” and “Oakwood” because there are three cities/villages in Ohio named Oakwood and two named Centerville (each in a different county). This means that these cities would not be able to uniquely identify county locations (and thus cannot be used for verification later on, like in Step 5). More on the merging process to bring the county/fips variables into the main dataset can be found under the explanation for step1_merge.do.
INPUT:
county_`1-3’_fips.dta
>$int_dir/county_`i'_fips.dta
INTERMEDIARY:
county_city.dta
	>$int_dir/county_city.dta
OUTPUT: 
city_final.dta
>$int_dir/city_final.dta

step1_names.do
This file primarily cleans names taken from the OhioBreathalyzer.csv file. This .csv file was scraped from https://publicapps.odh.ohio.gov/BreathInstrument/default.aspx and contains breath test data for potential DUI stops in the state of Ohio from 2009-2018. It is the basis for the main dataset used for analysis in this project. This .do file uses OhioBreathalyzer.csv in addition to first_names_race.csv and surnames_race.csv to clean and prepare a dataset that will be used for the regression in a later step. Specifics about the name cleaning process can be found within the file itself, although we want to highlight that the alternate versions of first and last name variables account for the fact that some individuals have multiple first or last names. By creating alternate name variables, we are able to more effectively merge with other datasets regardless of which first or last name appears. These files are separated and sorted into the files A-Z by first initial.
The .csv files for race/names came via Jacob Kaplan from https://rdrr.io/github/jacobkap/predictrace/. Specifics about the data and its origin can be found on his website. The name .csv files supplement race data in the form of probable race based on first and last names. Together, these name/race files create the 14 race probability variables seen in the final dataset output from this .do file. Specifics for these variables can be found under the variable labels created at the end of the third (and final) step 1 .do file, step1_final_cleaning.do. Note that these race probability variables differ from the recorded race variable that was included in the raw OhioBreathalyzer.csv file. The recorded_race variable lists only four possible races: white, black, hispanic, and none. We keep both the recorded race variable and the race probability variables to bulken the accuracy of the race probability variables and fill in possible race gaps that may have been overlooked in recorded_race. 
We’d like to note that this is the file where the RD variables are created. These variables are: index, above_limit, and interact. Specific descriptions of these variables can be found in the file itself. 
Also noteworthy is that the “result” variable of the raw OhioBreathalyzer.csv has distinct missing codes that came with the data. The final output of step1_final_cleaning.do preserves these in the variable result_raw. The missing codes are as follows:
.r            subject test refused (43% of missings)
.t            input timeout (20% of missings)
.d           deficient sample (10% of missings)
.n           no sample given
.o           out of tolerance
.f            rfi defect (8% of missings)
.i            invalid sample
.a           no .020 agreement
.s           sequence aborted
.id          interferent detect
.af          ambient fail
.is          improper sample
.p           purge fail
.df          diagnostic fail
.us         unstable signal 
.re          range exceeded
.x           (null)
Note that this .do file keeps three main results-related variables: result_raw (as described above), “result” (which converts all missing codes to . and is used to create dui_highest/lowest_result and dui_first/last_result in the output data file), and refused (a dummy for whether or not the original observation for the result variable was “.r”).
In this step, we also prep the OhioBreathalyzer dataset before our merge by cleaning the arrestingagency variable. This is necessary because there does not appear to be a universal naming convention for department titles. For example, while some recording officers recorded full versions of titles such as “Sheriff’s Office”, others used acronyms such as “S.O.”, etc.. 
The output of this .do file, step1_names.dta, contains the cleaned names from the Ohio breathalyzer data in addition to possible birthdate variables, the adjusted results variables, the RD variables, the new and recorded race variables that were all created throughout this file. This output file will be further cleaned in the next .do file, step1_merge.do. An explanation of this process will be found under the description of that .do file.
INPUT:	 
first_names_race.csv
>$raw_dir/race_prediction/first_names_race.csv
surnames_race.csv
>$raw_dir/race_prediction/surnames_race.csv
OhioBreathalyzer.csv
>$raw_dir/breath_test_data/OhioBreathalyzer.csv
INTERMEDIARY:
first_name_race.dta
>$int_dir/first_name_race.dta
last_name_race.dta
>$int_dir/last_name_race.dta
OUTPUT:
step1_names.dta
>$int_dir/step1_names.dta

step1_merge.do
This file will merge the cleaned Ohio breath test data (step1_names.do) with the previously scraped information in order to add county and FIPS code information to the dataset. This is done in a “ladder” process, which is why we created each of the scrape .do files before the step 1 .do files. This means that we merge on the variables city, arrestingagency, and testingagency in order to obtain these county variables (listed in order of priority). 
There are 144,110 total observations in the breath test data (step1_names.dta). We first attempt the merge on the variable city (using city_final.dta), which resulted in 120,370 matched observations and 23,740 unmatched observations (from the “master file”, aka step1_names.dta). We chose city as the first variable to attempt merging on because we believe that “city” as recorded in the initial OhioBreathalyzer.csv file in step1_names.do is the city of residence– rather than the city where the incident occured– because there were several (although not many) instances of “Indianapolis'' being the recorded observation for city, a city not in Ohio as far as we can tell. The variable city being the residential city of the tested person makes this the most preferable match because it will allow us to more accurately merge this data with voter data in step 2. After merging on city, we set aside the 120,370 matched observations (now having county and FIPS variables) and merge on arrestingagency (using arrestingagency_final.dta) for the remaining unmatched observations. Of the ~24,000 observations that are fed into this merge, there are a resulting 17,020 matched observations and 6,793 unmatched observations (from the master file). After merging on arrestingagency, we set aside the 17,900 matched observations (now having county and FIPS variables) and merge on testingagency (using testingagency_final.dta) for the remaining unmatched observations. Of the ~7,000 observations that are fed into this merge, there are a resulting 1,129 matched observations and 5,888 unmatched observations (from the master file). 
At this point, we recombine the data from the three merges + the unmatched data from master to find that of the 144,110 total observations in the breath test data, there are only 5,888 unmatched observations. After browsing the data, we saw that many of these were either from the data being recorded incorrectly (such as an address for arrestingagency or a number for city), but some of these unmatched observations were blank for the name variables. Thus we decided to drop the empty observations where fullname was blank, leading to a final 5,591 unmatched observations of the 144,110 total observations in the final output of this .do file, merged_data.dta. The output of this file contains the cleaned names and other extra variables (as described in the explanation for step1_names.do above) in addition to the county and FIPS variables for 138,519 of these observations (aka the observations that matched in one of the merges performed in this .do file). 
INPUT:	 
step1_names.do
	>$int_dir/step1_names.dta
city_final.dta
>$int_dir/city_final
arrestingagency_final.dta
>$int_dir/arrestingagency_final
testingagency_final.dta
>$int_dir/testingagency_final
INTERMEDIARY:
city_merge.dta
>$int_dir/city_merge.dta
arrestingagency_merge.dta
>$int_dir/arrestingagency_merge.dta
testingagency_merge.dta
>$int_dir/testingagency_merge.dta
OUTPUT:
merged_data.dta
>$int_dir/merged_data

step1_final_cleaning.do
In this file, we take in the merged data from step1_merge.do. We then perform several steps of variable cleaning to match variable names, etc. to the Texas code. Next, we generate variables called dui_first_cnty1-3 and dui_last_cnty1-3. These variables hold the county names for the county in which the first and last incident occurred for each individual. 
After this initial cleaning, we save our progress to OhioBreathalyzer.dta, and then reopen the file to create a version of the data with only one observation per incident. Note that at this point, we use dui_first_cnty1-3 to make a single variable for the primary first county, dui_first_primary_county (and same with dui_first_cnty_fips_1-3, dui_last_cnty_fips_1-3, dui_last_cnty1-3 for their respective names) in order to more closely mirror the Texas data (which only has the four county variables instead of twelve). We select which of the up to three counties will become the primary county variable in one of three ways:
If a merged observation only has a county_1 variable, that county becomes the primary county regardless of which merge phase (from the previous .do file) it merged in.
If a merged observation has county variables county_2-3 and merged on city in the previous .do file, we used https://www.randymajors.org/city-limits-on-google-maps?x=-83.4079900&y=41.1592720&cx=-83.4079900&cy=41.1592720&zoom=12&cities=show&counties=show (which shows city limits relative to county lines on a map through Google Maps) to see which county the city mostly belongs to. For example, Columbus extends through the counties Delaware, Fairfield, and Franklin, but on the map it has the most overlap with Franklin County. So Franklin is the primary county for Columbus.
If a merged observation has county variables county_2-3 and merged on arrestingagency or testingagency in the previous .do file, we first checked to see if the counties listed were adjacent. If adjacent and the agency is a State Patrol or Highway Patrol post, we chose a primary county based on the county in which the patrol post was located based on https://www.statepatrol.ohio.gov/doc/OHP1178.pdf. If adjacent and the agency is a police department, we cross checked the departments’ city names with the primary county for the cities described above (in the 2nd way) and used that as the primary county. Note that there were several agencies for which the associated counties were not adjacent (e.g. Jackson Township PD with counties Stark and Montgomery). We found that in these cases, there were actually different agencies with the same name in different parts of Ohio and thus cannot be used to uniquely identify agency locations. However, only six agency names both face this issue and have observations, and between these six there are only 19 observations for which this is an issue. This being the case, we did not assign these 19 observations a value for dui_first/last_primary_county. 
Specifics on how we made these changes can be found in the file itself. We proceed to keep only important variables and then save this version under “ohio_breath_tests_uniqueincident.dta”. 
Finally, we label all of our variables, and then save our final data into 36 files called ohio_breath_tests_uniqueincident_`A-Z'.dta. These files each contain all observations where the first letter of the first name of the individual in question matches the associated letter of the file.
These output files: ohio_breath_tests_uniqueincident_`A-Z’.dta are cleaned versions of the OhioBreathalyzer dataset. These new datasets differ from the original dataset in that it includes predicted race, county, and FIPS code variables in addition to cleaned variables for first, last, and middle names as well as suffixes and alternate versions of first and last names. It also includes multiple county variables (up to 3 + associated FIPS) in addition to the primary county + FIPS variables (which are the closest analogue to the first and last county variables in the TX code).
INPUT:	 
merged_data.dta
>$int_dir/merged_data.dta
INTERMEDIARY:
OhioBreathalyzer.dta
>$int_dir/breath_test_data/OhioBreathalyzer.dta
ohio_breath_tests_uniqueincident.dta
>$int_dir/ohio_breath_tests_uniqueincident.dta
OUTPUT:
ohio_breath_tests_uniqueincident_`A-Z'.dta
>$int_dir/ohio_breath_tests_uniqueincident_`A-Z'.dta

Step 1 Summary
In this step, we scrape county, patrol post/police department, FIPS code, and city data to be merged with Ohio breathalyzer and voter data. We perform several cleaning steps, and end up with 26 alphabetically sorted (by first initial) unique incident files. The main purpose of these scrapes and merges is to allow us to adjust the weights of each observation in our final regression depending on whether and how we were able to match observations (from breathalyzer data) with actual people (from voter data). 
This file differs from step 1 of the Texas code in three major ways. First, in this file we construct county and FIPS code variables. Such variables are already present in the Texas data, and the analog for this process is thus unnecessary. Second, the name variables from the raw Ohio data had significantly different formatting. This required some slight alterations and additions to the original name-cleaning code drawn from the Texas Step 1 file. Third, there are some variable differences; no marker for test validity was administratively recorded in the Ohio data so we skipped this variable in the Ohio code, and race was recorded as a variable in the Ohio data and is kept in addition to the race probability variables described above.  

STEP 2: Clean Voter data
STEP2_ohio_read_voter_vers1.do
This file takes in the Ohio voter .txt files, given by county, for cleaning. These files are publicly available, originally taken from https://www6.ohiosos.gov/ords/f?p=VOTERFTP:HOME::::::. Ultimately, this will allow us to verify the identities of individuals from the main data set (from Step 1) in Step 5. Specifically, this file cleans the voter data and creates frequency tables for first and last names. Specifics for the cleaning process can be found within the file itself. After cleaning, county_fips_1.dta from Step 1 is merged to add FIPS codes to the dataset. Most notably, the voter data for each county file are separated then recombined into 26 files, each file being organized by first initial rather than by county. 
INPUT:	 
`countyname’.txt
>$raw_dir/`var'.txt
county_1_fips.dta
	>$int_dir/county_1_fips.dta
INTERMEDIARY:
voter_file_`countyname’_`A-Z’.dta
>$int_dir/voter_file_`var'_`var2’.dta
voter_file_A.dta
	>$int_dir/voter_file_A.dta
 voter_file_`A-Z’.dta
	>$int_dir/voter_file_`y'.dta
OUTPUT: 
 voter_file_`A-Z’.dta
	>$int_dir/voter_file_`var2'.dta
voter_f_name_freq.dta
	>$int_dir/voter_f_name_freq.dta
 voter_l_name_freq.dta
	>$int_dir/voter_l_name_freq.dta

STEP 4: Read Hamilton County
STEP4_read_hamiltoncounty.do
(file explanation) – ask Boston (:
INPUT:	 
INTERMEDIARY:
OUTPUT:

STEP 5: Merge DUI and Voter Datasets
STEP5_ohio_merge_dui_voter.do
This file takes the unique breath test incident data files (output from Step 1) and merges them with the voter data files (output from step 2) on the variables first_name and last_name. We do this to identify which observations from the breathalyzer data match with voter data so that we can put more weight on these observations in regressions. We want more weight on matched observations because they have more verification of the identity of an individual.
There are two main loops in this file: the first loop merges each unique breath test incident data file A-Z with each voter file A-Z, and the second loop combines each of these files into a single dataset. The first loop is broken into 11 phases. Phases 1-9 merge the datasets and compare identifying variables such as county, birthdate, etc to find true matches that are not duplicates. Specifics about each of these phases can be found in the file itself. Phase 10 adds the datasets that did not merge back so that each of the “dui_voter_merged_`A-Z’.dta” files have both the matched and unmatched observations. The second loop takes each of these merged DUI/voter files and creates the output: “dui_voter_merged_all.dta”, which is now the most up-to-date version of the primary dataset.
Note 1: mmerge is a command that merges on all possible matches (hence the increase in number of observations after the merge). The different phases in the loops that we use to try to verify true matches are because we are trying to identify a true match (e.g. same name and birthday instead of just same name).
Note 2: we do not replicate the regression at the end of Step 5 of the Texas code because the Ohio data does not have an analogue for the variable “vtest”, an administratively recorded indicator for breath test validity.
INPUT:	 
ohio_breath_tests_uniqueincident_`A-Z’.dta
	>$int_dir/ohio_breath_tests_uniqueincident_`y'.dta
 voter_file_`A-Z’.dta
	>$int_dir/voter_file_`y'.dta
INTERMEDIARY:
dui_voter_merged_’A-Z’.dta
>$int_dir/dui_voter_merged_`y'.dta
OUTPUT:
dui_voter_merged_all.dta
>$int_dir/dui_voter_merged_all.dta


STEP 7: Merge DUI and Hamilton County
STEP7_merge_dui_hamiltoncnty.do
In this step, we clean court records from criminal cases in the Hamilton County District Court. This file goes through 6 rounds of merging to ultimately create a clean dataset of all the court records in Hamilton County. Before we initialize the rounds of merging, we create a new dataset called ohio_breath_tests_uniqueincident_hc that is, essentially, a set of unique incident data that only includes observations from Hamilton County. 
In the round 1 merge, we bring in the hamilton county breath test data from step 4, and merge it with our hamilton county unique incident set on the basis of first and last names. We keep only the merged observations and proceed to perform several checks to verify the quality of these matches. Details on these checks can be found in the file itself. Finally, we merge this cleaned set back into the original DUI set in order to restore all the observations that did not match in our original merge. 
In the round 2 merge, we account for individuals with composite last names. These individuals may need to be merged on a different last name than that used in the initial merge. 
Round 3 of the merge serves essentially the same purpose as round 2, but it starts with the data from Hamilton County. 
In the round 4 merge, the Hamilton County breath test year data from step 4 is used again to compare birth years between datasets. The DUI full names are compared to the court record full names with the Jaro-Winkler distance (a measure of how similar two dissimilar strings are), keeping the names with a birth year match and high Jaro-Winkler score, saving these names independently as well as merging them into the Hamilton County unique incident dataset.
In the round 5 merge, we again merge HC breath test years data with our original using set. This time, we compare strings using jarowrinkler distance. 
Round 6 merges the same two sets as rounds 1 and 5, but only by last name. We once again compare the differences in strings using the jarowinkler distance. 
May want to update these merge rounds more once we understand step 7 a little better!

INPUT:	 
ohio_breath_tests_uniqueincident.dta
>$int_dir/ohio_breath_tests_uniqueincident
hamilton_county_breathtestyears.dta
>$int_dir/hamilton_county_breathtestyears
INTERMEDIARY:
ohio_breath_tests_uniqueincident_hc.dta
>$int_dir/ohio_breath_tests_uniqueincident_hc
dui_hc_round1.dta
>$int_dir/dui_hc_round1
dui_hc_round2.dta
>$int_dir/dui_hc_round2
tmpdat.dta
>$int_dir/tmpdat.dta
dui_hc_round3.dta
>$int_dir/dui_hc_round3
dui_hc_round4.dta
>$int_dir/dui_hc_round4
dui_hc_round5.dta
>$int_dir/dui_hc_round5
dui_hc_round6.dta
>$int_dir/dui_hc_round6
OUTPUT:
dui_hc_merged.dta
>$clean_dir/dui_hc_merged.dta


STEP 8: Merge DUI and Convictions
STEP8.do
(file explanation)
INPUT:	 
INTERMEDIARY:
OUTPUT:	 

STEP 9: Merge DUI and Voter Hamilton County (HC)
STEP9.do
(file explanation)
INPUT:	 
INTERMEDIARY:
OUTPUT: 

STEP 10: RD Validity
STEP10.do
(file explanation)
INPUT:	 
INTERMEDIARY:
OUTPUT:

STEP 11: Hamilton County First Stage
STEP11.do
(file explanation)
INPUT:	 
INTERMEDIARY:
OUTPUT: 
