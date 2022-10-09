*******************************************************************
/*
INPUT: ***
INTERMEDIARY: ***
OUTPUT: ***
*/

*This file will ****
*******************************************************************

**Root directory
*global root_dir "/Volumes/Elements/DUI/ohio"
*global root_dir "/Users/ll263/Box/DUI/ohio"\
global root_dir C:/Users/ashlyn04/Box/DUIAshlyn/ohio
*global root_dir "/Users/mukaie/Box/DUIAshlyn/ohio"


**Note that the directory pir_sos_20210118 may need to be uncompressed for the file to run
global raw_dir "$root_dir/raw_data/courts_data"
global int_dir "$root_dir/intermediate_data"
global clean_dir "$root_dir/clean_data"



*ssc install mmerge


clear all
set more off


***** CLEANING


	**open person file from conviction database.
	*use /Users/ct32696/Dropbox/NonPecuniary_TX/data/person_linked.dta, clear
	use "C:\Users\ct\Dropbox\NonPecuniary_TX\data/person_linked.dta", clear

	**keep relevant vars, focusing only on BASE name record for now.
	keep per_idn sex_cod rac_cod eth_cod hgt_qty wgt_qty hai_cod eye_cod ind_idn date_dob* lname_txt1 fname_txt1

	*rename name variables to standard format
	rename lname_txt1 last_name
	rename fname_txt1 first_name

	*for every other variable give it a prefix to indicate it is from the conv data.
	foreach v of varlist per_idn sex_cod rac_cod eth_cod hgt_qty wgt_qty hai_cod eye_cod ind_idn date_dob*{
		rename `v' conv_`v'
	}

	*store raw last names into these temp ln and fn variables for inspecting data along the way
	g ln=last_name
	g fn=first_name

	*rename the main name variables to _raw since we'll be cleaning them in a lot of ways.
	rename last_name last_name_raw
	rename first_name first_name_raw

	compress


	****this block of code is trying to convert the first name and last name variables
	**** into the format that we typically use. typically, first name only contains
	**** one or two words and last name contains everything else. in this data,
	**** last name is often one word and first name contains everything else
	**** in the code below, i am pulling out suffix from the first name variable
	**** and then moving everything that isn't a first name or middle name
	**** to the beginning of the last name string. so after this code, basically:
	**** first_name_raw = first name + middle name
	**** last_name_raw = all other words originally after middle name in the first name var (excl. suffix) + last name

	***Code name suffix at end of first name variable:
		** Create new variable for suffixes
		gen conv_suffix=""
		gen temp=word(first_name_raw,-1)
		foreach i in JR SR II III IV V VI IIII IIIII IIIIII {
			replace conv_suffix = "`i'" if temp=="`i'"
		}
		drop temp

	***Code first word of first name variable:
		gen temp_first_name_raw_word1 = word(first_name_raw,1)

	***Using those two variables, create var for only middle name words
		gen temp_conv_name_middle_words = subinstr(first_name_raw,temp_first_name_raw_word1,"",1)
		replace temp_conv_name_middle_words = subinstr(temp_conv_name_middle_words,conv_suffix,"",.)
		replace temp_conv_name_middle_words = trim(temp_conv_name_middle_words)

	***Code middle initial based on first letter of first word in middle name words
		gen conv_middle_initial = substr(temp_conv_name_middle_words,1,1)
		gen temp_conv_middle_name = word(temp_conv_name_middle_words,1)
		
	***Code names that aren't first name, aren't middle name, and aren't suffix but are still in first name variable
		gen addtl_last_names=subinstr(first_name_raw,temp_first_name_raw_word1,"",1)
		replace addtl_last_names=subinstr(addtl_last_name,conv_suffix,"",.)
		replace addtl_last_names=subinstr(addtl_last_name,temp_conv_middle_name,"",1)
		replace addtl_last_names=trim(itrim(addtl_last_names))
		replace addtl_last_names="" if length(addtl_last_names)==1
		replace addtl_last_names=temp_conv_middle_name+" "+addtl_last_names if inlist(temp_conv_middle_name,"DE","VAN")
		drop temp_*
		
		replace last_name_raw=addtl_last_names+" "+last_name_raw
		replace first_name_raw=subinstr(first_name_raw,addtl_last_names,"",1)
		replace first_name_raw=subinstr(first_name_raw,conv_suffix,"",.)
		drop addtl_last_names	
		

	*Standardize case and remove punctuation + extra spaces
	replace last_name_raw=upper(trim(itrim(last_name_raw)))
	replace first_name_raw=upper(trim(itrim(first_name_raw)))
	replace conv_middle_initial=upper(trim(itrim(conv_middle_initial)))

	replace last_name_raw=subinstr(last_name_raw,"-"," ",.)
	replace last_name_raw=subinstr(last_name_raw,".","",.)
	replace last_name_raw=subinstr(last_name_raw,"  "," ",.)
	replace last_name_raw=subinstr(last_name_raw,"  "," ",.)
	replace last_name_raw=subinstr(last_name_raw,"'","",.)
	replace last_name_raw=subinstr(last_name_raw,"?"," ",.)
	replace last_name_raw=subinstr(last_name_raw,"*"," ",.)
	replace last_name_raw=strtrim(last_name_raw)
	replace last_name_raw=stritrim(last_name_raw)

	replace first_name_raw=subinstr(first_name_raw,"-"," ",.)
	replace first_name_raw=subinstr(first_name_raw,".","",.)
	replace first_name_raw=subinstr(first_name_raw,"  "," ",.)
	replace first_name_raw=subinstr(first_name_raw,"  "," ",.)
	replace first_name_raw=subinstr(first_name_raw,"'","",.)
	replace first_name_raw=subinstr(first_name_raw,"?","",.)
	replace first_name_raw=subinstr(first_name_raw,"*","",.)
	replace first_name_raw = strtrim(first_name_raw)
	replace first_name_raw = stritrim(first_name_raw)


	** Make first name vars (one with first word, one with all words)
	split first_name_raw

	gen first_name=first_name_raw1
	gen conv_alt_first_name=subinstr(first_name_raw," ","",.)

	drop first_name_raw*

	** Make last name vars
	split last_name_raw

	**Finish last name cleaning (combine last name phrases like "de la" into a single word, but not cases that look like multiple distinct surnames)
	*One-word last names
	gen last_name=last_name_raw1 if last_name_raw2==""	
	gen conv_alt_last_name = ""

	*Two-word last names
	*Take care of compound names
	replace last_name=last_name_raw1+last_name_raw2 if last_name_raw3=="" & ///
			(last_name_raw1=="DOS" | last_name_raw1=="D" | last_name_raw1=="DE" | ///
			 last_name_raw1=="DEL" | last_name_raw1=="DELLA" | last_name_raw1=="DELA" ///
			 | last_name_raw1=="DA" | last_name_raw1=="VAN" | last_name_raw1=="VON" | ///
			 last_name_raw1=="VOM" | last_name_raw1=="SAINT" | last_name_raw1=="ST" | ///
			 last_name_raw1=="SANTA" | last_name_raw1=="A" | last_name_raw1=="AL" | ///
			 last_name_raw1=="BEN" | last_name_raw1=="DI" | last_name_raw1=="EL" | ///
			 last_name_raw1=="LA" | last_name_raw1=="LE" | last_name_raw1=="MC" | ///
			 last_name_raw1=="MAC" | last_name_raw1=="O" | last_name_raw1=="SAN")

	*Last names that seem to be 2 different surnames
	replace conv_alt_last_name=last_name_raw1+last_name_raw2 if last_name_raw3=="" & last_name==""
	replace last_name=last_name_raw2 if last_name_raw3=="" & last_name==""


	*Three-word last names
	*Take care of compound names
	replace last_name=last_name_raw1+last_name_raw2+last_name_raw3 if last_name_raw4=="" & ///
			(last_name_raw1+last_name_raw2=="DELA" | last_name_raw1+last_name_raw2=="DELOS" | ///
			last_name_raw1+last_name_raw2=="DELAS"  | last_name_raw1+last_name_raw2=="VANDE" | last_name_raw1+last_name_raw2=="VANDER")

	*Three-word names that seem a compound of one two word name and another 1 word name
	replace conv_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3 if last_name=="" & last_name_raw4=="" & ///
		(last_name_raw1=="DOS" | last_name_raw1=="D" | last_name_raw1=="DE" | last_name_raw1=="DEL" | ///
		last_name_raw1=="DELLA" | last_name_raw1=="DELA" | last_name_raw1=="DA" | last_name_raw1=="VAN" | ///
		last_name_raw1=="VON" | last_name_raw1=="VOM" | last_name_raw1=="SAINT" | last_name_raw1=="ST" | ///
		last_name_raw1=="SANTA" | last_name_raw1=="A" | last_name_raw1=="AL" | last_name_raw1=="BEN" | ///
		last_name_raw1=="DI" | last_name_raw1=="EL" | last_name_raw1=="LA" | last_name_raw1=="LE" | ///
		last_name_raw1=="MC" | last_name_raw1=="MAC" | last_name_raw1=="O" | last_name_raw1=="SAN")

	replace last_name=last_name_raw3 if last_name=="" & last_name_raw4=="" & ///
		(last_name_raw1=="DOS" | last_name_raw1=="D" | last_name_raw1=="DE" | ///
		last_name_raw1=="DEL" | last_name_raw1=="DELLA" | last_name_raw1=="DELA" | ///
		last_name_raw1=="DA" | last_name_raw1=="VAN" | last_name_raw1=="VON" | ///
		last_name_raw1=="VOM" | last_name_raw1=="SAINT" | last_name_raw1=="ST" | ///
		last_name_raw1=="SANTA" | last_name_raw1=="A" | last_name_raw1=="AL" | ///
		last_name_raw1=="BEN" | last_name_raw1=="DI" | last_name_raw1=="EL" | ///
		last_name_raw1=="LA" | last_name_raw1=="LE" | last_name_raw1=="MC" | ///
		last_name_raw1=="MAC" | last_name_raw1=="O" | last_name_raw1=="SAN")


	*Three-word names that seem a compound of 1 one-word name and another two-word name
	replace conv_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3 if last_name=="" & last_name_raw4=="" & ///
		(last_name_raw2=="DOS" | last_name_raw2=="D" | last_name_raw2=="DE" | last_name_raw2=="DEL" | last_name_raw2=="DELLA" | ///
		last_name_raw2=="DELA" | last_name_raw2=="DA" | last_name_raw2=="VAN" | last_name_raw2=="VON" | last_name_raw2=="VOM" | ///
		last_name_raw2=="SAINT" | last_name_raw2=="ST" | last_name_raw2=="SANTA" | last_name_raw2=="A" | last_name_raw2=="AL" | ///
		last_name_raw2=="BEN" | last_name_raw2=="DI" | last_name_raw2=="EL" | last_name_raw2=="LA" | last_name_raw2=="LE" | ///
		last_name_raw2=="MC" | last_name_raw2=="MAC" | last_name_raw2=="O" | last_name_raw2=="SAN")

	replace last_name=last_name_raw2+last_name_raw3 if last_name=="" & last_name_raw4=="" & ///
		(last_name_raw2=="DOS" | last_name_raw2=="D" | last_name_raw2=="DE" | last_name_raw2=="DEL" | ///
		last_name_raw2=="DELLA" | last_name_raw2=="DELA" | last_name_raw2=="DA" | last_name_raw2=="VAN" | ///
		last_name_raw2=="VON" | last_name_raw2=="VOM" | last_name_raw2=="SAINT" | last_name_raw2=="ST" | ///
		last_name_raw2=="SANTA" | last_name_raw2=="A" | last_name_raw2=="AL" | last_name_raw2=="BEN" | ///
		last_name_raw2=="DI" | last_name_raw2=="EL" | last_name_raw2=="LA" | last_name_raw2=="LE" | ///
		last_name_raw2=="MC" | last_name_raw2=="MAC" | last_name_raw2=="O" | last_name_raw2=="SAN")


	*The rest of the three-word names
	replace conv_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3 if last_name=="" & last_name_raw4==""
	replace last_name=last_name_raw3 if last_name=="" & last_name_raw4==""


	*Four-word names
	*Names that include de los or van de
	replace conv_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3+last_name_raw4 if last_name=="" & last_name_raw5=="" & ///
		(last_name_raw1+last_name_raw2=="DELA" | last_name_raw1+last_name_raw2=="DELOS" | last_name_raw1+last_name_raw2=="DELAS"  | ///
		last_name_raw1+last_name_raw2=="VANDE" | last_name_raw1+last_name_raw2=="VANDER")

	replace last_name=last_name_raw4 if last_name=="" & last_name_raw5=="" & ///
		(last_name_raw1+last_name_raw2=="DELA" | last_name_raw1+last_name_raw2=="DELOS" | last_name_raw1+last_name_raw2=="DELAS"  | ///
		last_name_raw1+last_name_raw2=="VANDE" | last_name_raw1+last_name_raw2=="VANDER")


	replace conv_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3+last_name_raw4 if last_name=="" & last_name_raw5=="" & ///
		(last_name_raw2+last_name_raw3=="DELA" | last_name_raw2+last_name_raw3=="DELOS" | last_name_raw2+last_name_raw3=="DELAS"  | ///
		last_name_raw2+last_name_raw3=="VANDE" | last_name_raw2+last_name_raw3=="VANDER")

	replace last_name=last_name_raw2+last_name_raw3+last_name_raw4 if last_name=="" & last_name_raw5=="" & ///
		(last_name_raw2+last_name_raw3=="DELA" | last_name_raw2+last_name_raw3=="DELOS" | last_name_raw2+last_name_raw3=="DELAS"  | ///
		last_name_raw2+last_name_raw3=="VANDE" | last_name_raw2+last_name_raw3=="VANDER")

		
	*Names that end with a two-word name like de santis
	replace conv_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3+last_name_raw4 if last_name=="" & last_name_raw5=="" & ///
		(last_name_raw3=="DOS" | last_name_raw3=="D" | last_name_raw3=="DE" | last_name_raw3=="DEL" | last_name_raw3=="DELLA" | ///
		last_name_raw3=="DELA" | last_name_raw3=="DA" | last_name_raw3=="VAN" | last_name_raw3=="VON" | last_name_raw3=="VOM" | ///
		last_name_raw3=="SAINT" | last_name_raw3=="ST" | last_name_raw3=="SANTA" | last_name_raw3=="A" | last_name_raw3=="AL" | ///
		last_name_raw3=="BEN" | last_name_raw3=="DI" | last_name_raw3=="EL" | last_name_raw3=="LA" | last_name_raw3=="LE" | ///
		last_name_raw3=="MC" | last_name_raw3=="MAC" | last_name_raw3=="O" | last_name_raw3=="SAN")

	replace last_name=last_name_raw3+last_name_raw4 if last_name=="" & last_name_raw5=="" & ///
		(last_name_raw3=="DOS" | last_name_raw3=="D" | last_name_raw3=="DE" | last_name_raw3=="DEL" | ///
		last_name_raw3=="DELLA" | last_name_raw3=="DELA" | last_name_raw3=="DA" | last_name_raw3=="VAN" | ///
		last_name_raw3=="VON" | last_name_raw3=="VOM" | last_name_raw3=="SAINT" | last_name_raw3=="ST" | ///
		last_name_raw3=="SANTA" | last_name_raw3=="A" | last_name_raw3=="AL" | last_name_raw3=="BEN" | ///
		last_name_raw3=="DI" | last_name_raw3=="EL" | last_name_raw3=="LA" | last_name_raw3=="LE" | ///
		last_name_raw3=="MC" | last_name_raw3=="MAC" | last_name_raw3=="O" | last_name_raw3=="SAN")


	*The rest of the 4 word names
	replace conv_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3+last_name_raw4 if last_name=="" & last_name_raw5==""
	replace last_name=last_name_raw4 if last_name=="" & last_name_raw5==""


	*Five-word names
	*We're just going to work on the last parts of the names
	replace conv_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3+last_name_raw4+last_name_raw5 if last_name=="" & ///
		(last_name_raw3+last_name_raw4=="DELA" | last_name_raw3+last_name_raw4=="DELOS" | last_name_raw3+last_name_raw4=="DELAS"  | ///
		last_name_raw3+last_name_raw4=="VANDE" | last_name_raw3+last_name_raw4=="VANDER")

	replace last_name=last_name_raw3+last_name_raw4+last_name_raw5 if last_name=="" & ///
		(last_name_raw3+last_name_raw4=="DELA" | last_name_raw3+last_name_raw4=="DELOS" | ///
		last_name_raw3+last_name_raw4=="DELAS"  | last_name_raw3+last_name_raw4=="VANDE" | ///
		last_name_raw3+last_name_raw4=="VANDER")

	*Names that end with a 2 word name like de santis
	replace conv_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3+last_name_raw4+last_name_raw5 if last_name=="" & ///
		(last_name_raw4=="DOS" | last_name_raw4=="D" | last_name_raw4=="DE" | last_name_raw4=="DEL" | last_name_raw4=="DELLA" | ///
		last_name_raw4=="DELA" | last_name_raw4=="DA" | last_name_raw4=="VAN" | last_name_raw4=="VON" | last_name_raw4=="VOM" | ///
		last_name_raw4=="SAINT" | last_name_raw4=="ST" | last_name_raw4=="SANTA" | last_name_raw4=="A" | last_name_raw4=="AL" | ///
		last_name_raw4=="BEN" | last_name_raw4=="DI" | last_name_raw4=="EL" | last_name_raw4=="LA" | last_name_raw4=="LE" | ///
		last_name_raw4=="MC" | last_name_raw4=="MAC" | last_name_raw4=="O" | last_name_raw4=="SAN")

	replace last_name=last_name_raw4+last_name_raw5 if last_name=="" & ///
		(last_name_raw4=="DOS" | last_name_raw4=="D" | last_name_raw4=="DE" | last_name_raw4=="DEL" | last_name_raw4=="DELLA" | ///
		last_name_raw4=="DELA" | last_name_raw4=="DA" | last_name_raw4=="VAN" | last_name_raw4=="VON" | last_name_raw4=="VOM" | ///
		last_name_raw4=="SAINT" | last_name_raw4=="ST" | last_name_raw4=="SANTA" | last_name_raw4=="A" | last_name_raw4=="AL" | ///
		last_name_raw4=="BEN" | last_name_raw4=="DI" | last_name_raw4=="EL" | last_name_raw4=="LA" | last_name_raw4=="LE" | ///
		last_name_raw4=="MC" | last_name_raw4=="MAC" | last_name_raw4=="O" | last_name_raw4=="SAN")


	*The rest of the five-word names
	replace conv_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3+last_name_raw4+last_name_raw5 if last_name=="" & last_name_raw6==""
	replace last_name=last_name_raw5 if last_name=="" & last_name_raw6==""


	*Six-word last names
	replace conv_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3+last_name_raw4+last_name_raw5+last_name_raw6 if last_name_raw6~=""
	replace last_name=last_name_raw6 if last_name=="" & last_name_raw6!=""


	replace last_name="DELAFUENTE" if last_name_raw=="DE LA GARZA DE LA FUENTE"
	replace last_name="DELAFUENTE" if last_name_raw=="DE LA CRUZ DE LA FUENTE"
	replace last_name="UK" if last_name_raw=="CEVO V AMOSU ONESI ASIAWU UK"
	replace last_name="DESAINTMARC" if last_name_raw=="BAUGNIES DE PAUL DE SAINT MARC"
	replace last_name="DEBORBON" if last_name_raw=="DE TODOS LOS SANTOS DE BORBON"
	replace last_name="DELAROSA" if last_name_raw=="DE LEON DE DE LA ROSA"
	replace last_name="DELAGARZA" if last_name_raw=="DE LOS REYES DE LA GARZA"
	replace last_name="DELACRUZ" if last_name_raw=="DE LA CRUZ DE LA CRUZ"


	drop last_name_raw*
		
	tempfile conv
	save `conv', replace





**** MERGING
	forval x=1/16{

	foreach y in A B C D E F G H I J K L M N O P Q R S T U V W X Y Z {
		cap {
		**ROUND #
		*display round # = # of dob_date used.
		display "Round `x'"
		
		*open conviction data with cleaned names, blocking on first letter of first name.
		use if substr(first_name,1,1)=="`y'" using `conv', clear
		
		*STEP 1: many-to-many merge to DUI breath incidents, blocking on first letter of first name.
		mmerge first_name last_name using "$int_dir/texas_breath_tests_uniqueincident_`y'.dta"
		
		*Work with observations that merged
		keep if _merge==3
		drop _merge
		
			
		* STEP 2:
		**Get rid of matches where the age at stop and conv birthdate aren't compatible
		keep if conv_date_dob`x' < dui_bdate_max & conv_date_dob`x' > dui_bdate_min
		

		
		**Compare middle initials
		gen dui_conv_mi_match = dui_middle_initial==conv_middle_initial
		gen dui_conv_mi_none = dui_middle_initial=="" & conv_middle_initial==""
		gen mi_mismatch = dui_middle_initial!=conv_middle_initial & dui_middle_initial!="" & conv_middle_initial!=""
		
		
		* STEP 3
		*Drop if the middle initials are mismatched (neither missing & they aren't the same)
		drop if mi_mismatch
		drop mi_mismatch
		
		
		**At this point we want to count how many people satisfy the criteria for each DUI incident.
		**We may use this to further prune observations when we perform the eventual analysis.
		
		duplicates tag dui_incident_id, gen(dui_conv_dup_initial)
		
		* STEP 4
		*Drop obs with a missing middle initial in the conv file when there exists another potential merge with matching middle initial
		egen dui_conv_num_mi_match = sum(dui_conv_mi_match), by(dui_incident_id)
		drop if dui_conv_mi_match==0 & dui_conv_num_mi_match>0
		
		
		* STEP 5
		*Drop obs where there is no middle initial in the DUI record, there is one in the conv file, and there exists another potential merge from the conv file with no middle initial
		egen dui_conv_num_mi_none = sum(dui_conv_mi_none), by(dui_incident_id)
		drop if dui_conv_mi_none==0 & dui_conv_num_mi_none>0
		
		
		* STEP 6
		**Compare alternative first and last names (which include more words from the raw names) 
		gen dui_conv_alt_first_name_match=dui_alt_first_name==conv_alt_first_name & dui_alt_first_name~=""
		gen dui_conv_alt_last_name_match=dui_alt_last_name==conv_alt_last_name & dui_alt_last_name~=""
		
		*Drop people with an alternative name match with some record but don't have an alternative name match with the current record.
		egen num_alt_first_match=sum(dui_conv_alt_first_name_match), by(dui_incident_id)
		egen num_alt_last_match=sum(dui_conv_alt_last_name_match), by(dui_incident_id)
			
		drop if dui_conv_alt_first_name_match==0 & num_alt_first_match>0
		drop if dui_conv_alt_last_name_match==0 & num_alt_last_match>0
		
		
		
		* STEP 7
		**Compare suffixes
		gen dui_conv_suffix_match = dui_suffix==conv_suffix & dui_suffix!=""
		
		egen dui_conv_num_suffix_match = sum(dui_conv_suffix_match), by(dui_incident_id)
		drop if dui_conv_suffix_match==0 & dui_conv_num_suffix_match>0
		
		
		* STEP 8
		** Compare counties
		*gen dui_voter_county_match=dui_last_cnty_fips==voter_county_fips // using county from last DUI test for incidents with multiple tests
		
		*Drop obs where counties are different but there's another match where they are the same
		*egen dui_voter_num_county_match=sum(dui_voter_county_match), by(dui_incident_id)
		*drop if dui_voter_county_match==0 & dui_voter_num_county_match>0
		
		drop num*

		
		
		* STEP 9
		**We have some multiple matches--some of these are people who presumably moved because
		**the birthdates are exactly the same.  Others are people with extremely common names.
		
		
		**getting rid of duplicates where the birthdate is exactly the same
		duplicates drop dui_incident_id first_name last_name conv_middle_initial conv_date_dob1, force
		
			
		
		**Inventory the obs we have left
		duplicates tag dui_incident_id, gen(dui_conv_dup)
		
		
		**Add back in incidents that didn't merge
		append using "$int_dir/texas_breath_tests_uniqueincident_`y'.dta"
		gen dui_conv_no_match = missing(conv_per_idn)
		bys dui_incident_id: egen temp = min(dui_conv_no_match)
		drop if dui_conv_no_match==1 & temp==0
		drop temp
		
		g match_round=`x' if dui_conv_no_match==0
		replace match_round=99 if dui_conv_no_match==1
		
		save "$int_dir/dui_conv_merged_`y'_`x'.dta", replace
		}
		continue
			
		}
	}
		
		
		
		
	*** Append all first-initial datasets together
	clear

	forval x=1/16{
	foreach y in A B C D E F G H I J K L M N O P Q R S T U V W X Y Z {
		di "`y' `x'"
		cap append using "$int_dir/dui_conv_merged_`y'_`x'.dta"
		
	}
	}
	tab match_round
	
	bys dui_incident_id: egen min_match_round=min(match_round)
	
	keep if min_match_round==match_round
	drop min_match_round
	
	by dui_incident_id: g firstob=_n==1
	drop if match_round==99 & firstob==0
	drop firstob
	
	tab match_round
	
	compress
	save "$int_dir/dui_conv_merged_all_multdob", replace



/*


u "$int_dir/dui_conv_merged_all_multdob", clear

rename conv_per_idn per_idn
merge m:1 per_idn using "$clean_dir/conviction_database/person_linked_duis.dta"
rename per_idn conv_per_idn
keep if _m==1 | _m==3

g match_to_any_conv=dui_conv_no_match==0
g match_to_dui_conv=_m==3
drop _m
duplicates tag dui_incident_id, gen(dup)


binscatter match_to_any_conv dui_lowest_vresult if inrange(rounded_lowest_result,0,.14), discrete rd(0.0799) line(qfit) ylabel(0(.25)1)
binscatter match_to_dui_conv dui_lowest_vresult if inrange(rounded_lowest_result,0,.14), discrete rd(0.0799) line(qfit) ylabel(0(.25)1)
binscatter match_to_dui_conv dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dui_first_cnty_name=="DALLAS", ///
			discrete rd(0.0799) line(qfit) ylabel(0(.25)1)


binscatter match_to_any_conv dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dup==0, discrete rd(0.0799) line(qfit) ylabel(0(.25)1)
binscatter match_to_dui_conv dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dup==0, discrete rd(0.0799) line(qfit) ylabel(0(.25)1)
binscatter match_to_dui_conv dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dup==0 & dui_first_cnty_name=="DALLAS", ///
			discrete rd(0.0799) line(qfit) ylabel(0(.25)1)

			



tempfile a
save `a', replace

*use "/Users/ct32696/Dropbox/NonPecuniary_TX/data/conv_person_linked_duis.dta", clear
use "C:/Users/ct/Dropbox/NonPecuniary_TX/data/conv_person_linked_duis.dta", clear
bys per_idn: g j=_n
by per_idn: egen maxj=max(j)
drop if maxj>10
drop maxj
keep per_idn date_doa j
reshape wide date_doa, i(per_idn) j(j)
rename per_idn conv_per_idn
tempfile c
save `c', replace

u `a', clear
merge m:1 conv_per_idn using `c'
keep if _m==1 | _m==3
drop _m


forval x=1/10{
g date_diff`x'=dui_test_date-date_doa`x'
}

g arrest_after=.
forval x=1/10{
replace arrest_after=1 if date_diff`x'<=0 & (arrest_after==. | arrest_after==0)
}

g arrest_before=.
forval x=1/10{
replace arrest_before=1 if date_diff`x'>0 & (arrest_before==. | arrest_before==0) & date_diff`x'!=.
}


replace arrest_after=0 if ~missing(date_doa1) & arrest_after==.
replace arrest_after=0 if missing(arrest_after)

replace arrest_before=0 if ~missing(date_doa1) & arrest_before==.
replace arrest_before=0 if missing(arrest_before)

binscatter arrest_after dui_lowest_vresult if inrange(rounded_lowest_result,0,.14), ///
			discrete rd(0.0799) line(qfit) ylabel(0(.25)1)
binscatter arrest_after dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dup==0, ///
			discrete rd(0.0799) line(qfit) ylabel(0(.25)1)		

			
binscatter arrest_after dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dup==0 & dui_first_cnty_name=="HARRIS", ///
			discrete rd(0.0799) line(qfit) ylabel(0(.2)1) name(gs1 ,replace) msymbol(oh) title("HARRIS")
binscatter arrest_after dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dup==0 & dui_first_cnty_name=="BEXAR", ///
			discrete rd(0.0799) line(qfit) ylabel(0(.2)1) name(gs2 ,replace) msymbol(oh) title("BEXAR")
binscatter arrest_after dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dup==0 & dui_first_cnty_name=="DALLAS", ///
			discrete rd(0.0799) line(qfit) ylabel(0(.2)1) name(gs3 ,replace) msymbol(oh) title("DALLAS")
binscatter arrest_after dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dup==0 & dui_first_cnty_name=="HIDALGO", ///
			discrete rd(0.0799) line(qfit) ylabel(0(.2)1) name(gs4 ,replace) msymbol(oh) title("HIDALGO")
binscatter arrest_after dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dup==0 & dui_first_cnty_name=="TARRANT", ///
			discrete rd(0.0799) line(qfit) ylabel(0(.2)1) name(gs5 ,replace) msymbol(oh) title("TARRANT")
binscatter arrest_after dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dup==0 & dui_first_cnty_name=="EL PASO", ///
			discrete rd(0.0799) line(qfit) ylabel(0(.2)1) name(gs6 ,replace) msymbol(oh) title("EL PASO")
binscatter arrest_after dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dup==0 & dui_first_cnty_name=="TRAVIS", ///
			discrete rd(0.0799) line(qfit) ylabel(0(.2)1) name(gs7 ,replace) msymbol(oh) title("TRAVIS")
graph combine gs1 gs2 gs3 gs4 gs5 gs6 gs7

			
g arrest_within_3=.
forval x=1/10{
replace arrest_within_3=1 if inlist(date_diff`x',0,-1,-2,-3) & (arrest_within_3==. | arrest_within_3==0)
}
replace arrest_within_3=0 if ~missing(date_doa1) & arrest_within_3==.
replace arrest_within_3=0 if missing(arrest_within_3)

			
g arrest_within_1=.
forval x=1/10{
replace arrest_within_1=1 if inlist(date_diff`x',0,-1) & (arrest_within_1==. | arrest_within_1==0)
}
replace arrest_within_1=0 if ~missing(date_doa1) & arrest_within_1==.
replace arrest_within_1=0 if missing(arrest_within_1)

binscatter arrest_within_3 dui_lowest_vresult if inrange(rounded_lowest_result,0,.14), discrete rd(0.0799) line(qfit) ylabel(0(.2)1)
binscatter arrest_within_3 dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dup==0, discrete rd(0.0799) line(qfit) ylabel(0(.2)1)
binscatter arrest_within_1 dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dup==0, discrete rd(0.0799) line(qfit) ylabel(0(.2)1)


drop if dup2>0

binscatter arrest_before dui_lowest_vresult if inrange(dui_lowest_vresult,0.03,.13), ///
		discrete rd(0.0799) line(qfit) ylabel(0(.2)1) xlabel(0.03(0.01).13) msymbol(oh) mcolor(black) ///
		ytitle("Share Convicted of DWI Before Test") xtitle("Lowest Measured BrAC") yscale(r(0,1))

binscatter arrest_within_3 dui_lowest_vresult if inrange(dui_lowest_vresult,0.03,.13), ///
		discrete rd(0.0799) line(qfit) ylabel(0(.2)1) xlabel(0.03(0.01).13) msymbol(oh) mcolor(black) ///
		ytitle("Share Convicted of a DWI Linked to Test") xtitle("Lowest Measured BrAC") yscale(r(0,1))

binscatter arrest_after dui_lowest_vresult if inrange(dui_lowest_vresult,0.03,.13), ///
		discrete rd(0.0799) line(qfit) ylabel(0(.2)1) xlabel(0.03(0.01).13) msymbol(oh) mcolor(black) ///
		ytitle("Share Convicted of Any DWI After Test") xtitle("Lowest Measured BrAC") yscale(r(0,1))


		
binscatter arrest_within_3 dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dup==0 & dui_first_cnty_name=="HARRIS", ///
			discrete rd(0.0799) line(qfit) ylabel(0(.2)1) name(gs1 ,replace) msymbol(oh) title("HARRIS")
binscatter arrest_within_3 dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dup==0 & dui_first_cnty_name=="BEXAR", ///
			discrete rd(0.0799) line(qfit) ylabel(0(.2)1) name(gs2 ,replace) msymbol(oh) title("BEXAR")
binscatter arrest_within_3 dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dup==0 & dui_first_cnty_name=="DALLAS", ///
			discrete rd(0.0799) line(qfit) ylabel(0(.2)1) name(gs3 ,replace) msymbol(oh) title("DALLAS")
binscatter arrest_within_3 dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dup==0 & dui_first_cnty_name=="HIDALGO", ///
			discrete rd(0.0799) line(qfit) ylabel(0(.2)1) name(gs4 ,replace) msymbol(oh) title("HIDALGO")
binscatter arrest_within_3 dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dup==0 & dui_first_cnty_name=="TARRANT", ///
			discrete rd(0.0799) line(qfit) ylabel(0(.2)1) name(gs5 ,replace) msymbol(oh) title("TARRANT")
binscatter arrest_within_3 dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dup==0 & dui_first_cnty_name=="EL PASO", ///
			discrete rd(0.0799) line(qfit) ylabel(0(.2)1) name(gs6 ,replace) msymbol(oh) title("EL PASO")
binscatter arrest_within_3 dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dup==0 & dui_first_cnty_name=="TRAVIS", ///
			discrete rd(0.0799) line(qfit) ylabel(0(.2)1) name(gs7 ,replace) msymbol(oh) title("TRAVIS")
graph combine gs1 gs2 gs3 gs4 gs5 gs6 gs7



g arrest_3_to_90=.
forval x=1/10{
replace arrest_3_to_90=1 if date_diff`x'<-3 & date_diff`x'>=-90 & (arrest_3_to_90==. | arrest_3_to_90==0)
}
replace arrest_3_to_90=0 if ~missing(date_doa1) & arrest_3_to_90==.
replace arrest_3_to_90=0 if missing(arrest_3_to_90)

binscatter arrest_3_to_90 dui_lowest_vresult if inrange(rounded_lowest_result,0,.14), discrete rd(0.0799) line(qfit) ylabel(0(.01).05)
binscatter arrest_3_to_90 dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dup==0, discrete rd(0.0799) line(qfit) ylabel(0(.01).05)



g arrest_90_to_3years=.
forval x=1/10{
replace arrest_90_to_3years=1 if date_diff`x'<-90 & date_diff`x'>=-1095 & (arrest_90_to_3years==. | arrest_90_to_3years==0)
}
replace arrest_90_to_3years=0 if ~missing(date_doa1) & arrest_90_to_3years==.
replace arrest_90_to_3years=0 if missing(arrest_90_to_3years)

binscatter arrest_90_to_3years dui_lowest_vresult if inrange(rounded_lowest_result,0,.14), discrete rd(0.0799) line(qfit) ylabel(0(.05).3)


g arrest_3_to_5years=.
forval x=1/10{
replace arrest_3_to_5years=1 if date_diff`x'<-1095 & date_diff`x'>=-1825 & (arrest_3_to_5years==. | arrest_3_to_5years==0)
}
replace arrest_3_to_5years=0 if ~missing(date_doa1) & arrest_3_to_5years==.
replace arrest_3_to_5years=0 if missing(arrest_3_to_5years)

binscatter arrest_3_to_5years dui_lowest_vresult if inrange(rounded_lowest_result,0,.14), discrete rd(0.0799) line(qfit) ylabel(0(.05).3)



g arrest_3days_to_5years=.
forval x=1/10{
replace arrest_3days_to_5years=1 if date_diff`x'<-3 & date_diff`x'>=-1825 & (arrest_3days_to_5years==. | arrest_3days_to_5years==0)
}
replace arrest_3days_to_5years=0 if ~missing(date_doa1) & arrest_3days_to_5years==.
replace arrest_3days_to_5years=0 if missing(arrest_3days_to_5years)

binscatter arrest_3days_to_5years dui_lowest_vresult if inrange(rounded_lowest_result,0,.14), discrete rd(0.0799) line(qfit) ylabel(0(.05).3)
binscatter arrest_3days_to_5years dui_lowest_vresult if inrange(rounded_lowest_result,0,.14) & dup==0, discrete rd(0.0799) line(qfit) ylabel(0(.05).3)



reg arrest_3days_to_5years above_limit index interact if abs(index)<=0.05, r


reg arrest_3days_to_5years above_limit index interact if abs(index)<=0.05 & dup==0, r










use "/Users/ct32696/Dropbox/NonPecuniary_TX/data/conv_person_linked.dta", clear
bys per_idn: g j=_n
by per_idn: egen maxj=max(j)
drop if maxj>10
drop maxj
keep per_idn date_doa j
reshape wide date_doa, i(per_idn) j(j)
rename per_idn conv_per_idn
tempfile c
save `c', replace































/*

*** MOVE TO CLEANING CODE***
* Create first-initial convictions datasets

	
use "$clean_dir/conviction_database/person_linked", clear

merge 1:m ind_idn using "$clean_dir/conviction_database/conv_linked"


gen conv_yob = year(date_dob1)
gen conv_yoa = year(date_doa) // year of arrest
gen conv_moa = month(date_doa) // month of arrest
	// For now, keep only convictions in breath test sample period
	keep if conv_yoa==2004 | inrange(conv_yoa,2009,2015) | (inlist(conv_yoa,2005,2016) & conv_moa==1)
	
// Still need to create sentencing vars, offense vars


** Clean name vars
* Normalize capitilization and punctuation of names
replace lname_txt1=upper(lname_txt1)
replace lname_txt1=subinstr(lname_txt1,"-"," ",.)
replace lname_txt1=subinstr(lname_txt1,".","",.)
replace lname_txt1=subinstr(lname_txt1,"  "," ",.)
replace lname_txt1=subinstr(lname_txt1,"  "," ",.)
replace lname_txt1=subinstr(lname_txt1,"'","",.)
replace lname_txt1=subinstr(lname_txt1,"?"," ",.)
replace lname_txt1=subinstr(lname_txt1,"*"," ",.)
replace lname_txt1=trim(lname_txt1)


replace fname_txt1=upper(fname_txt1)
replace fname_txt1=subinstr(fname_txt1,"-"," ",.)
replace fname_txt1=subinstr(fname_txt1,".","",.)
replace fname_txt1=subinstr(fname_txt1,"  "," ",.)
replace fname_txt1=subinstr(fname_txt1,"  "," ",.)
replace fname_txt1=subinstr(fname_txt1,"'","",.)
replace fname_txt1=trim(fname_txt1)



**Make first name vars (one with first word, one with all words)
split fname_txt1

*Pull out suffixes
gen conv_suffix=""
forvalues i=2/5 {
	replace conv_suffix="JR" if fname_txt1`i'=="JR"
	replace fname_txt1`i'="" if fname_txt1`i'=="JR"
	replace conv_suffix="JR" if fname_txt1`i'=="JR."
	replace fname_txt1`i'="" if fname_txt1`i'=="JR."
	replace conv_suffix="SR" if fname_txt1`i'=="SR"
	replace fname_txt1`i'="" if fname_txt1`i'=="SR"
	replace conv_suffix="II" if fname_txt1`i'=="II"
	replace fname_txt1`i'="" if fname_txt1`i'=="II"
	replace conv_suffix="III" if fname_txt1`i'=="III"
	replace fname_txt1`i'="" if fname_txt1`i'=="III"
	replace conv_suffix="IV" if fname_txt1`i'=="IIII"
	replace fname_txt1`i'="" if fname_txt1`i'=="IIII"
	replace conv_suffix="IV" if fname_txt1`i'=="IV"
	replace fname_txt1`i'="" if fname_txt1`i'=="IV"
	replace conv_suffix="V" if fname_txt1`i'=="V"
	replace fname_txt1`i'="" if fname_txt1`i'=="V"
	replace conv_suffix="VI" if fname_txt1`i'=="VI"
	replace fname_txt1`i'="" if fname_txt1`i'=="VI"
	replace conv_suffix="JR" if fname_txt1`i'=="JR"
	replace fname_txt1`i'="" if fname_txt1`i'=="JR"
}

gen first_name = fname_txt11
gen f_first3 = substr(first_name,1,3)

**The alt_first_name is everything but the suffix
gen conv_alt_first_name=fname_txt11+fname_txt12+fname_txt13+fname_txt14+fname_txt15 if fname_txt12~=""

**I pull a possible middle initial from the first character of the second word in the first name
gen conv_middle_initial =substr(fname_txt12,1,1)

drop fname_txt11-fname_txt15



**Make last name vars
// Need to return to code where lname_txt1, etc., were created to make sure we're making last name vars that are consistent with the last name vars from the breath tests. Right now, lname_txt1 is one word long for everyone in the data (very different from breath test data).
gen last_name = lname_txt1

gen l_first3 = substr(last_name,1,3)



gen conv_fullname = fname_txt1 + " " + lname_txt1
replace conv_fullname = stritrim(conv_fullname)


foreach i in A B C D E F G H I J K L M N O P Q R S T U V W X Y Z	{
preserve
keep if substr(fname_txt1,1,1)=="`i'"
save "$int_dir/person_conv_breathtestyears_`i'", replace
clear
restore

}
*/
*****************************



/*

clear
foreach i in A /*B C D E F G H I J K L M N O P Q R S T U V W X Y Z*/	{
use "$int_dir/texas_breath_tests_uniqueincident_`i'", clear // 28,928 obs


*** ROUND 1 MERGE
mmerge first_name last_name using "$int_dir/person_conv_breathtestyears_`i'"
keep if _merge==3
drop _merge

**Get rid of matches where arrest date is before stop date or more than 2 days after stop date (matches after that appear low quality)
drop if date_doa-cdot<0 | date_doa-cdot>2	// Now have 23,620 obs, 15,101 unique incidents

**Get rid of matches where the birth years aren't compatible
drop if dui_b_year_likely~=conv_yob & dui_b_year_unlikely~=conv_yob //22,488 obs, 14,595 unique incidents


**Compare middle initials
gen mi_match = middle_initial==conv_middle_initial
	gen mi_none = middle_initial=="" & conv_middle_initial==""
	gen mi_mismatch = middle_initial!=conv_middle_initial & middle_initial!="" & conv_middle_initial!=""
	
*Drop if the middle initials are mismatched (neither missing & they aren't the same)
drop if mi_mismatch
	
*Drop obs with a missing middle initial in the conviction database when there exists another potential merge with matching middle initial
egen num_mi_match = sum(mi_match), by(incident_id)
drop if mi_match==0 & num_mi_match>0	
	
	
*Drop obs where there is no middle initial in the DUI record, there is one in the conviction database, and there exists another potential merge from the conviction_database with no middle initial
egen num_mi_none = sum(mi_none), by(incident_id)
drop if mi_none==0 & num_mi_none>0

	
**Compare alternative first and last names (which include more words from the raw names) 
// Skipping alternative last names for now because we don't have this var constructed for conviction database
	gen alt_first_name_match=dui_alt_first_name==conv_alt_first_name & dui_alt_first_name~=""
	//gen alt_last_name_match=dui_alt_last_name==hc_alt_last_name & dui_alt_last_name~=""
	
	**Drop people who have an alternative name match with some record but don't have an alternative name match with the current record.
	egen num_alt_first_match=sum(alt_first_name_match), by(incident_id)
	//egen num_alt_last_match=sum(alt_last_name_match), by(incident_id)
		
	drop if alt_first_name_match==0 & num_alt_first_match>0
	//drop if alt_last_name_match==0 & num_alt_last_match>0
	
	
**Compare suffixes
	gen suffix_match = suffix==conv_suffix & suffix!=""
	
	egen num_suffix_match = sum(suffix_match), by(incident_id)
	drop if suffix_match==0 & num_suffix_match>0
	
	

	***Looking at observations that matched to multiple crimes, it appears that these are multiple offenses for the same breath test incident. We'll keep them all in for now.
	egen num_conv=sum(1), by(incident_id)
	
	// There are many people who seem to have multiple convictions connected to the same breath test (and who seem to have the same ind_id in the conviction database). Need to figure out what's going on here.
	

	
	
*** Save the incidents we've matched so far
	gen conv_round1match = 1
	
	save "$int_dir/dui_conv_round1", replace
	
	
	**Now we'll merge them back into the original DUI file so we get back all of the observations that didn't merge
	use "$int_dir/dui_conv_round1", clear
	
	mmerge incident_id using "$int_dir/texas_breath_tests_uniqueincident_`i'"
	tab _merge
	
	keep if _m==2
	drop _m
	
	
	drop per_idn-conv_round1match
	
	
	*** ROUND 2 MERGE
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
	
	
	**Now merge to the conviction database
	mmerge first_name last_name using "$int_dir/person_conv_breathtestyears_`i'"
	
	keep if _merge==3
	drop _merge length_*
	
	
	
** Now do the other restrictions from the round 1 merges

**Get rid of matches where arrest date is before stop date or more than 2 days after stop date (matches after that appear low quality)
drop if date_doa-cdot<0 | date_doa-cdot>2	

**Get rid of matches where the birth years aren't compatible
drop if dui_b_year_likely~=conv_yob & dui_b_year_unlikely~=conv_yob 


**Compare middle initials
gen mi_match = middle_initial==conv_middle_initial
	gen mi_none = middle_initial=="" & conv_middle_initial==""
	gen mi_mismatch = middle_initial!=conv_middle_initial & middle_initial!="" & conv_middle_initial!=""
	
*Drop if the middle initials are mismatched (neither missing & they aren't the same)
drop if mi_mismatch
	
*Drop obs with a missing middle initial in the conviction database when there exists another potential merge with matching middle initial
egen num_mi_match = sum(mi_match), by(incident_id)
drop if mi_match==0 & num_mi_match>0	
	
	
*Drop obs where there is no middle initial in the DUI record, there is one in the conviction database, and there exists another potential merge from the conviction_database with no middle initial
egen num_mi_none = sum(mi_none), by(incident_id)
drop if mi_none==0 & num_mi_none>0

	
**Compare alternative first and last names (which include more words from the raw names) 
// Skipping alternative last names for now because we don't have this var constructed for conviction database
	gen alt_first_name_match=dui_alt_first_name==conv_alt_first_name & dui_alt_first_name~=""
	//gen alt_last_name_match=dui_alt_last_name==hc_alt_last_name & dui_alt_last_name~=""
	
	**Drop people who have an alternative name match with some record but don't have an alternative name match with the current record.
	egen num_alt_first_match=sum(alt_first_name_match), by(incident_id)
	//egen num_alt_last_match=sum(alt_last_name_match), by(incident_id)
		
	drop if alt_first_name_match==0 & num_alt_first_match>0
	//drop if alt_last_name_match==0 & num_alt_last_match>0
	
	
**Compare suffixes
	gen suffix_match = suffix==conv_suffix & suffix!=""
	
	egen num_suffix_match = sum(suffix_match), by(incident_id)
	drop if suffix_match==0 & num_suffix_match>0
	
	

	***Looking at observations that matched to multiple crimes, it appears that these are multiple offenses for the same breath test incident. We'll keep them all in for now.
	egen num_conv=sum(1), by(incident_id)
	
	// There are many people who seem to have multiple convictions connected to the same breath test (and who seem to have the same ind_id in the conviction database). Need to figure out what's going on here.
	

*** Save the incidents we've matched in this round
	gen conv_round2match = 1
		
	save "$int_dir/dui_conv_round2", replace
	
	
	
	*** ROUND 3 MERGE
	*** Some people with composite last names in the courts data (e.g. CHAVARRIA-SANTOS, merge on CHAVARRIA, not SANTOS)--this
	*** merges these folks.  We need to start with the harris county data first for this merge.
	use "$int_dir/person_conv_breathtestyears_`i'", clear

	
	**In this case, the alternative last name is the categorized as a middle names
	gen tmpvar=fname_txt1
	split tmpvar
	replace last_name=tmpvar2
	drop tmpvar*
	
	save tmpdat, replace
	
	use "$int_dir/texas_breath_tests_uniqueincident_`i'", clear

	mmerge first_name last_name using tmpdat
	keep if _merge==3
	drop _merge

**Get rid of matches where arrest date is before stop date or more than 2 days after stop date (matches after that appear low quality)
drop if date_doa-cdot<0 | date_doa-cdot>2	// Now have 23,620 obs, 15,101 unique incidents

**Get rid of matches where the birth years aren't compatible
drop if dui_b_year_likely~=conv_yob & dui_b_year_unlikely~=conv_yob //22,488 obs, 14,595 unique incidents


**Comparing middle initials won't work given how we constructed the alternative last name
	
**Compare suffixes
	gen suffix_match = suffix==conv_suffix & suffix!=""
	
	egen num_suffix_match = sum(suffix_match), by(incident_id)
	drop if suffix_match==0 & num_suffix_match>0
	
	
	***Looking at observations that matched to multiple crimes, it appears that these are multiple offenses for the same breath test incident. We'll keep them all in for now.
	egen num_conv=sum(1), by(incident_id)
	
	// There are many people who seem to have multiple convictions connected to the same breath test (and who seem to have the same ind_id in the conviction database). Need to figure out what's going on here.
	

	
	
*** Save the incidents we've matched so far
	gen conv_round3match = 1
	
	save "$int_dir/dui_conv_round3", replace

exit
	
	append using "$int_dir/dui_conv_round1" "$int_dir/dui_conv_round2"
	**Now we'll merge them back into the original DUI file so we get back all of the observations that didn't merge
	
	mmerge incident_id using "$int_dir/texas_breath_tests_uniqueincident_`i'"
	tab _merge
	
	keep if _m==2
	drop _m
	
	drop per_idn-conv_round1match
	
	
		

** ROUND 4 MERGE
	mmerge f_first3 l_first3 using "$int_dir/person_conv_breathtestyears_`i'"
	
	keep if _merge==3
	drop _merge
	
**Get rid of matches where arrest date is before stop date or more than 2 days after stop date (matches after that appear low quality)
drop if date_doa-cdot<0 | date_doa-cdot>2


**Compare birth years. Drop obs where they are incompatible, but there is another match with a compatible birth year
	gen b_year_match = dui_b_year_likely==conv_yob | dui_b_year_unlikely==conv_yob
	bys incident_id: egen max_b_year_match = max(b_year_match)
	bys incident_id: egen min_b_year_match = min(b_year_match)
	
	drop if b_year_match==0 & max_b_year_match==1

	
**Compare names using Jaro-Winkler distance
	jarowinkler dui_fullname conv_fullname
	
	sort jarowinkler
	
	//browse incident_id jaro dui_fullname conv_fullname middle_initial conv_middle_initial dui_b_year_likely dui_b_year_unlikely conv_yob lowest_result
	
	keep if jarowinkler > .9 | (jarowinkler>=.8 & b_year_match==1)
	
	
	egen num_conv = sum(1), by(incident_id)
	
	drop *b_year_match jarowinkler
	
	gen conv_round4match = 1
	
	
	save "$int_dir/dui_conv_round4", replace
	
	
	use "$int_dir/dui_conv_round4", clear
	append using /*"$int_dir/dui_conv_round3" */ "$int_dir/dui_conv_round2" "$int_dir/dui_conv_round1"
	
	mmerge incident_id using "$int_dir/texas_breath_tests_uniqueincident_`i'"
	tab _merge
	
	keep if _m==2
	drop _m
	
	drop per_idn-conv_round1match
	
/* Haven't adapted rounds 5 and 6 yet - wait until we've sorted out last name

	*** ROUND 5 MERGE
	mmerge first_name using "$int_dir/harris_county_breathtestyears.dta"
	
	keep if _merge==3
	drop _merge
	
	**Get rid of matches where filing date is before stop date or more than 2 days after stop date (matches after that appear low quality)
	drop if file_date-cdot<0 | file_date-cdot>2
		
	**Compare birth years. Drop obs where they are incompatible, but there is another match with a compatible birth year
	gen b_year_match = dui_b_year_likely==def_yob | dui_b_year_unlikely==def_yob
	bys incident_id: egen max_b_year_match = max(b_year_match)
	bys incident_id: egen min_b_year_match = min(b_year_match)
	
	drop if b_year_match==0 & max_b_year_match==1
	

	**Compare names using Jaro-Winkler distance
	jarowinkler hc_fullname dui_fullname
	
	
		* It looks to me like all the matches with jarowinkler >=.95 are good. Matches with jarowinkler in something like [.877,.95) and compatible birth years also look good.
		
		
	* Figure out how many words and initials match (excluding first word and first initial)
	gen dui_namewordcount = wordcount(dui_fullname)
	gen hc_namewordcount = wordcount(hc_fullname)
	
	split dui_fullname, p(" ")
	split hc_fullname, p(" ")
	
	forval i = 1/6	{
		
		gen dui_initial`i' = substr(dui_fullname`i',1,1)
		gen hc_initial`i' = substr(hc_fullname`i',1,1)
		
		* Don't want to include matching initials or JRs in word match count
		replace dui_fullname`i' = "" if length(dui_fullname`i')==1 | dui_fullname`i'=="JR"
		replace hc_fullname`i' = "" if length(hc_fullname`i')==1 | hc_fullname`i'=="JR"
		
	}
	
	
	gen n_word_match = 0
	gen n_initial_match = 0
	
	forval i = 2/6	{
		forval j = 2/6	{
		
			replace n_word_match = n_word_match + 1 if dui_fullname`i'==hc_fullname`j' & dui_fullname`i'!="" & hc_fullname`j'!=""
			replace n_initial_match = n_initial_match + 1 if dui_initial`i'==hc_initial`j' & dui_initial`i'!="" & hc_initial`j'!=""
			
		}
	}
	
		* People with at least one matching word besides first name word seem to be the same person as well
	//keep if jarowinkler>=.95 | (jarowinkler>=.877 & b_year_match==1) | n_word_match>0
	keep if jarowinkler>=.95 | (jarowinkler>=.9 & b_year_match==1) | n_word_match>0
	
	egen num_off_filed=sum(1), by(incident_id)
	
	drop *b_year_match jarowinkler dui_namewordcount-n_initial_match
	
	gen hc_round5match = 1
	
	save "$int_dir/dui_hc_round5", replace
	append using  "$int_dir/dui_hc_round4" "$int_dir/dui_hc_round3" "$int_dir/dui_hc_round2" "$int_dir/dui_hc_round1"
	
	
	
	mmerge incident_id using "$int_dir/texas_breath_tests_uniqueincident_hc"
	tab _merge
	
	keep if _m==2
	drop _m
	drop cdi-num_off_filed
	
	
	
	*** ROUND 6 MERGE
	//gen def_age = cage //
	
	//mmerge l_first3 def_age using "$int_dir/harris_county_breathtestyears.dta"
	//mmerge l_first3 using "$int_dir/harris_county_breathtestyears.dta"
	mmerge last_name using "$int_dir/harris_county_breathtestyears.dta"
	
	keep if _merge==3
	drop _merge
	
	**Get rid of matches where filing date is before stop date or more than 2 days after stop date (matches after that appear low quality)
	drop if file_date-cdot<0 | file_date-cdot>2
		
	**Compare birth years. Drop obs where they are incompatible, but there is another match with a compatible birth year
	gen b_year_match = dui_b_year_likely==def_yob | dui_b_year_unlikely==def_yob
	bys incident_id: egen max_b_year_match = max(b_year_match)
	bys incident_id: egen min_b_year_match = min(b_year_match)
	
	drop if b_year_match==0 & max_b_year_match==1
	

	**Compare names using Jaro-Winkler distance
	jarowinkler hc_fullname dui_fullname
	
	keep if jarowinkler >= .95 | (jarowinkler >= .85 & b_year_match==1)
	
	
	
	egen num_off_filed=sum(1), by(incident_id)
	
	drop *b_year_match jarowinkler
	
	gen hc_round6match = 1
	
	save "$int_dir/dui_hc_round6", replace
	append using "$int_dir/dui_hc_round1" "$int_dir/dui_hc_round2" "$int_dir/dui_hc_round3" "$int_dir/dui_hc_round4" "$int_dir/dui_hc_round5"
	
	mmerge incident_id using "$int_dir/texas_breath_tests_uniqueincident_hc"
	replace num_off_filed=0 if _merge~=3
	assert num_off_filed~=.
	drop _merge
	
	
	
	** Cleaning up variables for analysis
	foreach var in dwi poss drugmanufdeliv recklessdriving resistarrest weapon hitandrun evadearrest licenseinvalid conviction deferredadjud nolocontend conv_defer_nolo dwiconviction dwideferredadju dwinolocontend dwiconv_defer_nolo	{
	    
		replace `var' = 0 if `var'==.
		
	}

foreach var in sentence_incarcmonths sentence_probation sentence_fine	{
    
	replace `var' = 0 if `var'==.
	gen has_`var' = `var' > 0
	
}

gen nondwiconviction = dwi==0 & conviction==1


	** Addressing incidents with multiple associated charges
	duplicates tag incident_id, gen(n_charges)
	replace n_charges = n_charges + 1 if cas!=.
	gen any_charges = n_charges>0
	
	foreach var in dwi dwiconviction conviction deferredadjud nolocontend conv_defer_nolo nondwiconviction	{
	    
		bys incident_id: egen n_`var' = sum(`var')
		bys incident_id: gen any_`var' = n_`var' > 0
	
	}
	
	* Check what happens with sentences when there are convictions on multiple charges. The sentences are almost always listed as identical, which makes me think they are NOT additive.
	foreach var in sentence_incarcmonths sentence_probation sentence_fine	{
	    
		bys incident_id: egen max_`var' = max(`var')
		bys incident_id: egen max_has_`var' = max(has_`var')
		
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

logit black f_probability_black l_probability_black f_prob_ms l_prob_ms int1 int2 int3
predict p_black

sum p_black lowest_result


gen likely_black = p_black>.5 & f_likely_race!="hispanic" & l_likely_race!="hispanic"
gen likely_hispanic = f_likely_race=="hispanic" & l_likely_race=="hispanic"
gen likely_white = p_black<.2 & !likely_hispanic & f_likely_race=="white" & l_likely_race=="white"


save "$clean_dir/dui_hc_merged.dta", replace
*/



**** Preliminary data merge
	use "$int_dir/dui_conv_round4", clear
	append using /*"$int_dir/dui_conv_round3" */ "$int_dir/dui_conv_round2" "$int_dir/dui_conv_round1"
	
	mmerge incident_id using "$int_dir/texas_breath_tests_uniqueincident_`i'"
	//tab _merge
	
	
	* Narrow down to one obs per incident
	bys incident_id: gen temp = _n==1
	keep if temp
	drop temp
	
	gen any_conviction = num_conv!=.
	
	save "$int_dir/dui_conv_merged_`i'", replace
}	
	
	
/*
*** Preliminary analysis of match quality/first stage
	bys lowest_result: egen mean_any_conviction = mean(any_conviction)
	
	scatter mean_any_conviction lowest_result
	scatter mean_any_conviction lowest_result if inrange(lowest_result,.03,.13)
