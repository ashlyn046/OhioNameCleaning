************************************************************
/*
INPUT: first_names_race.csv, surnames_race.csv, OhioBreathalyzer.csv

INTERMEDIARY: first_name_race.dta, last_name_race.dta

OUTPUT: step1_names.dta
*/

*This file will read in and clean Ohio DUI data
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



*** Prep predicted race vars to merge onto breath tests ***
import delimited using "$raw_dir/race_prediction/first_names_race.csv", clear


drop v1

rename name first_name

foreach var in likely_race probability_american_indian probability_asian probability_black probability_hispanic probability_white probability_2race	{
	
	rename `var' f_`var'
	
}

//upper("x"") returns the uppercase version of "x": "X"
replace first_name = upper(first_name)

save "$int_dir/first_name_race", replace


import delimited using "$raw_dir/race_prediction/surnames_race.csv", clear

drop v1

rename name last_name

foreach var in likely_race probability_american_indian probability_asian probability_black probability_hispanic probability_white probability_2race	{
	
	rename `var' l_`var'
	
}

replace last_name = upper(last_name)

save "$int_dir/last_name_race", replace



*** Bring in raw data
import delimited using "$raw_dir/breath_test_data/OhioBreathalyzer.csv", clear


//renaming name variable and replacing all double spaces with single spaces until there are no multiple spaces
rename name fullname
replace fullname=subinstr(fullname,"  "," ",.)
replace fullname=subinstr(fullname,"  "," ",.)
replace fullname=subinstr(fullname,"  "," ",.)
replace fullname=subinstr(fullname,"  "," ",.)

replace fullname=subinstr(fullname,"]","",.)

//changing "/" to " " so our code runs
replace fullname=subinstr(fullname,"/"," ",.)

//changing double commas to single commas
replace fullname=subinstr(fullname,",,",",",.)

********
//LAST NAMES
gen last_name_raw = ""

//some observations were of the form "fname mname lname, fname". This didn't fit the code, but there were fewer than 100, so the following code readjusts these names manually 
replace fullname = "SHEFFIELD, THOMAS ALLEN" if fullname == "THOMAS ALLEN SHEFFIELD, THOMAS"
replace fullname = "PETERSON, MEGAN ELIZABETH " if fullname == "MEGAN ELIZABETH PETERSON, MEGAN"
replace fullname = "BEGGS, BRENDAN" if fullname == "BRENDAN BEGGS, BRENDAN"
replace fullname = "WEYLER, HUGH FREDRICK" if fullname == "HUGH FREDRICK WEYLER, HUGH"
replace fullname = "LEVAN, EVIN JOSEPH" if fullname == "EVIN JOSEPH LEVAN, EVIN"
replace fullname = "TRESNESS, JESSE LAWRENCE" if fullname == "JESSE LAWRENCE TRESNESS, JESSE"
replace fullname = "RESCHAUER, MATTHEW E" if fullname == "MATTHEW E RESCHAUER, MATTHEW"
replace fullname = "DOE, JOHN" if fullname == "JOHN DOE, JOHN"
replace fullname = "NIETO, EILEEN CONSTANCE" if fullname == "EILEEN CONSTANCE NIETO, EILEEN"
replace fullname = "RIHN, THOMAS ALBERT" if fullname == "THOMAS ALBERT RIHN, THOMAS"
replace fullname = "OPEKA, EVAN MICHAEL" if fullname == "EVAN MICHAEL OPEKA, EVAN"
replace fullname = "TODD JR, PHIL LEE" if fullname == "PHIL LEE TODD JR, PHIL"
replace fullname = "BECK, BRENT ROBERT" if fullname == "BRENT ROBERT BECK, BRENT"
replace fullname = "SCOTT JR, JIMMY LAWRENCE" if fullname == "JIMMY LAWRENCE SCOTT JR, JIMMY"
replace fullname = "PARKER, KAY A" if fullname == "KAY A PARKER, KAY"
replace fullname = "MILLER, RANDY LEE" if fullname == "RANDY LEE MILLER, RANDY"
replace fullname = "KEILCH, MARSHA ELLEN" if fullname == "MARSHA ELLEN KEILCH, MARSHA"
replace fullname = "LONG, BRITTANI RENEE" if fullname == "BRITTANI RENEE LONG, BRITTANI"
replace fullname = "CAUDILL, WILLIAM KEITH" if fullname == "WILLIAM KEITH CAUDILL, WILLIAM"
replace fullname = "KELLEY, JAY ALAN" if fullname == "JAY ALAN KELLEY, JAY"
replace fullname = "BRIDGES, DARYL CORNELL" if fullname == "DARYL CORNELL BRIDGES, DARYL"
replace fullname = "BORGEN, RICHARD ALLAN" if fullname == "RICHARD ALLAN BORGEN, RICHARD"
replace fullname = "MARTIN, ALEC TREVOR" if fullname == "ALEC TREVOR MARTIN, ALEC"
replace fullname = "CORNER, HANNAH MARIA" if fullname == "HANNAH MARIA CORNER, HANNAH"
replace fullname = "JOSE, JUAN" if fullname == "JUAN JOSE, JUAN"
replace fullname = "WAUGH, JOHN EDWARD" if fullname == "JOHN EDWARD WAUGH, JOHN"
replace fullname = "AVERY, JASON MATTHEW" if fullname == "JASON MATTHEW AVERY, JASON"
replace fullname = "CAPAN, MICHAEL A" if fullname == "MICHAEL A CAPAN, MICHAEL"
replace fullname = "SHAFFER, JOSHUA MICHAEL" if fullname == "JOSHUA MICHAEL SHAFFER, JOSHUA"
replace fullname = "GOBLE, PETE J" if fullname == "PETE J GOBLE, PETE"
replace fullname = "LONG, CHRISTOPHER S" if fullname == "CHRISTOPHER S LONG, CHRISTOPHER"
replace fullname = "JOHNSON, HOLLY JACOBS" if fullname == "HOLLY JACOBS JOHNSON, HOLLY"
replace fullname = "DUNN, BRADY" if fullname == "BRADY DUNN, BRADY"
replace fullname = "SCUDDER, DEVIN LEE" if fullname == "DEVIN LEE SCUDDER, DEVIN"
replace fullname = "TRENKAMP, JOSHUA H" if fullname == "JOSHUA H TRENKAMP, JOSHUA"
replace fullname = "MARTIN III, JAMES OLIVER" if fullname == "JAMES OLIVER MARTIN III, JAMES"
replace fullname = "ADRIAN, RICHARD EUGENE" if fullname == "RICHARD EUGENE ADRIAN, RICHARD"
replace fullname = "ANGERMEIER JR, JOHN C" if fullname == "JOHN C ANGERMEIER JR, JOHN"
replace fullname = "DUDLEY, AMY JO" if fullname == "AMY JO DUDLEY, AMY"
replace fullname = "BURKE, FELICIA T" if fullname == "FELICIA T BURKE, FELICIA"
replace fullname = "BOYD, LARRY WADE" if fullname == "LARRY WADE BOYD, LARRY"
replace fullname = "DOWNS, BEVERLY CASEY" if fullname == "BEVERLY CASEY DOWNS, BEVERLY"
replace fullname = "MCCARTY, BRENDAN CHARLES" if fullname == "BRENDAN CHARLES MCCARTY, BRENDAN"
replace fullname = "JONES SR, CARL A" if fullname == "CARL A JONES SR, CARL"
replace fullname = "SHACKELFORD, ERIC LYNN" if fullname == "ERIC LYNN SHACKELFORD, ERIC"
replace fullname = "BLANKENSHIP, STEVEN R" if fullname == "STEVEN R BLANKENSHIP, STEVEN"
replace fullname = "STARNER, LEITH MICHAEL" if fullname == "LEITH MICHAEL STARNER, LEITH"
replace fullname = "STIDFOLE, JACOB MICHAEL" if fullname == "JACOB MICHAEL STIDFOLE, JACOB"
replace fullname = "POLTA, CORY ARTHUR" if fullname == "CORY ARTHUR POLTA, CORY"
replace fullname = "KING, JOHN F" if fullname == "JOHN F KING, JOHN"
replace fullname = "STATEN, BRYAN LEE" if fullname == "BRYAN LEE STATEN, BRYAN"
replace fullname = "NICOLAS, JUAN" if fullname == "JUAN NICOLAS, JUAN"
replace fullname = "PAPPAS-CONANT, MALA G" if fullname == "MALA G PAPPAS-CONANT, MALA"
replace fullname = "ABDELAZIZ, ALAA M" if fullname == "ABDELAZIZ ALAA M, ALAA M"
replace fullname = "SWEETEN JR, WILLIAM THOMAS" if strpos(fullname,"WILLIAM THOMAS SWEETEN JR,")
replace fullname = "THIEL, NICOLE ELIZABETH" if fullname == "NICOLE ELIZABETH THIEL, "
replace fullname = "RICHTER, KENNETH JOSEPH" if fullname == "KENNETH JOSEPH RICHTER, "
replace fullname = "USREY, ALISON LOUISE" if fullname == "ALISON LOUISE USREY,"
replace fullname = "GREENBERGER, MATTHEW RYAN" if fullname == "MATTHEW RYAN GREENBERGER, "
replace fullname = "KEITH, HAYDEN" if fullname == "HAYDEN, , KEITH"
replace fullname = "SHAH, RONAK N" if fullname == "RONAK N SHAH,"
replace fullname = "ARNOLD JR, TYRONE" if fullname == "ARNOLD, TYRONE JR, TYRONE"
replace fullname = "REID JR, JAMES" if fullname == "REID,JR, JAMES"
replace fullname = "RICHARDSON III, JAMES" if fullname == "RICHARDSON (III), JAMES"
replace fullname = "CLARK JR, CLIFFTON CLITTON" if fullname == "CLARK,CLIFFTON,JR,, CLITTON,JR,"


//Split on the basis of comma
split fullname, parse(", ") gen(tempname)

//dropping observations that are just one name and a comma
drop if tempname2 == ""

//browse fullname if tempname2 == ""

//replacing last_name_raw with tempname1 because tempname1 will always contain last_name_raw
replace last_name_raw = tempname1

//get rid of missing last name observations
drop if last_name_raw=="," | last_name_raw=="" | last_name_raw==" "

//replace dashes with spaces
replace last_name_raw = subinstr(last_name_raw, "-"," ", .)

//get rid of periods
replace last_name_raw = subinstr(last_name_raw, ".","", .)

//get rid of leading of following spaces
replace last_name_raw=trim(last_name_raw)

//get rid of test observations
drop if regexm(last_name_raw,"TEST") & last_name_raw~="MALATESTA"  & last_name_raw~="MEZZATESTA" & last_name_raw~="NOTESTINE" & last_name_raw~="TESTERMAN" & last_name_raw~="TESTA" 

drop if regexm(last_name_raw, "PROFICIENCY") | regexm(last_name_raw, "XXXXX") | regexm(last_name_raw, "ZZZZ") | regexm(last_name_raw, "PRACTICE") | regexm(last_name_raw, "OOOO")

//split last name based on spaces
split last_name_raw


*Pull out suffixes
gen dui_suffix=""

forvalues i=2/6 {
	replace dui_suffix="JR" if last_name_raw`i'=="JR"
	replace last_name_raw`i'="" if last_name_raw`i'=="JR"
	replace dui_suffix="JR" if last_name_raw`i'=="JR."
	replace last_name_raw`i'="" if last_name_raw`i'=="JR."
	replace dui_suffix="SR" if last_name_raw`i'=="SR"
	replace last_name_raw`i'="" if last_name_raw`i'=="SR"
	replace dui_suffix="SR" if last_name_raw`i'=="SR."
	replace last_name_raw`i'="" if last_name_raw`i'=="SR."
	replace dui_suffix="JR" if last_name_raw`i'=="II"
	replace last_name_raw`i'="" if last_name_raw`i'=="II"
	replace dui_suffix="III" if last_name_raw`i'=="III"
	replace last_name_raw`i'="" if last_name_raw`i'=="III"
	replace dui_suffix="IV" if last_name_raw`i'=="IIII"
	replace last_name_raw`i'="" if last_name_raw`i'=="IIII"
	replace dui_suffix="IV" if last_name_raw`i'=="IV"
	replace last_name_raw`i'="" if last_name_raw`i'=="IV"
	replace dui_suffix="V" if last_name_raw`i'=="V"
	replace last_name_raw`i'="" if last_name_raw`i'=="V"
	replace dui_suffix="VI" if last_name_raw`i'=="VI"
	replace last_name_raw`i'="" if last_name_raw`i'=="VI"
	replace dui_suffix="SR" if last_name_raw`i'=="1"
	replace last_name_raw`i'="" if last_name_raw`i'=="1"
	replace dui_suffix="JR" if last_name_raw`i'=="11"
	replace last_name_raw`i'="" if last_name_raw`i'=="11"
	replace dui_suffix="III" if last_name_raw`i'=="111"
	replace last_name_raw`i'="" if last_name_raw`i'=="111"
}

l fullname last_name_raw last_name_raw* if last_name_raw1 == tempname2
count if last_name_raw1 == tempname2 //118

//there are names of the form fname mname last_name_raw, fname the following code will fix that

**Some records are there for administrative purposes and don't correspond to an individual
//added above
drop if substr(last_name_raw,1,1)=="." | substr(last_name_raw,1,1)==";" | substr(last_name_raw,1,1)=="0" | substr(last_name_raw,1,1)=="1" | substr(last_name_raw,1,1)=="2" | substr(last_name_raw,1,1)=="3" | substr(last_name_raw,1,1)=="4" | substr(last_name_raw,1,1)=="5" | substr(last_name_raw,1,1)=="6" | substr(last_name_raw,1,1)=="7" | substr(last_name_raw,1,1)=="8" | substr(last_name_raw,1,1)=="9" 


**Finish last name cleaning (combine compound name phrases like "de la" into a single word, but not cases that look like multiple distinct surnames)
*One-word last names		
gen last_name=last_name_raw1 if last_name_raw2==""
gen dui_alt_last_name=""

*Two-word last names
*Take care of compound names
replace last_name=last_name_raw1+last_name_raw2 if last_name_raw3=="" & (last_name_raw1=="DOS" | last_name_raw1=="D" | last_name_raw1=="DE" | last_name_raw1=="DEL" | last_name_raw1=="DELLA" | last_name_raw1=="DELA" | last_name_raw1=="DA" | last_name_raw1=="VAN" | last_name_raw1=="VON" | last_name_raw1=="VOM" | last_name_raw1=="SAINT" | last_name_raw1=="ST" | last_name_raw1=="SANTA" | last_name_raw1=="A" | last_name_raw1=="AL" | last_name_raw1=="BEN" | last_name_raw1=="DI" | last_name_raw1=="EL" | last_name_raw1=="LA" | last_name_raw1=="LE" | last_name_raw1=="MC" | last_name_raw1=="MAC" | last_name_raw1=="O" | last_name_raw1=="SAN")

//browse last_name_raw last_name last_name_raw1 last_name_raw2 dui_alt_last_name if last_name_raw3 == "" 

*Last names that seem to be 2 different surnames
replace dui_alt_last_name=last_name_raw1+last_name_raw2 if last_name_raw3=="" & last_name==""
replace last_name=last_name_raw2 if last_name_raw3=="" & last_name==""


*Three-word last names
*Take care of compound names
replace last_name=last_name_raw1+last_name_raw2+last_name_raw3 if last_name_raw4=="" & (last_name_raw1+last_name_raw2=="DELA" | last_name_raw1+last_name_raw2=="DELOS" | last_name_raw1+last_name_raw2=="DELAS"  | last_name_raw1+last_name_raw2=="VANDE" | last_name_raw1+last_name_raw2=="VANDER")

*Three-word names that seem a compound of one two word name and another 1 word name
replace dui_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3 if last_name=="" & last_name_raw4=="" & (last_name_raw1=="DOS" | last_name_raw1=="D" | last_name_raw1=="DE" | last_name_raw1=="DEL" | last_name_raw1=="DELLA" | last_name_raw1=="DELA" | last_name_raw1=="DA" | last_name_raw1=="VAN" | last_name_raw1=="VON" | last_name_raw1=="VOM" | last_name_raw1=="SAINT" | last_name_raw1=="ST" | last_name_raw1=="SANTA" | last_name_raw1=="A" | last_name_raw1=="AL" | last_name_raw1=="BEN" | last_name_raw1=="DI" | last_name_raw1=="EL" | last_name_raw1=="LA" | last_name_raw1=="LE" | last_name_raw1=="MC" | last_name_raw1=="MAC" | last_name_raw1=="O" | last_name_raw1=="SAN")

replace last_name=last_name_raw3 if last_name=="" & last_name_raw4=="" & (last_name_raw1=="DOS" | last_name_raw1=="D" | last_name_raw1=="DE" | last_name_raw1=="DEL" | last_name_raw1=="DELLA" | last_name_raw1=="DELA" | last_name_raw1=="DA" | last_name_raw1=="VAN" | last_name_raw1=="VON" | last_name_raw1=="VOM" | last_name_raw1=="SAINT" | last_name_raw1=="ST" | last_name_raw1=="SANTA" | last_name_raw1=="A" | last_name_raw1=="AL" | last_name_raw1=="BEN" | last_name_raw1=="DI" | last_name_raw1=="EL" | last_name_raw1=="LA" | last_name_raw1=="LE" | last_name_raw1=="MC" | last_name_raw1=="MAC" | last_name_raw1=="O" | last_name_raw1=="SAN")


*Three-word names that seem a compound of 1 one-word name and another two-word name
replace dui_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3 if last_name=="" & last_name_raw4=="" & (last_name_raw2=="DOS" | last_name_raw2=="D" | last_name_raw2=="DE" | last_name_raw2=="DEL" | last_name_raw2=="DELLA" | last_name_raw2=="DELA" | last_name_raw2=="DA" | last_name_raw2=="VAN" | last_name_raw2=="VON" | last_name_raw2=="VOM" | last_name_raw2=="SAINT" | last_name_raw2=="ST" | last_name_raw2=="SANTA" | last_name_raw2=="A" | last_name_raw2=="AL" | last_name_raw2=="BEN" | last_name_raw2=="DI" | last_name_raw2=="EL" | last_name_raw2=="LA" | last_name_raw2=="LE" | last_name_raw2=="MC" | last_name_raw2=="MAC" | last_name_raw2=="O" | last_name_raw2=="SAN")

replace last_name=last_name_raw2+last_name_raw3 if last_name=="" & last_name_raw4=="" & (last_name_raw2=="DOS" | last_name_raw2=="D" | last_name_raw2=="DE" | last_name_raw2=="DEL" | last_name_raw2=="DELLA" | last_name_raw2=="DELA" | last_name_raw2=="DA" | last_name_raw2=="VAN" | last_name_raw2=="VON" | last_name_raw2=="VOM" | last_name_raw2=="SAINT" | last_name_raw2=="ST" | last_name_raw2=="SANTA" | last_name_raw2=="A" | last_name_raw2=="AL" | last_name_raw2=="BEN" | last_name_raw2=="DI" | last_name_raw2=="EL" | last_name_raw2=="LA" | last_name_raw2=="LE" | last_name_raw2=="MC" | last_name_raw2=="MAC" | last_name_raw2=="O" | last_name_raw2=="SAN")


*The rest of the three-word names
replace dui_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3 if last_name=="" & last_name_raw4==""
replace last_name=last_name_raw3 if last_name=="" & last_name_raw4==""

//dropping observations of the form A,A or PT,PT
drop if last_name_raw1 == tempname2 & (strlen(last_name_raw1) <=2)

//browse fullname last_name_raw last_name dui_alt_last_name tempname* if last_name_raw3 ~= ""


*Four-word names
*Names that include de los or van de
replace dui_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3+last_name_raw4 if last_name=="" & last_name_raw5=="" & (last_name_raw1+last_name_raw2=="DELA" | last_name_raw1+last_name_raw2=="DELOS" | last_name_raw1+last_name_raw2=="DELAS"  | last_name_raw1+last_name_raw2=="VANDE" | last_name_raw1+last_name_raw2=="VANDER")

replace last_name=last_name_raw4 if last_name=="" & last_name_raw5=="" & (last_name_raw1+last_name_raw2=="DELA" | last_name_raw1+last_name_raw2=="DELOS" | last_name_raw1+last_name_raw2=="DELAS"  | last_name_raw1+last_name_raw2=="VANDE" | last_name_raw1+last_name_raw2=="VANDER")


replace dui_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3+last_name_raw4 if last_name=="" & last_name_raw5=="" & (last_name_raw2+last_name_raw3=="DELA" | last_name_raw2+last_name_raw3=="DELOS" | last_name_raw2+last_name_raw3=="DELAS"  | last_name_raw2+last_name_raw3=="VANDE" | last_name_raw2+last_name_raw3=="VANDER")

replace last_name=last_name_raw2+last_name_raw3+last_name_raw4 if last_name=="" & last_name_raw5=="" & (last_name_raw2+last_name_raw3=="DELA" | last_name_raw2+last_name_raw3=="DELOS" | last_name_raw2+last_name_raw3=="DELAS"  | last_name_raw2+last_name_raw3=="VANDE" | last_name_raw2+last_name_raw3=="VANDER")

*Names that end with a two-word name like de santis
replace dui_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3+last_name_raw4 if last_name=="" & last_name_raw5=="" & (last_name_raw3=="DOS" | last_name_raw3=="D" | last_name_raw3=="DE" | last_name_raw3=="DEL" | last_name_raw3=="DELLA" | last_name_raw3=="DELA" | last_name_raw3=="DA" | last_name_raw3=="VAN" | last_name_raw3=="VON" | last_name_raw3=="VOM" | last_name_raw3=="SAINT" | last_name_raw3=="ST" | last_name_raw3=="SANTA" | last_name_raw3=="A" | last_name_raw3=="AL" | last_name_raw3=="BEN" | last_name_raw3=="DI" | last_name_raw3=="EL" | last_name_raw3=="LA" | last_name_raw3=="LE" | last_name_raw3=="MC" | last_name_raw3=="MAC" | last_name_raw3=="O" | last_name_raw3=="SAN")

replace last_name=last_name_raw3+last_name_raw4 if last_name=="" & last_name_raw5=="" & (last_name_raw3=="DOS" | last_name_raw3=="D" | last_name_raw3=="DE" | last_name_raw3=="DEL" | last_name_raw3=="DELLA" | last_name_raw3=="DELA" | last_name_raw3=="DA" | last_name_raw3=="VAN" | last_name_raw3=="VON" | last_name_raw3=="VOM" | last_name_raw3=="SAINT" | last_name_raw3=="ST" | last_name_raw3=="SANTA" | last_name_raw3=="A" | last_name_raw3=="AL" | last_name_raw3=="BEN" | last_name_raw3=="DI" | last_name_raw3=="EL" | last_name_raw3=="LA" | last_name_raw3=="LE" | last_name_raw3=="MC" | last_name_raw3=="MAC" | last_name_raw3=="O" | last_name_raw3=="SAN")

*The rest of the 4 word names
replace dui_alt_last_name=last_name_raw1+last_name_raw2+last_name_raw3+last_name_raw4 if last_name=="" & last_name_raw5==""
replace last_name=last_name_raw4 if last_name=="" & last_name_raw5==""

//Fixing result variables and creating a dummy for refused
gen result_raw = result
gen refused = 0
replace refused = 1 if result == ".r"

replace result = "." if (result == ".r" | result == ".t" | result == ".d" | result == ".n" | result == ".o" | result == ".f" | result == ".i" | result == ".a" | result == ".s" | result == ".id" | result == ".af" | result == ".is" | result == ".p" | result == ".df" | result == ".us" | result == ".re" | result == ".x")


//browse fullname last_name_raw last_name dui_alt_last_name if last_name_raw4 ~= ""

//dropping John Does because it's not a real name
drop if fullname == "DOE, JOHN"


**Renaming variables to match those in the Texas code
rename timeofviolation TIME

**Make the form of the variables match those in the Texas code
replace TIME = subinstr(TIME, ":", "", .)
	**take ";" out of TIME

//changes all variable names to lower case
rename *, lower

//renames age variable to match age variable in other data
rename age cage
//destrings the "cage" variable and forces it to go to int despite data loss
destring cage result, force replace


//drops all appearances of same observation after first (does it match on everything?)
duplicates drop

//. sample 50, count 


*****
//FIRST and MIDDLE
gen fname = ""
gen mname = ""

split fullname, parse(", ") gen(tempname_)

//most names are of the form "last_name_raw, fname mname" so fname should be name2 from the above parsing

//tempname is parsed on ", " check contents of tempname*
count if tempname_1 ~= "" //144,240
count if tempname_2 ~= "" //144,215
count if tempname_3 ~= "" //63
count if tempname_4 ~= "" //1

//some observations only have tempname2 because they are of the form "    , last_name_raw". I chose to drop these observations since they did not have a full name
//observations with tempname2 NOT tempname1
count if tempname_2 ~= "" & tempname_1 == "" //31
drop if tempname_2 ~= "" & tempname_1 == ""

//some observations only have tempname1 and not tempname2 because they just have a last name. I chose to drop these
//observations with tempname1 NOT tempname2
count if tempname_1 ~= "" & tempname_2 == "" //56
drop if tempname_1 ~= "" & tempname_2 == ""


//count observations with both tempname1 and tempname2
count if tempname_1 ~= "" & tempname_2 ~= "" //144,184


//generating a form variable based on how many tempname*s each observation has
gen form = 1 if (tempname_1 ~= "" & tempname_2 ~= "" & tempname_3 == "" & tempname_4 == "")
count if form == 1 //144,122

replace form = 2 if (tempname_1 ~= "" & tempname_2 ~= "" & tempname_3 ~= "")
count if form == 2 //62

replace form = 3 if (tempname_1 ~= "" & tempname_2 ~= "" & tempname_3 ~= "" & tempname_4 ~= "" )
count if form == 3 // 1

//browse fullname fname mname dui_suffix last_name if tempname_3 == "" & strpos(tempname_2, " ")

replace fname = tempname_2 if (form == 1)
split fname, parse(" ") gen(fnametemp)

count if fnametemp1 ~= "" //144,072
count if fnametemp2 ~= "" //1,209
count if fnametemp3 ~= "" //69
count if fnametemp4 ~= "" //3

//fixing all first names (fix all without fnametemp3)
replace fname = fnametemp1 if (form == 1)


******
//case where fnametemp3 == ""
//either first and middle or first and suffix

//creating a variable that is 1 if the second word after the comma is a suffix
gen issuff1 = 0
replace issuff1 = 1 if (fnametemp2 == "JR" | fnametemp2 == "JR." |fnametemp2 == "SR" |fnametemp2 == "SR." |fnametemp2 == "II" | fnametemp2 == "III" | fnametemp2 == "IV" | fnametemp2 == "V" | fnametemp2 == "VI") & fnametemp3 == ""

//making it the suffix
replace dui_suffix = fnametemp2 if (issuff1 == 1 & form == 1 & fnametemp3 == "")

//making it the middle name
replace mname = fnametemp2 if (issuff1 == 0 & form == 1 & fnametemp3 == "")
******

*****
//case where fnametemp4 == ""
gen issuff2 = 0
replace issuff2 = 1 if (fnametemp3 == "JR" | fnametemp3 == "JR." |fnametemp3 == "SR" |fnametemp3 == "SR." |fnametemp3 == "II" | fnametemp3 == "III" | fnametemp3 == "IV" | fnametemp3 == "V" | fnametemp3 == "VI") & fnametemp4 == ""

//making it the suffix
replace dui_suffix = fnametemp3 if (issuff2 == 1 & form == 1 & fnametemp4 == "")

//making it the middle name if the last word is a suffix
replace mname = fnametemp2 if (issuff2 == 1 & form == 1 & fnametemp4 == "")

//fixing the middle name when there are two middle names
replace mname = fnametemp2 + " " + fnametemp3 if (issuff2 == 0 & form == 1 & fnametemp4 == "")
*****

*****
//case where fnametemp4 ~= ""
//they are all of the form last_name, fname mname mname mname
replace mname = fnametemp2 + " " + fnametemp3 + " " + fnametemp4 if ( form == 1 & fnametemp4 ~= "")

*****
replace dui_suffix = "JR" if dui_suffix == "JR."
replace dui_suffix = "SR" if dui_suffix == "SR."

//browse fullname fname mname last_name dui_suffix if form == 1

drop fnametemp*

replace dui_suffix = strtrim(dui_suffix)
replace mname = strtrim(mname)


//if the name has tempname1 through tempname3 but not tempname4, it is of the form is of one of the following forms: last_name_raw, fname/suffix/fname&suffix/fname&mname, mname/fname/fname&mname

l fullname tempname_* if form ==2

//This portion is for the case where tempname2 is a suffix.
//in this case, tempname3 is fname or fname and mname
*****
//in this case, set fname = tempname3

//generating a variable to determine if tempname2 is a suffix
gen issuff = 0
replace issuff = 1 if (tempname_2 == "JR" | tempname_2 == "JR." |tempname_2 == "SR" |tempname_2 == "SR." |tempname_2 == "II" | tempname_2 == "III" | tempname_2 == "IV" | tempname_2 == "V" | tempname_2 == "VI")

//setting fnmane = tempname3
replace fname = tempname_3 if (form == 2 & (issuff == 1))

//split fname based on " " so that we can separate middle names from first names for those of this form that have a middle name
split fname, parse(" ") gen(fnametemp)

//setting fname equal to the first part of fname
replace fname = fnametemp1 if (form == 2 & (issuff == 1))

//setting mname equal to the second part of fname
replace mname = fnametemp2 if (form == 2 & (issuff == 1))

drop fnametemp*
*****


//This portion is for the case where tempname2 is not a suffix
*****
//if tempname2 is not a suffix, then it is either fname, fname and a suffix, or fname and mname

//set fname = tempname2
replace fname = tempname_2 if (form == 2 & issuff == 0)
l fname if (form == 2 & issuff == 0)
***

//split fname based on " " so that we can separate middle names from first names for those of this form that have a middle name
split fname, parse(" ") gen(fnametemp)
l fname fnametemp* if (form == 2 & issuff == 0)

//setting fname equal to the first part of fname
replace fname = fnametemp1 if (form == 2 & (issuff == 0))

//setting mname equal to the second part of fname
//there is one case where it is JR instead of a mname, so I fixed that
replace mname = fnametemp2 if (form == 2 & (issuff == 0) & fnametemp2 ~= "JR")

//some have mname in tempname3
replace mname = tempname_3 if (form == 2 & issuff == 0 & mname =="")

drop fnametemp*

//checking
l fullname tempname_* if form ==2
l fullname fname mname last_name_raw dui_suffix if form == 2 // looks okay
*****

//if a name has tempname1 through tempname4, it is of the form "last_name_raw, suffix, fname, minitial"
replace fname = tempname_3 if (form == 3)

//fixing those with fnmane twice (mname was set as fname)
replace mname = "" if mname == fname 
//end FIRST and MIDDLE

//browse fname last_name mname dui_suffix fullname if tempname_3 ~= ""
*****

//Not all suffixes were before the first comma, this code catches those suffixes

//Case where suffix is isolated between commas
*Pull out suffixes
forvalues i=1/4 {
	if(dui_suffix == ""){
		replace dui_suffix="JR" if tempname_`i'=="JR"
		replace last_name_raw`i'="" if tempname_`i'=="JR"
		replace dui_suffix="JR" if tempname_`i'=="JR."
		replace last_name_raw`i'="" if tempname_`i'=="JR."
		replace dui_suffix="SR" if tempname_`i'=="SR"
		replace last_name_raw`i'="" if tempname_`i'=="SR"
		replace dui_suffix="SR" if tempname_`i'=="SR."
		replace last_name_raw`i'="" if tempname_`i'=="SR."
		replace dui_suffix="II" if tempname_`i'=="II"
		replace last_name_raw`i'="" if tempname_`i'=="II"
		replace dui_suffix="III" if tempname_`i'=="III"
		replace last_name_raw`i'="" if tempname_`i'=="III"
		replace dui_suffix="IV" if tempname_`i'=="IIII"
		replace last_name_raw`i'="" if tempname_`i'=="IIII"
		replace dui_suffix="IV" if tempname_`i'=="IV"
		replace last_name_raw`i'="" if tempname_`i'=="IV"
		replace dui_suffix="V" if tempname_`i'=="V"
		replace last_name_raw`i'="" if tempname_`i'=="V"
		replace dui_suffix="VI" if tempname_`i'=="VI"
		replace last_name_raw`i'="" if tempname_`i'=="VI"
	}
}

//browse fullname fname last_name dui_suffix if (dui_suffix =="" & (strpos(fullname, "JR") | strpos(fullname, "JR.") | strpos(fullname, "SR") | strpos(fullname, "SR.") | strpos(fullname, "II") | strpos(fullname, "III")))

//dropping all auxillary variables
drop issuff*
drop form
drop tempname_*
drop last_name_raw*
drop tempname*

//browse fullname fname mname last_name dui_suffix
//. sample 100, count

//dropping observations of the form JR, fname. we assume these are bad observations
drop if last_name == "JR"


drop if fname=="" // vast majority are clearly test/inspection runs


//eliminate parts of code that reference it test whether the breathalizer test worked
** Drop tests marked as not valid
//drop if vtest==0 // If we add this restriction back in, delete lines 138-140 and remove dui_all_tests_invalid from keep command near the bottom.

//fixing suffixes that are II to be JR
replace dui_suffix = "JR" if dui_suffix == "II"

**Some records are there for administrative purposes and don't correspond to an individual
//added above
drop if substr(fname,1,1)==";" |substr(fname,1,1)=="." | substr(fname,1,1)=="0" | substr(fname,1,1)=="1" | substr(fname,1,1)=="2" | substr(fname,1,1)=="3" | substr(fname,1,1)=="4" | substr(fname,1,1)=="5" | substr(fname,1,1)=="6" | substr(fname,1,1)=="7" | substr(fname,1,1)=="8" | substr(fname,1,1)=="9" 

drop if substr(last_name,1,1)==";" |substr(last_name,1,1)=="." | substr(last_name,1,1)=="0" | substr(last_name,1,1)=="1" | substr(last_name,1,1)=="2" | substr(last_name,1,1)=="3" | substr(last_name,1,1)=="4" | substr(last_name,1,1)=="5" | substr(last_name,1,1)=="6" | substr(last_name,1,1)=="7" | substr(last_name,1,1)=="8" | substr(last_name,1,1)=="9" 

//dropping "." from mname
replace mname = subinstr(mname,".","", .)

//fixing mnames that are suffixes
replace mname = "" if (mname == "SR" | mname == "JR" | mname == "II" | mname == "III" | mname == "IV" | mname == "V" | mname == "VI")


//dropping observations with last name "'"
drop if last_name == "'"

/*
The Ohio data does not have any incidents with more than 8 observations; 
90% of observations have two or less tests for a person from the same day, and 
the largest number of tests for a person from the same day is 8. 75% of observations
only have one test for a person from the same day. So this code doesn't do anything 
for the Ohio data.

**Some tests are from certification exercises - show up as names with many duplicates, mostly
**happening in Sept and Oct
bys last_name fname: gen n = _N
drop if n > 100 // This is arbitrary (and probably errs on the side of keeping some certification tests)
drop n
*/

** Prep test date vars
// matching texas code, making a time variable for date 
gen cdot = date(date, "YMD")

gen dui_test_year = year(cdot)
gen dui_test_month = month(cdot)
gen dui_test_day = day(cdot)

rename cdot dui_test_date
rename cage dui_age

gen dui_male = sex=="M"
//drop sex

** Create variables with earliest and latest possible birthdate
/*
If I'm tested on 2/10/2022 and my age is recorded as 30 years old, on one extreme, I turned 30 that very day, in which case bdate is 2/10/1992. On the other extreme, I turn 31 tomorrow, in which case my bdate is 2/11/1991.
*/
gen dui_bdate_max = mdy(dui_test_month,dui_test_day,dui_test_year-dui_age)
replace dui_bdate_max = mdy(dui_test_month,dui_test_day-1,dui_test_year-dui_age) if dui_bdate_max==. // takes care of leap years
format dui_bdate_max %td

gen dui_bdate_min = mdy(dui_test_month,dui_test_day+1,dui_test_year-dui_age-1)
replace dui_bdate_min = mdy(dui_test_month+1,1,dui_test_year-dui_age-1) if dui_bdate_min==. // takes care of the last day of the month for Jan-November
replace dui_bdate_min = mdy(1,1,dui_test_year-dui_age) if dui_bdate_min==. // takes care of 12/31 stops
format dui_bdate_min %td


//There is no analogue to vtest in the Ohio data; we use dui_incident_id to separate tests
**Sometimes one person will have multiple tests back-to-back
**(this is relatively rare--over 75% of tests are uniquely identified by last_name_raw fname and dui_test_date)
//creating a group identifier: an incident is same person on same date 
egen dui_incident_id = group(last_name fname dui_test_date)

//checking distribution
bys dui_incident_id: gen temp = _N
summ temp, d
drop temp

/*
bys dui_incident_id: egen max_vtest = max(vtest)
bys dui_incident_id: egen min_vtest = min(vtest)
gen dui_all_tests_invalid = max_vtest==min_vtest & vtest==0
*/

bys dui_incident_id: egen double dui_highest_result = max(result)
bys dui_incident_id: egen double dui_lowest_result = min(result)

count if dui_highest_result ~= dui_lowest_result //289


* Make sure all the BrAC readings have exactly 3 digits (numeric var formats have given us some trouble)
foreach var in dui_highest_result dui_lowest_result {
	
	replace `var' = round(`var',.001)
}




** Make rounded BrAC for binning
gen double rounded_lowest_result = floor(dui_lowest_result*100)/100


** Make vars for RD
//there is already an "index" variable in the data, its meaning is unclear so we drop it
drop index
gen index = dui_lowest_result - .08
gen above_limit = index>-.0001
gen interact = above_limit*index


***Clean names and construct name variables for merging with other files
rename fname first_name_raw
gen dui_middle_initial = substr(mname, 1, 1)

gen dui_fullname = first_name + " " + dui_middle_initial + " " + last_name

replace dui_fullname = stritrim(dui_fullname)

*Standardize case and remove punctuation + extra spaces
replace last_name=upper(last_name)
replace last_name=upper(last_name)
replace dui_middle_initial=upper(dui_middle_initial)
replace last_name=subinstr(last_name,"-"," ",.)
replace last_name=subinstr(last_name,".","",.)
replace last_name=subinstr(last_name,"  "," ",.)
replace last_name=subinstr(last_name,"'","",.)
replace last_name=subinstr(last_name,"?"," ",.)
replace last_name=subinstr(last_name,"*"," ",.)
replace last_name=trim(last_name)


replace last_name=subinstr(last_name,"-"," ",.)
replace last_name=subinstr(last_name,".","",.)
replace last_name=subinstr(last_name,"  "," ",.)
replace last_name=subinstr(last_name,"'","",.)
replace last_name = trim(last_name)


//making first names with "-" have " " instead
replace first_name_raw = subinstr(first_name_raw, "-", " ", .)

**Make first name vars (one with first word, one with all words)
split first_name_raw

gen first_name = first_name_raw1

//gen first_name_second_word = last_name_raw2
gen dui_alt_first_name=first_name_raw1+first_name_raw2+first_name_raw3 if first_name_raw2~=""

gen f_first3 = substr(first_name,1,3)

drop first_name_raw*

//generating a name_last_word variable that is the last word in the name. We do this to match the Texas code
gen name_last_word = last_name

//generating a variable that is the first three characters of alst name
gen l_first3 = substr(last_name,1,3)

*** Rename a few vars
rename time dui_test_time


foreach var in sex {
    
	rename `var' dui_`var'
	
}


***create race indicators
gen race_black=1 if race =="black" //11,134 
replace race_black=0 if race_black ==.

gen race_hispanic=1 if race =="hispanic" //6,389
replace race_hispanic=0 if race_hispanic ==.

gen race_white=1 if race =="white" //126,523 
replace race_white=0 if race_white ==.

gen race_none=1 if race =="none" //64 ... these four variables add to 144,110 (aka all observations are covered)
replace race_none=0 if race_none ==.
rename race recorded_race



***Merge on predicted race vars
merge m:1 first_name using "$int_dir/first_name_race"
drop if _m==2
drop _m

merge m:1 last_name using "$int_dir/last_name_race"
drop if _m==2
drop _m

count if recorded_race~="none" & (f_likely_race~=recorded_race | l_likely_race ~= recorded_race) //36,196

//br fullname recorded_race f_likely_race l_likely_race if recorded_race~="none" & (f_likely_race~=recorded_race | l_likely_race ~= recorded_race)

*prepping data for merge

**************
//CLEANING ARRESTINGAGENCY

*normalizing "POLICE DEPARTMENT"
replace arrestingagency = trim(arrestingagency)
replace arrestingagency = subinstr(arrestingagency, "'", "", .)
replace arrestingagency = subinstr(arrestingagency, "DEPT", "DEPARTMENT", .)
replace arrestingagency = subinstr(arrestingagency, " PD", " POLICE DEPARTMENT", .)
replace arrestingagency = subinstr(arrestingagency, "P.D.", "POLICE DEPARTMENT", .)
replace arrestingagency = subinstr(arrestingagency, "P.D", "POLICE DEPARTMENT", .)
replace arrestingagency = subinstr(arrestingagency, "POLIC", "POLICE DEPARTMENT", .) if !(strpos(arrestingagency, "POLICE"))
replace arrestingagency = subinstr(arrestingagency, "POLICE", "POLICE DEPARTMENT", .) if !(strpos(arrestingagency, "DEPARTMENT"))
replace arrestingagency = subinstr(arrestingagency, "DEPAR", "DEPARTMENT", .) if !(strpos(arrestingagency, "DEPART"))
replace arrestingagency = subinstr(arrestingagency, "DEPART", "DEPARTMENT", .) if !(strpos(arrestingagency, "DEPARTME"))
replace arrestingagency = subinstr(arrestingagency, "DEPARTME", "DEPARTMENT", .) if !(strpos(arrestingagency, "DEPARTMEN"))
replace arrestingagency = subinstr(arrestingagency, "DEPARTMEN", "DEPARTMENT", .) if !(strpos(arrestingagency, "DEPARTMENT"))
replace arrestingagency = subinstr(arrestingagency, "POLICE", "POLICE DEPARTMENT", .) if !(strpos(arrestingagency, "DEPARTMENT"))
replace arrestingagency = subinstr(arrestingagency, "DEPARTMENT DEPA", "DEPARTMENT", .)
replace arrestingagency = subinstr(arrestingagency, "DEPARTMENT DEP", "DEPARTMENT", .)
replace arrestingagency = subinstr(arrestingagency, " DEPR", " DEPARTMENT", .) if !(strpos(arrestingagency, "DEPART")) & !(strpos(arrestingagency, "DEPUTY"))& !(strpos(arrestingagency, "DEFIANCE"))& !(strpos(arrestingagency, "ROAD"))
replace arrestingagency = subinstr(arrestingagency, " DEPA", " DEPARTMENT", .) if !(strpos(arrestingagency, "DEPART")) & !(strpos(arrestingagency, "DEPUTY"))& !(strpos(arrestingagency, "DEFIANCE"))& !(strpos(arrestingagency, "ROAD"))
replace arrestingagency = subinstr(arrestingagency, " DEP.", " DEPARTMENT", .) if !(strpos(arrestingagency, "DEPART")) & !(strpos(arrestingagency, "DEPUTY"))& !(strpos(arrestingagency, "DEFIANCE"))& !(strpos(arrestingagency, "ROAD"))
replace arrestingagency = subinstr(arrestingagency, " DEP", " DEPARTMENT", .) if !(strpos(arrestingagency, "DEPART"))& !(strpos(arrestingagency, "DEPUTY"))& !(strpos(arrestingagency, "DEFIANCE"))& !(strpos(arrestingagency, "ROAD"))
replace arrestingagency = subinstr(arrestingagency, " DE", " DEPARTMENT", .) if !(strpos(arrestingagency, "DEPART"))& !(strpos(arrestingagency, "DEPUTY"))& !(strpos(arrestingagency, "DEFIANCE"))& !(strpos(arrestingagency, "ROAD"))
replace arrestingagency = subinstr(arrestingagency, "DEPARTMENTRTM", "DEPARTMENT", .)
replace arrestingagency = subinstr(arrestingagency, "DEPARTMENTRTMEN", "DEPARTMENT", .)
replace arrestingagency = subinstr(arrestingagency, "DEPARTMENTRT", "DEPARTMENT", .)
replace arrestingagency = subinstr(arrestingagency, "DEPARTMENTRTME", "DEPARTMENT", .)
replace arrestingagency = subinstr(arrestingagency, "DEPARTMENT.", "DEPARTMENT", .)
replace arrestingagency = subinstr(arrestingagency, "DEPARTMENTEN", "DEPARTMENT", .)
replace arrestingagency = subinstr(arrestingagency, "DEPARTMENTARTME", "DEPARTMENT", .)
replace arrestingagency = subinstr(arrestingagency, "DEPARTMENTE", "DEPARTMENT", .)
replace arrestingagency = subinstr(arrestingagency, "DEPARTMENTR", "DEPARTMENT", .)
replace arrestingagency = subinstr(arrestingagency, "DEPARTMENTEN", "DEPARTMENT", .)

*normalizing "SHERIFFS OFFICE"
replace arrestingagency = subinstr(arrestingagency, " SO", " SHERIFFS OFFICE", .)
replace arrestingagency = subinstr(arrestingagency, "S.O.", "SHERIFFS OFFICE", .)
replace arrestingagency = subinstr(arrestingagency, "S.O", "SHERIFFS OFFICE", .)
replace arrestingagency = subinstr(arrestingagency, "OFFI", "OFFICE", .) if !(strpos(arrestingagency, "OFFICE"))
replace arrestingagency = subinstr(arrestingagency, "OFFICEC", "OFFICE", .)
replace arrestingagency = subinstr(arrestingagency, "SHERRIF", "SHERIFFS OFFICE", .) if !(strpos(arrestingagency, "OFFICE"))
replace arrestingagency = subinstr(arrestingagency, "SHERRIF", "SHERIFFS OFFICE", .) if !(strpos(arrestingagency, "OFFICE"))
replace arrestingagency = subinstr(arrestingagency, "SHERIFFS DEPARTMENT", "SHERIFFS OFFICE", .)

*normalizing "HIGHWAY PATROL"
replace arrestingagency = subinstr(arrestingagency, "PATRO", "PATROL", .) if !(strpos(arrestingagency, "PATROL"))
replace arrestingagency = subinstr(arrestingagency, "PATR", "PATROL", .) if !(strpos(arrestingagency, "PATRO"))
***************

****************
//CLEANING TESTINGAGENCY
*normalizing "POLICE DEPARTMENT"
replace testingagency = trim(testingagency)
replace testingagency = subinstr(testingagency, "'", "", .)
replace testingagency = subinstr(testingagency, "DEPT", "DEPARTMENT", .)
replace testingagency = subinstr(testingagency, " PD", " POLICE DEPARTMENT", .)
replace testingagency = subinstr(testingagency, "P.D.", "POLICE DEPARTMENT", .)
replace testingagency = subinstr(testingagency, "P.D", "POLICE DEPARTMENT", .)
replace testingagency = subinstr(testingagency, "POLIC", "POLICE DEPARTMENT", .) if !(strpos(testingagency, "POLICE"))
replace testingagency = subinstr(testingagency, "POLICE", "POLICE DEPARTMENT", .) if !(strpos(testingagency, "DEPARTMENT"))
replace testingagency = subinstr(testingagency, "DEPAR", "DEPARTMENT", .) if !(strpos(testingagency, "DEPART"))
replace testingagency = subinstr(testingagency, "DEPART", "DEPARTMENT", .) if !(strpos(testingagency, "DEPARTME"))
replace testingagency = subinstr(testingagency, "DEPARTME", "DEPARTMENT", .) if !(strpos(testingagency, "DEPARTMEN"))
replace testingagency = subinstr(testingagency, "DEPARTMEN", "DEPARTMENT", .) if !(strpos(testingagency, "DEPARTMENT"))
replace testingagency = subinstr(testingagency, "POLICE", "POLICE DEPARTMENT", .) if !(strpos(testingagency, "DEPARTMENT"))
replace testingagency = subinstr(testingagency, "DEPARTMENT DEPA", "DEPARTMENT", .)
replace testingagency = subinstr(testingagency, "DEPARTMENT DEP", "DEPARTMENT", .)
replace testingagency = subinstr(testingagency, " DEPR", " DEPARTMENT", .) if !(strpos(testingagency, "DEPART")) & !(strpos(testingagency, "DEPUTY"))& !(strpos(testingagency, "DEFIANCE"))& !(strpos(testingagency, "ROAD"))
replace testingagency = subinstr(testingagency, " DEPA", " DEPARTMENT", .) if !(strpos(testingagency, "DEPART")) & !(strpos(testingagency, "DEPUTY"))& !(strpos(testingagency, "DEFIANCE"))& !(strpos(testingagency, "ROAD"))
replace testingagency = subinstr(testingagency, " DEP.", " DEPARTMENT", .) if !(strpos(testingagency, "DEPART")) & !(strpos(testingagency, "DEPUTY"))& !(strpos(testingagency, "DEFIANCE"))& !(strpos(testingagency, "ROAD"))
replace testingagency = subinstr(testingagency, " DEP", " DEPARTMENT", .) if !(strpos(testingagency, "DEPART"))& !(strpos(testingagency, "DEPUTY"))& !(strpos(testingagency, "DEFIANCE"))& !(strpos(testingagency, "ROAD"))
replace testingagency = subinstr(testingagency, " DE", " DEPARTMENT", .) if !(strpos(testingagency, "DEPART"))& !(strpos(testingagency, "DEPUTY"))& !(strpos(testingagency, "DEFIANCE"))& !(strpos(testingagency, "ROAD"))
replace testingagency = subinstr(testingagency, "DEPARTMENTRTM", "DEPARTMENT", .)
replace testingagency = subinstr(testingagency, "DEPARTMENTRTMEN", "DEPARTMENT", .)
replace testingagency = subinstr(testingagency, "DEPARTMENTRT", "DEPARTMENT", .)
replace testingagency = subinstr(testingagency, "DEPARTMENTRTME", "DEPARTMENT", .)
replace testingagency = subinstr(testingagency, "DEPARTMENT.", "DEPARTMENT", .)
replace testingagency = subinstr(testingagency, "DEPARTMENTEN", "DEPARTMENT", .)
replace testingagency = subinstr(testingagency, "DEPARTMENTARTME", "DEPARTMENT", .)
replace testingagency = subinstr(testingagency, "DEPARTMENTE", "DEPARTMENT", .)
replace testingagency = subinstr(testingagency, "DEPARTMENTR", "DEPARTMENT", .)
replace testingagency = subinstr(testingagency, "DEPARTMENTEN", "DEPARTMENT", .)

*normalizing "SHERIFFS OFFICE"
replace testingagency = subinstr(testingagency, " SO", " SHERIFFS OFFICE", .)
replace testingagency = subinstr(testingagency, "S.O.", "SHERIFFS OFFICE", .)
replace testingagency = subinstr(testingagency, "S.O", "SHERIFFS OFFICE", .)
replace testingagency = subinstr(testingagency, "OFFI", "OFFICE", .) if !(strpos(testingagency, "OFFICE"))
replace testingagency = subinstr(testingagency, "OFFICEC", "OFFICE", .)
replace testingagency = subinstr(testingagency, "SHERRIF", "SHERIFFS OFFICE", .) if !(strpos(testingagency, "OFFICE"))
replace testingagency = subinstr(testingagency, "SHERRIF", "SHERIFFS OFFICE", .) if !(strpos(testingagency, "OFFICE"))
replace testingagency = subinstr(testingagency, "SHERIFFS DEPARTMENT", "SHERIFFS OFFICE", .)

*normalizing "HIGHWAY PATROL"
replace testingagency = subinstr(testingagency, "PATRO", "PATROL", .) if !(strpos(testingagency, "PATROL"))
replace testingagency = subinstr(testingagency, "PATR", "PATROL", .) if !(strpos(testingagency, "PATRO"))

*************

*normalizing abbreviations such as MT and SAINT
replace arrestingagency = subinstr(arrestingagency, "MT.", "MT", .)
replace arrestingagency = subinstr(arrestingagency, "ST.", "SAINT", .)
replace arrestingagency = subinstr(arrestingagency, "ST ", "SAINT ", .) if !(strpos(arrestingagency, "WEST")) & !(strpos(arrestingagency, "EAST")) & !(strpos(arrestingagency, "FOREST"))& !(strpos(arrestingagency, "HURST")) & !(strpos(arrestingagency, "HERST")) & !(strpos(arrestingagency, "POST"))
*fixing the spacing on SAINT (some were combined with the next word)
replace arrestingagency = subinstr(arrestingagency, "SAINT", "SAINT ", .) if !(strpos(arrestingagency, "SAINT "))& !(strpos(arrestingagency, "DISAINT"))& !(strpos(arrestingagency, "CENTER"))

*normalizing "CLEVELAND" offices by renaming any office containing the word cleveland to "CLEVELAND" (all will be in the same county)
replace arrestingagency = "CLEVELAND" if strpos(arrestingagency, "CLEVELAND")

*normalizing patrol post names
replace arrestingagency = "HIRAM HIGHWAY PATROL" if arrestingagency == "HIRAM OSHP" | arrestingagency == "OSHP HIRAM" | arrestingagency == "OSP HIRAM" | arrestingagency == "HIRAM OSP" | arrestingagency == "HIRAM HIGHWAY PATROL POST" | arrestingagency == "HIRAM PATROL POST"
replace arrestingagency = "MILAN HIGHWAY PATROL" if arrestingagency == "MILAN OSHP" | arrestingagency == "OSHP MILAN" | arrestingagency == "OSP MILAN" | arrestingagency == "MILAN OSP" | arrestingagency == "MILAN HIGHWAY PATROL POST" | arrestingagency == "MILAN PATROL POST"
replace arrestingagency = "SWANTON HIGHWAY PATROL" if arrestingagency == "SWANTON OSHP" | arrestingagency == "OSHP SWANTON" | arrestingagency == "OSP SWANTON" | arrestingagency == "SWANTON OSP" | arrestingagency == "SWANTON HIGHWAY PATROL POST" | arrestingagency == "SWANTON PATROL POST"
replace arrestingagency = "ASHLAND HIGHWAY PATROL" if arrestingagency == "ASHLAND OSHP" | arrestingagency == "OSHP ASHLAND" | arrestingagency == "OSP ASHLAND" | arrestingagency == "ASHLAND OSP" | arrestingagency == "ASHLAND HIGHWAY PATROL POST" | arrestingagency == "ASHLAND PATROL POST"
replace arrestingagency = "ASHTABULA HIGHWAY PATROL" if arrestingagency == "ASHTABULA OSHP" | arrestingagency == "OSHP ASHTABULA" | arrestingagency == "OSP ASHTABULA" | arrestingagency == "ASHTABULA OSP" | arrestingagency == "ASHTABULA HIGHWAY PATROL POST" | arrestingagency == "ASHTABULA PATROL POST"
replace arrestingagency = "ATHENS HIGHWAY PATROL" if arrestingagency == "ATHENS OSHP" | arrestingagency == "OSHP ATHENS" | arrestingagency == "OSP ATHENS" | arrestingagency == "ATHENS OSP" | arrestingagency == "ATHENS HIGHWAY PATROL POST" | arrestingagency == "ATHENS PATROL POST"
replace arrestingagency = "BATAVIA HIGHWAY PATROL" if arrestingagency == "BATAVIA OSHP" | arrestingagency == "OSHP BATAVIA" | arrestingagency == "OSP BATAVIA" | arrestingagency == "BATAVIA OSP" | arrestingagency == "BATAVIA HIGHWAY PATROL POST" | arrestingagency == "BATAVIA PATROL POST"
replace arrestingagency = "BOWLING GREEN HIGHWAY PATROL" if arrestingagency == "BOWLING GREEN OSHP" | arrestingagency == "OSHP BOWLING GREEN" | arrestingagency == "OSP BOWLING GREEN" | arrestingagency == "BOWLING GREEN OSP" | arrestingagency == "BOWLING GREEN HIGHWAY PATROL POST" | arrestingagency == "BOWLING GREEN PATROL POST"
replace arrestingagency = "BUCYRUS HIGHWAY PATROL" if arrestingagency == "BUCYRUS OSHP" | arrestingagency == "OSHP BUCYRUS" | arrestingagency == "OSP BUCYRUS" | arrestingagency == "BUCYRUS OSP" | arrestingagency == "BUCYRUS HIGHWAY PATROL POST" | arrestingagency == "BUCYRUS PATROL POST"
replace arrestingagency = "CAMBRIDGE HIGHWAY PATROL" if arrestingagency == "CAMBRIDGE OSHP" | arrestingagency == "OSHP CAMBRIDGE" | arrestingagency == "OSP CAMBRIDGE" | arrestingagency == "CAMBRIDGE OSP" | arrestingagency == "CAMBRIDGE HIGHWAY PATROL POST" | arrestingagency == "CAMBRIDGE PATROL POST"
replace arrestingagency = "CANFIELD HIGHWAY PATROL" if arrestingagency == "CANFIELD OSHP" | arrestingagency == "OSHP CANFIELD" | arrestingagency == "OSP CANFIELD" | arrestingagency == "CANFIELD OSP" | arrestingagency == "CANFIELD HIGHWAY PATROL POST" | arrestingagency == "CANFIELD PATROL POST"
replace arrestingagency = "CANTON HIGHWAY PATROL" if arrestingagency == "CANTON OSHP" | arrestingagency == "OSHP CANTON" | arrestingagency == "OSP CANTON" | arrestingagency == "CANTON OSP" | arrestingagency == "CANTON HIGHWAY PATROL POST" | arrestingagency == "CANTON PATROL POST"
replace arrestingagency = "CHARDON HIGHWAY PATROL" if arrestingagency == "CHARDON OSHP" | arrestingagency == "OSHP CHARDON" | arrestingagency == "OSP CHARDON" | arrestingagency == "CHARDON OSP" | arrestingagency == "CHARDON HIGHWAY PATROL POST" | arrestingagency == "CHARDON PATROL POST"
replace arrestingagency = "CHILLICOTHE HIGHWAY PATROL" if arrestingagency == "CHILLICOTHE OSHP" | arrestingagency == "OSHP CHILLICOTHE" | arrestingagency == "OSP CHILLICOTHE" | arrestingagency == "CHILLICOTHE OSP" | arrestingagency == "CHILLICOTHE HIGHWAY PATROL POST" | arrestingagency == "CHILLICOTHE PATROL POST"
replace arrestingagency = "CINCINNATI HIGHWAY PATROL" if arrestingagency == "CINCINNATI OSP" | arrestingagency == "OSHP CINCINNATI METRO" | arrestingagency == "CINCINNATI METRO PATROL" | arrestingagency == "CINCINNATI METRO" | arrestingagency == "CINCINNATI OSHP" | arrestingagency == "OSHP CINCINNATI" | arrestingagency == "OSP CINCINNATI"| arrestingagency == "CINCINNATI HIGHWAY PATROL POST" | arrestingagency == "CINCINNATI PATROL POST" | arrestingagency == "CINCINNATI METRO OSHP" | arrestingagency == "CINCINNATI METRO PATROL POST" | arrestingagency == "CINCINATI METRO HIGHWAY PATROL" | arrestingagency == "CINCINNATI METRO POST"
replace arrestingagency = "CIRCLEVILLE HIGHWAY PATROL" if arrestingagency == "CIRCLEVILLE OSHP" | arrestingagency == "OSHP CIRCLEVILLE" | arrestingagency == "OSP CIRCLEVILLE" | arrestingagency == "CIRCLEVILLE OSP" | arrestingagency == "CIRCLEVILLE HIGHWAY PATROL POST" | arrestingagency == "CIRCLEVILLE PATROL POST"
replace arrestingagency = "COLUMBUS HIGHWAY PATROL" if arrestingagency == "COLUMBUS METRO HIGHWAY PATROL" | arrestingagency == "COLUMBUS DISTRICT HEADQUARTERS" | arrestingagency == "OSHP COLUMBUS MOTORCYCLE UNIT" | arrestingagency == "COLUMBUS OSHP" | arrestingagency == "OSHP COLUMBUS" | arrestingagency == "OSP COLUMBUS" | arrestingagency == "COLUMBUS OSP" | arrestingagency == "COLUMBUS HIGHWAY PATROL POST" | arrestingagency == "COLUMBUS PATROL POST"
replace arrestingagency = "DAYTON HIGHWAY PATROL" if arrestingagency == "DAYTON OSHP" | arrestingagency == "OSHP DAYTON" | arrestingagency == "OSP DAYTON" | arrestingagency == "DAYTON OSP" | arrestingagency == "DAYTON HIGHWAY PATROL POST" | arrestingagency == "DAYTON PATROL POST"
replace arrestingagency = "DEFIANCE HIGHWAY PATROL" if arrestingagency == "DEFIANCE OSHP" | arrestingagency == "OSHP DEFIANCE" | arrestingagency == "OSP DEFIANCE" | arrestingagency == "DEFIANCE OSP" | arrestingagency == "DEFIANCE HIGHWAY PATROL POST" | arrestingagency == "DEFIANCE PATROL POST"
replace arrestingagency = "DELAWARE HIGHWAY PATROL" if arrestingagency == "DELAWARE OSHP" | arrestingagency == "OSHP DELAWARE" | arrestingagency == "OSP DELAWARE" | arrestingagency == "DELAWARE OSP" | arrestingagency == "DELAWARE HIGHWAY PATROL POST" | arrestingagency == "DELAWARE PATROL POST"
replace arrestingagency = "ELYRIA HIGHWAY PATROL" if arrestingagency == "ELYRIA OSHP" | arrestingagency == "OSHP ELYRIA" | arrestingagency == "OSP ELYRIA" | arrestingagency == "ELYRIA OSP" | arrestingagency == "ELYRIA HIGHWAY PATROL POST" | arrestingagency == "ELYRIA PATROL POST"
replace arrestingagency = "FINDLAY HIGHWAY PATROL" if arrestingagency == "FINDLAY OSHP" | arrestingagency == "OSHP FINDLAY" | arrestingagency == "OSP FINDLAY" | arrestingagency == "FINDLAY OSP" | arrestingagency == "FINDLAY HIGHWAY PATROL POST" | arrestingagency == "FINDLAY PATROL POST"
replace arrestingagency = "FREMONT HIGHWAY PATROL" if arrestingagency == "FREMONT OSHP" | arrestingagency == "OSHP FREMONT" | arrestingagency == "OSP FREMONT" | arrestingagency == "FREMONT OSP" | arrestingagency == "FREMONT HIGHWAY PATROL POST" | arrestingagency == "FREMONT PATROL POST"
replace arrestingagency = "GALLIPOLIS HIGHWAY PATROL" if arrestingagency == "GALLIPOLIS OSHP" | arrestingagency == "OSHP GALLIPOLIS" | arrestingagency == "OSP GALLIPOLIS" | arrestingagency == "GALLIPOLIS OSP" | arrestingagency == "GALLIPOLIS HIGHWAY PATROL POST" | arrestingagency == "GALLIPOLIS PATROL POST"
replace arrestingagency = "GEORGETOWN HIGHWAY PATROL" if arrestingagency == "GEORGETOWN OSHP" | arrestingagency == "OSHP GEORGETOWN" | arrestingagency == "OSP GEORGETOWN" | arrestingagency == "GEORGETOWN OSP" | arrestingagency == "GEORGETOWN HIGHWAY PATROL POST" | arrestingagency == "GEORGETOWN PATROL POST"
replace arrestingagency = "GRANVILE HIGHWAY PATROL" if arrestingagency == "GRANVILE OSHP" | arrestingagency == "OSHP GRANVILE" | arrestingagency == "OSP GRANVILE" | arrestingagency == "GRANVILE OSP" | arrestingagency == "GRANVILE HIGHWAY PATROL POST" | arrestingagency == "GRANVILE PATROL POST"
replace arrestingagency = "HAMILTON HIGHWAY PATROL" if arrestingagency == "HAMILTON OSHP" | arrestingagency == "OSHP HAMILTON" | arrestingagency == "OSP HAMILTON" | arrestingagency == "HAMILTON OSP" | arrestingagency == "HAMILTON HIGHWAY PATROL POST" | arrestingagency == "HAMILTON PATROL POST" | arrestingagency == "OSHP HAMILTON POST"
replace arrestingagency = "IRONTON HIGHWAY PATROL" if arrestingagency == "IRONTON OSHP" | arrestingagency == "OSHP IRONTON" | arrestingagency == "OSP IRONTON" | arrestingagency == "IRONTON OSP" | arrestingagency == "IRONTON HIGHWAY PATROL POST" | arrestingagency == "IRONTON PATROL POST"
replace arrestingagency = "JACKSON HIGHWAY PATROL" if arrestingagency == "JACKSON OSHP" | arrestingagency == "OSHP JACKSON" | arrestingagency == "OSP JACKSON" | arrestingagency == "JACKSON OSP" | arrestingagency == "JACKSON HIGHWAY PATROL POST" | arrestingagency == "JACKSON PATROL POST"
replace arrestingagency = "LANCASTER HIGHWAY PATROL" if arrestingagency == "LANCASTER OSHP" | arrestingagency == "OSHP LANCASTER" | arrestingagency == "OSP LANCASTER" | arrestingagency == "LANCASTER OSP" | arrestingagency == "LANCASTER HIGHWAY PATROL POST" | arrestingagency == "LANCASTER PATROL POST"
replace arrestingagency = "LEBANON HIGHWAY PATROL" if arrestingagency == "LEBANON OSHP" | arrestingagency == "OSHP LEBANON" | arrestingagency == "OSP LEBANON" | arrestingagency == "LEBANON OSP" | arrestingagency == "LEBANON HIGHWAY PATROL POST" | arrestingagency == "LEBANON PATROL POST"
replace arrestingagency = "LIMA HIGHWAY PATROL" if arrestingagency == "LIMA OSHP" | arrestingagency == "OSHP LIMA" | arrestingagency == "OSP LIMA" | arrestingagency == "LIMA OSP" | arrestingagency == "LIMA HIGHWAY PATROL POST" | arrestingagency == "LIMA PATROL POST"
replace arrestingagency = "LISBON HIGHWAY PATROL" if arrestingagency == "LISBON OSHP" | arrestingagency == "OSHP LISBON" | arrestingagency == "OSP LISBON" | arrestingagency == "LISBON OSP" | arrestingagency == "LISBON HIGHWAY PATROL POST" | arrestingagency == "LISBON PATROL POST"
replace arrestingagency = "MANSFIELD HIGHWAY PATROL" if arrestingagency == "MANSFIELD OSHP" | arrestingagency == "OSHP MANSFIELD" | arrestingagency == "OSP MANSFIELD" | arrestingagency == "MANSFIELD OSP" | arrestingagency == "MANSFIELD HIGHWAY PATROL POST" | arrestingagency == "MANSFIELD PATROL POST"
replace arrestingagency = "MARIETTA HIGHWAY PATROL" if arrestingagency == "MARIETTA OSHP" | arrestingagency == "OSHP MARIETTA" | arrestingagency == "OSP MARIETTA" | arrestingagency == "MARIETTA OSP" | arrestingagency == "MARIETTA HIGHWAY PATROL POST" | arrestingagency == "MARIETTA PATROL POST"
replace arrestingagency = "MARION HIGHWAY PATROL" if arrestingagency == "OSHP MARION" | arrestingagency == "MARION PATROL POST" | arrestingagency == "OSP MARION" | arrestingagency == "MARION OSP" | arrestingagency == "MARION OSHP"| arrestingagency == "MARION HIGHWAY PATROL POST" 
replace arrestingagency = "MARYSVILLE HIGHWAY PATROL" if arrestingagency == "MARYSVILLE OSHP" | arrestingagency == "OSHP MARYSVILLE" | arrestingagency == "OSP MARYSVILLE" | arrestingagency == "MARYSVILLE OSP" | arrestingagency == "MARYSVILLE HIGHWAY PATROL POST" | arrestingagency == "MARYSVILLE PATROL POST"
replace arrestingagency = "MEDINA HIGHWAY PATROL" if arrestingagency == "MEDINA OSHP" | arrestingagency == "OSHP MEDINA" | arrestingagency == "OSP MEDINA" | arrestingagency == "MEDINA OSP" | arrestingagency == "MEDINA HIGHWAY PATROL POST" | arrestingagency == "MEDINA PATROL POST"
replace arrestingagency = "OSHP MY GILEAD" if arrestingagency == "MT GILEAD OSHP" | arrestingagency == "MT GILEAD HIGHWAY PATROL" | arrestingagency == "OSP MT GILEAD" | arrestingagency == "MT GILEAD OSP" | arrestingagency == "MT GILEAD HIGHWAY PATROL POST" | arrestingagency == "MT GILEAD PATROL POST"
replace arrestingagency = "NEW PHILADELPHIA HIGHWAY PATROL" if arrestingagency == "NEW PHILADELPHIA OSHP" | arrestingagency == "OSHP NEW PHILADELPHIA" | arrestingagency == "OSP NEW PHILADELPHIA" | arrestingagency == "NEW PHILADELPHIA OSP" | arrestingagency == "NEW PHILADELPHIA HIGHWAY PATROL POST" |arrestingagency == "NEW PHILADELPHIA PATROL POST"
replace arrestingagency = "NORWALK HIGHWAY PATROL" if arrestingagency == "NORWALK OSHP" | arrestingagency == "OSHP NORWALK" | arrestingagency == "OSP NORWALK" | arrestingagency == "NORWALK OSP" | arrestingagency == "NORWALK HIGHWAY PATROL POST" | arrestingagency == "NORWALK PATROL POST"
replace arrestingagency = "PIQUA HIGHWAY PATROL" if arrestingagency == "PIQUA OSHP" | arrestingagency == "OSHP PIQUA" | arrestingagency == "OSP PIQUA" | arrestingagency == "PIQUA OSP" | arrestingagency == "PIQUA HIGHWAY PATROL POST"| arrestingagency == "PIQUA PATROL POST"
replace arrestingagency = "PORTSMOUTH HIGHWAY PATROL" if arrestingagency == "PORTSMOUTH OSHP" | arrestingagency == "OSHP PORTSMOUTH" | arrestingagency == "OSP PORTSMOUTH" | arrestingagency == "PORTSMOUTH OSP" | arrestingagency == "PORTSMOUTH HIGHWAY PATROL POST" | arrestingagency == "PORTSMOUTH PATROL POST"
replace arrestingagency = "RAVENNA HIGHWAY PATROL" if arrestingagency == "RAVENNA OSHP" | arrestingagency == "OSHP RAVENNA" | arrestingagency == "OSP RAVENNA" | arrestingagency == "RAVENNA OSP" | arrestingagency == "RAVENNA HIGHWAY PATROL POST" | arrestingagency == "RAVENNA PATROL POST"
replace arrestingagency = "SAINT CLAIRSVILLE HIGHWAY PATROL" if arrestingagency == "SAINT CLAIRSVILLE OSHP" | arrestingagency == "OSHP SAINT CLAIRSVILLE" | arrestingagency == "OSP SAINT CLAIRSVILLE" | arrestingagency == "SAINT CLAIRSVILLE OSP" | arrestingagency == "SAINT CLAIRSVILLE HIGHWAY PATROL POST"| arrestingagency == "SAINT CLAIRSVILLE PATROL POST"
replace arrestingagency = "SANDUSKY HIGHWAY PATROL" if arrestingagency == "SANDUSKY OSHP" | arrestingagency == "OSHP SANDUSKY" | arrestingagency == "OSP SANDUSKY" | arrestingagency == "SANDUSKY OSP" | arrestingagency == "SANDUSKY HIGHWAY PATROL POST" | arrestingagency == "SANDUSKY PATROL POST"
replace arrestingagency = "SPRINGFIELD HIGHWAY PATROL" if arrestingagency == "SPRINGFIELD OSHP" | arrestingagency == "OSHP SPRINGFIELD" | arrestingagency == "OSP SPRINGFIELD" | arrestingagency == "SPRINGFIELD OSP" | arrestingagency == "SPRINGFIELD HIGHWAY PATROL POST" | arrestingagency == "SPRINGFIELD PATROL POST"
replace arrestingagency = "STEUBENVILLE HIGHWAY PATROL" if arrestingagency == "STEUBENVILLE OSHP" | arrestingagency == "OSHP STEUBENVILLE" | arrestingagency == "OSP STEUBENVILLE" | arrestingagency == "STEUBENVILLE OSP" | arrestingagency == "STEUBENVILLE HIGHWAY PATROL POST" | arrestingagency == "STEUBENVILLE PATROL POST"
replace arrestingagency = "TOLEDO HIGHWAY PATROL" if arrestingagency == "TOLEDO OSHP" | arrestingagency == "OSHP TOLEDO" | arrestingagency == "OSP TOLEDO" | arrestingagency == "TOLEDO OSP" | arrestingagency == "TOLEDO HIGHWAY PATROL POST" | arrestingagency == "TOLEDO PATROL POST"
replace arrestingagency = "VANWERT HIGHWAY PATROL" if arrestingagency == "VANWERT OSHP" | arrestingagency == "OSHP VANWERT" | arrestingagency == "OSP VANWERT" | arrestingagency == "VANWERT OSP" | arrestingagency == "VANWERT HIGHWAY PATROL POST" | arrestingagency == "VANWERT PATROL POST"
replace arrestingagency = "WAPAKONETA HIGHWAY PATROL" if arrestingagency == "WAPAKONETA OSHP" | arrestingagency == "OSHP WAPAKONETA" | arrestingagency == "OSP WAPAKONETA" | arrestingagency == "WAPAKONETA OSP" | arrestingagency == "WAPAKONETA HIGHWAY PATROL POST" | arrestingagency == "WAPAKONETA PATROL POST"
replace arrestingagency = "WARREN HIGHWAY PATROL" if arrestingagency == "WARREN OSHP" | arrestingagency == "OSHP WARREN" | arrestingagency == "OSP WARREN" | arrestingagency == "WARREN OSP" | arrestingagency == "WARREN HIGHWAY PATROL POST" | arrestingagency == "WARREN PATROL POST"
replace arrestingagency = "WEST JEFFERSON HIGHWAY PATROL" if arrestingagency == "WEST JEFFERSON OSHP" | arrestingagency == "OSHP WEST JEFFERSON" | arrestingagency == "OSP WEST JEFFERSON" | arrestingagency == "WEST JEFFERSON OSP" | arrestingagency == "WEST JEFFERSON HIGHWAY PATROL POST" | arrestingagency == "WEST JEFFERSON PATROL POST"
replace arrestingagency = "WILMINGTON HIGHWAY PATROL" if arrestingagency == "WILMINGTON OSHP" | arrestingagency == "OSHP WILMINGTON" | arrestingagency == "OSP WILMINGTON" | arrestingagency == "WILMINGTON OSP" | arrestingagency == "WILMINGTON HIGHWAY PATROL POST" | arrestingagency == "WILMINGTON PATROL POST"
replace arrestingagency = "WOOSTER HIGHWAY PATROL" if arrestingagency == "WOOSTER OSHP" | arrestingagency == "OSHP WOOSTER" | arrestingagency == "OSP WOOSTER" | arrestingagency == "WOOSTER OSP" | arrestingagency == "WOOSTER HIGHWAY PATROL POST" | arrestingagency == "WOOSTER PATROL POST"
replace arrestingagency = "XENIA HIGHWAY PATROL" if arrestingagency == "XENIA OSHP" | arrestingagency == "OSHP XENIA" | arrestingagency == "OSP XENIA" | arrestingagency == "XENIA OSP" | arrestingagency == "XENIA HIGHWAY PATROL POST" | arrestingagency == "XENIA PATROL POST"
replace arrestingagency = "ZANESVILLE HIGHWAY PATROL" if arrestingagency == "ZANESVILLE OSHP" | arrestingagency == "OSHP ZANESVILLE" | arrestingagency == "OSP ZANESVILLE" | arrestingagency == "ZANESVILLE OSP" | arrestingagency == "ZANESVILLE HIGHWAY PATROL POST" | arrestingagency == "ZANESVILLE PATROL POST"

*normalizing office names of the form *CSHERIFF... by splitting the *C from the SHERIFF
*(there were occurences of the form ACSHERIFFS, BCSHERIFFS, LC*, UC*, MC*, HC*, RC*, PC*, WC*, CC*, EC*, TC*)
split arrestingagency, parse("C") gen(temp)
replace arrestingagency = temp1 + "C" + " " + temp2 + "C" + temp3 if substr(temp2, 1, 4) == "SHER"
replace arrestingagency = trim(arrestingagency)
drop temp*
//I don't think there is a way to split based just on the first occurrence of a delimiter

*normalizing "UNIVERSITY POLICE"
replace arrestingagency = subinstr(arrestingagency, "UNIV.", "UNIVERSITY", .) if !(strpos(arrestingagency, "UNIVERSITY"))
replace arrestingagency = subinstr(arrestingagency, "UNIV", "UNIVERSITY", .) if !(strpos(arrestingagency, "UNIVERSITY"))

*cleaning a common abbreviationtion in city
replace city = subinstr(city, "HTS", "HEIGHTS", .)

save "$int_dir/step1_names", replace