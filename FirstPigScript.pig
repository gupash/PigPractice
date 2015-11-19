
--1) Generate TOTAL COUNT based on gender.
lines = load '/Users/ashish/Documents/DataExample/users.tsv' USING PigStorage('\t') AS (SWID : chararray, birthDt: chararray, gender: chararray);
linewtheader = FILTER lines BY SWID != 'SWID';
lineswithgender = FILTER linewtheader BY gender is not null;
gendergroup = GROUP lineswithgender BY gender;
gendercount = FOREACH gendergroup GENERATE group,COUNT(lineswithgender);
dump gendercount;

--2) Filter Out all users having birth year = 85 OR 90.
/*The method used here by splitting the date to just obtain the year and then type casting it, is just another way to do it(experimenting),
  otherwise best way is like done in the 5th example by converting to date and then extracting year*/
lines = load '/Users/ashish/Documents/DataExample/users.tsv' USING PigStorage('\t') AS (SWID : chararray, birthDt: chararray, gender: chararray);
linewtheader = FILTER lines BY SWID != 'SWID';
lineswithbdt = FILTER linewtheader BY birthDt is not null;
lineswithbdtexpanded = FOREACH lineswithbdt GENERATE $0,(int) STRSPLIT($1, '-').$2,$2;
filteredcustomerwithdt = FILTER lineswithbdtexpanded BY $1 == 85 or $1 == 90;
dump filteredcustomerwithdt;

--3) Find out the SWID for users with minimum birthdate and maximum birth date.
/*Done this One using the ToDate funciton, hence didn't need any UDF. But still wan't
  to discuss more on how to extract the swid for the smallest date in the same loop*/
lines = load '/Users/ashish/Documents/DataExample/users.tsv' USING PigStorage('\t') AS (SWID : chararray, birthDt: chararray, gender: chararray);
linewtheader = FILTER lines BY SWID != 'SWID';
lineswithbdt = FILTER linewtheader BY birthDt is not null;
lineswithbdtexpanded = FOREACH lineswithbdt GENERATE $0,ToDate($1, 'dd-MMM-yy') AS BirthDt,$2;
usergroupondt = GROUP lineswithbdtexpanded ALL;
requiredDates = FOREACH usergroupondt GENERATE MIN(lineswithbdtexpanded.BirthDt) AS minDateVal, MAX(lineswithbdtexpanded.BirthDt) AS maxDateVal;
smallestUser = FILTER lineswithbdtexpanded BY BirthDt == requiredDates.minDateVal;
smallestUserSwid = FOREACH smallestUser GENERATE SWID;
oldestUser = FILTER lineswithbdtexpanded BY BirthDt == requiredDates.maxDateVal;
oldestUserSwid = FOREACH oldestUser GENERATE SWID;
dump smallestUserSwid
dump oldestUserSwid;
--dump UNION smallestUserSwid, oldestUserSwid;


--4) Count the distinct number of SWID.
lines = load '/Users/ashish/Documents/DataExample/users.tsv' USING PigStorage('\t') AS (SWID : chararray, birthDt: chararray, gender: chararray);
linewtheader = FILTER lines BY SWID != 'SWID';
lineswithSwid = FILTER linewtheader BY SWID is not null;
dtlnWithSwid = DISTINCT lineswithSwid;
SwidOnly = FOREACH dtlnWithSwid GENERATE SWID;
SwidOnlyGroup = GROUP SwidOnly ALL;
SwidUniqueCount = FOREACH SwidOnlyGroup GENERATE COUNT(SwidOnly);
dump SwidUniqueCount;

--5) Filter Out the users having birth month = Jun.
lines = load '/Users/ashish/Documents/DataExample/users.tsv' USING PigStorage('\t') AS (SWID : chararray, birthDt: chararray, gender: chararray);
linewtheader = FILTER lines BY SWID != 'SWID';
lineswithbdt = FILTER linewtheader BY birthDt is not null;
lineswithbdtexpanded = FOREACH lineswithbdt GENERATE $0,ToDate($1, 'dd-MMM-yy') AS BirthDt,$2;
userDataOnlyMonth = FOREACH lineswithbdtexpanded GENERATE $0,GetMonth($1) AS BirthMonth,$2;
requiredUsers = FILTER userDataOnlyMonth BY BirthMonth == 6;
requiredUsersSwid = FOREACH requiredUsers GENERATE SWID AS finalSwid;
joinedUserData = JOIN lineswithbdt by SWID, requiredUsersSwid BY finalSwid;
finalRequiredOutput = FOREACH joinedUserData GENERATE $0,$1,$2;
dump finalRequiredOutput;

--6) Perform certain String functions on SWID -- Experimenting with INDEXOF, REPLACE, STRSPLIT, TRIM, UPPER and SUBSTRING
data = LOAD '/Users/ashish/Documents/DataExample/demo_data.txt' AS (line: chararray);
indexoutput = FOREACH data GENERATE INDEXOF(*, 'are', 0);
replaceoutput = FOREACH data GENERATE REPLACE(line, 'a.e', 'I');
strsplitoutput = FOREACH data GENERATE STRSPLIT(line,'\\W+');
trimoutput = FOREACH data GENERATE TRIM(line);
upperoutput = FOREACH data GENERATE UPPER(line);
substringOutput = FOREACH data GENERATE SUBSTRING(TRIM(line), 2, 5);


--7) Attempted Self -- Word Count Program in Pig
lines = load '/Users/ashish/Documents/DataExample/shakespeare/comedies' AS (line : chararray);
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) AS word;
filteredwords = FILTER words BY word is not null;
wordgroup = GROUP filteredwords by word;
wordcount = FOREACH wordgroup GENERATE group, COUNT(words);
dump wordcount;



