/*
SalesForce POC
- Number of Users (count of unique case owners)
- Number of Tickets (count of Case ID’s)
- Number of Tickets in various categories (count on the basis of each subject)
- Number of interactions via various channels – Chat, Email, Call, Scheduled Callback, OKM. (interactions per customer via various channels/subjects)
  Customer
- Number of tickets per customer and interaction and usage statistics
- Classify customer as Active or Passive

--------------- */

-- Common Loading and Parsing of Initial Data
file = LOAD '/Users/ashish/IdeaProjects/PigPractice/Resources/report_sample.txt' AS (line: chararray);
Fline = FOREACH file GENERATE REPLACE(line, '\\"', '');
sptLine = FOREACH Fline GENERATE FLATTEN (STRSPLIT($0, ',')) AS (name: chararray, subject: chararray, dtm : chararray, age: chararray, open: chararray, closed: chararray, accountName: chararray, caseID: chararray);
register '/Users/ashish/IdeaProjects/PigPractice/Resources/DateFormatter.jar'
define CustDate org.ashish.pig.DateFormatter();
a = FOREACH sptLine GENERATE name,subject,CustDate(dtm) AS date, (int) TRIM(age) AS age, (int) TRIM(open) AS open, (int) TRIM(closed) AS close, accountName AS acc_name, caseID as case_id;
----------------------


--1. Number of Users

b = group a by name;
c = foreach b {
	uniq = DISTINCT a.name;
	generate group, COUNT(uniq) as name_cnt;
     };
d = group c by name_cnt;
e = foreach d generate 'number of users', SUM(c.name_cnt);
STORE e into '/Users/ashish/IdeaProjects/PigPractice/POC_Result/num_uniq_users' USING JsonStorage();

-- 2. Number of Tickets per customer

b = foreach a generate name, subject;
c = group b by name;
d = foreach c generate group, COUNT(b.subject);
STORE d into '/Users/ashish/IdeaProjects/PigPractice/POC_Result/num_tik_per_cust' USING JsonStorage();


-- 3. Number of Tickets in various categories

b = group a by subject;
c = foreach b generate group, COUNT(a.subject);
STORE c into '/Users/ashish/IdeaProjects/PigPractice/POC_Result/num_tik_per_catg' USING JsonStorage();

-- 4. Number of interactions via various channels – Chat, Email, Call, Scheduled Callback, OKM. (interactions per customer via various channels/subjects)

b = foreach a generate  name, subject;
c = group b by (name,subject);
d = foreach c generate group, COUNT(b.subject);
STORE d into '/Users/ashish/IdeaProjects/PigPractice/POC_Result/num_interactions' USING JsonStorage();


-- 5. Number of tickets per customer and interaction and usage statistics

b = foreach a generate  name, subject, age;
c = group b by (name,subject);
d = foreach c generate group, COUNT(b.subject), SUM(b.age);
STORE d into '/Users/ashish/IdeaProjects/PigPractice/POC_Result/statistics' USING JsonStorage();

-- 6. Classify customer as Active or Passive

b = foreach a generate  name, open, close;
c = group b by name;
d = foreach c {
    sum = SUM (b.open);
    GENERATE group, sum AS open_cnt;
};
f = FOREACH d GENERATE group, (open_cnt > 0 ? CONCAT(' Active , ',(chararray)open_cnt) : CONCAT(' Passive , ',(chararray)0));
STORE f into '/Users/ashish/IdeaProjects/PigPractice/POC_Result/act_pas_cust' USING JsonStorage();