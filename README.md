# SQL

A collection of SQLs that I have used for projects.
  
## pivot.sql

**Scenario:** 
This query was written for generating a report to gather insights on the number of students per semester(from 2009 to 2020) who have college level math and english credits. A small sample template is shown below. The original report had around 10,000 rows and the generation involved joining around 8 tables and materialized views.

| Campus | Part-time/Full-time | Age Range | Ethnicity | AU - 2009 | SP - 2010 | AU - 2014 | SP - 2014 | AU - 2015 | SP - 2015 | ..................... |  

| ABC | P | >=25 | HS | 10 | 5 | 6 | 0 | 4 | ..................... |  

| EFG | F | >=25 | HS | 15 | 25 | 6 | 10 | 0 | ..................... |
