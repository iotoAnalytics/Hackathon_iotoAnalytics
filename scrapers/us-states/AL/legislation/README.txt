- Will have issues with the current version of scraper utils 

- Since page urls for this site are not unique,
Scraper utils cannot use url as a UNIQUE field in the table

- Also when inserting into the database,
ON CONFLICT() should be for goverlytics ID, 
not state_url or the scraper will not insert all bills.