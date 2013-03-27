Feature: GetUpdateDelete
	In order to manage my documents in the database
	As a developer
	I want to save, get or delete a document from the database

Scenario: Saving a new document in the database	
	When I update a document with Key=1 and Value="Doron" 
	Then Document is in the database with Key=1 and Value="Doron"

Scenario: Getting already stored document
	Given Document Key=1 and Value="Doron" in the DB
	When I get document with key=1
	Then Document should exist and Value="Doron"

Scenario: Update already exist document
	Given Document Key=1 and Value="Doron" in the DB
	When I update a document with Key=1 and Value="Doron2" 
	Then Document is in the database with Key=1 and Value="Doron2"	 

Scenario: Delete already exist document
	Given Document Key=1 and Value="Doron" in the DB
	When I delete document with key=1
	Then Document with key=1 should not exist

Scenario: Reupdate an object after got deleted
	Given Document Key=1 and Value="Doron" in the DB
	When I delete document with key=1
	And I update a document with Key=1 and Value="Doron2" 
	Then Document is in the database with Key=1 and Value="Doron2"	 

Scenario: Document is in database after restart
	When I update a document with Key=1 and Value="Doron"
	And Restrt the database
	Then Document is in the database with Key=1 and Value="Doron"

Scenario: Deleting document already in the database before starting
	Given Document Key=1 and Value="Doron" in the DB
	And Restrt the database
	When I delete document with key=1
	And Restrt the database
	Then Document with key=1 should not exist

Scenario: Update document already exist in the database before starting
	Given Document Key=1 and Value="Doron" in the DB
	And Restrt the database
	When I update a document with Key=1 and Value="Doron2" 
	And Restrt the database
	Then Document is in the database with Key=1 and Value="Doron2"	 
