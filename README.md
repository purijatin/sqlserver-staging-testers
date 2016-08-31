sqlserver-staging-testers
=========================

Test bed to see different ways to stage data to a sql server instance


To run
=========

mvn test

The test cases are available in `StageDAOTest.java` for reference. Though you will have to enter username and password in filter.properties (src/main/filters). For some other database, you would have to then configure the respective driver.
