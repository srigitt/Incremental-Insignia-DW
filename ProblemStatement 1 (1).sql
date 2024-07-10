/*
Insignia Corporation are doing business of selling gifts online and they have recently started their business.
Their Product,sales along with customer and employees and the geography at which they have sales is sent to a Data processing team in a Table. 
One day their CEO decided to hire a CDO to improve operational efficiencies in the organization and modernize the capabilities within the organization.

First recommendation which was given by CDO was to form a Team which can load this csv data into a datawarehouse and create some tables out of the single table.

Then CDO asked the Team lead to create a Data model out of the Table.

Dataset Details:

You are provided with two tables called Insignia_staging and Insignia_incremental.
Insignia_staging is the main table that you will be using. Insignia_incremental contains the incremental data.
Whenever you will perform the incremental load step, you will have to create a copy of the Insignia_staging table with a name Insignia_staging_copy and insert the data present in the Insignia_incremental table into the Insignia_staging_copy table.


Part 1: Create a Data Model.

Part one involves creation of a data model.
Analyze the Insignia_staging table provided to you.
You will have to identify the Dimensions and Facts yourself and create the Dimension and Fact tables accordingly.

Note that you are not required to load the data in this step. You are just required to create the data model.

Also note that you might not have the permission to create certain key constraints such as foreign key constraints.
In that case, simply maintain the referential integrity without creating the foreign key.

You, being the part of the BI /Data engineeering team has to identify all the dimensions and create the dimensions with tracking columns by using lineages .

The lineage table must have a load_id and this lineage load_id/lineage_id would be used to identify each data load and number of rows in the source and destination.

Lineage table must be created as follows:

Lineage_Id bigint
Source_System Varchar(100)
Load_Stat_Datetime datetime
Load_EndDatetime datetime
Rows_at_Source int
Rows_at_destination_Fact int
Load_Status bit

The lineage_id columns must be used in all the tables of dimension and fact to identify what data is loaded by the ETL,from which source, how many rows were affected , at what time etl stated and ended and lastly whether the etl succeeded or failed.

A date dimension table must also be implmented with the following fields:

DateKey
Date
Day_Number(Number of the month)
Month_Name
Short_Month(Short Month Name in three characters)
Calendar_Month_Number
Calendar_Year
Fiscal_Month_Number
Fiscal_Year
Week_Number

Datekey must be in integer format and must be used to map the single fact table .

fiscal year of the company starts with July and completes in the month of June so fiscal date fields must have right mapping to create the financial reports .
Note: Dates in the Date dimension must be loaded from 2000 to 2023 Calendar Years.

You can name the Stored procedures and Tables as per your own accordance but do keep the naming convention consistent and use the best practices wherever required.

Part 2: ETL and SCD Implementation

Once the Model is created, then ETL is to be created in order to process the data incrementally.
Often then not, the company faces data ingestion issues and data sent previously needs some corrections as well.

The ETL must be created in the following steps:

1.Create a copy of the Insignia_staging table with the name Insignia_staging_copy. 
2.Data from Insignia_staging_copy table must be loaded in the Dimensions.
3.Data in fact must be loaded  in the last step.
4.The incremental data present in the Insignia_incremental table should be inserted into the Insignia_staging_copy table.
5.The Insignia_staging_copy must be truncated before incremental load.
6.The incremental data then should be loaded in the Dimension and Fact tables accordingly.
7.None of the dimension and fact tables must be truncated.


The Employee Dimension along with Customer Dimension must have SCD Type 2 implemented. 

Geography Dimension must implement the SCD type 3 on the population column/attribute.

The other dimensions should be SCD Type 1

Late arriving dimension concept for all the dimensions must be implmented.

Also implement the fact data load logic.


guidelines---sql server:

1.Use database statement must be in all the sql scripts .
2.Create table scripts .
3.Data manipulation scripts if any .
5.Best practices must be used while developing the solution and for loading data for scd dimensions.
6.Staging table (Insignia_staging_copy) must be created to perform the ETL.
7.The ETL can be created with metadata driven tables in mind as well but it is not mandatory.
8.The solution must implement surrogate keys of auto generate feature of the underlying database.
9.Solution must have a reconciliation module to check the no of rows processed after a full etl run. (this is good to have and is a bonus pointer).
10.Implement scd using left joins and updates . no merge statement must be used. --important


*/



