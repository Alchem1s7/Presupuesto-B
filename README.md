## UNDERSTANDING THE SITUATION



Derived from the previous version of the Budget B dashboard, new ideas arose to implement to improve data visualization, navigation in the application and the organization of menus, in addition to new metrics that corresponded to data from various sources, including historical data and data from each monthly closing. All these improvements will be implemented in this new version of the B budget dashboard.



## AIM



Deployment in Power Bi Services of an interactive dashboard application to show all metrics and history relevant to the progress of Budget B



## SPECIFIC OBJECTIVES



- Creation of a Python script for the data transformation process.

- Creation of a SQL database to store information.

- Loading the data and creating an appropriate relational model in Power Bi.

- Creation and deployment of the dashboard in Power Bi Services, in its different versions: Complete and Mobile



## SCOPE OF THE PROJECT



This project has the scope:



- The creation of code in the Python programming language for the standardization of source data, creating a summary of summary tables, in addition to the creation of dimensional tables for the model.

- Insertion of the standardized collected data into a PostgreSQL database for storage and facilitation of automation for subsequent updates.

- Creation of a relational model in Power Bi that responds to the needs of the dashboard.

- Redesign of the existing dashboard, facilitating navigation and organization of information, in addition to pages specifically requested for easy consultation.

- Creation of the mobile version of the dashboard for consultation on mobile devices.



## METHODOLOGY



### DATA EXTRACTION



To carry out the ETL process included in Python, it is necessary to run the script called 'main\_etl.py', which will display a screen in the terminal, which asks for the name of the database created in PostgreSQL. Before executing the script, it is necessary to first create a database that is running in PostgreSQL. A shell base that does not contain any other boards.



Once the database is created and the script is executed, it will display the following screen:



_ **Instructions in the terminal at the time of execution of the dashboard** _

![image](https://github.com/Alchem1s7/Presupuesto-B/assets/100399598/9722df61-c19e-4d36-b89c-d5907540571b)




It is only necessary to enter the name of the database, since the other connection parameters are found explicitly in the function within the "database\_creation" script:



_ **Editable connection parameters to establish the same.** _

![image](https://github.com/Alchem1s7/Presupuesto-B/assets/100399598/a9a4fd97-eaf4-46f1-9baa-509d905ef936)




This line of code shows the connection parameters to the database, which can be modified as needed, depending on the port, username, password and server, for more information on this create\_engine object see: [ https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls](https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls)



Once the name of the database has been specified in the script, it is necessary to enter an access path where the necessary data is located for both the ETL process and the Power Bi Queries.



Inside this folder there should be two more folders: "Ins" and "Outs"



Within "Ins" there must be the month closings, in csv format with the full name of the months, otherwise the script will fail when mapping the different closing bases.



In addition, there must be two Excel .xlsx files, one for the historical data called "1. DPB Est. Entities Resources 2017-2023" and another for the ingestion in Power Bi titled "categorized entities"



_ **Example of the contents of the "Ins" folder.** _

![image](https://github.com/Alchem1s7/Presupuesto-B/assets/100399598/48b5f7bc-c0d6-4c0c-b7ad-6a46cf84812e)






The "Outs" folder must be empty, in this folder the history file will be written for ingestion into Power Bi. The Script considers the existence of these two folders so, if they are not created, it will inevitably fail, unless the source code is modified.



### 5.1 DATA TRANSFORMATION



The Python script is designed to provide the name of the database and the path to the main folder, thus giving rise to data transformation, in this part the tables are shaped to be able to manage the information, Columns are created, some others are dropped, the data is normalized and its integrity is verified, since if it is not correct, it would give an error at the time of insertion into the database. All these processes are written separately following good software development practices.



_ **Section of code where all the functions that are responsible for the ETL process are called.** _

![image](https://github.com/Alchem1s7/Presupuesto-B/assets/100399598/96f0e302-b1c0-4466-9864-ef70e9c2a0e2)






Once the ETL is finished, the following message will be displayed on the terminal screen:



_ **Process completion message** _
![image](https://github.com/Alchem1s7/Presupuesto-B/assets/100399598/359acb03-a6ea-4a5d-ba72-6bbddcf5ee16)


 **Database manager interface showing the loaded entities.** 

![image](https://github.com/Alchem1s7/Presupuesto-B/assets/100399598/460fa41d-b1e6-4f94-8c30-0efe8ddbba7d)






_ **Relational model loaded in Power Bi** _

![image](https://github.com/Alchem1s7/Presupuesto-B/assets/100399598/165c225e-65df-4cfe-9724-12993cb586a6)



## DELIVERABLES



### 6.1 SCRIPT



The main script used in .py



### 6.2 DASHBOARD



(Deployed in Power BI services), there are also the .pbix files of the two versions of the dashboard.




The figures are censored because the information is private.
The main menu:
![image](https://github.com/Alchem1s7/Presupuesto-B/assets/100399598/bd5e2931-a465-45a2-938e-68c1df3e0e71)

General:
![image](https://github.com/Alchem1s7/Presupuesto-B/assets/100399598/02540ecd-f4f1-4bdb-840e-8fa7267bb21e)
![image](https://github.com/Alchem1s7/Presupuesto-B/assets/100399598/da78025c-cf54-4284-a38b-596d1ef67e6f)

Mobile version:
![image](https://github.com/Alchem1s7/Presupuesto-B/assets/100399598/ab3c51e5-8c55-49bd-8be5-ad4bd63d1b5c)

An example of the division of sectors:
![image](https://github.com/Alchem1s7/Presupuesto-B/assets/100399598/f6a4a075-886d-4dee-ab52-c662fd9de2e7)

The data model:
![image](https://github.com/Alchem1s7/Presupuesto-B/assets/100399598/bfe36781-5c6c-4e55-81ed-cfc144be416f)
