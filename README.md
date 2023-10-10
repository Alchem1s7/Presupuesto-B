# Presupuesto-B
In this repo I'm going to show you the ETL I created in python to store the data in a PostgreSQL database in order to generrate a power bi application that shows the relevant metricts  of Presupuesto B of the financial department of queretaro. Obviously this data is confidential that's why I'm going to show you just my code, the files are empty


This ETL and dashboard are created for local purposes. You have to previously have been created a SQL database that is alocated locally. Then you just need to change the connection parameters, that are created in the engine object of sqlalchemy:
![image](https://github.com/Alchem1s7/Presupuesto-B/assets/100399598/43f51d91-cccf-4450-bce8-fa452005f26c)

When you have set everything up, now you can  run the only script I used, named main_etl.py and follow the instructions for the correct operation.
The principal path is the one that inside contains two folders, Ins and Outs, inside Ins you would find the monthly files for the close of the budget. In the Outs folder you'll encounter the historic data in CSV that then is used to load it into Power Bi.
That's everything you need to know to run properly the script. As I said this information is confidential and I can't provide it. This is the dashboard that the leaders of presupuesto B use to see insights and advances in the budget.
Here are some screenshots of the dashboard, I created a lot of DAX measures that are useful for tooltips and tables, everything is automated:

The main menu:
![image](https://github.com/Alchem1s7/Presupuesto-B/assets/100399598/8dae6354-8524-4e94-bf1b-3fc5f05c677a)

An example of the division of sectors:
![image](https://github.com/Alchem1s7/Presupuesto-B/assets/100399598/f6a4a075-886d-4dee-ab52-c662fd9de2e7)


The data model:

![image](https://github.com/Alchem1s7/Presupuesto-B/assets/100399598/bfe36781-5c6c-4e55-81ed-cfc144be416f)
