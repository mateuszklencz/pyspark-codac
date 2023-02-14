# General Overview <a name="overview"></a>

Codac PySpark assignment done by **Mateusz Klencz** . This task tests the following skills:

* joining, filtering, renaming columns and saving of pyspark dataframes 
* writing tests (chispa)
* encapsulation of data using class and generic functions 
* CI/CD with GitHub Actions


## Tables of Contents

- [General Overview](#general-overview)
- [Task Description](#task-description)
  * [Background:](#background-)
  * [Things to be aware:](#things-to-be-aware-)
- [Data](#data)
  * [dataset 1 - personal data](#dataset-1---personal-data)
  * [dataset 2 - financial data](#dataset-2---financial-data)


# Task Description <a name="task_desc"></a>

## Background:
A very small company called **KommatiPara** that deals with bitcoin trading has two separate datasets dealing with clients that they want to collate to starting interfacing more with their clients. One dataset contains information about the clients and the other one contains information about their financial details.

The company now needs a dataset containing the emails of the clients from the United Kingdom and the Netherlands and some of their financial details to starting reaching out to them for a new marketing push.

Since all the data in the datasets is fake and this is just an exercise, one can forego the issue of having the data stored along with the code in a code repository.


## Things to be aware:

- Use Python **3.7**
- Avoid using notebooks, like **Jupyter** for instance. While these are good for interactive work and/or prototyping in this case they shouldn't be used.
- Only use clients from the **United Kingdom** or the **Netherlands**.
- Remove personal identifiable information from the first dataset, **excluding emails**.
- Remove credit card number from the second dataset.
- Data should be joined using the **id** field.
- Rename the columns for the easier readability to the business users:

|Old name|New name|
|--|--|
|id|client_identifier|
|btc_a|bitcoin_address|
|cc_t|credit_card_type|

- The project should be stored in GitHub and you should only commit relevant files to the repo.
- Save the output in a **client_data** directory in the root directory of the project.
- Add a **README** file explaining on a high level what the application does.
- Application should receive three arguments, the paths to each of the dataset files and also the countries to filter as the client wants to reuse the code for other countries.
- Use **logging**.
- Create generic functions for filtering data and renaming.
Recommendation: Use the following package for Spark tests - https://github.com/MrPowers/chispa
- **Bonus** - If possible it should have an automated build pipeline using GitHub Actions
- **Bonus** - If possible log to a file with a rotating policy.
- **Bonus** - Code should be able to be packaged into a source distribution file.
- **Bonus** - Requirements file should exist.
- **Bonus** - Document the code with docstrings as much as possible using the reStructuredText (reST) format.


# Data <a name="data"></a>

All data are provided in *.csv* files

## dataset 1 - personal data

| name  | description | type |
| ----- | ------------- | ---- |	
| id | unique identifier  | int |		
| first_name | first name  | str |
| last_name | last name  | str |		
| email | email address  | str |
| country | customer's location  | str |

## dataset 2 - financial data

| name  | description | type |
| ------------- | ------------- | ---- |	
| id  | unique identifier | int |		
| btc_a | bitcoin address  | str |
| cc_t | credit card type | str |		
| cc_n | credit card number  | int |
