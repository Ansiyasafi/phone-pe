
# Phonepe Pulse Data Visualization
## Introduction
PhonePe has become one of the most popular digital payment platforms in India, with millions of users relying on it for their day-to-day transactions. The app is known for its simplicity, user-friendly interface, and fast and secure payment processing. It has also won several awards and accolades for its innovative features and contributions to the digital payments industry.

We create a web app to analyse the Phonepe transaction and users depending on various Years, Quarters, States, and Types of transaction and give a Geographical and Geo visualization output based on given requirements.

" Disclaimer:-This data between 2018 to 2023 in INDIA only "
![Screenshot (3)](https://github.com/Ansiyasafi/phone-pe/assets/159064188/79f7d34f-2b77-4b2f-ac5d-f5fb78b20a2d)
## tabs for data exploration
* All india                              *Top categories
    * aggregated transaction                *top transaction
    * aggregated user                       *top user
    * map transaction
    * map user

we can explore the data and get many valuable insights from the data exploration.      
##Tools install
* virtual code.
* Jupyter notebook.
* Python 3.11.0 or higher.
* MySQL
* Git
## Import Libraries
* import pandas as pd
* import numpy as np
* import os
* import json
* import mysql.connector
* import plotly.express as px
* import requests
* import json
* import sqlite3
## E T L Process
* Extract data
Initially, we Clone the data from the Phonepe GitHub repository by using Python libraries https://github.com/PhonePe/pulse.git
* Process and Transform the data
Process the clone data by using Python algorithms and transform the processed data into DataFrame formate.
* Load data
Finally, create a connection to the MySQL server and create a Database and stored the Transformed data in the MySQL server

