# YouTube-Data-Harvesting-and-Warehousing-using-SQL-MongoDB-and-Streamlit.

Problem Statement: The problem statement is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels.

NAME : Dhuwaraganath S

BATCH: MDTM16

DOMAIN : DATA SCIENCE

## Introduction:

The YouTube Data Harvesting and Warehousing project is designed to provide a comprehensive solution for gathering, storing, and analyzing data from the YouTube platform. YouTube, being one of the largest video-sharing platforms globally, hosts an immense amount of valuable data ranging from video metadata to user engagement metrics. This project aims to leverage this data to extract insights that can be beneficial for content creators, marketers, researchers, and decision-makers alike.

## Features:

* Collects channel information, video details, and comments using the YouTube Data API.
* Stores the data in MongoDB for flexible document-based storage.
* Transfers the data from MongoDB to MySQL for structured relational storage.
* Provides a Streamlit dashboard for visualizing and querying the data.

## Tools install:
* Python 3.9
* Google API Key
* MongoDB
* MySQL
* Streamlit

## Requirement Libraries to Install
 * pip install google-api-python-client, pymongo, mysql-connector-python,pymysql,pandas,streamlit.

## Import Libraries

### Youtube API libraries
* import googleapiclient.discovery

### MongoDB
* import pymongo

### SQL libraries
* import mysql.connector
* import pymysql

### pandas
* import pandas as pd

### Dashboard libraries
* import streamlit as st

## Data Collection
Explain how data is collected from YouTube channels using the YouTube Data API. Provide details on the specific information gathered, such as channel details, video details, and comments details.

## Data Storage
Discuss the choice of MongoDB and MySQL for data storage. Explain the benefits of using each database type and how they complement each other in your solution.

## Streamlit Dashboard
Describe the Streamlit dashboard and its functionalities. Provide instructions on how users can interact with the dashboard to visualize data and perform queries.

