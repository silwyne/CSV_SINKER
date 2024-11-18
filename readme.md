# CSV SINKER

A powerful tool to effortlessly transfer CSV data into database tables.

## Project Overview

- **Status**: In Progress
- **Usability**: Ready for Use
- **Language**: Java (JDK 11)
- **Supported Databases**: PostgreSQL (more coming soon!)
- **Supported DataTypes**:
  - String
  - Integer
  - Long
  - Double
  - Boolean
  - Timestamp
  - INET

## Features

- Seamless CSV to database table conversion
- Handles complex CSV structures, including fields with commas
- Configurable through a simple environment file
- Efficient bulk insert operations

## Why CSV SINKER?

Imagine you have a CSV file and need its data in your database table. Manually creating INSERT queries can be tedious and error-prone, especially with large datasets or complex CSV structures. CSV SINKER automates this process, saving you time and reducing errors.

> [!NOTE]
> CSV SINKER intelligently handles CSV fields containing commas, a common pitfall in manual conversions.

## Prerequisites

- Java Runtime Environment (JRE) 11 or higher
- Access to a supported database (currently PostgreSQL)
- Basic knowledge of your database connection details

## Quick Start Guide

1. **Configure the Environment**:
   Create an `env.csv_sinker` file in your working directory with the following structure:
2. **Download Jar file** from Latest-version of this repo
3. **Run the script**:
   run the script using this command: \
```shell
java -jar /path/to/CSV_SINKER.jar
```