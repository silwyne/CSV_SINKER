# CSV SINKER
#### repo info :
  - [Project state : In Progress]
  - [Use state : Usable]
  - [Lang : Java (JDK 11)]

#### Supported Databases (for now):
  - Postgres


This is a simple script which writes your `csv data` into your `Table` in some `Database`.
Suppose you have a csv file, and you want to have the data in a `Table` in your `database`\
What would you do?\
Some might copy and paste the csv file in an IDE and tries to make the `INSERT QUERY` :), which is funny!\
> [!NOTE] \
> It is funny because if one field in your csv contains comma you can't do that! And sometimes it gets really hard to edit that big csv into an insert query.

### USING GUIDE
1. config env.csv_sinker file!
2. use this command to run the scrip\
```shell
java -jar /path/to/CSV_SINKER.jar
```
> [!NOTE] \
> Be careful `env.csv_sinker` must be in the same file as you run the command to run the script\
> or you'll face error.