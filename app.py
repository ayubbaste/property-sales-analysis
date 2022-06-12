#!/usr/bin/env python3
import csv
import mysql.connector

"""
House Property Sales Analysis

Dataset: The House Property Sales dataset on Kaggle contains a file
named ‘raw_sales.csv.’ It includes the following variables:

Datesold: The date when an owner sold the house to a buyer.
Postcode: 4 digit postcode of the suburb where the owner sold the property
Price: Price for which the owner sold the property.
Bedrooms: Number of bedrooms

Business tasks
1. Which date corresponds to the highest number of sales?
2. Find out the postcode with the highest average price per sale? (Using Aggregate Functions)
3. Which year witnessed the lowest number of sales?
4. Use the window function to deduce the top 3 postcodes by year's average price.

"""

data_set_file = 'dataset.csv'
database = 'testdb'
user = 'testuser'
passwd = 'MyPass'
host = 'localhost'


def connect_to_db():
    db = mysql.connector.connect(
        database = database,
        user = user,
        passwd = passwd,
        host = host
    )

    return db


def insert_data_to_database(data_set_file):
    db = connect_to_db()
    mycursor = db.cursor()

    # create table if not exists
    mycursor.execute("CREATE TABLE IF NOT EXISTS sales (datesold DATETIME(6), \
                     postcode INT, price INT, propertytype VARCHAR(255), \
                     bedrooms INT);")


    # import data to database table
    with open(data_set_file) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            if 'datesold' in row:
                pass
            else:
                # compose and execute sql statement
                insert_query = """INSERT INTO sales (datesold, postcode, price, 
                propertytype, bedrooms) VALUES (%s,%s,%s,%s,%s)
                """
                values = (row[0],row[1],row[2],row[3],row[4])
                mycursor.execute(insert_query, values)

    # save db and disconnect
    db.commit()
    db.close()


def get_the_most_profitable_day():
    db = connect_to_db()
    mycursor = db.cursor()

    # get the most profitable day
    mycursor.execute(
        """
        SELECT datesold as the_most_profitable_date, SUM(price) AS profit 
        FROM sales GROUP BY datesold ORDER BY profit DESC LIMIT 1;
        """
    )

    # fetche all rows from the last executed statement.
    the_most_profitable_day = mycursor.fetchall()

    print(f"\nThe most profitable day is:\n{the_most_profitable_day}\n")

    db.close()


def get_the_most_sales_day():
    db = connect_to_db()
    mycursor = db.cursor()

    # get the day with most sales amount 
    mycursor.execute(
        """
        SELECT datesold, COUNT(datesold) AS sales_count FROM sales 
        GROUP BY datesold ORDER BY sales_count DESC LIMIT 1;
        """
    )
    # fetche all rows from the last executed statement.
    the_most_sales_day = mycursor.fetchall()

    print(f"\nThe day with most sales amount is:\
          \n{the_most_sales_day}\n")


def get_the_postcode_with_the_highest_average_price_per_sale():
    db = connect_to_db()
    mycursor = db.cursor()

    # get the day with most sales amount 
    mycursor.execute(
        """
        SELECT postcode, AVG(price) AS highest_avg_price FROM sales 
        GROUP BY postcode ORDER BY highest_avg_price DESC LIMIT 1;
        """
    )
    # fetche all rows from the last executed statement.
    the_highest_average_per_sale = mycursor.fetchall()

    print(f"\nThe postcode with highest average price per sale is:\
          \n{the_highest_average_per_sale}\n")


def get_the_lowest_number_of_sales_per_year():
    db = connect_to_db()
    mycursor = db.cursor()

    # get the year with lowest number of sales 
    mycursor.execute(
        """
        SELECT YEAR(datesold) lowest_sales_year, COUNT(datesold) AS sales_count 
        FROM sales GROUP BY YEAR(datesold) ORDER BY sales_count ASC LIMIT 1;
        """
    )
    # fetche all rows from the last executed statement.
    the_lowest_number_of_sales_per_year = mycursor.fetchall()

    print(f"\nThe lowest sales year is:\
          \n{the_lowest_number_of_sales_per_year}\n")


def get_top_postcodes_per_year_avg_price():
    db = connect_to_db()
    mycursor = db.cursor()

    # get the top 3 postcode ranked with highest avg sales price from every year
    mycursor.execute(
        """
        WITH top3 AS(SELECT YEAR(datesold), postcode, AVG(price), DENSE_RANK() 
        OVER(PARTITION BY YEAR(datesold) ORDER BY AVG(price) DESC) post_rank 
        FROM sales GROUP BY YEAR(datesold), postcode) SELECT * FROM top3 
        WHERE post_rank <=3;
        """
    )
    # fetche all rows from the last executed statement.
    top_postcodes_per_year_price = mycursor.fetchall()

    print(f"\nThe top postcodes per year average price are:\
          \n{top_postcodes_per_year_price}\n")


# RESULTS
get_the_most_profitable_day()
get_the_most_sales_day()
get_the_postcode_with_the_highest_average_price_per_sale()
get_the_lowest_number_of_sales_per_year()
get_top_postcodes_per_year_avg_price()
