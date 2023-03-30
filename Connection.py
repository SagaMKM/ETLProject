import mysql.connector
from faker import Faker
import random

# Create connection
mydb = mysql.connector.connect(
  host="localhost",
  user="bigdatauser",
  password="bigdata12"
)

cursor = mydb.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS ETLProject")
cursor.execute("USE ETLProject")
fake = Faker()


# Emails format
emails = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com", "mail.com"]


# first table
def generate_table1_data(num_records):
    data = []
    for i in range(num_records):
        age = random.randint(18, 60)
        phone = fake.msisdn()
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = first_name.lower() + last_name.lower() + "@" + random.choice(emails)
        data.append((age, phone, email, first_name, last_name))
    return data

# second table
#def generate_table2_data(num_records):
    data = []
    for i in range(num_records):
        address = fake.address()
        city = fake.city()
        state = fake.state()
        zip_code = fake.zipcode()
        data.append((address, city, state, zip_code))
    return data


# second table
def generate_table2_data(num_records):
    data = []
    for i in range(num_records):
        licenceplate = fake.license_plate()
        color = fake.safe_color_name()
        date = fake.year()
        data.append((licenceplate, color, date))
    return data

# third table
def generate_table3_data(num_records):
    data = []
    for i in range(num_records):
        job_title = fake.job()
        salary = random.randint(30000, 100000)
        #company = fake.company()+" "+fake.company_suffix()
        data.append((job_title, salary))
    return data







#Generate the data and how much
table1_data = generate_table1_data(1000)
table2_data = generate_table2_data(1000)
table3_data = generate_table3_data(1000)



# Create table1 and insert the data
cursor.execute("CREATE TABLE contact_info (id INT AUTO_INCREMENT PRIMARY KEY, age INT, phone VARCHAR(255),email VARCHAR(255), first_name VARCHAR(255), last_name VARCHAR(255))")
for record in table1_data:
    cursor.execute("INSERT INTO contact_info (age, phone, email, first_name, last_name) VALUES (%s, %s, %s, %s, %s)", record)
mydb.commit()

# Create table2 and insert the data
#cursor.execute("CREATE TABLE address (id INT AUTO_INCREMENT PRIMARY KEY, address VARCHAR(255), city VARCHAR(255), state VARCHAR(255), zip_code VARCHAR(255))")
#for record in table2_data:
    #cursor.execute("INSERT INTO address (address, city, state, zip_code) VALUES ( %s, %s, %s, %s)", record)
#mydb.commit()

# Create table2 and insert the data
cursor.execute("CREATE TABLE address (id INT AUTO_INCREMENT PRIMARY KEY, licence_plate VARCHAR(255), color VARCHAR(255), date VARCHAR(255))")
for record in table2_data:
    cursor.execute("INSERT INTO address (licence_plate, color, date) VALUES ( %s, %s, %s)", record)
mydb.commit()


# Create table3 and insert the data
cursor.execute("CREATE TABLE job_title (id INT AUTO_INCREMENT PRIMARY KEY, job_title VARCHAR(255), salary INT)")
for record in table3_data:
    cursor.execute("INSERT INTO job_title (job_title, salary) VALUES (%s, %s)", record)
mydb.commit()



# Close the database connection
mydb.close()
print("Run succesfull")