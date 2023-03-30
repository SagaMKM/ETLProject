import mysql.connector
import csv


db_config = {
    'host': 'localhost',
    'user': 'bigdatauser',
    'password': 'bigdata12',
    'database': 'ETLProject'
}
conn = mysql.connector.connect(**db_config)


cursor = conn.cursor()
cursor.execute("SHOW TABLES")
tables = [table[0] for table in cursor.fetchall()]

#Exporting the tables in ETLProject
for table in tables:
    #get each column
    cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s", (table,))
    headers = [header[0] for header in cursor.fetchall()]
    
    #Create a CSV file
    with open(table + '.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        
        #get data
        cursor.execute("SELECT * FROM " + table)
        rows = cursor.fetchall()
        
       #write csv
        for row in rows:
            writer.writerow(row)


conn.close()


print("Tables exported successfully!")
