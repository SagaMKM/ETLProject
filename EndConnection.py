import mysql.connector

# Connect to MySQL server
cnx = mysql.connector.connect(user='bigdatauser',
                            password='bigdata12',
                            host='localhost', 
                           )
cursor = cnx.cursor()


drop_query = "DROP DATABASE ETLProject"
cursor.execute(drop_query)
cnx.commit()


cursor.close()
cnx.close()

print("Database ETLProject has been dropped.")
