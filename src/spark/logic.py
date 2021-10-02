import pyspark

EXAMPLE_SQL = "SELECT age FROM Movies WHERE age > 5"


# rdds is a list of rdds with index vs dataset as follows
# 0 : movies
# 1 : rating
# 2 : users
# 3 : zipcodes

def parseAndExecute( sql, rdds ):
    tokens = sql.split(" ") 
    fromRDD = fromRDDAction( tokens[3], rdds )
    print("Executed")

def fromRDDAction( tableName, rdds ):
    print("recieved: ", tableName)
    return tableName
    
#parseAndExecute(EXAMPLE_SQL, " ")
