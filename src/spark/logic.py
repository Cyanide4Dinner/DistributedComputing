import pyspark
import re

EXAMPLE_SQL = "SELECT age FROM Movies WHERE age > 5"


# rdds is a list of rdds with index vs dataset as follows
# 0 : movies
# 1 : rating
# 2 : users
# 3 : zipcodes

columnNumbers = {
        'Movies' : [
            'movieid',
            'title',
            'releasedate',
            'unknown',
            'Action',
            'Adventure',
            'Animation',
            'Children',
            'Comedy',
            'Crime',
            'Documentary',
            'Drama',
            'Fantasy',
            'Film_Noir',
            'Horror',
            'Musical',
            'Mystery',
            'Romance',
            'Sci_Fi',
            'Thriller',
            'War',
            'Western'
            ],
        'Rating' : [
            'userid',
            'movieid',
            'rating',
            'timestamp'
            ],
        'Users'  : [
            'userid',
            'age',
            'gender',
            'occupation',
            'zipcode'
            ],
        'Zipcodes' : [
            'zipcode',
            'zipcodetype',
            'city',
            'state'
            ]
        }

def parseAndExecute( sql, rdds ):
    selectColumns = sql[7:sql.find("FROM")-1].split(", ")
    tokens = sql[sql.find("FROM"):].split(" ") 
    curr = fromRDDAction( tokens[1], rdds )
    curr = whereRDDAction( tokens[3], tokens[4], tokens[5], tokens[1], curr)
    curr = selectRDDAction(selectColumns, tokens[1], curr)
    print("Executed")
    return curr

def fromRDDAction( tableName, rdds ):
    if(tableName == "Movies"):
        return rdds[0]
    elif(tableName == "Rating"):
        return rdds[1]
    elif(tableName == "Users"):
        return rdds[2]
    else:
        return rdds[3]

def whereRDDAction( columnName, operator, value, tableName, rdd ):
    print("column: "+ columnName+" operator: "+operator+" value: "+value+" table: "+tableName)
    col = columnNumbers[tableName].index(columnName) 
    if(operator == "="): 
        return rdd.filter(lambda row: int(row[col]) == int(value))
    elif(operator == ">"):
        return rdd.filter(lambda row: int(row[col]) > int(value))
    elif(operator == "<"):
        return rdd.filter(lambda row: int(row[col]) < int(value))
    elif(operator == ">="):
        return rdd.filter(lambda row: int(row[col]) >= int(value))
    elif(operator == "<="):
        return rdd.filter(lambda row: int(row[col]) <= int(value))
    elif(operator == "<>"):
        return rdd.filter(lambda row: int(row[col]) != int(value))
    elif(operator == "LIKE"):
        return rdd.filter(lambda row: re.match(value, row[col]) != None)
    elif(operator == "IN"):
        #TODO
        return rdd
    else:
        return rdd

def selectRDDAction( columnNames, tableName, rdd ):
    def selectColumnValues (row):
        fin = []
        for column in columnNames:
            col = columnNumbers[tableName].index(column) 
            fin.append(row[col])
        return fin
    return rdd.map(selectColumnValues)
    
#parseAndExecute(EXAMPLE_SQL, " ")
