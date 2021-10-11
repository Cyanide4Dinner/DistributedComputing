from pyspark import RDD
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

def parseAndExecute(sql, rdds):
    print(sql)

    #    selectColumns = sql[7:sql.find("FROM")-1].split(", ")
    if "(" in sql[:sql.find("FIND")]:
        selectAggregated, sql = getSelectAggregate(sql)

    print(sql)


    selectFromColumns = getSelectColumns(sql)
    #tokens = sql[sql.find("FROM"):].split(" ") 
    fromTableName = getFromTableName(sql)

    print("table name:\t" + fromTableName)

    curr = fromRDDAction( fromTableName, rdds)

    print(sql)
    input(int())
    
    print("loaded")

    if "WHERE" in sql:
        whereColumn, operator, value = getWhereCondition(sql)
        curr = whereRDDAction(whereColumn, operator, value, fromTableName, curr)
        # result = curr.collect()
        # for row in result: print(row)
        
    a = curr.collect()

    for row in a:
        print(row)

    if "GROUP" in sql:
        if "HAVING" in sql:
            havingAggregated = getHavingAggregate(sql)
        groupColumns = getGroupColumns(sql)
        print(groupColumns)
        curr = groupRDDAction(selectAggregated, havingAggregated, groupColumns, fromTableName, curr)
        return curr
        # curr = havingRDDAction(aggregator, 'aggregatedColumn', curr)

    


    curr = selectRDDAction(selectFromColumns, fromTableName, curr)

    print("Executed")
    return curr

def getFromTableName (sql):
    return sql[sql.find("FROM"):].split(" ", maxsplit=2)[1]

def getSelectColumns (sql):
    return sql[7:sql.find("FROM")-1].split(", ")

def getSelectAggregate (sql):
    selectAggregated = []
    
    selectAggregated.append(sql[:sql.find("(")].split(" ")[-1])
    selectAggregated.append(sql[sql.find("(")+1: sql.find(")")].strip())
    print(selectAggregated, sql[:sql.find(selectAggregated[0])])
    # selectAggregated.append(sql[sql.find(")"):].split(" ")[1])
    # selectAggregated.append(sql[sql.find(")"):]).split(" ")[2]

    return selectAggregated, (sql[:sql.find(selectAggregated[0])] + sql[sql.find(")")+3:])
    
    # selectAggregated = [[]]
    # while "(" in sql:
    #     aggregator = sql[:sql.find("(")].split()[-1]
    #     aggregatedColumn = sql[sql.find("(")+1: sql.find(")")].strip()
    #     selectAggregated.append([aggregator, aggregatedColumn])
    #     sql = sql[:sql.find(aggregator)] + sql[sql.find(")")+2:]

    # return selectAggregated, sql

def getGroupColumns (sql):
    return sql[sql.find("GROUP BY")+9:sql.find("HAVING")-1].split(", ")

def getHavingAggregate (sql):
    havingAggregated = []
    sql = sql[sql.find("HAVING")+7:]
    havingAggregated.append(sql[:sql.find("(")].split(" ")[-1])
    havingAggregated.append(sql[sql.find("(")+1: sql.find(")")].strip())
    havingAggregated.append(sql[sql.find(")"):].split(" ")[1])
    havingAggregated.append(sql[sql.find(")"):].split(" ")[2])

    print(havingAggregated)

    return havingAggregated




    # havingAggregated = [[]]
    # sql = sql[sql.find("HAVING")+7:]
    # while "(" in sql:
    #     aggregator = sql[:sql.find("(")].split()[-1]
    #     aggregatedColumn = sql[sql.find("(")+1: sql.find(")")].strip()
    #     havingAggregated.append([aggregator, aggregatedColumn])
    #     sql = sql[:sql.find(aggregator)] + sql[sql.find(")")+2:]

    # return havingAggregated

def getWhereCondition (sql):
    where = sql[sql.find("WHERE"):].split(" ")
    return where[1], where[2], where[3]

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

def groupRDDAction(selectAggregated, havingAggregated, groupColumns, tableName, curr):
    cols = [columnNumbers[tableName].index(x) for x in groupColumns]

    print(cols)

    aggSelectColumn = columnNumbers[tableName].index(selectAggregated[1])
    aggHavingColumn = columnNumbers[tableName].index(havingAggregated[1])

    mapped = curr.map(lambda row: ((*[row[i] for i in cols],), [row[aggSelectColumn], row[aggHavingColumn]]))
    # grouped = mapped.aggregateByKey().mapValues(list).map(lambda x: list(x))

    # a = mapped.collect()


    for row in mapped.collect():
        print(row)
    
    input((int))

    selectAggregator = selectAggregated[0]
    havingAggregator = havingAggregated[0]

    if (selectAggregator == "AVG"):
        grouped = mapped.aggregateByKey((0,0), lambda acc, val: (int(acc[0]) + int(val[0]), int(acc[1]) + 1), lambda acc1, acc2: (int(acc1[0]) + int(acc2[0]), int(acc1[1]) + int(acc2[1])))
        grouped = grouped.map(lambda q: (q[0], (1.0*q[1][0])/q[1][1]))

    elif (selectAggregator == "SUM"):
        grouped = mapped.aggregateByKey(0, lambda acc, val: acc + val[0], lambda acc1, acc2: acc1 + acc2)
        grouped = grouped.map(lambda q: (q[0], q[1]))

    elif (selectAggregator == "MAX"):
        grouped = mapped.aggregateByKey(0, lambda acc, val: max(int(acc), int(val[0])), lambda acc1, acc2: max(int(acc1), int(acc2)))
        # grouped = grouped.map(lambda q: list(q[0]).append(q[1]))
        grouped = grouped.map(lambda q: list(q[0]).append( q[1]))
        input()
        for row in grouped.collect():
            print(row, " ", type(row))
        print("grouped")
        input()

    elif (selectAggregator == "MIN"):
        grouped = mapped.aggregateByKey(10000000000, lambda acc, val: min(int(acc), int(val[0])), lambda acc1, acc2: min(int(acc1), int(acc2)))
        grouped = grouped.map(lambda q: (q[0], q[1]))

    elif (selectAggregator == "COUNT"):
        grouped = mapped.aggregateByKey(0, lambda acc, val: val+1, lambda acc1, acc2: acc1 + acc2)
        grouped = grouped.map(lambda q: (q[0], q[1]))


    if (havingAggregator == "AVG"):
        controlled = mapped.aggregateByKey(0, lambda acc, val: acc + val[0], lambda acc1, acc2: acc1 + acc2)

    

    
    

def selectRDDAction( columnNames, tableName, rdd ):
    def selectColumnValues (row):
        fin = []
        for column in columnNames:
            col = columnNumbers[tableName].index(column) 
            fin.append(row[col])
        return fin
    return rdd.map(selectColumnValues)
    
    
#parseAndExecute(EXAMPLE_SQL, " ")
