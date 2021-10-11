from pyspark import RDD
from math import trunc
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
	if "(" in sql[:sql.find("FIND")]:
		selectAggregated, sql = getSelectAggregate(sql)

	selectFromColumns = getSelectColumns(sql)
	fromTableName = getFromTableName(sql)

	curr = fromRDDAction( fromTableName, rdds)

	if "WHERE" in sql:
		whereColumn, relate, value = getWhereCondition(sql)
		print(whereColumn, relate, value, fromTableName)
		curr = whereRDDAction(whereColumn, relate, value, fromTableName, curr)

	for row in curr.collect():
		print(row)
		
	if "JOIN" in sql:
		return joinRDDAction(sql, selectFromColumns, curr, rdds)

	if "GROUP" in sql:
		havingAggregated = []
		if "HAVING" in sql:
			havingAggregated = getHavingAggregate(sql)
		groupColumns = getGroupColumns(sql)
		curr = groupRDDAction(selectAggregated, havingAggregated, groupColumns, fromTableName, curr)
		return curr

	curr = selectRDDAction(selectFromColumns, fromTableName, curr)
	return curr


def getFromTableName (sql):
	return sql[sql.find("FROM"):].split(" ", maxsplit=2)[1]

def getSelectColumns (sql):
	return sql[7:sql.find("FROM")-1].split(", ")

def getSelectAggregate (sql):
	selectAggregated = []
	
	selectAggregated.append(sql[:sql.find("(")].split(" ")[-1])
	selectAggregated.append(sql[sql.find("(")+1: sql.find(")")].strip())

	return selectAggregated, (sql[:sql.find(selectAggregated[0])] + sql[sql.find(")")+3:])

def getGroupColumns (sql):
	return sql[sql.find("GROUP BY")+9:sql.find("HAVING")-1].split(", ")

def getHavingAggregate (sql):
	havingAggregated = []
	sql = sql[sql.find("HAVING")+7:]
	havingAggregated.append(sql[:sql.find("(")].split(" ")[-1])
	havingAggregated.append(sql[sql.find("(")+1: sql.find(")")].strip())
	havingAggregated.append(sql[sql.find(")"):].split(" ")[1])
	havingAggregated.append(sql[sql.find(")"):].split(" ")[2])

	return havingAggregated

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

def whereRDDAction( columnName, relate, value, tableName, rdd ):
	# print("column: "+ columnName+" operator: "+relate+" value: "+value+" table: "+tableName)
	col = columnNumbers[tableName].index(columnName) 
	if(relate == "="): 
		return rdd.filter(lambda row: int(row[col]) == int(value))
	elif(relate == ">"):
		return rdd.filter(lambda row: int(row[col]) > int(value))
	elif(relate == "<"):
		return rdd.filter(lambda row: int(row[col]) < int(value))
	elif(relate == ">="):
		return rdd.filter(lambda row: int(row[col]) >= int(value))
	elif(relate == "<="):
		return rdd.filter(lambda row: int(row[col]) <= int(value))
	elif(relate == "<>"):
		return rdd.filter(lambda row: int(row[col]) != int(value))
	elif(relate == "LIKE"):
		return rdd.filter(lambda row: re.match(value, row[col]) != None)
	elif(relate == "IN"):
		#TODO
		return rdd
	else:
		return rdd


def groupRDDAction(selectAggregated, havingAggregated, groupColumns, tableName, curr):
	cols = [columnNumbers[tableName].index(x) for x in groupColumns]

	aggSelectColumn = columnNumbers[tableName].index(selectAggregated[1])
	aggHavingColumn = columnNumbers[tableName].index(havingAggregated[1])

	mapped = curr.map(lambda row: ((*[row[i] for i in cols],), [row[aggSelectColumn], row[aggHavingColumn]]))
	
	selectAggregator = selectAggregated[0]
	havingAggregator = havingAggregated[0]

	if (selectAggregator == "AVG"):
		grouped = mapped.aggregateByKey((0,0), lambda acc, val: (int(acc[0]) + int(val[0]), int(acc[1]) + 1), lambda acc1, acc2: (int(acc1[0]) + int(acc2[0]), int(acc1[1]) + int(acc2[1])))
		grouped = grouped.map(lambda q: (q[0], trunc((1.0*q[1][0])/q[1][1])), 3)
	elif (selectAggregator == "SUM"):
		grouped = mapped.aggregateByKey(0, lambda acc, val: acc + val[0], lambda acc1, acc2: acc1 + acc2)
		grouped = grouped.map(lambda q: (q[0], q[1]))
	elif (selectAggregator == "MAX"):
		grouped = mapped.aggregateByKey(0, lambda acc, val: max(int(acc), int(val[0])), lambda acc1, acc2: max(int(acc1), int(acc2)))
	elif (selectAggregator == "MIN"):
		grouped = mapped.aggregateByKey(10000000000, lambda acc, val: min(int(acc), int(val[0])), lambda acc1, acc2: min(int(acc1), int(acc2)))
	elif (selectAggregator == "COUNT"):
		grouped = mapped.aggregateByKey(tuple(), lambda acc, val: acc + (val[0], ), lambda acc1, acc2: acc1 + acc2)
		grouped = grouped.map(lambda q: (q[0], len(set(q[1]))))


	if (havingAggregator == "AVG"):
		controlled = mapped.aggregateByKey((0,0), lambda acc, val: (int(acc[0]) + int(val[1]), int(acc[1]) + 1), lambda acc1, acc2: (int(acc1[0]) + int(acc2[0]), int(acc1[1]) + int(acc2[1])))
		controlled = controlled.map(lambda q: (q[0], trunc((1.0*q[1][0])/q[1][1])), 3)
	elif (havingAggregator == "SUM"):
		controlled = mapped.aggregateByKey(0, lambda acc, val: acc + val[1], lambda acc1, acc2: acc1 + acc2)
		controlled = controlled.map(lambda q: (q[0], q[1]))
	elif (havingAggregator == "MAX"):
		controlled = mapped.aggregateByKey(0, lambda acc, val: max(int(acc), int(val[1])), lambda acc1, acc2: max(int(acc1), int(acc2)))
	elif (selectAggregator == "MIN"):
		controlled = mapped.aggregateByKey(10000000000, lambda acc, val: min(int(acc), int(val[1])), lambda acc1, acc2: min(int(acc1), int(acc2)))
	elif (havingAggregator == "COUNT"):
		controlled = mapped.aggregateByKey(tuple(), lambda acc, val: acc + (val[1], ), lambda acc1, acc2: acc1 + acc2)
		controlled = controlled.map(lambda q: (q[0], len(set(q[1]))))
	else:
		return grouped.map(lambda q: [x for x in q[0]] + [x for x in q[1:]])


	relate = havingAggregated[2]
	if(relate == "="): 
		controlled = controlled.filter(lambda q: float(q[1]) == float(havingAggregated[3]))
	elif(relate == ">"):
		controlled = controlled.filter(lambda q: float(q[1]) > float(havingAggregated[3]))
	elif(relate == "<"):
		controlled = controlled.filter(lambda q: float(q[1]) < float(havingAggregated[3]))
	elif(relate == ">="):
		controlled = controlled.filter(lambda q: float(q[1]) >= float(havingAggregated[3]))
	elif(relate == "<="):
		controlled = controlled.filter(lambda q: float(q[1]) <= float(havingAggregated[3]))
	elif(relate == "<>"):
		controlled = controlled.filter(lambda q: float(q[1]) != float(havingAggregated[3]))

	grouped = grouped.join(controlled)

	return grouped.map(lambda q: [x for x in q[0]] + [x for x in q[1][:1]])


def selectRDDAction( columnNames, tableName, rdd ):
	def selectColumnValues (row):
		fin = []
		for column in columnNames:
			col = columnNumbers[tableName].index(column) 
			fin.append(row[col])
		return fin
	return rdd.map(selectColumnValues)
	

def joinRDDAction(sql, selectColumns, rdd1, rdds):
	table1 = getFromTableName(sql)
	table2 = sql[sql.find("JOIN")+5:].split(" ")[0]

	table1ColumnON = getJoinColumns(sql, table1)
	table2ColumnON = getJoinColumns(sql, table2)

	selectColumns1 = getSelectJoinColumns(table1, selectColumns)
	selectColumns2 = getSelectJoinColumns(table2, selectColumns)

	print(table1ColumnON, table2ColumnON, selectColumns1, selectColumns2, selectColumns)

	rdd2 = fromRDDAction(table2, rdds)

	mapped1 = 

	return

def getJoinColumns(sql, tableName):
	sql = sql[sql.find("ON") + 3:]
	return sql[sql.find(tableName+".")+len(tableName+"."):].split()[0]

def getSelectJoinColumns(tableName, selectColumns):
	columns = []
	for element in selectColumns:
		if tableName in element:
			columns.append(element[element.find(".")+1:])

	return columns