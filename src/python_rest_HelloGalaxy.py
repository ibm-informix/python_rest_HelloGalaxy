##
# Python Sample Application: Connection to Informix using REST
##

# Topics
# 1 Data Structures
# 1.1 Create collection
# 1.2 Create table
# 2 Inserts
# 2.1 Insert a single document into a collection 
# 2.2 Insert multiple documents into a collection 
# 3 Queries
# 3.1 Find one document in a collection 
# 3.2 Find documents in a collection 
# 3.3 Find all documents in a collection 
# 3.4 Count documents in a collection 
# 3.5 Order documents in a collection 
# 3.6 Find distinct fields in a collection 
# 3.7 Joins
# 3.7a Collection-Collection join
# 3.7b Table-Collection join
# 3.7c Table-Table join 
# 3.8 Modifying batch size 
# 3.9 Find with projection clause 
# 4 Update documents in a collection 
# 5 Delete documents in a collection 
# 6 SQL passthrough 
# 7 Transactions
# 8 Catalog
# 8.1 Collections + relational tables
# 8.2 Collections + relational tables + system tables
# 9 Commands
# 8.1 CollStats 
# 8.2 DBStats 
# 9 Drop a collection


import json
import os
import requests
from flask import Flask, render_template

app = Flask(__name__)
baseUrl = "http://username:password@yoururl:port"
database = "dbTest"
port = int(os.getenv('VCAP_APP_PORT', 8080))
baseDbUrl=baseUrl + "/" + database
collectionName = "pythonREST"
commands = []

# parsing vcap services
def parseVCAP():
    global database
    global url
    altadb = json.loads(os.environ['VCAP_SERVICES'])['altadb-dev'][0]
    credentials = altadb['credentials']
    ssl = False
    if ssl == True:
        url = credentials['ssl_rest_url']
    else:
        url = credentials['rest_url']

class City:
    def __init__(self, name, population, longitude, latitude, countryCode):
        self.name = name
        self.population = population
        self.longitude = longitude
        self.latitude = latitude
        self.countryCode = countryCode
    def toJSON(self):
        return json.loads("{\"name\" : \"%s\" , \"population\" : %d , \"longitude\" : %.4f , \"latitude\" : %.4f , \"countryCode\" : %d}"
                      %(self.name, self.population, self.longitude, self.latitude, self.countryCode))
        
kansasCity = City("Kansas City", 467007, 39.0997, 94.5783, 1)
seattle = City("Seattle", 652405, 47.6097, 122.3331, 1)
newYork = City("New York", 8406000, 40.7127, 74.0059, 1)
london = City("London", 8308000, 51.5072, 0.1275, 44)
tokyo = City("Tokyo", 13350000, 35.6833, -139.6833, 81)
madrid = City("Madrid", 3165000, 40.4000, 3.7167, 34)
melbourne = City("Melbourne", 4087000, -37.8136, -144.9631, 61)
sydney = City("Sydney", 4293000, -33.8650, -151.2094, 61)


def printError(message, reply):
    commands.append("Error: " + message)
    commands.append("status code: " + str(reply.status_code))
    commands.append("content: " + str(reply.content))

def doEverything():
    collectionName = "pythonRESTGalaxy"
    joinCollectionName = "pyRESTJoin"
    codeTableName = "codeTable"
    cityTableName = "cityTable"
    
    commands.append("# 1 Data Structures")
    commands.append( "# 1.1 Create Collection")
    data = json.dumps({"name": collectionName})
    reply = requests.post(baseDbUrl, data)
    if reply.status_code == 200:
        doc = reply.json()
        commands.append("Created collection")
    else:
        printError("Unable to create collection", reply)
         
    data = json.dumps({"name": joinCollectionName})
    reply = requests.post(baseDbUrl, data)
    if reply.status_code == 200:
        doc = reply.json()
        commands.append("Created collection")
    else:
        printError("Unable to create collection", reply)
     
    commands.append("# 1.2 Create Table")
    data = json.dumps({"create" : codeTableName, "columns":[{"name":"countryCode","type":"int"},
                                                                        {"name": "countryName", "type": "varchar(50)"}]})
     
    reply = requests.post(baseDbUrl + "/cmd", data)
    if reply.status_code == 200:
        doc = reply.json()
        commands.append("Created Table: " + codeTableName)
    else:
        printError("Unable to create table", reply)
     
    data= json.dumps({"create" : cityTableName, "columns":[{"name":"name","type":"varchar(50)"},
                                                               {"name": "population", "type": "int"}, 
                                                               {"name": "longitude", "type": "decimal(8,4)"},
                                                               {"name": "latitude", "type": "decimal(8,4)"}, 
                                                               {"name": "countryCode", "type": "int"}]})
     
    reply = requests.post(baseDbUrl + "/cmd", data)
    if reply.status_code == 200:
        doc = reply.json()
        commands.append("Created Table: " + cityTableName)
    else:
        printError("Unable to create table", reply)
         
    commands.append("# 1 Inserts")
    commands.append( "# 1.1 Insert a single document to a collection")
    data = json.dumps(kansasCity.toJSON())
    reply = requests.post(baseDbUrl + "/" + collectionName, data)
    if reply.status_code == 200:
        doc = reply.json()
        commands.append("Inserted " + str(doc.get('n')) + " document")
    else:
        printError("Unable to insert document", reply)
         
    reply = requests.post(baseDbUrl + "/" + cityTableName, data)
    if reply.status_code == 200:
        doc = reply.json()
        commands.append("Inserted " + str(doc.get('n')) + " document")
    else:
        printError("Unable to insert document", reply)
     
    commands.append("# 1.2 Insert multiple documents to a collection")
    data = json.dumps([seattle.toJSON(), newYork.toJSON(), london.toJSON(), tokyo.toJSON(), madrid.toJSON()])
    reply = requests.post(baseDbUrl + "/" + collectionName, data)
    if reply.status_code == 202:
        doc = reply.json()
        commands.append("Inserted " + str(doc.get('n')) + " documents")
    else:
        printError("Unable to insert multiple documents", reply)
         
    data = json.dumps([seattle.toJSON(), newYork.toJSON(), london.toJSON(), tokyo.toJSON(), madrid.toJSON()])
    reply = requests.post(baseDbUrl + "/" + cityTableName, data)
    if reply.status_code == 202:
        doc = reply.json()
        commands.append("Inserted " + str(doc.get('n')) + " documents")
    else:
        printError("Unable to insert multiple documents", reply)
 
    commands.append("# 2 Queries")
    commands.append("# 2.1 Find a document in a collection that matches a query condition")
    query = json.dumps({"longitude": {"$gt" : 40.0}})
    reply = requests.get(baseDbUrl + "/" + collectionName + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        commands.append("Query result: " + str(docs[0]))
    else:
        printError("Unable to query documents in collection", reply)
            
    commands.append("# 2.2 Find all documents in a collection that match a query condition")
    query = json.dumps({"longitude": {"$gt" : 40.0}})
    reply = requests.get(baseDbUrl + "/" + collectionName + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        commands.append("Query result: ")
        for doc in docs:
            commands.append(str(doc))
    else:
        printError("Unable to query documents in collection", reply)
        
    commands.append("# 2.3 Find all documents in a collection")
    reply = requests.get(baseDbUrl+ "/" + collectionName)
    if reply.status_code == 200:
        docs = reply.json()
        commands.append("query result: ")
        for doc in docs:
            commands.append(str(doc))
    else:
        printError("Unable to query documents in collection", reply)
     
    commands.append("# 2.4 Count documents in a collection")
    cmd = "$cmd"
    query = json.dumps({"count": collectionName, "query": {"longitude": {"$lt" : 40.0}}})
    reply = requests.get(baseDbUrl + "/" + cmd + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        commands.append("Number of documents match query: " +str(int(docs[0].get('n'))))
    else:
        printError("Unable to count documents in collection", reply)
     
    commands.append("# 2.5 Order documents in a collection")
    sort = json.dumps({"population": 1})
    reply = requests.get(baseDbUrl + "/" + collectionName + "?sort=" + sort)
    if reply.status_code == 200:
        docs = reply.json()
        commands.append("Sorted result: ")
        for doc in docs:
            commands.append(str(doc))
    else:
        printError("Unable to sort documents in collection", reply)
         
    commands.append("# 2.6 Find distinct documents in a collection")
    cmd = "$cmd"
    query = json.dumps({"distinct": collectionName, "key": "countryCode", "query": {"longitude": {"$lt" : 40.0}}})
    reply = requests.get(baseDbUrl + "/" + cmd + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        commands.append("Number of documents match query: " + str(docs))
    else:
        printError("Unable to count documents in collection", reply)
     
    commands.append("# 2.7 Join collection")
    data = json.dumps([{"countryCode" : 1, "countryName" : "United States of America"}, 
                       {"countryCode" : 44, "countryName" : "United Kingdom"},
                       {"countryCode" : 81, "countryName" : "Japan"},
                       {"countryCode" : 34, "countryName" : "Spain"},
                       {"countryCode" : 61, "countryName" : "Australia"}])
    reply = requests.post(baseDbUrl + "/" + codeTableName, data)
    if reply.status_code == 202:
        doc = reply.json()
        commands.append("Inserted " + str(doc.get('n')) + " document")
    else:
        printError("Unable to insert documents", reply)
     
    reply = requests.post(baseDbUrl + "/" + joinCollectionName, data)
    if reply.status_code == 202:
        doc = reply.json()
        commands.append("Inserted " + str(doc.get('n')) + " document")
    else:
        printError("Unable to insert documents", reply)
     
    commands.append("# 2.7a Join collection-collection")
    query = json.dumps({"$collections": {collectionName: {"$project": {"name" : 1, "population" : 1, "longitude": 1, "latitude" : 1}},
                                         joinCollectionName: {"$project": {"countryCode" : 1, "countryName" : 1}}}, 
                        "$condition" : {"pythonRESTGalaxy.countryCode" : "pyRESTJoin.countryCode"}})
    reply = requests.get(baseDbUrl + "/" + "system.join" + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        commands.append("Join collection-collection: ")
        for doc in docs:
            commands.append(str(doc))
    else:
        printError("Unable to join collections", reply)
         
    commands.append("# 2.7b Join table-collection")
    query = json.dumps({"$collections": {collectionName: {"$project": {"name" : 1, "population" : 1, "longitude": 1, "latitude" : 1}},
                                         codeTableName: {"$project": {"countryCode" : 1, "countryName" : 1}}}, 
                        "$condition" : {"pythonRESTGalaxy.countryCode" : "codeTable.countryCode"}})
    reply = requests.get(baseDbUrl + "/" + "system.join" + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        commands.append("Join collection-collection: ")
        for doc in docs:
            commands.append(str(doc))
    else:
        printError("Unable to join collections", reply)
         
    commands.append("# 2.7a Join table-table")
    query = json.dumps({"$collections": {cityTableName: {"$project": {"name" : 1, "population" : 1, "longitude": 1, "latitude" : 1}},
                                         codeTableName: {"$project": {"countryCode" : 1, "countryName" : 1}}}, 
                        "$condition" : {"cityTable.countryCode" : "codeTable.countryCode"}})
    reply = requests.get(baseDbUrl + "/" + "system.join" + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        commands.append("Join table-table: ")
        for doc in docs:
            commands.append(str(doc))
    else:
        printError("Unable to join collections", reply)
         
    commands.append("#2.8 Batch Size")
    batchSize = 2
    reply = requests.get(baseDbUrl + "/" + collectionName + "?batchsize=" + str(batchSize))
    if reply.status_code == 200:
        docs = reply.json()
        commands.append("New batch size: " + str(batchSize))
    else:
        printError("Unable to change batch size", reply)
     
    commands.append("# 2.9 Find all documents in a collection with projection")
    query = json.dumps({"longitude": {"$gt" : 40.0}})
    projection = json.dumps({"name" : 1, "population": 1, "_id": 0})
    reply = requests.get(baseDbUrl + "/" + collectionName + "?query=" + query + "&fields=" + projection)
    if reply.status_code == 200:
        docs = reply.json()
        commands.append("Query result: ")
        for doc in docs:
            commands.append(str(doc))
    else:
        printError("Unable to query documents in collection", reply)   
        
    commands.append("# 4 Update documents in a collection")
    query = json.dumps({'name': seattle.name})
    data = json.dumps({'$set' : {'countryCode' : 999} })
    reply = requests.put(baseDbUrl + "/" + collectionName + "?query=" + query, data)
    if reply.status_code == 200:
        doc = reply.json()
        commands.append("Updated " + str(doc.get('n')) + " documents")
    else:
        printError("Unable to update documents in collection", reply)
        
    commands.append("# 5 Delete documents in a collection")
    query = json.dumps({'name': tokyo.name})
    reply = requests.delete(baseDbUrl + "/" + collectionName + "?query=" + query)
    if reply.status_code == 200:
        doc = reply.json()
        commands.append("Deleted " + str(doc.get('n')) + " documents")
    else:
        printError("Unable to delete documents in collection", reply)      
         
    commands.append("# 6 SQL Passthrough")
    query = json.dumps({'$sql': "create table if not exists town (name varchar(255), countryCode int)"})
    reply = requests.get(baseDbUrl + "/" + "system.sql"+ "?query=" + query)
    if reply.status_code == 200:
        doc = reply.json()
        commands.append("Created table")
    else:
        printError("Unable to create table with sql passthrough", reply)
         
    query = json.dumps({"$sql": "insert into town values ('Lawrence', 1)"})
    reply = requests.get(baseDbUrl + "/" + "system.sql"+ "?query=" + query)
    if reply.status_code == 200:
        doc = reply.json()
        commands.append("Inserted " + str(doc) + " document")
    else:
        printError("Unable to insert with sql passthrough", reply)
         
    query = json.dumps({'$sql': "drop table town"})
    reply = requests.get(baseDbUrl + "/" + "system.sql"+ "?query=" + query)
    if reply.status_code == 200:
        doc = reply.json()
        commands.append("Dropped table")
    else:
        printError("Unable to drop table with sql passthrough", reply)
         
    commands.append("# 7 Transactions")
    cmd = "$cmd"
    query = json.dumps({"transaction": "enable"})
    reply = requests.get(baseDbUrl + "/" + cmd + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        commands.append("Transactions enabled")
    else:
        printError("Unable to enable transactions", reply)  
     
    data = json.dumps(melbourne.toJSON())
    reply = requests.post(baseDbUrl + "/" + collectionName, data)
    if reply.status_code == 200:
        doc = reply.json()
        commands.append("Inserted " + str(doc.get('n')) + " document")
    else:
        printError("Unable to insert document", reply)
         
    query = json.dumps({"transaction": "commit"})
    reply = requests.get(baseDbUrl + "/" + cmd + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        commands.append("Transactions committed")
    else:
        printError("Unable to commit transactions", reply) 
         
    data = json.dumps(sydney.toJSON())
    reply = requests.post(baseDbUrl + "/" + collectionName, data)
    if reply.status_code == 200:
        doc = reply.json()
        commands.append("Inserted " + str(doc.get('n')) + " document")
    else:
        printError("Unable to insert document", reply)
     
    query = json.dumps({"transaction": "rollback"})
    reply = requests.get(baseDbUrl + "/" + cmd + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        commands.append("Transactions rolled back")
    else:
        printError("Unable to roll back transactions", reply)   
     
    reply = requests.get(baseDbUrl+ "/" + collectionName)
    if reply.status_code == 200:
        docs = reply.json()
        commands.append("query result: ")
        for doc in docs:
            commands.append(str(doc))
    else:
        printError("Unable to query documents in collection", reply)   
         
    query = json.dumps({"transaction": "disable"})
    reply = requests.get(baseDbUrl + "/" + cmd + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        commands.append("Transactions disabled")
    else:
        printError("Unable to disable transactions", reply)  
     
    commands.append("# 8 Catalog")
    commands.append("# 8.1 Relational Tables")
    option = "?options="
    query = json.dumps({"includeRelational": True})
    reply = requests.get(baseDbUrl + "/" + option + query)
    if reply.status_code == 200:
        docs = reply.json()
        commands.append("Catalog " + str(docs))
    else:
        printError("Unable to display relational tables", reply) 
         
    commands.append("# 8.2 Relational Tables + System Tables")
    option = "?options="
    query = json.dumps({"includeRelational": True, "includeSystem" : True})
    reply = requests.get(baseDbUrl + "/" + option + query)
    if reply.status_code == 200:
        docs = reply.json()
        commands.append("Catalog " + str(docs))
    else:
        printError("Unable to display relational and system tables", reply) 
         
    commands.append("# 9 Commands")
    query = json.dumps({"collstats": collectionName})
    reply = requests.get(baseDbUrl + "/" + cmd + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        commands.append("Collection stats " + str(docs))
    else:
        printError("Unable to display collection stats", reply)
         
    commands.append("# 9 Commands")
    query = json.dumps({"dbstats": 1})
    reply = requests.get(baseDbUrl + "/" + cmd + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        commands.append("Database stats " + str(docs))
    else:
        printError("Unable to display database stats", reply)
         
    commands.append("# 10 Get a listing of collections")
    reply = requests.get(baseDbUrl)
    if reply.status_code == 200:
        doc = reply.json()
        dbList = ""
        for db in doc:
            dbList += "\'" + db + "\' "
        commands.append("Collections: " + str(dbList))
    else:
        printError("Unable to retrieve collection listing", reply)
     
    commands.append("# 11 Drop a collection")
    reply = requests.delete(baseDbUrl + "/" + collectionName)
    if reply.status_code == 200:
        doc = reply.json()
        commands.append("Delete collection result: " + str(doc))
    else:
        printError("Unable to drop collection", reply)
        
    reply = requests.delete(baseDbUrl + "/" + joinCollectionName)
    if reply.status_code == 200:
        doc = reply.json()
        commands.append("Delete collection result: " + str(doc))
    else:
        printError("Unable to drop collection", reply)
        
    reply = requests.delete(baseDbUrl + "/" + codeTableName)
    if reply.status_code == 200:
        doc = reply.json()
        commands.append("Delete collection result: " + str(doc))
    else:
        printError("Unable to drop collection", reply) 
           
    reply = requests.delete(baseDbUrl + "/" + cityTableName)
    if reply.status_code == 200:
        doc = reply.json()
        commands.append("Delete collection result: " + str(doc))
        
    else:
        printError("Unable to drop collection", reply)
      
@app.route("/")
def displayPage():
    return render_template('index.html')

@app.route("/databasetest")
def printCommands():
    global commands
    commands = []
#     parseVCAP()
    doEverything()
    return render_template('tests.html', commands=commands)
 
if (__name__ == "__main__"):
    app.run(host='0.0.0.0', port=port)