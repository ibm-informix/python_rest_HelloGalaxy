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
# 9.1 CollStats 
# 9.2 DBStats 
# 10 List collections in a database
# 11 Drop a collection

import logging
import json
import os
import requests
from flask import Flask, render_template

app = Flask(__name__)

# To run locally, set URL. The url should include the database name.
# Otherwise, url and database information will be parsed from the Bluemix VCAP_SERVICES.
URL = ""

USE_SSL = False     # Set to True to use SSL url from VCAP_SERVICES
SERVICE_NAME = os.getenv('SERVICE_NAME', 'timeseriesdatabase')
port = int(os.getenv('VCAP_APP_PORT', 8080))

def getDatabaseUrl():
    """
    Get database url
    
    :returns: (url)
    """
    
    # Use defaults
    if URL :
        return URL
    
    # Parse database info from VCAP_SERVICES
    if (os.getenv('VCAP_SERVICES') is None):
        raise Exception("VCAP_SERVICES not set in the environment")
    vcap_services = json.loads(os.environ['VCAP_SERVICES'])
    try:
        tsdb = vcap_services[SERVICE_NAME][0]
        credentials = tsdb['credentials']
        if USE_SSL:
            url = credentials['rest_url_ssl']
        else:
            url = credentials['rest_url']
        return url
    except KeyError as e:
        logging.error(e)
        raise Exception("Error parsing VCAP_SERVICES. Key " + str(e) + " not found.")

         
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


def printError(output, message, reply):
    output.append("Error: " + message)
    output.append("status code: " + str(reply.status_code))
    output.append("content: " + str(reply.content))


def doEverything():
    # Get database connectivity information
    url = getDatabaseUrl()

    collectionName = "pythonRESTGalaxy"
    joinCollectionName = "pyRESTJoin"
    codeTableName = "codeTable"
    cityTableName = "cityTable"
    
    output = []
    output.append("# 1 Data Structures")
    output.append( "# 1.1 Create Collection")
    data = json.dumps({"name": collectionName})
    reply = requests.post(url, data)
    if reply.status_code == 200:
        doc = reply.json()
        output.append("Created collection")
    else:        
        printError(output, "Unable to create collection", reply)
         
    data = json.dumps({"name": joinCollectionName})
    reply = requests.post(url, data)
    if reply.status_code == 200:
        doc = reply.json()
        output.append("Created collection")
    else:
        printError(output, "Unable to create collection", reply)
     
    output.append("# 1.2 Create Table")
    data = json.dumps({"create" : codeTableName, "columns":[{"name":"countryCode","type":"int"},
                                                                        {"name": "countryName", "type": "varchar(50)"}]})
     
    reply = requests.get(url + "/$cmd", data)
    if reply.status_code == 200:
        doc = reply.json()
        output.append("Created Table: " + codeTableName)
    else:
        printError(output, "Unable to create table", reply)
     
    data= json.dumps({"create" : cityTableName, "columns":[{"name":"name","type":"varchar(50)"},
                                                               {"name": "population", "type": "int"}, 
                                                               {"name": "longitude", "type": "decimal(8,4)"},
                                                               {"name": "latitude", "type": "decimal(8,4)"}, 
                                                               {"name": "countryCode", "type": "int"}]})
     
    reply = requests.get(url + "/$cmd", data)
    if reply.status_code == 200:
        doc = reply.json()
        output.append("Created Table: " + cityTableName)
    else:
        printError(output, "Unable to create table", reply)
         
    output.append("# 2 Inserts")
    output.append( "# 2.1 Insert a single document to a collection")
    data = json.dumps(kansasCity.toJSON())
    reply = requests.post(url + "/" + collectionName, data)
    if reply.status_code == 200:
        doc = reply.json()
        output.append("Inserted " + str(doc.get('n')) + " document")
    else:
        printError(output, "Unable to insert document", reply)
         
    reply = requests.post(url + "/" + cityTableName, data)
    if reply.status_code == 200:
        doc = reply.json()
        output.append("Inserted " + str(doc.get('n')) + " document")
    else:
        printError(output, "Unable to insert document", reply)
     
    output.append("# 2.2 Insert multiple documents to a collection")
    data = json.dumps([seattle.toJSON(), newYork.toJSON(), london.toJSON(), tokyo.toJSON(), madrid.toJSON()])
    reply = requests.post(url + "/" + collectionName, data)
    if reply.status_code == 202:
        doc = reply.json()
        output.append("Inserted " + str(doc.get('n')) + " documents")
    else:
        printError(output, "Unable to insert multiple documents", reply)
         
    data = json.dumps([seattle.toJSON(), newYork.toJSON(), london.toJSON(), tokyo.toJSON(), madrid.toJSON()])
    reply = requests.post(url + "/" + cityTableName, data)
    if reply.status_code == 202:
        doc = reply.json()
        output.append("Inserted " + str(doc.get('n')) + " documents")
    else:
        printError(output, "Unable to insert multiple documents", reply)
 
    output.append("# 3 Queries")
    output.append("# 3.1 Find a document in a collection that matches a query condition")
    query = json.dumps({"longitude": {"$gt" : 40.0}})
    reply = requests.get(url + "/" + collectionName + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        output.append("Query result: " + str(docs[0]))
    else:
        printError(output, "Unable to query documents in collection", reply)
            
    output.append("# 3.2 Find all documents in a collection that match a query condition")
    query = json.dumps({"longitude": {"$gt" : 40.0}})
    reply = requests.get(url + "/" + collectionName + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        output.append("Query result: ")
        for doc in docs:
            output.append(str(doc))
    else:
        printError(output, "Unable to query documents in collection", reply)
        
    output.append("# 3.3 Find all documents in a collection")
    reply = requests.get(url+ "/" + collectionName)
    if reply.status_code == 200:
        docs = reply.json()
        output.append("query result: ")
        for doc in docs:
            output.append(str(doc))
    else:
        printError(output, "Unable to query documents in collection", reply)

    output.append("# 3.4 Count documents in a collection")
    cmd = "$cmd"
    query = json.dumps({"count": collectionName, "query": {"longitude": {"$lt" : 40.0}}})
    reply = requests.get(url + "/" + cmd + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        output.append("Count query result: " + str(docs[0]))
    else:
        printError(output, "Unable to count documents in collection", reply)

    output.append("# 3.5 Order documents in a collection")
    sort = json.dumps({"population": 1})
    reply = requests.get(url + "/" + collectionName + "?sort=" + sort)
    if reply.status_code == 200:
        docs = reply.json()
        output.append("Sorted result: ")
        for doc in docs:
            output.append(str(doc))
    else:
        printError(output, "Unable to sort documents in collection", reply)
          
    output.append("# 3.6 Find distinct values in a collection")
    cmd = "$cmd"
    query = json.dumps({"distinct": collectionName, "key": "countryCode", "query": {"longitude": {"$lt" : 40.0}}})
    reply = requests.get(url + "/" + cmd + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        output.append("Distinct values: ")
        for doc in docs:
            output.append(str(doc))
    else:
        output.append("error")
        printError(output, "Unable to find distinct values in collection", reply)
      
    output.append("# 3.7 Join collection")
    data = json.dumps([{"countryCode" : 1, "countryName" : "United States of America"}, 
                       {"countryCode" : 44, "countryName" : "United Kingdom"},
                       {"countryCode" : 81, "countryName" : "Japan"},
                       {"countryCode" : 34, "countryName" : "Spain"},
                       {"countryCode" : 61, "countryName" : "Australia"}])
    reply = requests.post(url + "/" + codeTableName, data)
    if reply.status_code == 202:
        doc = reply.json()
        output.append("Inserted " + str(doc.get('n')) + " document")
    else:
        printError(output, "Unable to insert documents", reply)
            
    reply = requests.post(url + "/" + joinCollectionName, data)
    if reply.status_code == 202:
        doc = reply.json()
        output.append("Inserted " + str(doc.get('n')) + " document")
    else:
        printError(output, "Unable to insert documents", reply)
      
    output.append("# 3.7a Join collection-collection")
    query = json.dumps({"$collections": {collectionName: {"$project": {"name" : 1, "population" : 1, "longitude": 1, "latitude" : 1}},
                                         joinCollectionName: {"$project": {"countryCode" : 1, "countryName" : 1}}}, 
                        "$condition" : {"pythonRESTGalaxy.countryCode" : "pyRESTJoin.countryCode"}})
    reply = requests.get(url + "/" + "system.join" + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        output.append("Join collection-collection: ")
        for doc in docs:
            output.append(str(doc))
    else:
        printError(output, "Unable to join collections", reply)
                 
    output.append("# 3.7b Join table-collection")
    query = json.dumps({"$collections": {collectionName: {"$project": {"name" : 1, "population" : 1, "longitude": 1, "latitude" : 1}},
                                         codeTableName: {"$project": {"countryCode" : 1, "countryName" : 1}}}, 
                        "$condition" : {"pythonRESTGalaxy.countryCode" : "codeTable.countryCode"}})
    reply = requests.get(url + "/" + "system.join" + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        output.append("Join collection-collection: ")
        for doc in docs:
            output.append(str(doc))
    else:
        printError(output, "Unable to join collections", reply)
                   
    output.append("# 3.7c Join table-table")
    query = json.dumps({"$collections": {cityTableName: {"$project": {"name" : 1, "population" : 1, "longitude": 1, "latitude" : 1}},
                                         codeTableName: {"$project": {"countryCode" : 1, "countryName" : 1}}}, 
                        "$condition" : {"cityTable.countryCode" : "codeTable.countryCode"}})
    reply = requests.get(url + "/" + "system.join" + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        output.append("Join table-table: ")
        for doc in docs:
            output.append(str(doc))
    else:
        printError(output, "Unable to join collections", reply)
             
    output.append("#3.8 Batch Size")
    batchSize = 2
    reply = requests.get(url + "/" + collectionName + "?batchsize=" + str(batchSize))
    if reply.status_code == 200:
        docs = reply.json()
        output.append("New batch size: " + str(batchSize))
    else:
        printError(output, "Unable to change batch size", reply)
         
    output.append("# 3.9 Find all documents in a collection with projection")
    query = json.dumps({"longitude": {"$gt" : 40.0}})
    projection = json.dumps({"name" : 1, "population": 1, "_id": 0})
    reply = requests.get(url + "/" + collectionName + "?query=" + query + "&fields=" + projection)
    if reply.status_code == 200:
        docs = reply.json()
        output.append("Query result: ")
        for doc in docs:
            output.append(str(doc))
    else:
        printError(output, "Unable to query documents in collection", reply)   
           
    output.append("# 4 Update documents in a collection")
    query = json.dumps({'name': seattle.name})
    data = json.dumps({'$set' : {'countryCode' : 999} })
    reply = requests.put(url + "/" + collectionName + "?query=" + query, data)
    if reply.status_code == 200:
        doc = reply.json()
        output.append("Updated " + str(doc.get('n')) + " documents")
    else:
        printError(output, "Unable to update documents in collection", reply)
           
    output.append("# 5 Delete documents in a collection")
    query = json.dumps({'name': tokyo.name})
    reply = requests.delete(url + "/" + collectionName + "?query=" + query)
    if reply.status_code == 200:
        doc = reply.json()
        output.append("Deleted " + str(doc.get('n')) + " documents")
    else:
        printError(output, "Unable to delete documents in collection", reply)      
            
    output.append("# 6 SQL Passthrough")
    query = json.dumps({'$sql': "create table if not exists town (name varchar(255), countryCode int)"})
    reply = requests.get(url + "/" + "system.sql"+ "?query=" + query)
    if reply.status_code == 200:
        doc = reply.json()
        output.append("Created table")
    else:
        printError(output, "Unable to create table with sql passthrough", reply)
            
    query = json.dumps({"$sql": "insert into town values ('Lawrence', 1)"})
    reply = requests.get(url + "/" + "system.sql"+ "?query=" + query)
    if reply.status_code == 200:
        doc = reply.json()
        output.append("Inserted " + str(doc) + " document")
    else:
        printError(output, "Unable to insert with sql passthrough", reply)
            
    query = json.dumps({'$sql': "drop table town"})
    reply = requests.get(url + "/" + "system.sql"+ "?query=" + query)
    if reply.status_code == 200:
        doc = reply.json()
        output.append("Dropped table")
    else:
        printError(output, "Unable to drop table with sql passthrough", reply)
            
    output.append("# 7 Transactions")
    cmd = "$cmd"
    query = json.dumps({"transaction": "enable"})
    reply = requests.get(url + "/" + cmd + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        output.append("Transactions enabled")
    else:
        printError(output, "Unable to enable transactions", reply)  
       
    data = json.dumps(melbourne.toJSON())
    reply = requests.post(url + "/" + collectionName, data)
    if reply.status_code == 200:
        doc = reply.json()
        output.append("Inserted " + str(doc.get('n')) + " document")
    else:
        printError(output, "Unable to insert document", reply)
           
    query = json.dumps({"transaction": "commit"})
    reply = requests.get(url + "/" + cmd + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        output.append("Transactions committed")
    else:
        printError(output, "Unable to commit transactions", reply) 
            
    data = json.dumps(sydney.toJSON())
    reply = requests.post(url + "/" + collectionName, data)
    if reply.status_code == 200:
        doc = reply.json()
        output.append("Inserted " + str(doc.get('n')) + " document")
    else:
        printError(output, "Unable to insert document", reply)
        
    query = json.dumps({"transaction": "rollback"})
    reply = requests.get(url + "/" + cmd + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        output.append("Transactions rolled back")
    else:
        printError(output, "Unable to roll back transactions", reply)   
        
    reply = requests.get(url+ "/" + collectionName)
    if reply.status_code == 200:
        docs = reply.json()
        output.append("query result: ")
        for doc in docs:
            output.append(str(doc))
    else:
        printError(output, "Unable to query documents in collection", reply)   
            
    query = json.dumps({"transaction": "disable"})
    reply = requests.get(url + "/" + cmd + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        output.append("Transactions disabled")
    else:
        printError(output, "Unable to disable transactions", reply)  
       
    output.append("# 8 Catalog")
    output.append("# 8.1 Relational Tables")
    option = "?options="
    query = json.dumps({"includeRelational": True})
    reply = requests.get(url + "/" + option + query)
    if reply.status_code == 200:
        docs = reply.json()
        output.append("Catalog " + str(docs))
    else:
        printError(output, "Unable to display relational tables", reply) 
           
    output.append("# 8.2 Relational Tables + System Tables")
    option = "?options="
    query = json.dumps({"includeRelational": True, "includeSystem" : True})
    reply = requests.get(url + "/" + option + query)
    if reply.status_code == 200:
        docs = reply.json()
        output.append("Catalog " + str(docs))
    else:
        printError(output, "Unable to display relational and system tables", reply) 
         
    output.append("# 9 output")
    output.append("# 9.1 collstats command")
    query = json.dumps({"collstats": collectionName})
    reply = requests.get(url + "/" + cmd + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        output.append("Collection stats " + str(docs))
    else:
        printError(output, "Unable to display collection stats", reply)
          
    output.append("# 9.1 dbstats command")
    query = json.dumps({"dbstats": 1})
    reply = requests.get(url + "/" + cmd + "?query=" + query)
    if reply.status_code == 200:
        docs = reply.json()
        output.append("Database stats " + str(docs))
    else:
        printError(output, "Unable to display database stats", reply)
         
    output.append("# 10 Get a listing of collections")
    reply = requests.get(url)
    if reply.status_code == 200:
        doc = reply.json()
        dbList = ""
        for db in doc:
            dbList += "\'" + db + "\' "
        output.append("Collections: " + str(dbList))
    else:
        printError(output, "Unable to retrieve collection listing", reply)
     
    output.append("# 11 Drop a collection")
    reply = requests.delete(url + "/" + collectionName)
    if reply.status_code == 200:
        doc = reply.json()
        output.append("Delete collection result: " + str(doc))
    else:
        printError(output, "Unable to drop collection", reply)
        
    reply = requests.delete(url + "/" + joinCollectionName)
    if reply.status_code == 200:
        doc = reply.json()
        output.append("Delete collection result: " + str(doc))
    else:
        printError(output, "Unable to drop collection", reply)
        
    reply = requests.delete(url + "/" + codeTableName)
    if reply.status_code == 200:
        doc = reply.json()
        output.append("Delete collection result: " + str(doc))
    else:
        printError(output, "Unable to drop collection", reply) 
           
    reply = requests.delete(url + "/" + cityTableName)
    if reply.status_code == 200:
        doc = reply.json()
        output.append("Delete collection result: " + str(doc))
        
    else:
        printError(output, "Unable to drop collection", reply)
    
    return output
      
@app.route("/")
def displayPage():
    return render_template('index.html')

@app.route("/databasetest")
def runSample():
    output = []
    try:
        output = doEverything()
    except Exception as e:
        logging.exception(e) 
        output.append("EXCEPTION (see log for details): " + str(e))
    return render_template('tests.html', output=output)
 
if (__name__ == "__main__"):
    app.run(host='0.0.0.0', port=port)
