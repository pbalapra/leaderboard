# -*- coding: utf-8 -*-
import sys
import getopt
import json
from pymongo import MongoClient
from pymongo import DESCENDING, ASCENDING


'''Allow insertion only for the following valid benchmarks'''
VALID_BENCHMARKS = ['benchmark%d' % i for i in range(10)]
'''Name of the database'''
DBNAME = 'lboard'
"""MongoDB should run at the following URI"""
URI = 'mongodb://localhost:27017/'
"""JSON record should have the following keys"""
FIELDS = ['benchmark_name', 'user_name', 'method', 'software', 'score', 'metric']
""" Flag to clear the db; set to False in production mode """
CLEARDB = True

class LeaderBoard:
    """The main class for the leaderboard."""

    def __init__(self):
        """Initialize required vars for the leaderboard class."""
        self.db = None
        self.mgDbClient = None

    def openMongoDB(self, uri):
        """Initialize mongodb for the given uri."""
        if uri is None:
            print "uri name %s not found", uri
            sys.exit(2)
        try:
            self.mgDbClient = MongoClient(uri)
            self.db = self.mgDbClient[DBNAME]
        except:
            print "error in initializing MongoDB: %s", sys.exc_info()[0]

    def closeMongoDB(self):
        """Close mongodb."""
        try:
            self.mgDbClient.close()
        except:
            print "error in closing MongoDB: %s", sys.exc_info()[0]

    def clearDataBase(self, uri):
        """Clear the DBNAME database."""
        mgDbClient = MongoClient(uri)
        mgDbClient.drop_database(DBNAME)

    def clearTable(self, table_name):
        """Clear the table_name database."""
        self.db.drop_collection(table_name)

    def validateRecord(self, record):
        """validate the json record."""
        keys = record.keys()
        for key in keys:
            if key not in FIELDS:
                print "key %s is missing in JSON file"
                sys.exit(2)

    def readJasonFile(self, json_file_name):
        """Read the json file."""
        with open(json_file_name) as json_data:
            data = json.load(json_data)
            for record in data:
                if 'benchmark_name' in record.keys():
                    benchmarkName = record['benchmark_name']
                    if benchmarkName in VALID_BENCHMARKS:
                        self.db[benchmarkName].insert(record)
                    else:
                        print "benchmark name %s not found", benchmarkName
                else:
                    print "benchmark_name key is missing in JSON file"
                    sys.exit(2)

    def queryBenchmarkResults(self, benchmark_name, ascending):
        """Query the benchmark table for the given benchmark name."""
        if benchmark_name is not None:
            benchmarkData = self.db[benchmark_name]
            if ascending:
                result = list(benchmarkData.find().sort("score", ASCENDING))
            else:
                result = list(benchmarkData.find().sort("score", DESCENDING))
        return result

    def printResult(self, result):
        """print the result; the result should iterator data type (list)."""
        for i, record in enumerate(result):
            print i, '==>', record


if __name__ == '__main__':
    json_file_name = None
    benchmark_name = None

    """ Process the command line arguments """
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h:i:q:', ["help", "json_file_name=", "benchmark_name="])
    except getopt.GetoptError as err:
        print 'leaderboard.py -i <json_file_name> | -q <benchmark_name>'
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print 'leaderboard.py -i <json_file_name> -q <benchmark_name>'
            sys.exit()
        elif opt in ("-i", "--json_file_name"):
            json_file_name = str(arg)
        elif opt in ("-q", "--benchmark_name"):
            benchmark_name = str(arg)

    """ Initialize LeaderBoard class """
    lb = LeaderBoard()
    """ Open database connection """
    lb.openMongoDB(URI)

    """ Clear the db based on CLEARDB flag """
    if CLEARDB and json_file_name is not None:
        lb.clearDataBase(URI)
        lb.clearTable('benchmark1')

    """ Upload the data in the json file """
    if json_file_name is not None:
        lb.readJasonFile(json_file_name)

    """ Return the results for benchmark_name """
    if benchmark_name is not None:
        result = lb.queryBenchmarkResults(benchmark_name, 1)
        lb.printResult(result)

    """ Close database connection """
    lb.closeMongoDB()
