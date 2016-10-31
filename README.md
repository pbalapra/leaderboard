# leaderboard command line

#see requirements.txt for the required python packages
pip install -r requirements.txt

# help
python leaderboard.py --help


# start mongodb
mongod --dbpath <<path>>

#import the data in the json file to the mongodb
python leaderboard.py -i <<json_file_name>>

#query the results from mongodb for benchmark_name
python leaderboard.py -q <<benchmark_name>>
