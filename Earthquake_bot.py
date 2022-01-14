import sqlite3
import requests
import json
from requests_oauthlib import OAuth1
from datetime import datetime
import time

#Function for finding latest earthquake by comparing timestamps. Takes a list
#of earthquake data from database as argument and returns a parameter suitable
#for query against the USGS earthquake API.
def FindLatest(eql):
    times = []
    for quakes in eql:
        times.append(quakes['Time'])
    #Return value of the function will be time of latest earthquake + 1 second
    #to allow the system to find any earthquake after the latest one
    ts = int(max(times))/1000+1
    #Return value will be in format "YYYY-MM-DDTHH:MM:SS" (EX:"2020-03-04T11:21:00")
    return(datetime.utcfromtimestamp(ts).strftime('%Y-%m-%dT%H:%M:%S'))

def getquakes(time = None, address="https://earthquake.usgs.gov/fdsnws/event/1/query?",
              parameters="format=geojson&minmagnitude=4"):
    #Fetches list of earthquakes after set time, with chosen parameters
    #API returns GeoJSON format data. "Features" of earthquakes is then added
    #to list. Features are JSON
    quakes = list()
    if time != None:
        parameters += time
    response = requests.get(address+parameters)
    respjson = json.loads(response.text)
    if time == None:
        try:
            quakes.append(respjson['features'][0]['properties'])
            quakes.append(respjson['features'][0]['geometry']['coordinates'])
            return(quakes)
        except IndexError:
            return(quakes)
    else:
        try:
            for i in respjson['features']:
                quakes.append(i['properties'])
                quakes.append(i['geometry']['coordinates'])
            return quakes
        except IndexError:
            return(quakes)

#Creates a dict where words are key and number of times they occur
#is value.
def RankWords(ListToRank):
    WordDict = {}
    for i in ListToRank:
        Words = i.split()
        for x in Words:
            if x.startswith('@'):
                continue
            else:
                if x in WordDict:
                    WordDict[x] += 1
                elif x not in WordDict and len(x) > 3:
                    WordDict[x] = 1
    return WordDict

#Creates a list of top three occurring words from dict created by
#RankWords function
def GetTopThree(ListToRank):
    DictToRank = RankWords(ListToRank)
    BaseLine = list(DictToRank.values())
    Baseline = BaseLine.sort()
    TopThree = BaseLine[-3:]
    ReturnWords = []
    for x,y in DictToRank.items():
        if len(ReturnWords) < 3:
            if y in TopThree:
                ReturnWords.append(x)
    return ReturnWords

#Twitter API query(GET) URL
QTWURL = 'https://api.twitter.com/1.1/search/tweets.json?q=earthquake'

#Twitter API post(POST) URL
PTWURL = "https://api.twitter.com/1.1/statuses/update.json"

#OAUTH1 values for Twitter API
AUTH = OAuth1(u"NOT ACTUAL KEY",
              u"NOT ACTUAL SECRET",
              u"NOT ACTUAL TOKEN",
              u"NOT ACTUAL TOKEN SECRET")

#Connection for database
connection = sqlite3.connect("Earthquakes.db")
#Cursor for database connection
curs = connection.cursor()

#Overarching always true loop for automation
while True:

    #Retrieves all entries from the Earthquakes table
    result = curs.execute("""SELECT * FROM Earthquakes""")

    Earthquakes_list = []

    for i in result:
        Earthquakes_list.append({'ID':i[0],
                                 'Mag':i[1],
                                 'Coord':i[2],
                                 'Time':i[3],
                                 'Place':i[4],
                                 'Resolved':i[5]})

    #Code to execute if there are no entries in the Earthquakes table.
    #Queries the USGS API with no time to find latest earthquake.
    #Adds this to database
    if not len(Earthquakes_list):
        latestquake = getquakes()
        if len(latestquake):
            Earthquakes_list.append({'ID':latestquake[0]['ids'],
                                     'Mag':latestquake[0]['mag'],
                                     'Coord':latestquake[1],
                                     'Time':latestquake[0]['time'],
                                     'Place':latestquake[0]['place'],
                                     'Resolved':0})
            curs.execute("""INSERT INTO Earthquakes (ID,Magnitude,Coordinates,Time,Place,Resolved) VALUES (?,?,?,?,?,?)""",
            (str(latestquake[0]['ids']),float(latestquake[0]['mag']),str(latestquake[1]),int(latestquake[0]['time']),str(latestquake[0]['place']),0))
            connection.commit()

    #Queries the USGS earthquake API with time if there are entries in table.
    #Adds any new earthquakes to database
    else:
        latestquake = getquakes("&starttime="+FindLatest(Earthquakes_list))
        if len(latestquake):
            Earthquakes_list.append({'ID':latestquake[0]['ids'],
                                     'Mag':latestquake[0]['mag'],
                                     'Coord':latestquake[1],
                                     'Time':latestquake[0]['time'],
                                     'Place':latestquake[0]['place'],
                                     'Resolved':0})
            curs.execute("""INSERT INTO Earthquakes (ID,Magnitude,Coordinates,Time,Place,Resolved) VALUES (?,?,?,?,?,?)""",
            (str(latestquake[0]['ids']),float(latestquake[0]['mag']),str(latestquake[1]),int(latestquake[0]['time']),str(latestquake[0]['place']),0))
            connection.commit()

    #Takes all unresolved earthquakes for twitter query
    Twitter_Search_list = []
    for quake in Earthquakes_list:
        if quake['Resolved'] == 0:
            Twitter_Search_list.append(quake)

    #Empties Earthquakes_list in effort to reduce memory usage for the webhosting app
    del Earthquakes_list

    #Executes twitter query if there are any valid earthquakes in table
    if len(Twitter_Search_list):
        for parameters in Twitter_Search_list:
            #SEARCH twitter using time after earthquake, coordinates of earthquake
            #and recent results as parameters for query.
            TweTime = datetime.utcfromtimestamp(parameters['Time']/1000).strftime('%Y-%m-%d')
            URLcoords = parameters['Coord'][1:-2]
            URLcoords = URLcoords.split()
            print(URLcoords)
            url = QTWURL+'%20since%3A'+str(TweTime)+'&geocode='+URLcoords[1]+URLcoords[0]+str(float(URLcoords[2])*1000)+'km'+'&result_type=recent'
            print(url)
            Resp = requests.get(url, auth=AUTH)
            RespJson = json.loads(Resp.text)
            Tweets_list = RespJson['statuses']

            #Executes a POST request if there are more than 15 tweets found.
            #Status is updated in the format found in the ToTweet variable
            if len(Tweets_list) >= 15:
                text = []
                for tweets in Tweets_list:
                    text.extend(str(tweets['text']).split())

                ToTweet = GetTopThree(text)
                Tweet = "Earthquake of magnitude {} found in: \' {} \'. Described as \'{}\', \'{}\', \'{}\'".format(parameters['Mag'],
                parameters['Place'],ToTweet[0],ToTweet[1],ToTweet[2])
                print(Tweet)
                DataTweet = {'status': Tweet}
                status = requests.post("https://api.twitter.com/1.1/statuses/update.json", data=DataTweet, auth=AUTH)

                curs.execute("""UPDATE Earthquakes SET Resolved=? WHERE ID=?""",(1,parameters['ID']))
                connection.commit()

    #Sleeps system for 5 minutes upon completion of loop
    time.sleep(300)
