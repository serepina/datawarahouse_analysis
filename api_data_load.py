import psycopg2
import requests
import logging
import http.client as http_client
import json

try:
    import http.client as http_client
except ImportError:
    # Python 2
    import httplib as http_client
http_client.HTTPConnection.debuglevel = 0

genReqUrl = 'http://openapi.seoul.go.kr:8088/786775726f6a756235384856584867/json/bikeList/1/1000/'

data = requests.get(genReqUrl)
result = json.loads(data.text)
print(result)

rows = result["rentBikeStatus"]["row"]


def insertDB(schema,table,colume,data):
    sql = " INSERT INTO {schema}.\"{table}\" VALUES ({data}) ;".format(schema=schema,table=table,data=data)
    try:
        db = psycopg2.connect(host='localhost', dbname='public_bike_star_schema',user='postgres',password='0609',port=5432)
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()
    except Exception as e :
        print(" insert DB  ",e) 

def creat_station_table():

    for i in rows:
        stationId = i["stationId"]
        stationName = i["stationName"]
        rackTotCnt = i["rackTotCnt"]
        #parkingBikeTotCnt = i["parkingBikeTotCnt"]
        #shared = i["shared"]
        stationLatitude = i["stationLatitude"]
        stationLongitude = i["stationLongitude"]
        
        s = stationName.split('.')
        stationName = s[1].rstrip()
        stationId = s[0]

        d = '\'' + str(stationName) + '\',' + stationId + ',' + rackTotCnt + ',' +  stationLatitude + ',' + stationLongitude

        c = 'stationName, stationId, rackTotCnt, parkingBikeTotCnt, shared, stationLatitude, stationLongitude'
        insertDB(schema='star',table='STATION',colume = c, data=d)

def creat_station_state_table():

    for i in rows:
        stationName = i["stationName"]
        parkingBikeTotCnt = i["parkingBikeTotCnt"]
        shared = i["shared"]
        
        s = stationName.split('.')
        stationId = s[0]

        d = stationId + ',' + shared + ',' + parkingBikeTotCnt
        print(d)
        
        c = 'stationId, parkingBikeTotCnt, shared'
        insertDB(schema='star',table='STATION_STATE',colume = c, data=d)

#creat_station_table()
creat_station_state_table()