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

genReqUrl = 'http://openapi.seoul.go.kr:8088/786775726f6a756235384856584867/json/bikeList/1/10/'

data = requests.get(genReqUrl)
result = json.loads(data.text)
print(result)

rows = result["getStationOpenApiJson"]["row"]


def insertDB(schema,table,colume,data):
    sql = " INSERT INTO {schema}.\"{table}\" VALUES ({data}) ;".format(schema=schema,table=table,data=data)
    try:
        db = psycopg2.connect(host='localhost', dbname='public_bike_star_schema',user='postgres',password='0609',port=5432)
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()
    except Exception as e :
        print(" insert DB  ",e) 

def creat_station_table(): # api를 통해 얻은 자전거 거치대 정보들을 데이터베이스에 insert

    for i in rows:
        # extract
        stationId = i["stationId"]
        stationName = i["stationName"]
        rackTotCnt = i["rackTotCnt"]
        stationLatitude = i["stationLatitude"]
        stationLongitude = i["stationLongitude"]
        
        # transform
        s = stationName.split('.')
        stationName = s[1].rstrip()
        stationId = s[0]

        d = '\'' + str(stationName) + '\',' + stationId + ',' + rackTotCnt + ',' +  stationLatitude + ',' + stationLongitude
        print(d)

        # load
        c = 'stationName, stationId, rackTotCnt, parkingBikeTotCnt, shared, stationLatitude, stationLongitude'
        insertDB(schema='star',table='STATION',colume = c, data=d)

def creat_station_state_table(): # api를 통해 얻은 자전거 거치대의 상태 정보들을 데이터베이스에 insert

    for i in rows:
        # extract
        stationName = i["stationName"]
        parkingBikeTotCnt = i["parkingBikeTotCnt"]
        shared = i["shared"]
        
        # transform
        s = stationName.split('.')
        stationId = s[0]

        # load
        d = stationId + ',' + shared + ',' + parkingBikeTotCnt
        print(d)
        
        c = 'stationId, parkingBikeTotCnt, shared'
        insertDB(schema='star',table='STATION_STATE',colume = c, data=d)


def creat_new_user_table():
    genReqUrl = 'http://openapi.seoul.go.kr:8088/4a496365596a756236395565775545/xml/cycleNewMemberRentInfoMonth/1/5/202111/'
    
    data = requests.get(genReqUrl)
    result = json.loads(data.text)
    print(result)
    
    rows = result["cycleNewMemberRentInfoMonth"]["row"]


creat_station_table()