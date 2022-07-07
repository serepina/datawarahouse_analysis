import csv
import psycopg2
import copy

def insertDB(schema,table,data):
    sql = " INSERT INTO {schema}.\"{table}\" VALUES ({data}) ;".format(schema=schema,table=table,data=data)
    try:
        db = psycopg2.connect(host='localhost', dbname='public_bike_star_schema',user='postgres',password='0609',port=5432)
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()
    except Exception as e :
        print(" insert DB  ",e) 

def csv_save_to_db(file):
    with open(file, newline='', encoding='cp949') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        count = 0
        for row in spamreader:
                
            print(row[0], row[1], row[5].replace('"',''), row[2].replace('"','').lstrip('0'), row[6].replace('"','').lstrip('0'), row[9].replace('"',''), row[10].replace('"','')) # , 

            d = '\'' + row[0].replace('"','') + '\',\'' + row[1].replace('"','') + '\',' + row[2].replace('"','').lstrip('0') + ',\'' + row[5].replace('"','') + '\','  + row[6].replace('"','').lstrip('0') + ',' + row[9].replace('"','') + ',' + row[10].replace('"','')
            insertDB(schema='star',table='RENTAL_INF', data=d)

            if count == 200:
                break
            count = count + 1

def csv_save_to_db_use(file):
    with open(file, newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        count = 0
        for row in spamreader:
            
            if row[4].replace('"','') != 'M' and row[4].replace('"','') != 'F':
                row[4] = ''
            else: 
                row[4] = row[4].replace('"','')

            d = row[1].replace('"','').lstrip('0') + ',\'' + row[3].replace('"','') + '\',\'' + row[4] + '\',\''  + row[5].replace('"','') + '\',' + row[6].replace('"','') + ',' + row[7].replace('"','') + ',' + row[8].replace('"','') + ',to_date(\'' + row[0].replace('"','') + '\', \'YYYY-MM\')'
            
            insertDB(schema='star',table='RENTAL_INF_MONTH', data=d)

            if count == 1000:
                break
            count = count + 1

def csv_save_to_db_broken(file):
    with open(file, newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        count = 0
        for row in spamreader:
            
            print(', '.join(row))
            #print(row[0], row[1], row[5].replace('"',''), row[2].replace('"','').lstrip('0'), row[6].replace('"','').lstrip('0'), row[9].replace('"',''), row[10].replace('"','')) # , 


            d = '\'' + row[0].replace('"','') + '\',to_date(\'' + row[1].replace('"','') + '\', \'YYYY-MM-DD\'),\''+ row[2].replace('"','') + '\''
            insertDB(schema='star',table='BIKE_BROKEN', data=d)

            if count == 1000:
                break
            count = count + 1

def csv_save_to_db_user(file):
    with open(file, newline='', encoding='utf-8') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        count = 0
        for row in spamreader:
            
            #print(', '.join(row))

            if row[2].replace('"','') == '20대':
                row[2] = 'AGE_002'

            d = 'to_date(\'' + row[0].replace('"','') + '\', \'YYYY-MM-DD\'),\'' + row[2]+ '\',' + row[4].replace('"','') 
            
            insertDB(schema='star',table='NEW_USER', data=d)

            if count == 600:
                break
            count = count + 1

csv_save_to_db_user('.\data\서울특별시 공공자전거 신규가입자 정보(일별)_18.07.20-21.12.31.csv')