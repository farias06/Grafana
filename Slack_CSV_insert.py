#! /usr/bin/env python3
# ~*~ utf-8 ~*~

# @author <@cyber-neurones.org>
# @version 1

import csv
from datetime import datetime
from dateutil.parser import parse
import mysql.connector
from mysql.connector import errorcode
from mysql.connector import (connection)

cnx = connection.MySQLConnection(user='slack', password='slack',
                                 host='127.0.0.1',
                                 database='SLACK')
cursor = cnx.cursor();
now = datetime.now().date();

#cursor.execute("DROP TABLE SLACK;");
#cursor.execute("CREATE TABLE SLACK (DATE datetime, DATE_LAST datetime, USER_AGENT varchar(50),USER_AGENT_FULL varchar(256), IP varchar(26), NUMBER int);");
cursor.execute("DELETE FROM SLACK");
cnx.commit();

with open('access_logs.csv', 'r') as csvfile:
    reader = csv.reader(csvfile, quotechar='"')
    for row in reader:
        MyDate=row[0];
        MyDate = MyDate.rsplit('(',1)[0];
        #print ("MyDate:"+MyDate)
        if (MyDate == "Date Accessed"):
           print("No");
        else:
           Dt = parse(MyDate)
           MyUser=row[1];
           MyUser=MyUser.replace("'", " ")
           MyUserFull=row[2];
           MyUserFull=MyUserFull.replace("'", " ")
           MyIP=row[3];
           MyNumber=row[4];
           MyDateLast=row[5];
           MyDateLast = MyDateLast.rsplit('(',1)[0];
           DtLast = parse(MyDateLast)
           #print ("MyDateLast:"+MyDateLast)
           try :
              SQLREQUEST = "INSERT INTO SLACK (DATE, USER_AGENT, USER_AGENT_FULL, IP, DATE_LAST, NUMBER) VALUES ('"+str(Dt.date())+" "+str(Dt.time())+"', '"+MyUser+"', '"+MyUserFull+"','"+MyIP+"', '"+str(DtLast.date())+" "+str(DtLast.time())+"', "+MyNumber+" );";
              #print(SQLREQUEST);
              cursor.execute(SQLREQUEST);
           except mysql.connector.Error as err:
              print("Something went wrong: {}".format(err))
              if err.errno == errorcode.ER_BAD_TABLE_ERROR:
                 print("Creating table SLACK")
              else:
                 None

cnx.commit();
cursor.close();
cnx.close();

# END 
