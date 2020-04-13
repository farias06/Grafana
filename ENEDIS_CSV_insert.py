#! /usr/bin/env python3
# -*-coding:Latin-1 -* 

# @author <@cyber-neurones.org>
# @version 1 

import csv
from datetime import datetime
import mysql.connector
import re
from mysql.connector import errorcode
from mysql.connector import (connection)
#import numpy as np

def days_between(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d %H:%M:%S")
    d2 = datetime.strptime(d2, "%Y-%m-%d %H:%M:%S")
    return abs((d2 - d1).days)

def clean_tab(d):
     if d != "":
         return int(d);
     else:
         return 0

cnx = connection.MySQLConnection(user='enedis', password='enedis',
                                 host='127.0.0.1',
                                 database='ENEDIS')
cursor = cnx.cursor();
now = datetime.now().date();

#cursor.execute("DROP TABLE COMPTEUR;");
#cursor.execute("CREATE TABLE COMPTEUR (DATE datetime,TYPE_RELEVE varchar(50),EAS_F1 int, EAS_F2 int, EAS_F3 int , EAS_F4 int, EAS_F5 int, EAS_F6 int , EAS_F7 int, EAS_F8 int, EAS_F9 int, EAS_F10 int, EAS_D1 int, EAS_D2 int, EAS_D3 int,EAS_D4 int, EAS_T  int );");
cursor.execute("DELETE FROM COMPTEUR");
cnx.commit();

MyType_Previous = "None";
MyEAS_F1_Previous = 0;
MyEAS_F1 = 0
Diff_EAS_T_int = 0

with open('Enedis_Conso_Jour_XXXX.csv', 'r') as csvfile:
    reader = csv.reader(csvfile, delimiter=';')
    for row in reader:
        Nb = len(row);
        #row.replace(np.nan, 0)
        #print ("Nb:"+str(Nb));
        if (Nb == 17):
            MyDate=row[0].replace("+02:00", "")
            MyDate=MyDate.replace("T", " ")
            MyDate=MyDate.replace("+01:00", "")
            MyType=row[1].replace("'", " ")
            if (MyType == "Arrêté quotidien"):
                MyEAS_F1=clean_tab(row[2])
                MyEAS_F2=clean_tab(row[3])
                MyEAS_F3=clean_tab(row[4])
                MyEAS_F4=clean_tab(row[5])
                MyEAS_F5=clean_tab(row[6])
                MyEAS_F6=clean_tab(row[7])
                MyEAS_F7=clean_tab(row[8])
                MyEAS_F8=clean_tab(row[9])
                MyEAS_F9=clean_tab(row[10])
                MyEAS_F10=clean_tab(row[11])
                MyEAS_D1=clean_tab(row[12])
                MyEAS_D2=clean_tab(row[13])
                MyEAS_D3=clean_tab(row[14])
                MyEAS_D4=clean_tab(row[15])
                MyEAS_T=clean_tab(row[16])

            if (MyType_Previous == MyType):
                #print(MyType_Previous+"/"+MyType);
                Day=days_between(MyDate,MyDate_Previous);
                #print("Diff in days"+str(Day));
            else:
                Day = 0    

            if (Day == 1):
                Diff_EAS_F1 = str(MyEAS_F1-MyEAS_F1_Previous);
                Diff_EAS_F2 = str(MyEAS_F2-MyEAS_F2_Previous);
                Diff_EAS_F3 = str(MyEAS_F3-MyEAS_F3_Previous);
                Diff_EAS_F4 = str(MyEAS_F4-MyEAS_F4_Previous);
                Diff_EAS_F5 = str(MyEAS_F5-MyEAS_F5_Previous);
                Diff_EAS_F6 = str(MyEAS_F6-MyEAS_F6_Previous);
                Diff_EAS_F7 = str(MyEAS_F7-MyEAS_F7_Previous);
                Diff_EAS_F8 = str(MyEAS_F8-MyEAS_F8_Previous);
                Diff_EAS_F9 = str(MyEAS_F9-MyEAS_F9_Previous);
                Diff_EAS_F10 = str(MyEAS_F10-MyEAS_F10_Previous);
                Diff_EAS_D1 = str(MyEAS_D1-MyEAS_D1_Previous);
                Diff_EAS_D2 = str(MyEAS_D2-MyEAS_D2_Previous);
                Diff_EAS_D3 = str(MyEAS_D3-MyEAS_D3_Previous);
                Diff_EAS_D4 = str(MyEAS_D4-MyEAS_D4_Previous);
                Diff_EAS_T_int = (MyEAS_T-MyEAS_T_Previous)/Day;
                Diff_EAS_T = str(Diff_EAS_T_int);

                if ((MyType == "Arrêté quotidien") and (Diff_EAS_T_int > 0)):
                    try :
                        Requesq_SQL="INSERT INTO COMPTEUR (DATE,TYPE_RELEVE,EAS_F1, EAS_F2, EAS_F3 , EAS_F4, EAS_F5, EAS_F6 , EAS_F7 , EAS_F8 , EAS_F9 , EAS_F10 , EAS_D1 , EAS_D2 , EAS_D3 ,EAS_D4 , EAS_T) VALUES ('"+MyDate+"', '"+MyType+"', "+Diff_EAS_F1+","+Diff_EAS_F2+", "+Diff_EAS_F3+", "+Diff_EAS_F4+", "+Diff_EAS_F5+", "+Diff_EAS_F6+", "+Diff_EAS_F7+","+Diff_EAS_F8+", "+Diff_EAS_F9+", "+Diff_EAS_F10+","+Diff_EAS_D1+","+Diff_EAS_D2+","+Diff_EAS_D3+","+Diff_EAS_D4+","+Diff_EAS_T+");";
                        #print Requesq_SQL;
                        cursor.execute(Requesq_SQL);
                    except mysql.connector.Error as err:
                        print("Something went wrong: {}".format(err))
                        if err.errno == errorcode.ER_BAD_TABLE_ERROR:
                            print("Creating table COMPTEUR")
                        else:
                            None

            if (Day > 1):
                print ("Day > 1 :"+str(Day)) 
                Diff_EAS_F1 = str((MyEAS_F1-MyEAS_F1_Previous)/Day);
                Diff_EAS_F2 = str((MyEAS_F2-MyEAS_F2_Previous)/Day);
                Diff_EAS_F3 = str((MyEAS_F3-MyEAS_F3_Previous)/Day);
                Diff_EAS_F4 = str((MyEAS_F4-MyEAS_F4_Previous)/Day);
                Diff_EAS_F5 = str((MyEAS_F5-MyEAS_F5_Previous)/Day);
                Diff_EAS_F6 = str((MyEAS_F6-MyEAS_F6_Previous)/Day);
                Diff_EAS_F7 = str((MyEAS_F7-MyEAS_F7_Previous)/Day);
                Diff_EAS_F8 = str((MyEAS_F8-MyEAS_F8_Previous)/Day);
                Diff_EAS_F9 = str((MyEAS_F9-MyEAS_F9_Previous)/Day);
                Diff_EAS_F10 = str((MyEAS_F10-MyEAS_F10_Previous)/Day);
                Diff_EAS_D1 = str((MyEAS_D1-MyEAS_D1_Previous)/Day);
                Diff_EAS_D2 = str((MyEAS_D2-MyEAS_D2_Previous)/Day);
                Diff_EAS_D3 = str((MyEAS_D3-MyEAS_D3_Previous)/Day);
                Diff_EAS_D4 = str((MyEAS_D4-MyEAS_D4_Previous)/Day);
                Diff_EAS_T_int = (MyEAS_T-MyEAS_T_Previous)/Day;
                Diff_EAS_T = str(Diff_EAS_T_int);

                if ((MyType == "Arrêté quotidien") and (Diff_EAS_T_int > 0)):
                    try :
                        Requesq_SQL="INSERT INTO COMPTEUR (DATE,TYPE_RELEVE,EAS_F1, EAS_F2, EAS_F3 , EAS_F4, EAS_F5, EAS_F6 , EAS_F7 , EAS_F8 , EAS_F9 , EAS_F10 , EAS_D1 , EAS_D2 , EAS_D3 ,EAS_D4 , EAS_T) VALUES ('"+MyDate+"', '"+MyType+"', "+Diff_EAS_F1+","+Diff_EAS_F2+", "+Diff_EAS_F3+", "+Diff_EAS_F4+", "+Diff_EAS_F5+", "+Diff_EAS_F6+", "+Diff_EAS_F7+","+Diff_EAS_F8+", "+Diff_EAS_F9+", "+Diff_EAS_F10+","+Diff_EAS_D1+","+Diff_EAS_D2+","+Diff_EAS_D3+","+Diff_EAS_D4+","+Diff_EAS_T+");";
                        print Requesq_SQL;
                        cursor.execute(Requesq_SQL);
                    except mysql.connector.Error as err:
                        print("Something went wrong: {}".format(err))
                        if err.errno == errorcode.ER_BAD_TABLE_ERROR:
                            print("Creating table COMPTEUR")
                        else:
                            None

            # Save Previous
            if ((MyType == "Arrêté quotidien") and (Diff_EAS_T_int >= 0)):
                MyDate_Previous=MyDate;
                MyType_Previous=MyType;
                MyEAS_F1_Previous=MyEAS_F1;
                MyEAS_F2_Previous=MyEAS_F2;
                MyEAS_F3_Previous=MyEAS_F3;
                MyEAS_F4_Previous=MyEAS_F4;
                MyEAS_F5_Previous=MyEAS_F5;
                MyEAS_F6_Previous=MyEAS_F6;
                MyEAS_F7_Previous=MyEAS_F7;
                MyEAS_F8_Previous=MyEAS_F8;
                MyEAS_F9_Previous=MyEAS_F9;
                MyEAS_F10_Previous=MyEAS_F10;
                MyEAS_D1_Previous=MyEAS_D1;
                MyEAS_D2_Previous=MyEAS_D2;
                MyEAS_D3_Previous=MyEAS_D3;
                MyEAS_D4_Previous=MyEAS_D4;
                MyEAS_T_Previous=MyEAS_T;


cnx.commit();
cursor.close();
cnx.close();

# END 
