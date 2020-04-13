#! /usr/bin/env python3
# ~*~ utf-8 ~*~

# @author <@cyber-neurones.org>
# @version 1

import csv
from datetime import datetime
import mysql.connector
import re
from mysql.connector import errorcode
from mysql.connector import (connection)

cnx = connection.MySQLConnection(user='twitter', password='twitter',
                                 host='127.0.0.1',
                                 database='TWITTERS')
cursor = cnx.cursor();
now = datetime.now().date();

#cursor.execute("DROP TABLE TWITTER;");
#cursor.execute("CREATE TABLE TWITTER (IDENTIFIANT varchar(30) UNIQUE,PERMALIEN varchar(200),TEXTE varchar(500),DATE datetime,IMPRESSION float,ENGAGEMENT float,TAUX_ENGAGEMENT float, RETWEET float,REPONSE float, JAIME float, CLIC_PROFIL float, CLIC_URL float, CLIC_HASTAG float, OUVERTURE_DETAIL float, CLIC_PERMALIEN float, OUVERTURE_APP int, INSTALL_APP int, ABONNEMENT int, EMAIL_TWEET int, COMPOSER_NUMERO int, VUE_MEDIA int, ENGAGEMENT_MEDIA int);");
#cursor.execute("CREATE TABLE TWITTER_USER (USER varchar(20),IDENTIFIANT varchar(30), DATE datetime, PRIMARY KEY (USER,IDENTIFIANT));");
cursor.execute("DELETE FROM TWITTER_USER")
cursor.execute("DELETE FROM TWITTER");
cnx.commit();

with open('input.csv', 'r') as csvfile:
    reader = csv.reader(csvfile, quotechar='"')
    for row in reader:
        MyDate=row[3].replace(" +0000", ":00")
        MyTexte=row[2].replace("'", " ")
        MyTexte=MyTexte.replace(",", " ")
        MyC4=row[4].replace("Infinity", "0")
        MyC5=row[5].replace("Infinity", "0")
        MyC6=row[6].replace("Infinity", "0")
        MyC6=MyC6.replace("NaN", "0")
        MyC7=row[7].replace("Infinity", "0")
        User = re.findall(r'(?<=\W)[@]\S*', MyTexte)
        for MyUser in User:
            try :
                cursor.execute("INSERT INTO TWITTER_USER (IDENTIFIANT,USER,DATE) VALUES ('"+row[0]+"','"+MyUser+"','"+MyDate+"');");
            except mysql.connector.Error as err:
                print("Something went wrong: {}".format(err))
                if err.errno == errorcode.ER_BAD_TABLE_ERROR:
                    print("Creating table TWITTER_USER")
                else:
                    None
        try :
            cursor.execute("INSERT INTO TWITTER (IDENTIFIANT,PERMALIEN,TEXTE,DATE,IMPRESSION,ENGAGEMENT,TAUX_ENGAGEMENT,RETWEET,REPONSE, JAIME, CLIC_PROFIL, CLIC_URL, CLIC_HASTAG, OUVERTURE_DETAIL, CLIC_PERMALIEN, OUVERTURE_APP, INSTALL_APP, ABONNEMENT, EMAIL_TWEET, COMPOSER_NUMERO, VUE_MEDIA, ENGAGEMENT_MEDIA) VALUES ('"+row[0]+"', '"+row[1]+"', '"+MyTexte+"','"+MyDate+"', "+MyC4+", "+MyC5+", "+MyC6+", "+MyC7+", "+row[8]+","+row[9]+", "+row[10]+", "+row[11]+","+row[12]+","+row[13]+","+row[14]+","+row[15]+","+row[16]+","+row[17]+","+row[18]+","+row[19]+","+row[20]+","+row[21]+");");
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
            if err.errno == errorcode.ER_BAD_TABLE_ERROR:
                print("Creating table TWITTER")
            else:
                None

cnx.commit();
cursor.close();
cnx.close();

# END 
