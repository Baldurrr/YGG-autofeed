import xml.etree.ElementTree as ET
import requests as req
import re
import itertools
from hurry.filesize import size, si
import wget
import time
from time import sleep
from os import listdir
from os.path import isfile, join
import sys, os, hashlib
import bencoding
import mysql.connector
from mysql.connector import errors
from datetime import datetime
import shutil
import threading
from environs import Env

# retry feature ??

## REGEX ##
seedPatterns=r'torznab:attr name="seeders" value="\d+'
peersPatterns=r'torznab:attr name="peers" value="\d+'
digitPatterns=r"\d+"

## LISTS ##
title_list=[]
size_list=[]
seed_list=[]
peer_list=[]
link_list=[]
merged_list=[]
hash_list=[]
delete_list=[]

######  FUNCTION  #######
def torrent_delete(element):
    os.remove(TmpBlackholeDir+element)


def torrent_verification(mytitle,mysize,myseed,mypeer,mylink):
    if mysize <= max_size and mysize > min_size:
        str_size=size(mysize, system=si)
        log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | FILE SIZE :"+str_size+" NAME : "+mytitle+"\n")
        mytitle=mytitle.replace('/','_').replace('*','_').replace(':','_').replace('"','_').replace('<','_').replace('>','_').replace('|','_').replace('?','_').replace('\'','_')
        pathFile=TmpBlackholeDir+mytitle+'.torrent'

        wget.download(mylink,pathFile)
        log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | Successfully DL torrent\n")
        time.sleep(0.5)

        objTorrentFile = open(pathFile, "rb")
        decodedDict = bencoding.bdecode(objTorrentFile.read())
        info_hash = hashlib.sha1(bencoding.bencode(decodedDict[b"info"])).hexdigest()
        # log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | Extracting HASH\n")

        mark=0
        for hash_data in hash_list:
            if info_hash == hash_data[0]:
                log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | Hash already present\n")
                mark=1
                delete_list.append(mytitle+'.torrent')
                print("mark = 1\n")

        if mark == 0:
            print("mark = 0")
            cur.execute("INSERT into torrent_hash (TORRENT_TITLE, TORRENT_HASH) VALUES ('{}', '{}')".format(mytitle,info_hash)) 

    else:
        print("\n#Not downloading\n")


########### TRUNC CODE #############

if __name__ == '__main__':

    while True:
        sleep(60 - time.time() % 60)

        ## FILE LOGGING ##
        env = Env()
        env.read_env()
        max_size= int(os.environ.get('FILE_MAX_SIZE'))
        # print(max_size)
        min_size= int(os.environ.get('FILE_MIN_SIZE'))
        # print(min_size)
        myJackettPath= str(os.environ.get('JACKETT_YGG_TORZNAB'))
        # print(myJackettPath)
        DelugeBlackhole= str(os.environ.get('DELUGE_BLACKHOLE_DIR'))
        # print(DelugeBlackhole)
        TmpBlackholeDir= str(os.environ.get('TMP_BLACKHOLE_DIR'))
        # print(TmpBlackholeDir)
        MyLogFile= str(os.environ.get('LOG_FILE_PATH'))
        # print(MyLogFile)
        sql_user= str(os.environ.get('SQL_USER'))
        # print(sql_user)
        sql_password= str(os.environ.get('SQL_PASSWORD'))
        # print(sql_password)
        sql_host= str(os.environ.get('SQL_HOST'))
        # print(sql_host)
        sql_db= str(os.environ.get('SQL_DB'))
        # print(sql_db)
        sql_port= int(os.environ.get('SQL_PORT'))
        # print(sql_port)
        
        tic = time.perf_counter()

        log_file= open(MyLogFile, "a")

        log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | # SCRIPT STARTED # \n")

        ## DATABASE CONNEXION ##
        try:
            conn = mysql.connector.connect(
            user=sql_user,
            password=sql_password,
            host=sql_host,
            database=sql_db, 
            port=sql_port)

            log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | Connected to database at "+sql_host+" \n")

        except mysql.connector.Error as e:
            log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | Error connecting to MysqlDB Platform \n")
            log_file.write(e+"\n")
            sys.exit(1)

        cur = conn.cursor()
        log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | Creating cursor \n")

        try:
            cur.execute("SELECT TORRENT_HASH from torrent_hash")
            log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | SELECT query successfull \n")

        except mysql.connector.Error as e:
            log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | SELECT query ERROR !!! \n")
            print("Error: {e}")
            log_file.write(e+"\n")

        myresult = cur.fetchall()

        for somehash in myresult:
            hash_list.append(somehash)
        len_hashlist=str(len(hash_list))

        if len(hash_list) == 0:
            hash_list.append("default str")

        log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | "+len_hashlist+" Hashs in DB\n")

        # ## SCRAPING ##
        resp = req.get(myJackettPath)
        log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | Requesting Jackett API\n")
        data=str(resp.text)
        myroot = ET.fromstring(data)

        ## EXTRACTION ##
        seedMatch = re.findall(seedPatterns, data)
        log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | Parsing Torznab feed\n")
        for seed_item in seedMatch:
            match = re.findall(digitPatterns,seed_item)
            seed_list.append(int(match[0]))

        peersMatch = re.findall(peersPatterns, data)
        for peer_item in peersMatch:
            match = re.findall(digitPatterns,peer_item)
            peer_list.append(int(match[0]))

        for xml_item in myroot[0]:
            for title_tag in xml_item.findall('title'):
                title_text=title_tag.text
                title_list.append(str(title_text))

            for link_tag in xml_item.findall('link'):
                link_text=link_tag.text
                link_list.append(str(link_text))

            for size_tag in xml_item.findall('size'):
                size_text=size_tag.text
                size_list.append(int(size_text))

        ## MERGING ##
        for (title,filesize,seed,peer,link) in zip(title_list,size_list,seed_list,peer_list,link_list):
            temp_list=[]
            temp_list.append(title)
            temp_list.append(filesize)
            temp_list.append(seed)
            temp_list.append(peer)
            temp_list.append(link)
            merged_list.append(temp_list)

        ## DOWNLOAD
        counter=0
        for element in merged_list:
            mytitle=element[0]
            mysize=element[1]
            myseed=element[2]
            mypeer=element[3]
            mylink=element[4]

            task_number="t"+str(counter)
            tx=threading.Thread(target=torrent_verification(mytitle,mysize,myseed,mypeer,mylink), name=task_number)
            print("Task "+task_number+"assigned\n")
            tx.start()

            counter=counter+1

        conn.commit()
        conn.close()

        log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | Closing cursor \n")

        counter2=0
        for element in delete_list:

            task_number2="tx"+str(counter2)
            tx2=threading.Thread(target=torrent_delete(element), name=task_number2)
            print("Remove Taskx "+task_number2+"assigned\n")
            tx2.start()

            counter2=counter2+1
        
        for file in os.listdir(TmpBlackholeDir):
            try:
                print(file)
                shutil.move(TmpBlackholeDir+file, DelugeBlackhole+file)

            except PermissionError:
                print("File in use")

        title_list.clear()
        size_list.clear()
        seed_list.clear()
        peer_list.clear()
        link_list.clear()
        merged_list.clear()
        hash_list.clear()
        delete_list.clear()

        toc = time.perf_counter()
        tictoc=toc - tic
        rounded_tictoc=round(tictoc,2)
        log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | Script executed in "+str(rounded_tictoc)+" seconds \n")
        print("Script executed in "+str(rounded_tictoc)+" seconds")
        
        log_file.close()


