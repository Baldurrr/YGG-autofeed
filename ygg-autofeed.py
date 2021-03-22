import xml.etree.ElementTree as ET
import requests as req
import re
from re import search
import itertools
from hurry.filesize import size, si
import wget
import time
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
import urllib.request
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

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

######  FUNCTIONS  #######
def breaking_function(mytitle,banned_word):
    test_list=[]
    lower_title=mytitle.lower()
    banned_list=banned_word.split(",")

    for banned in banned_list:
        
        if search(" ",banned):
            splited_banned_words=banned.split(" ")
            len_splited_banned_words=len(splited_banned_words)

            word_counter=0
            for item in splited_banned_words:
                if search(item,lower_title):
                    print("trouver")
                    word_counter=word_counter+1

            if len_splited_banned_words == word_counter:
                print("Resplited banned word OK")
                ok='ok'
                test_list.append(ok)
                break

            else:
                print("No banned words!\n")

        elif search(banned,lower_title):
            print("Direct banned word OK")
            ok='ok'
            test_list.append(ok)
            break

    len_test=len(test_list)
    print("Breaking state: "+str(len_test))
    return len_test

def retry_feature(url):

    s = req.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500,502,503,504,520,522,523,524])
    s.mount('http://', HTTPAdapter(max_retries=retries))
    response=s.get(url)
    print("GET feed")
    return response

def torrent_delete(element):
    os.remove(TmpBlackholeDir+element)

def torrent_move():
    for file in os.listdir(TmpBlackholeDir):
        try:
            shutil.move(TmpBlackholeDir+file, DelugeBlackhole+file)

        except PermissionError:
            log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | File in use\n")

    log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | Torrent files moved\n")

def keyword_researcher(mytitle,mysize,mylink,keywords):

    keywords_list=keywords.split(",")
    lower_title=mytitle.lower()
    
    for words in keywords_list:

        if search(" ",words):
            splited_words=words.split(" ")
            len_splited_words=len(splited_words)

            word_counter=0
            for item in splited_words:
                if search(item,lower_title):
                    word_counter=word_counter+1

            if len_splited_words == word_counter:
                print("Resplited Keyword OK")
                torrent_downloading(mytitle,mysize,mylink)

            else:
                print("Keyword not recognize in title")

        elif search(words,lower_title):
            print("Direct Keyword OK")
            torrent_downloading(mytitle,mysize,mylink)

def torrent_downloading(mytitle,mysize,mylink):

    str_size=size(mysize, system=si)
    log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | FILE SIZE: "+str_size+" NAME: "+mytitle+"\n")
    mytitle=mytitle.replace('/','_').replace('*','_').replace(':','_').replace('"','_').replace('<','_').replace('>','_').replace('|','_').replace('?','_').replace('\'','_')
    pathFile=TmpBlackholeDir+mytitle+'.torrent'

    # wget.download(mylink,pathFile)
    urllib.request.urlretrieve(mylink,pathFile)

    log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | Successfully DL torrent\n")
    print("--DOWNLOADING--\n")

    objTorrentFile = open(pathFile, "rb")
    decodedDict = bencoding.bdecode(objTorrentFile.read())
    info_hash = hashlib.sha1(bencoding.bencode(decodedDict[b"info"])).hexdigest()

    mark=0
    for hash_data in hash_list:
        if info_hash == hash_data[0]:
            log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | Hash already present\n")
            mark=1
            delete_list.append(mytitle+'.torrent')

    if mark == 0:
        cur.execute("INSERT into torrent_hash (TORRENT_TITLE, TORRENT_HASH) VALUES ('{}', '{}')".format(mytitle,info_hash)) 


def feature_verification(mytitle,mysize,myseed,mypeer,mylink,keyword_activated,keywords,peers_seeds_activated,banned_word):

    if mysize <= max_size and mysize > min_size and ((keyword_activated == "FALSE" or keyword_activated == "")  and (peers_seeds_activated == "FALSE" or peers_seeds_activated == "")):
        print("# Size only #\n")
        isbreaking = breaking_function(mytitle,banned_word)

        if isbreaking == 0 :
            print("Pas de mots bannis detecter")
            torrent_downloading(mytitle,mysize,mylink)

        else:
            print("Mot banni reconnu: FIN")
            log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | BANNED WORD DETECTED\n")

        
    elif mysize <= max_size and mysize > min_size and keyword_activated == "TRUE":
        print("# Size + Keyword #\n")
        isbreaking = breaking_function(mytitle,banned_word)

        if isbreaking == 0 :
            print("Pas de mots bannis detecter")
            keyword_researcher(mytitle,mysize,mylink,keywords)

        else:
            print("Mot banni reconnu: FIN")
            log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | BANNED WORD DETECTED\n")

    elif mysize <= max_size and mysize > min_size and peers_seeds_activated == "TRUE":
        print("# Size + Peers/Seeds #\n")
        # torrent_downloading(mytitle,mysize,myseed,mypeer,mylink)

    elif mysize <= max_size and mysize > min_size and keyword_activated == "TRUE" and peers_seeds_activated == "TRUE":
        print("# Size + Keyword + Peers/Seeds #\n")
        # torrent_downloading(mytitle,mysize,myseed,mypeer,mylink)

    else:
        print("* No match with feature *\n")

########### ROOT CODE #############
if __name__ == '__main__':

    while True:

        ## ENV VAR ##
        env = Env()
        env.read_env()
        max_size= int(os.environ.get('FILE_MAX_SIZE'))
        min_size= int(os.environ.get('FILE_MIN_SIZE'))
        scrape_time= int(os.environ.get('SCRAPE_TIME'))
        keyword_activated= str(os.environ.get('KEYWORD_FEATURE_ACTIV'))
        keywords= str(os.environ.get('KEYWORD_LIST'))
        banned_word=str(os.environ.get('BANNED_WORDS'))
        peers_seeds_activated= str(os.environ.get('PEERS_SEEDS_FEATURE_ACTIV'))

        myJackettPath= str(os.environ.get('JACKETT_YGG_TORZNAB'))
        DelugeBlackhole= str(os.environ.get('DELUGE_BLACKHOLE_DIR'))
        TmpBlackholeDir= str(os.environ.get('TMP_BALCKHOLE_DIR'))
        MyLogFile= str(os.environ.get('LOG_FILE_PATH'))

        sql_user= str(os.environ.get('SQL_USER'))
        sql_password= str(os.environ.get('SQL_PASSWORD'))
        sql_host= str(os.environ.get('SQL_HOST'))
        sql_db= str(os.environ.get('SQL_DB'))
        sql_port= int(os.environ.get('SQL_PORT'))
        
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
        resp = retry_feature(myJackettPath)
        # resp = req.get(myJackettPath)

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
            tx=threading.Thread(target=feature_verification(mytitle,mysize,myseed,mypeer,mylink,keyword_activated,keywords,peers_seeds_activated,banned_word), name=task_number)
            tx.start()
            counter=counter+1

        conn.commit()
        conn.close()
        log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | Closing cursor \n")

        ## PURGE ##
        counter2=0
        for element in delete_list:

            task_number2="tx"+str(counter2)
            tx2=threading.Thread(target=torrent_delete(element), name=task_number2)
            tx2.start()

            counter2=counter2+1
        
        torrent_move()
        
        title_list.clear()
        size_list.clear()
        seed_list.clear()
        peer_list.clear()
        link_list.clear()
        merged_list.clear()
        hash_list.clear()
        delete_list.clear()
        log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | Purging all lists\n")

        toc = time.perf_counter()
        tictoc=toc - tic
        rounded_tictoc=round(tictoc,2)
        log_file.write("["+datetime.now().strftime("%d/%m/%Y - %H:%M:%S")+"] | Script executed in "+str(rounded_tictoc)+" seconds \n")
        print("Script executed in "+str(rounded_tictoc)+" seconds")
        
        log_file.close()
        time.sleep(scrape_time)
