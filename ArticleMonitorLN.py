# -*- coding: utf-8 -*-
"""
Created on Fri Sep  6 10:04:45 2019

@author: yzhao
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Sep  5 14:17:33 2019

@author: yzhao
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Aug 28 10:08:42 2019

@author: yzhao
"""

import psycopg2
import pandas as pd
import datetime
from datetime import datetime
from datetime import timedelta
import csv





def conncet(dbname,user,host,password,port):
    conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s' port='%s'"%(dbname,user,host,password,port))
    return(conn)


def monitor(conn,infotype,sday,eday):
    conn.rollback()
    cur=conn.cursor()
    if infotype=='webhoset100':
        try:
            cur.execute("SELECT COUNT(*) from sentiment.t100articles WHERE publish_timestamp between '%s' and '%s' "%(sday,eday)+"and s3_url like '%lexis%';")
            caocao=cur.fetchall()
        except:
            print('webhoset100 not found')
    if infotype=='webhoset600':
        try:
            cur.execute("SELECT COUNT(*) FROM sentiment.t600sentiment_tracker WHERE fail_at between '%s' and '%s' "%(sday,eday)+" and s3_url like '%lexis%';")
            caocao=cur.fetchall()
        except:
            print('webhoset600 not found')
    if infotype=='djt100':
        try:
            cur.execute("SELECT COUNT(*) from sentiment.t100articles WHERE publish_timestamp between '%s' and '%s' "%(sday,eday)+" and article_source like '%Dow%';")
            caocao=cur.fetchall()
        except:
            print('dowjonest100 not found')
    if infotype=='djt600':
        try:
            cur.execute("SELECT COUNT(*) FROM sentiment.t600sentiment_tracker WHERE fail_at between '%s' and '%s' "%(sday,eday)+" and s3_url like '%dowjones%';")
            caocao=cur.fetchall()
        except:
            print('djt600 not found')
    if infotype=='webhosenomapping':
        try:
            cur.execute("SELECT COUNT(*) FROM sentiment.t600sentiment_tracker WHERE fail_at between '%s' and '%s' "%(sday,eday)+" and s3_url like '%lexis%' and valid_fail_reason like '%No mapping%';")
            caocao=cur.fetchall()
        except:
            print('Webhose no mapping not found')
    if infotype=='webhosenoentity':
        try:
            cur.execute("SELECT COUNT(*) FROM sentiment.t600sentiment_tracker WHERE fail_at between '%s' and '%s' "%(sday,eday)+" and s3_url like '%lexis%' and valid_fail_reason like '%No entity%';")
            caocao=cur.fetchall()
        except:
            print('webhose no entity not found')
    if infotype=='djnomapping':
        try:
            cur.execute("SELECT COUNT(*) FROM sentiment.t600sentiment_tracker WHERE fail_at between '%s' and '%s' "%(sday,eday)+" and s3_url like '%dowjones%' and valid_fail_reason like '%No mapping%';")
            caocao=cur.fetchall()
        except:
            print('dowjones no mapping not found')
    if infotype=='djnoentity':
        try:
            cur.execute("SELECT COUNT(*) FROM sentiment.t600sentiment_tracker WHERE fail_at between '%s' and '%s' "%(sday,eday)+" and s3_url like '%dowjones%' and valid_fail_reason like '%No entity%';")
            caocao=cur.fetchall()
        except:
            print('dowjones no entity not found')
    if infotype=='webhosenoprocessor':
        try:
            cur.execute("SELECT COUNT(*) FROM sentiment.t600sentiment_tracker WHERE fail_at between '%s' and '%s' "%(sday,eday)+" and s3_url like '%lexis%' and fail_reason is not null';")
            caocao=cur.fetchall()
        except:
            print('webhose no processor not found')
    if infotype=='dowjonesnoprocessor':
        try:
            cur.execute("SELECT COUNT(*) FROM sentiment.t600sentiment_tracker WHERE fail_at between '%s' and '%s' "%(sday,eday)+" and s3_url like '%dowjones%' and fail_reason is not null';")
            caocao=cur.fetchall()
        except:
            print('dowjones no processor not found')
    return(caocao)


conn=conncet(dbname='sentiment',user='senti',host='db-sentiment.dev.recognia.com',password='Zse4rfV',port='6432')


def gen_dates(b_date, days):
    day = timedelta(days=1)
    for i in range(days):
        yield b_date + day*i

start_date='2019-04-01'
end_date='2019-05-02'


start = datetime.strptime(start_date, "%Y-%m-%d")
end = datetime.strptime(end_date, "%Y-%m-%d")
datelist=[]
for d in gen_dates(start, (end-start).days):
    datelist.append(d)
    
    

finanl_list=[]
for i in range(0,len(datelist)):
    dicdic={}
    sday=str(datelist[i]).split(' ')[0]
    if i<len(datelist)-1:
        eday=str(datelist[i+1]).split(' ')[0]
    else:
        eday=sday
    webt100=monitor(conn,'webhoset100',sday,eday)
    print('-----------web100 Finished------------------')
    webt600=monitor(conn,'webhoset600',sday,eday)
    print('----------web600 Finished-------------------')
    djt100=monitor(conn,'djt100',sday,eday)
    print('----------dj100 Finished--------------------')
    djt600=monitor(conn,'djt600',sday,eday)
    print('--------djt600 Finished----------------------')
    webhose_nomapping=monitor(conn,'webhosenomapping',sday,eday)
    print('--------webhose no mapping Finished----------------------')
    webhose_noentity=monitor(conn,'webhosenoentity',sday,eday)
    print('--------webhose no entity Finished----------------------')
    dj_nomapping=monitor(conn,'djnomapping',sday,eday)
    print('--------dj nomapping Finished----------------------')
    dj_noentity=monitor(conn,'djnoentity',sday,eday)
    print('--------dj no entity Finished----------------------')
    webhose_processingproblem=(conn,'webhosenoprocessor',sday,eday)
    print('--------webhose processproblem Finished----------------------')
    dj_processingproblem=(conn,'dowjonesnoprocessor',sday,eday)
    print('--------dj processproblem Finished----------------------')
    dicdic['Date']=sday    
    dicdic['Webhoset600']=str(webt600[0][0])
    dicdic['Webhoset100']=str(webt100[0][0])
    dicdic['WebhoseNomapping']=str(webhose_nomapping[0][0])
    dicdic['WebhoseNoentity']=str(webhose_noentity[0][0])
    dicdic['DJt600']=str(djt600[0][0])
    dicdic['DJt100']=str(djt100[0][0])
    dicdic['DJNomapping']=str(dj_nomapping[0][0])
    dicdic['DJNoentity']=str(dj_noentity[0][0])
    if type(webhose_processingproblem) is list:
        dicdic['WebhoseProcessingProblem']=str(webhose_processingproblem[0][0])
    else:
        dicdic['WebhoseProcessingProblem']='0'
    if type(dj_processingproblem) is list:
        dicdic['DJProcessingProblem']=str(dj_processingproblem[0][0])
    else:
        dicdic['DJProcessingProblem']='0'
    finanl_list.append(dicdic)
    dicdic={}

Final_DF=pd.DataFrame(finanl_list)
Final_DF=Final_DF[['Date','Webhoset600','Webhoset100','WebhoseNomapping','WebhoseNoentity','DJt600'
           ,'DJt100','DJNomapping','DJNoentity','WebhoseProcessingProblem','DJProcessingProblem']]

Final_DF.to_excel('MonitorApril.xlsx')


#date=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#
#
#
#with open('BuzzMonitor.txt','a+',encoding='utf8') as w:
#    w.write(date)
#    w.write(' ')
#    w.write(str(webt600[0][0]))
#    w.write(' ')
#    w.write(str(webt100[0][0]))
#    w.write(' ')
#    w.write(str(webhose_nomapping[0][0]))
#    w.write(' ')
#    w.write(str(webhose_noentity[0][0]))
#    w.write(' ')
#    w.write(str(djt600[0][0]))
#    w.write(' ')
#    w.write(str(djt100[0][0]))
#    w.write(' ')
#    w.write(str(dj_nomapping[0][0]))
#    w.write(' ')
#    w.write(str(dj_noentity[0][0]))
#    if type(webhose_processingproblem) is list:
#        w.write(' ')
#        w.write(str(webhose_processingproblem[0][0]))
#    else:
#        w.write(' ')
#        w.write('0')
#    if type(dj_processingproblem) is list:
#        w.write(' ')
#        w.write(dj_processingproblem[0][0])
#    else:
#        w.write(' ')
#        w.write('0')  
#    w.write('\n')
    