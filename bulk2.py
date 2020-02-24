# -*- coding: utf-8 -*-
"""
Created on Sun Feb 23 16:53:24 2020

@author: sushila.sipai
"""

import cx_Oracle

ip='10.2.3.21'
sid='DANPHE'
port='1521'

try:
   dns_tns=cx_Oracle.makedsn(ip,port,sid)
   db=cx_Oracle.connect('system','manager',dns_tns)

except :
   print('Connection Failed')
db.autocommit=True

mesg_req_type=['1200', '1420','1220','1804'] 
mesg_type=['1210', '1430','1230','1814'] 
with open ('LISRVR_NPS_SWIF_119993_240220_08.log', 'r') as fo :

   mesg=[]
   i=0
   flg=False
   mesg.append([])

   for line in fo:
         if "MessageId" in line.strip():
            if((line.partition(':')[2]).strip() in (mesg_type)):
                flg=True
            mesg[i].append((line.partition(':')[2]).strip())      
         elif "Field 003" in line.strip():
            mesg[i].append((line.partition(':')[2]).strip())
         elif "Field 011" in line.strip():
            mesg[i].append((line.partition(':')[2]).strip())
         elif "Field 017" in line.strip():
           mesg[i].append((line.partition(':')[2]).strip())
         elif "Field 039" in line.strip():
           mesg[i].append((line.partition(':')[2]).strip())
           if(flg==True):
               i+=1
               mesg.append([])
               flg=False
           else:
            pass
   print(len(mesg))
   print (mesg)   

updatecount=0
zeroappendcount=0 
for n in range(len(mesg)):
    if(len(mesg[n])!=9):
        print (n)
        print (len(mesg[n]))
        if(len(mesg[n]) >0):
            if(mesg  [n][0] in (mesg_type)):
                cursor1=db.cursor()
                cursor1.execute("select QFIELD011 from custom.c24logs where QFIELD003='%s' and qfield011='%s' and qfield017='%s'"%(mesg[n][1],mesg[n][2],mesg[n][3]))
                if(len(cursor1.fetchall())> 0):
                    cursor2=db.cursor()
                    cursor2.execute("update custom.c24logs set rmessageid='%s', rfield003='%s', rfield011='%s', rfield017='%s', rfield039='%s' where QFIELD003='%s' and qfield011='%s' and qfield017='%s'"%(mesg[n][0],mesg[n][1],mesg[n][2],mesg[n][3],mesg[n][4],mesg[n][1],mesg[n][2],mesg[n][3]))         
                    updatecount+=1
                print ("updated")
                print(mesg[n])    
            elif(mesg[n][0] in mesg_req_type):
                for j in range(len(mesg[n]),9):
                    mesg[n].append(0)
                zeroappendcount+=1
                print("zero appended")
                print(mesg[n])    
        print("update count and zero append count:")
        print(updatecount)
        print(zeroappendcount)



cursor=db.cursor()
insertfail=0
for n in range(len(mesg)):
    if(len(mesg[n])==9):
        cursor.execute("Insert into custom.c24logs (qMessageId,qField003,qField011,qField017,rMessageId,rField003,rField011,rField017,rField039) values('%s','%s','%s','%s','%s','%s','%s','%s','%s')" %(mesg[n][0],mesg[n][1],mesg[n][2],mesg[n][3],mesg[n][4],mesg[n][5],mesg[n][6],mesg[n][7],mesg[n][8])) 
    else:
        insertfail+=1
        print ("failed to insert")
        print(mesg[n])
print("inset fail count:")
print(insertfail)
