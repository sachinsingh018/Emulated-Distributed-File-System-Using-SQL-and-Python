import streamlit as st
import streamlit as st
import pandas as pd
import pymysql
import base64
import numpy as np
import random


connection = pymysql.connect(host='mydbm.chhjs88knp6j.us-west-1.rds.amazonaws.com',
                             user='admin',
                             password='mmiikkoo',
                             database='MYDBM',
                             
                            autocommit=True)
cursor=connection.cursor()

def mapP(tablename,query,option,i):
    sequ="select * from "+str(tablename)+str(i)+" where "+str(option)+"="+str(query)
    df=pd.read_sql(sequ,con=connection)
    return df
def reducer(tablename,query,option,part):
    s=[]
    for i in range(part):
        df11=mapP(tablename,query,option,i)
        s.append(df11)
    ddf=pd.concat(s)
    st.write(ddf)


def get_base64(bin_file):
    with open(bin_file, 'rb') as f:
        data = f.read()
    return base64.b64encode(data).decode()

def set_background(png_file):
    bin_str = get_base64(png_file)
    page_bg_img = '''
    <style>
    .stApp {
    background-image: url("data:image/png;base64,%s");
    background-size: cover;
    }
    </style>
    ''' % bin_str
    st.markdown(page_bg_img, unsafe_allow_html=True)
def put(filename,k): # k is the partitions and filename is the file being given to us
    dataframe=pd.read_csv(filename,encoding='unicode_escape')
    df=pd.DataFrame(dataframe)
    df=df.rename(columns={'AREA ':'AREA_'})
    df=df.rename(columns={'Part_1-2':'Part_1'})

    dd=list(df.columns.values)
    df=df.replace("None","NULL")
    #print(dd)
    #df=df.drop(['Unnamed: 28', 'Unnamed: 29', 'Unnamed: 30', 'Unnamed: 31', 'Unnamed: 32'], axis=1)
    df = df.where((pd.notnull(df)), None)    
    N_PARTITIONS=k
    np.random.seed(2)
    permuted_indices = np.random.permutation(len(df))
    dfs=[]

    for i in range(N_PARTITIONS):
        dfs.append(df.iloc[permuted_indices[i::N_PARTITIONS]])
    for i in range(k):
        csv_to_sql(dfs[i],i)
    cursor.execute("Create table CRIMEO_PARTITIONS(id int,partition_location int)")
    for i in range(k):
        rando=random.randint(0,999999)
        stringy="insert into CRIMEO_PARTITIONS values ("+str(i)+","+str(rando)+")"
        cursor.execute(stringy)
    loc=pwde()
    stringness="insert into Files_Locator values ('"+loc+"','CRIMEO')"
    cursor.execute(stringness)
    
def csv_to_sql(dataframe,i):## filename,i):
#    dataframe=pd.read_csv(filename,encoding='unicode_escape')
#    df=pd.DataFrame(data)
#    df=df.rename(columns={'AREA ':'AREA_'})
#    df=df.rename(columns={'Part_1-2':'Part_1'})
#    dd=list(df.columns.values)
#    df=df.replace("None","NULL")
#    print(dd)
#    df=df.drop(['Unnamed: 28', 'Unnamed: 29', 'Unnamed: 30', 'Unnamed: 31', 'Unnamed: 32'], axis=1)
#    df = df.where((pd.notnull(df)), None)
    df=dataframe
    cursor.execute("CREATE TABLE CrimeO"+str(i)+"(DR_NO varchar(50), Date_Rptd datetime, DATE_OCC datetime, TIME_OCC  varchar(50), AREA varchar(50), AREA_NAME varchar(50), Rpt_Dist_No varchar(20), Part12 int, Crm_Cd varchar(50), Crm_Cd_Desc varchar(100), Mocodes varchar(50), Vict_age  varchar(50), Vict_sex varchar(20), Vict_desc varchar(20), Premis_Cd varchar(20), Premis_Desc varchar(50),Weapon_Used_Cd varchar(30),Weapon_Desc varchar(50), status varchar(20), Crm_Cd1 varchar(50),Crm_Cd2  varchar(50),Crm_Cd3  varchar(50),Crm_Cd4  varchar(50), LOCATION varchar(100),Cross_Street varchar(50),LAT decimal(9,6),LON decimal(8,6))")
    #print(df)
    for row in df.itertuples():
        try:
        #print(row.DR_NO)
        #cursor.execute("CREATE TABLE CrimeO(DR_NO varchar(50), Date_Reported datetime, Date_Occurance datetime, Time_Occurance  varchar(50), Area varchar(50), Area_Name varchar(50), Dist_No varchar(20), Part12 int, Crimes_Committed varchar(50), Crimecodedesc varchar(100), mocodes varchar(50), victime_age  varchar(50), vict_sex varchar(20), vict_desc varchar(20), premis_cd varchar(20), premis_desc varchar(50),weapon varchar(30),weapondesc varchar(50), status varchar(20), status_desc varchar(50), crm1  varchar(50),crm2  varchar(50),crm3  varchar(50), crm4  varchar(50), location varchar(100),street varchar(50),latitude decimal(9,6),longitude decimal(8,6))")
            sequences="Insert into CrimeO"+str(i)+" values('"+str(row.DR_NO)+"','"+str(row.Date_Rptd)+"','"+str(row.DATE_OCC)+"',"+str(row.TIME_OCC)+","+str(row.AREA_)+",'"+str(row.AREA_NAME)+"','"+str(row.Rpt_Dist_No)+"',"+str(row.Part_1)+",'"+str(row.Crm_Cd)+"','"+str(row.Crm_Cd_Desc)+"','"+str(row.Mocodes)+"','"+str(row.Vict_Age)+"','"+str(row.Vict_Sex)+"','"+str(row.Vict_Descent)+"','"+str(row.Premis_Cd)+"','"+str(row.Premis_Desc)+"','"+str(row.Weapon_Used_Cd)+"','"+str(row.Weapon_Desc)+"','"+str(row.Status)+"','"+str(row.Status_Desc)+"','"+str(row.Crm_Cd1)+"','"+str(row.Crm_Cd2)+"','"+str(row.Crm_Cd3)+"','"+str(row.Crm_Cd4)+"','"+str(row.LOCATION)+"','"+str(row.Cross_Street)+"',"+str(row.LAT)+","+str(row.LON)+")"
            cursor.execute(sequences)                        #SAMPLE EXECUTIONS
            #print("\n")
            connection.commit()
            
        #    print(sequences)
        #print('\n')
        except:
            print("Some problem")
        
    print("I am done here \n")    
def putP(filename,k): # k is the partitions and filename is the file being given to us
    dataframe=pd.read_csv(filename,encoding='unicode_escape')
    df=pd.DataFrame(dataframe)
    df=df.rename(columns={'AREA ':'AREA_'})
    df=df.rename(columns={'Part_1-2':'Part_1'})
    dd=list(df.columns.values)
    df=df.replace("None","NULL")
    #print(dd)
    #df=df.drop(['Unnamed: 28', 'Unnamed: 29', 'Unnamed: 30', 'Unnamed: 31', 'Unnamed: 32'], axis=1)
    df = df.where((pd.notnull(df)), None)    
    N_PARTITIONS=k
    np.random.seed(2)
    permuted_indices = np.random.permutation(len(df))
    dfs=[]

    for i in range(N_PARTITIONS):
        dfs.append(df.iloc[permuted_indices[i::N_PARTITIONS]])
    for i in range(k):
        csv_to_sqlP(dfs[i],i)
    cursor.execute("Create table CRIMEP_PARTITIONS(id int,partition_location int)")
    for i in range(k):
        rando=random.randint(0,999999)
    stringy="insert into CRIMEP_PARTITIONS values ("+str(i)+","+str(rando)+")"
    cursor.execute(stringy)
    loc=pwde()
    stringness="insert into Files_Locator values ('"+loc+"','CRIMEP')"
    cursor.execute(stringness)
    
def csv_to_sqlP(dataframe,i):## filename,i):
#    dataframe=pd.read_csv(filename,encoding='unicode_escape')
#    df=pd.DataFrame(data)
#    df=df.rename(columns={'AREA ':'AREA_'})
#    df=df.rename(columns={'Part_1-2':'Part_1'})
#    dd=list(df.columns.values)
#    df=df.replace("None","NULL")
#    print(dd)
#    df=df.drop(['Unnamed: 28', 'Unnamed: 29', 'Unnamed: 30', 'Unnamed: 31', 'Unnamed: 32'], axis=1)
#    df = df.where((pd.notnull(df)), None)
    df=dataframe
    cursor.execute("CREATE TABLE CrimeP"+str(i)+"(DR_NO varchar(50), Date_Reported datetime, Date_Occurance datetime, Time_Occurance  varchar(50), Area varchar(50), Area_Name varchar(50), Dist_No varchar(20), Part12 int, Crimes_Committed varchar(50), Crimecodedesc varchar(100), mocodes varchar(50), victime_age  varchar(50), vict_sex varchar(20), vict_desc varchar(20), premis_cd varchar(20), premis_desc varchar(50),weapon varchar(30),weapondesc varchar(50), status varchar(20), status_desc varchar(50), crm1  varchar(50),crm2  varchar(50),crm3  varchar(50), crm4  varchar(50), location varchar(100),street varchar(50),latitude decimal(9,6),longitude decimal(8,6))")
    #print(df)
    for row in df.itertuples():
        try:
        #print(row.DR_NO)
        #cursor.execute("CREATE TABLE CrimeO(DR_NO varchar(50), Date_Reported datetime, Date_Occurance datetime, Time_Occurance  varchar(50), Area varchar(50), Area_Name varchar(50), Dist_No varchar(20), Part12 int, Crimes_Committed varchar(50), Crimecodedesc varchar(100), mocodes varchar(50), victime_age  varchar(50), vict_sex varchar(20), vict_desc varchar(20), premis_cd varchar(20), premis_desc varchar(50),weapon varchar(30),weapondesc varchar(50), status varchar(20), status_desc varchar(50), crm1  varchar(50),crm2  varchar(50),crm3  varchar(50), crm4  varchar(50), location varchar(100),street varchar(50),latitude decimal(9,6),longitude decimal(8,6))")
            sequences="Insert into CrimeP"+str(i)+" values('"+str(row.DR_NO)+"','"+str(row.Date_Rptd)+"','"+str(row.DATE_OCC)+"',"+str(row.TIME_OCC)+","+str(row.AREA_)+",'"+str(row.AREA_NAME)+"','"+str(row.Rpt_Dist_No)+"',"+str(row.Part_1)+",'"+str(row.Crm_Cd)+"','"+str(row.Crm_Cd_Desc)+"','"+str(row.Mocodes)+"','"+str(row.Vict_Age)+"','"+str(row.Vict_Sex)+"','"+str(row.Vict_Descent)+"','"+str(row.Premis_Cd)+"','"+str(row.Premis_Desc)+"','"+str(row.Weapon_Used_Cd)+"','"+str(row.Weapon_Desc)+"','"+str(row.Status)+"','"+str(row.Status_Desc)+"','"+str(row.Crm_Cd1)+"','"+str(row.Crm_Cd2)+"','"+str(row.Crm_Cd3)+"','"+str(row.Crm_Cd4)+"','"+str(row.LOCATION)+"','"+str(row.Cross_Street)+"',"+str(row.LAT)+","+str(row.LON)+")"
            cursor.execute(sequences)                        #SAMPLE EXECUTIONS
            #print("\n")
            connection.commit()
            
        #    print(sequences)
        #print('\n')
        except:
            print("Some problem")
        
    print("I am done here \n")    


def mapP(tablename,query,option,i):
    sequ="select * from "+str(tablename)+str(i)+" where "+str(option)+"="+str(query)
    df=pd.read_sql(sequ,con=connection)
    return df
def reducer(tablename,query,option,part):
    s=[]
    for i in range(part):
        df11=mapP(tablename,query,option,i)
        s.append(df11)
    ddf=pd.concat(s)
    st.write(ddf)


def pwde():
    cursor.execute("select wd from pwd")
    result=cursor.fetchall()
    x=list(result)
    y=x[0]
    z=y[0]
    st.markdown("**"+z+"**")
    return z
def header(url):
     st.markdown(f'<p style="backgroundcolor:#000000;color:#ff0000;font-size:11px;border-radius:2%;">{url}</p>', unsafe_allow_html=True)

def header2(url):
     st.markdown(f'<p style="backgroundcolor:#000000;color:#ff00f0;font-size:13px;border-radius:2%;">{url}</p>', unsafe_allow_html=True)
def csvsql(df,i):
    cursor.execute("CREATE TABLE CrimeO"+str(i)+"(DR_NO varchar(50), Date_Rptd datetime, DATE_OCC datetime, TIME_OCC  varchar(50), AREA varchar(50), AREA_NAME varchar(50), Rpt_Dist_No varchar(20), Part12 int, Crm_Cd varchar(50), Crm_Cd_Desc varchar(100), Mocodes varchar(50), Vict_age  varchar(50), Vict_sex varchar(20), Vict_desc varchar(20), Premis_Cd varchar(20), Premis_Desc varchar(50),Weapon_Used_Cd varchar(30),Weapon_Desc varchar(50), status varchar(20), Crm_Cd1 varchar(50),Crm_Cd2  varchar(50),Crm_Cd3  varchar(50),Crm_Cd4  varchar(50), LOCATION varchar(100),Cross_Street varchar(50),LAT decimal(9,6),LON decimal(8,6))")
    df=df.rename(columns={'AREA ':'AREA_'})                                                                                                              #'DR_NO','Date_Rptd','DATE_OCC','TIME_OCC','AREA','AREA_NAME','Rpt_Dist_No','Part_1-2','Crm_Cd','Crm_Cd_Desc','Mocodes','Vict_Age','Vict_Sex','Vict_Descent','Premis_Cd','Premis_Desc','Weapon_Used_Cd','Weapon_Desc','Status','Crm_Cd1','Crm_Cd2','Crm_Cd3','Crm_Cd4','LOCATION','Cross_Street','LAT','LON'],

    df=df.rename(columns={'Part_1-2':'Part_1'})
    dd=list(df.columns.values)
    st.write(dd)
    df=df.replace("None","NULL")
    df = df.where((pd.notnull(df)), None) 
    for row in df.itertuples():
        try:
        #print(row.DR_NO) #'DR_NO','Date_Rptd','DATE_OCC','TIME_OCC','AREA','AREA_NAME','Rpt_Dist_No','Part_1-2','Crm_Cd','Crm_Cd_Desc','Mocodes','Vict_Age','Vict_Sex','Vict_Descent','Premis_Cd','Premis_Desc','Weapon_Used_Cd','Weapon_Desc','Status','Crm_Cd1','Crm_Cd2','Crm_Cd3','Crm_Cd4','LOCATION','Cross_Street','LAT','LON'],

        #cursor.execute("CREATE TABLE CrimeO(DR_NO varchar(50), Date_Reported datetime, Date_Occurance datetime, Time_Occurance  varchar(50), Area varchar(50), Area_Name varchar(50), Dist_No varchar(20), Part12 int, Crimes_Committed varchar(50), Crimecodedesc varchar(100), mocodes varchar(50), victime_age  varchar(50), vict_sex varchar(20), vict_desc varchar(20), premis_cd varchar(20), premis_desc varchar(50),weapon varchar(30),weapondesc varchar(50), status varchar(20), status_desc varchar(50), crm1  varchar(50),crm2  varchar(50),crm3  varchar(50), crm4  varchar(50), location varchar(100),street varchar(50),latitude decimal(9,6),longitude decimal(8,6))")
            sequences="Insert into CrimeO"+str(i)+" values('"+str(row.DR_NO)+"','"+str(row.Date_Rptd)+"','"+str(row.DATE_OCC)+"',"+str(row.TIME_OCC)+","+str(row.AREA_)+",'"+str(row.AREA_NAME)+"','"+str(row.Rpt_Dist_No)+"',"+str(row.Part_1)+",'"+str(row.Crm_Cd)+"','"+str(row.Crm_Cd_Desc)+"','"+str(row.Mocodes)+"','"+str(row.Vict_Age)+"','"+str(row.Vict_Sex)+"','"+str(row.Vict_Descent)+"','"+str(row.Premis_Cd)+"','"+str(row.Premis_Desc)+"','"+str(row.Weapon_Used_Cd)+"','"+str(row.Weapon_Desc)+"','"+str(row.Status)+"','"+str(row.Status_Desc)+"','"+str(row.Crm_Cd1)+"','"+str(row.Crm_Cd2)+"','"+str(row.Crm_Cd3)+"','"+str(row.Crm_Cd4)+"','"+str(row.LOCATION)+"','"+str(row.Cross_Street)+"',"+str(row.LAT)+","+str(row.LON)+")"
            cursor.execute(sequences)                        #SAMPLE EXECUTIONS
            #print("\n")
            st.text("Done")
            connection.commit()
            
        #    print(sequences)
        #print('\n')
        except:
            st.text("Problems")
        
def putt(df,k):
    np.random.seed(2)
    permuted_indices = np.random.permutation(len(df))
    dfs=[]
    st.write(permuted_indices)
    N_PARTITIONS=k
    for i in range(N_PARTITIONS):
        dfs.append(df.iloc[permuted_indices[i:N_PARTITIONS]])
    for i in range(k):
        csvsql(dfs[i],i)
    cursor.execute("Create table CRIMEO_PARTITIONS(id int,partition_location int)")
    for i in range(k):
        rando=random.randint(0,999999)
    stringy="insert into CRIMEO_PARTITIONS values ("+str(i)+","+str(rando)+")"
    cursor.execute(stringy)
    loc=pwde()
    stringness="insert into Files_Locator values ('"+loc+"','CRIMEO')"
    cursor.execute(stringness)
def csvsqlP(df,i):
    cursor.execute("CREATE TABLE CrimeP"+str(i)+"(DR_NO varchar(50), Date_Reported datetime, Date_Occurance datetime, Time_Occurance  varchar(50), Area varchar(50), Area_Name varchar(50), Dist_No varchar(20), Part12 int, Crimes_Committed varchar(50), Crimecodedesc varchar(100), mocodes varchar(50), vict_age  varchar(50), vict_sex varchar(20), vict_desc varchar(20), premis_cd varchar(20), premis_desc varchar(50),weapon varchar(30),weapondesc varchar(50), status varchar(20), status_desc varchar(50), crm1  varchar(50),crm2  varchar(50),crm3  varchar(50), crm4  varchar(50), location varchar(100),street varchar(50),latitude decimal(9,6),longitude decimal(8,6))")
    df=df.rename(columns={'AREA ':'AREA_'})
    df=df.rename(columns={'Part_1-2':'Part_1'})
    for row in df.itertuples():
        try:
        #print(row.DR_NO)
        #cursor.execute("CREATE TABLE CrimeO(DR_NO varchar(50), Date_Reported datetime, Date_Occurance datetime, Time_Occurance  varchar(50), Area varchar(50), Area_Name varchar(50), Dist_No varchar(20), Part12 int, Crimes_Committed varchar(50), Crimecodedesc varchar(100), mocodes varchar(50), victime_age  varchar(50), vict_sex varchar(20), vict_desc varchar(20), premis_cd varchar(20), premis_desc varchar(50),weapon varchar(30),weapondesc varchar(50), status varchar(20), status_desc varchar(50), crm1  varchar(50),crm2  varchar(50),crm3  varchar(50), crm4  varchar(50), location varchar(100),street varchar(50),latitude decimal(9,6),longitude decimal(8,6))")
            sequences="Insert into CrimeP"+str(i)+" values('"+str(row.DR_NO)+"','"+str(row.Date_Rptd)+"','"+str(row.DATE_OCC)+"',"+str(row.TIME_OCC)+","+str(row.AREA_)+",'"+str(row.AREA_NAME)+"','"+str(row.Rpt_Dist_No)+"',"+str(row.Part_1)+",'"+str(row.Crm_Cd)+"','"+str(row.Crm_Cd_Desc)+"','"+str(row.Mocodes)+"','"+str(row.Vict_Age)+"','"+str(row.Vict_Sex)+"','"+str(row.Vict_Descent)+"','"+str(row.Premis_Cd)+"','"+str(row.Premis_Desc)+"','"+str(row.Weapon_Used_Cd)+"','"+str(row.Weapon_Desc)+"','"+str(row.Status)+"','"+str(row.Status_Desc)+"','"+str(row.Crm_Cd1)+"','"+str(row.Crm_Cd2)+"','"+str(row.Crm_Cd3)+"','"+str(row.Crm_Cd4)+"','"+str(row.LOCATION)+"','"+str(row.Cross_Street)+"',"+str(row.LAT)+","+str(row.LON)+")"
            cursor.execute(sequences)                        #SAMPLE EXECUTIONS
            #print("\n")
            connection.commit()
            
        #    print(sequences)
        #print('\n')
        except:
            pass
        
def puttP(df,k):
    np.random.seed(2)
    permuted_indices = np.random.permutation(len(df))
    dfs=[]
    N_PARTITIONS=k
    for i in range(N_PARTITIONS):
        dfs.append(df.iloc[permuted_indices[i::N_PARTITIONS]])
    for i in range(k):
        csvsqlP(dfs[i],i)
    cursor.execute("Create table CRIMEP_PARTITIONS(id int,partition_location int)")
    for i in range(k):
        rando=random.randint(0,999999)
        stringy="insert into CRIMEP_PARTITIONS values ("+str(i)+","+str(rando)+")"
        cursor.execute(stringy)
    loc=pwde()
    stringness="insert into Files_Locator values ('"+loc+"','CRIMEP')"
    cursor.execute(stringness)
    
def extractoro(xl):
    O=[]
#    df0=pd.read_sql("select * from CrimeO0",con=connection)
 #   df1=pd.read_sql("select * from CrimeO1",con=connection)
    #st.write(df1)
    #df1=df1.append(df0)
    for i in range(xl):
        sentence= "select * from CrimeO"+str(xl)
        df=pd.read_sql(sentence,con=connection)
        #df1=df1.append(df)
        #st.write(df)
        O.append(df)
    df1=pd.concat(O)
    return df1
def extractorp(xl):
    P=[]
    #df0=pd.read_sql("select * from CrimeP0",con=connection)
    #df1=pd.read_sql("select * from CrimeP1",con=connection)
    #st.write(df1)
    #df1=df1.append(df0)
    for i in range(xl):
        sentence= "select * from CrimeP"+str(xl)
        df=pd.read_sql(sentence,con=connection)
        #df1=df1.append(df)
        #st.write(df)
        P.append(df)
    df1=pd.concat(P)
    return df1
    
def specialreducer(table,optional,stringcom,partitions):
    S=[]
    for i in range(partitions):
        df=specialmap(table,optional,stringcom,i)
        S.append(df)
    ddf=pd.concat(S)
    return ddf
def specialmap(table,optional,stringcom,i):
    sentence="select "+optional+" from CrimeO"+str(i)+" where "+str(stringcom)
    df=pd.read_sql(sentence,con=connection)
    if(len(df)==0):
        pass
    else:
        st.text("This is what we found in partition "+str(i))
        st.write(df)
    return df

set_background("./mine.jpg")
def my_widget(key):
    st.title("DS551 Project Emulated Distributed File System")
    st.text("The semester long project aims to emulate the functioning of a Distributed file system")
    st.text("In this project, we attempt to utilize MySQL and Python for the successful implementation of the same")
    st.text("The DFS is capable of only accepting data in csv format")
    st.header("Let us upload our data in this section")
    st.markdown("First, we attempt to upload our Crime Dataset to our distributed file system with the number of desired partitions")
    xl=st.text_input("Enter the number of partitions you desire to have")
    st.text("Please select any one of them as selecting both of them would further elongate the process of uploading the same")
    
    result1=st.button("Wanna Upload Crimes between 2010 to 2019?")
    dfo=pd.read_csv("./CRIMEP.csv")
    dfo=dfo.drop(['Unnamed: 0'],axis=1)
    if result1:
        #st.write(dfo.head(10))
        put("./CRIMEO.csv",int(xl))
    
    #st.text("OR")
    #result2=st.button("Wanna Upload Crimes between 2020 and present?")
    #dfp=pd.read_csv("./CRIMEP.csv")
    
    #if result2:
     #   st.write(dfp)
      #  #st.write(dfp.head(10))
       # put("./CRIMEP.csv",8)



# This works in the main are
def rm(k):
    for i in range(k):
        sentence="drop table CrimeO"+str(i)
        cursor.execute(sentence)
    cursor.execute("drop table CRIMEO_PARTITIONS")

# And within an expander
my_expander = st.expander("'cat' command ", expanded=True)
with my_expander:
    tab1= st.tabs(["'cat' for CRIMEO"])

    texter=st.text_input("Final index of the partition you wanna look at, i.e. total number of partitions - 1")
    #texting=int(texter)
        
    onetwokafour=st.button("cat CRIMEO")
    

    if onetwokafour:
        dataf=extractoro(int(texter))

        st.text("DB size as a whole")
        st.write(len(dataf))
        st.write(dataf)
       
        

##    with tab2:
   #     twofourkaone=st.button("cat CRIMEP")
  #      if twofourkaone:
    #        datf=extractorp(5)
     #       st.write(datf)
    



# AND in st.sidebar!
    with st.sidebar:
        clicked = my_widget("third")
my_expander = st.expander("Current Directory Structure", expanded=True)
with my_expander:
    tab1 = st.tabs(["'Current Directory Structure for CRIMEO"])
    st.text("Current Directory Structure")

    sentenced="select * from Directory_Organizer"

    ip=pd.read_sql(sentenced,con=connection)
    for i in range(len(ip)):
        x=str(ip.iloc[i][0])
        y=str(ip.iloc[i][1])
        scri=x+"/"+y
        st.text(scri)
    pwd=st.button("Present working directory?")
    if pwd:
        pwde()

    st.markdown("**Wanna change directories?**")
    state=st.button("Wanna Change Directory?")
    text=st.text_input("Which directory do you wanna go to?")
    if state:
        settled="update pwd set wd='"+text+"' where id=0"
        cursor.execute(settled)
        if settled:
            sequioa="The new present working directory is "+text
            st.text(sequioa)

my_expander = st.expander("Directory Content Checker", expanded=True)
with my_expander:
    tab1 = st.tabs(["'Similar to the ls command"])
    
    header("Implementation of ls that is list of contents of a directory")

    rice=st.text_input("Which directory do you wanna lookup")
    wantlist=st.button("Wanna see the list?")
    syntacter="select child_dir from Directory_Organizer where parent_dir='"+str(rice)+"'"
    syntacter2="select child_file from Files_Locator where parent_dir='"+str(rice)+"'"
    directories=pd.read_sql(syntacter,con=connection)
    directories2=pd.read_sql(syntacter2,con=connection)
    st.text("DIRECTORIES:")
    st.write(directories)
    st.text("FILES:")
    st.write(directories2)

my_expander = st.expander("Create a directory", expanded=True)
with my_expander:
    tab1 = st.tabs(["'Similar to the mkdir command"])
    
    st.text("Wanna create a directory?")

    mkdir=st.button("Sure")
    st.text("If yes, fill in the box below with the name you want")
    direc=pwde()

    title = st.text_input('New Dir to create ')
    if mkdir:
        sentence="Insert into Directory_Organizer values ('"+direc+"','"+title+"')"
        cursor.execute(sentence)
        st.write('The new directory shall be', title)

my_expander = st.expander("Delete a directory", expanded=True)
with my_expander:
    tab1 = st.tabs(["'Similar to the rmdir command"])

    st.text("Wanna delete a directory?")
    st.markdown("Note that it should be located in the present directory")
    rmdir=st.button("For Sure")

    st.text("If yes, fill in the box below with the name you want")

    title = st.text_input('Directory to be delete')

    if rmdir:                  #We print the directory structure here
        sent="Delete from Directory_Organizer where child_dir='"+title+"' and parent_dir='"+direc+"'"
        cursor.execute(sent)

    st.write('The directory has been deleted :', title)


my_expander = st.expander("Partition checker", expanded=True)
with my_expander:
    tab1 = st.tabs(["'Similar to the readpartitions command"])
    header("readPartitions implemented")
    st.text("Want to see the content of a particular partition for your upload?")

    title = st.text_input('Partition #')
    st.write('Partition # is ', title)
    recording=st.button("Go on for CRIMEO")
    recording2=st.button("Go on for CRIMEP")
    if recording:
        queryy="Select * from CrimeO"+str(title)
        dfod=pd.read_sql(queryy,con=connection)
        st.write(dfod)
    if recording2:
        qerr="select * from CrimeP"+str(title)
        dfdo=pd.read_sql(qerr,con=connection)
        st.write(dfdo)

my_expander = st.expander("Get Partitions", expanded=True)
with my_expander:
    tab1 = st.tabs(["'Similar to the getpartitions command"])
    st.text("Wanna see the location of all your partitions?")
    dataframing = pd.read_sql("""
            SELECT *
            FROM CRIMEO_PARTITIONS
            """, con = connection)
    dataframing=dataframing.iloc[:,1:]
    
    getp=st.button("Yup")
    if getp:                  #We print the directory structure here
        st.write(dataframing)

my_expander = st.expander("PMR", expanded=True)
with my_expander:
    tab1 = st.tabs(["'Similar to the PMR functions"])
    header("Implementation of the PMR which maps a search query across all the partitions of our original crime datasets; CRIMEO and CRIMEP")
    st.subheader("Now, we move to the part where we can search our partitioned database for certain elements")
    st.text("Which Database you wanna check out?")
    options = st.multiselect(
    'Which one?',
    ['CRIMEO','CRIMEP'],
    ['CRIMEO'])

    st.write('You selected:', options)
    st.text("Select the one single attribute below whose output you wish to check out")
    option = st.selectbox(
    'Which one',
    ('DR_NO','Date_Reported','Date_Occurance','Time_Occurance','Area','Area_Name','Dist_No','Part12','Crimes_committed','Crimecodedesc','mocodes','Victim_age','Vict_Sex','Vict_Desc','premis_cd','premis_desc','weapon','Weapondesc','Status','crm1','crm2','crm3','crm4','Location','street','latitude','longitude'))

    #st.text(type(option))

    st.write('You selected:', option)
    st.text("Please input the value as well")

    query = st.text_input('Query text?')
    st.write('The queried text is', query)
    yvetal=st.button("Go Ahead")
    if yvetal:
        reducer("CrimeO",query,option,4)

my_expander = st.expander("More configurable PMR", expanded=True)
with my_expander:
    tab1 = st.tabs(["A better versino of PMR."])
    
    st.text("If you want any selective set of output attributes, click the below button for further process else skip:")
    options = st.multiselect(
    'What are your attribute choices',
    ['DR_NO','Date_Rptd','DATE_OCC','TIME_OCC','AREA','AREA_NAME','Rpt_Dist_No','Part12','Crm_Cd','Crm_Cd_Desc','Mocodes','Vict_Age','Vict_Sex','Vict_Descent','Premis_Cd','Premis_Desc','Weapon_Used_Cd','Weapon_Desc','Status','Crm_Cd1','Crm_Cd2','Crm_Cd3','Crm_Cd4','LOCATION','Cross_Street','LAT','LON'],
    ['DR_NO'])
    if(len(options)==0):
        options=['DR_NO','Date_Rptd','DATE_OCC','TIME_OCC','AREA','AREA_NAME','Rpt_Dist_No','Part_12','Crm_Cd','Crm_Cd_Desc','Mocodes','Vict_Age','Vict_Sex','Vict_Descent','Premis_Cd','Premis_Desc','Weapon_Used_Cd','Weapon_Desc','Status','Crm_Cd1','Crm_Cd2','Crm_Cd3','Crm_Cd4','LOCATION','Cross_Street','LAT','LON']
    else:
        pass
    st.write(options)
    optional=','.join(options)
    st.write('You selected:', options)
    st.text("Give the values required by you for each seperated by an and i.e., should be akin to a SQL queries where keyword's query terms")
    curious=st.text_input("Values:")
    S=[]
    st.write('The queried text values are',curious)
    sss=st.button("Should we search now")
    if sss:
        df=specialreducer("CrimeO",optional,curious,4)
        st.write(df)
my_expander = st.expander("Delete the file i.e. rm", expanded=True)
with my_expander:
    tab1 = st.tabs(["'Similar to the rm command"])

    st.text("Wanna delete the file?")
    st.markdown("Note that it should be located in the present directory")

    rmdire=st.button("Okay, Here we go")

    st.text("If yes, fill in the box below with the name you want")

    title = st.text_input('File to be delete')
    trait = st.text_input('Total partitions given initially')

    if rmdire:                  #We print the directory structure here
        cursor.execute("Select parent_dir from Files_Locator where child_file='CRIMEO'")
        result=cursor.fetchall()
        result=list(result)
        a=[]
        for i in range(len(result)):
            result[i]=list(result[i])
            x=result[i]
            a.append(x[0])
        t=pwde()
        if t in a:
            rm(int(trait))
        else:
            st.text("We are outside the directory")
        
    st.write('The file has been deleted :', title)


#DR_NO,Date_Rptd,DATE_OCC,TIME_OCC,AREA_,AREA_NAME,Rpt_Dist_No,Part_1-2,Crm_Cd,Crm_Cd_Desc,Mocodes,Vict_Age,Vict_Sex,Vict_Descent,Premis_Cd,Premis_Desc,Weapon_Used_Cd,Weapon_Desc,Status,Crm_Cd1,Crm_Cd2,Crm_Cd3,Crm_Cd4,LOCATION,Cross_Street,LAT,LON
#dataobt=dfe[['DR_NO','Date_Rptd','TIME_OCC','Crm_Cd_Desc','Status','LOCATION','Vict_Age']]
#dataobt=dfe[['DR_NO','DATE_OCC','TIME_OCC','AREA_','AREA_NAME']]
#st.text("Obtained the following in partition 29:")
#st.write(dataobt)

#Pandas filterer





