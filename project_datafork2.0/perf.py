import pandas as pd
from sodapy import Socrata
from datetime import datetime
import time
import psycopg2
from psycopg2 import sql
import requests
import urllib.request
from bs4 import BeautifulSoup
import dateutil.relativedelta
import os
from matplotlib import pyplot as plt
import matplotlib as mpl
from collections import Counter
import numpy as np
from sqlalchemy import create_engine
import xml.etree.cElementTree as ET
import xml.etree.ElementTree as xml

#url  = 'https://www.consumerfinance.gov/data-research/consumer-complaints/'

#return text associated with given tag in configutation xml_file
def parseXML(xml_file,tag):
    tree = ET.ElementTree(file=xml_file)
    root = tree.getroot()
    for child in root:
        if (child.tag == tag):
            return( child.text)

# Gets link for downloading
def get_link(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'lxml')
    x = soup.find('div', class_ = parseXML('config.xml','section')).find('a', class_ = parseXML('config.xml','link'))[parseXML('config.xml','typ')]
    return x

# Downloads datatable to csv file
# We allways use current.csv because we need only one csv file
def download1(url, file_name):
    beg = datetime.now()
    # na vsyakyi a to hren ego znaet
    if os.path.isfile(file_name) :
        os.remove(file_name)

    with open(file_name, "wb") as file:
        # get request
        response = requests.get(url)
        # write to file
        file.write(response.content)
        file.close()
    end = datetime.now()
    print('Downloading time: '+ str(end-beg))

#Copies csv(current.csv) file to sql table 
def to_sql(table_name):
    beg = datetime.now()
    csv_full = parseXML('config.xml','csvfull')
    #print(csv_full)
    conn = psycopg2.connect(parseXML('config.xml','connection'))
    cur = conn.cursor()
    cur.execute("SET CLIENT_ENCODING TO 'utf8';")
    cur.execute(
        sql.SQL("""CREATE TABLE {} (date_received date, product text,
                    sub_product text,issue text,sub_issue text,complaint_what_happened text,
                    company_public_response text, company text, state text, zip_code text, tags text, 
                    consumer_consent_provided text, submitted_via text, date_sent_to_company date, 
                    company_response text, timely text, consumer_disputed text,
                    complaint_id integer);""").format(psycopg2.sql.Identifier(table_name)))

    query = sql.SQL("""copy {} FROM %s
        HEADER DELIMITER ',' 
        CSV""").format(psycopg2.sql.Identifier(table_name))
    cur.execute(query, (csv_full,))

    conn.commit()
    cur.close()
    conn.close()
    end = datetime.now()
    print('Copying to sql time: '+ str(end-beg))

#just delete dataframe
def delete_datatable(tablename):
    conn = psycopg2.connect(parseXML('config.xml','connection'))
    cur = conn.cursor()
    cur.execute(sql.SQL("DROP TABLE{};").format(psycopg2.sql.Identifier(tablename)))
    #print('Deleted Datatable'+prev_data)
    conn.commit()
    cur.close()
    conn.close()


# creates pandas datafrane consisted of id, company and date received for all complaints
def all_to_pandas(table):
    conn = psycopg2.connect(parseXML('config.xml','connection'))
    query = sql.SQL(" select complaint_id, company, date_received  from  {}").format(psycopg2.sql.Identifier(table))
    #print (query)
    df = pd.read_sql(query,conn,index_col='complaint_id')
    conn.commit()
    return df

# plot the amount of updates per day
def graph_of_updates(df,figure_name):
    nof_comp= df.groupby('date_received').size()

    mpl.rcParams['figure.figsize'] = (14.0, 9.0)
    plt.plot(nof_comp)
    plt.grid(True)
    #plt.show()
    plt.savefig(figure_name)
    plt.close()


# plot the amount of distinct companies per day
def graph_of_companies(df,figure_name):
    nof_companies = df[['company','date_received']].groupby('date_received').agg(lambda x : len(set(x)))
    plt.plot(nof_companies)
    plt.grid(True)
    plt.savefig(figure_name)
    #plt.show()
    plt.close()

# make these 2 plots
def make_plots(table, upd_name, comp_name):
    beg = datetime.now()
    df = all_to_pandas(table)
    graph_of_updates(df,upd_name)
    graph_of_companies(df,comp_name)
    end = datetime.now()
    print('Plotting time: '+ str(end-beg))

#load last months from sql table to pandas
def to_pandas(table, n_of_months):

    conn = psycopg2.connect(parseXML('config.xml','connection'))
    d = str(datetime.today() - dateutil.relativedelta.relativedelta(months=n_of_months))[0:10]
    query = sql.SQL("select * from  {} where date_received > '{}'::date;").format(psycopg2.sql.Identifier(table),
                                                                               psycopg2.sql.Identifier(d))
    df = pd.read_sql(query,conn,index_col='complaint_id')
    df.fillna(value=np.nan, inplace=True)
    
    return df

#load last months from
def load_last_months(n_of_months):
    client = Socrata(parseXML('config.xml','clienturl'), None)
    socrata_dataset_identifier = parseXML('config.xml','socrata_ind')

    conn = psycopg2.connect(parseXML('config.xml','connection'))
    cur = conn.cursor()
    cur.execute("SET DATESTYLE TO 'ISO,MDY';")
    
    d = str(datetime.today() - dateutil.relativedelta.relativedelta(months=1))[0:10]
    
    query = """date_received > '""" +d+"""T00:00:00.000'"""
    
   # print(query)
    results = client.get(socrata_dataset_identifier, where = query,limit = 10000000)
    df = pd.DataFrame.from_records(results,index='complaint_id')
    df.date_received = pd.to_datetime(df.date_received,format='%Y-%m-%dT%H:%M:%S.%f').dt.date
    df.date_sent_to_company = pd.to_datetime(df.date_sent_to_company,format='%Y-%m-%dT%H:%M:%S.%f').dt.date
    df.index = pd.to_numeric(df.index)
    df.fillna(value=np.nan, inplace=True)
    return df


def compare_pandas(df1,df2):
    # concatenate, reset index to elevate index to series, drop duplicates
    df = pd.concat([df1, df2]).reset_index().drop_duplicates()

    # add change series dependent on duplicates by index

    df['changes'] = np.where(df.duplicated('complaint_id'), datetime.strftime(datetime.now(), "d%Y_%m_%d_%H_%M_%S_%f"), None)

    # reset index for desired output
    df = df.set_index('complaint_id')
    return df 

def add_changes_column(table_name):
    conn = psycopg2.connect(parseXML('config.xml','connection'))
    cur = conn.cursor()
    cur.execute(sql.SQL("ALTER TABLE {} ADD changes text;").format(psycopg2.sql.Identifier(table_name)))
    conn.commit()
    cur.close()
    conn.close()

def drop_lm(table, n_of_months):

    conn = psycopg2.connect(parseXML('config.xml','connection'))
    cur = conn.cursor()
    d = str(datetime.today() - dateutil.relativedelta.relativedelta(months=n_of_months))[0:10]
    query = sql.SQL("DELETE from  {} where date_received > '{}'::date;").format(psycopg2.sql.Identifier(table),
                                                                               psycopg2.sql.Identifier(d))
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()
    

def create_datatable(table_name):
    to_sql(table_name)
    try:
        add_changes_column(table_name)
    except:
        print('ERROR: Double Indexation')
    conn = psycopg2.connect(parseXML('config.xml','connection'))
    cur = conn.cursor()
    try:
        cur.execute(sql.SQL("CREATE INDEX {} ON {} (complaint_id, changes);").format(psycopg2.sql.Identifier(table_name+'ind'),
                                                            psycopg2.sql.Identifier(table_name)))
    except: pass
    conn.commit()
    cur.close()
    conn.close()



def main():
    download1(get_link(parseXML('config.xml','url')),parseXML('config.xml','csv'))
    current_data = datetime.strftime(datetime.now(), "d%Y_%m_%d_%H_%M_%S_%f")  
    create_datatable(current_data)

    print('Created Datatable '+current_data)

    while True:
        beg = datetime.now()

        try:
            new = load_last_months(1)
        except:
            print('downloading new data error')
            continue
        
        prev = to_pandas(current_data,1)
        drop_lm(current_data,1)

        prev_prev = prev[prev.changes == prev.changes]
        prev_latest = prev[prev.changes != prev.changes].drop(['changes'], axis = 1)

        print(prev_prev.shape)
        print(prev_latest.shape)

        current = compare_pandas(new,prev_latest)

        engine = create_engine(parseXML('config.xml','engine'))
        current.to_sql(current_data, engine,if_exists='append')
        prev_prev.to_sql(current_data, engine,if_exists='append')

        make_plots(current_data, 'updates.png', 'companies.png')

        end = datetime.now()
        print('duration of last cycle: '+ str(end-beg))
        print('sleeping\n')
        time.sleep(30)
        print('AWAKENING\n')
            



            
if __name__ == '__main__':
    main()
