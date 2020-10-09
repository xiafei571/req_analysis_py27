# coding=utf-8
import csv
import datetime
import glob
import time
import sqlite3

from google.appengine.api import datastore
from google.appengine.datastore import entity_pb
import time


def read_sql3(file_path):
    all_files = glob.glob(file_path + "*.sql3")
    t = int(time.time())
    csvwriter = csv.writer(open('./csv/result_%s.csv' % t, 'w'))
    csvwriter.writerow(['time_stamp', 'date_time', 'user_agent', 'client_ip', 'uri'])
    for sql3_file in all_files:
        print('read file:', sql3_file)
        count = 0
        conn = sqlite3.connect(sql3_file)
        cursor = conn.cursor()

        cursor.execute('select id,value from result')
        for unused_entity_id, entity in cursor:
            entity_proto = entity_pb.EntityProto(contents=entity)
            f = datastore.Entity._FromPb(entity_proto)
            date_time = f['time'].replace(microsecond=0) + datetime.timedelta(hours=9)
            time_stamp = int(time.mktime(date_time.timetuple()))
            user_agent = f['agent']
            client_ip = f['clientip']
            uri = f['uri']
            csvwriter.writerow([time_stamp, date_time, user_agent, client_ip, uri])
            count = count + 1
        cursor.close()
        conn.close()
        print('count:', count)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    read_sql3('./sql3/')
