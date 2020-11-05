#-*- coding: utf-8 -*-

import os
import json
import logging
import cx_Oracle

os.putenv('NLS_LANG', '.UTF8')

with open('config.json') as f:
  config = json.load(f)

logging.basicConfig(
  filename='sometrend-media-rank-batch.log', 
  filemode='a', 
  level=logging.DEBUG, 
  format='%(asctime)s [%(process)d] - %(filename)s - %(funcName)s - %(levelname)s - %(message)s (line: %(lineno)d)')

def getConnection():
  try:
    dsn = cx_Oracle.makedsn(config['oracle']['host'], config['oracle']['port'], config['oracle']['sid'])
    connection = cx_Oracle.connect(config['oracle']['user'], config['oracle']['password'], dsn)
    logging.debug('Connected from db complete!')

    return connection
  except Exception as e:
    logging.error(e)

def selectList(conn):
  try:
    cur = conn.cursor()
    cur.execute('select * from PO_PROG where RGST_DT is not null order by RGST_DT desc')
    rows = cur.fetchall()
    cur.close()
    
    return rows
  except Exception as e:
    logging.error(e)