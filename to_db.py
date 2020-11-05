#-*- coding: utf-8 -*-

import os
import time
import json
import logging
import pymysql

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
    connection = pymysql.connect(
      user = config['mysql']['user'], 
      passwd = config['mysql']['password'], 
      host = config['mysql']['host'], 
      db = config['mysql']['db'], 
      charset = config['mysql']['charset']
    )

    logging.debug('Connected from db complete!')

    return connection
  except Exception as e:
    logging.error(e)

def save(conn, rows):
  try:
    logging.info('po_prog -> tb_prog 데이터 입력 중입니다.')
    start_time = time.time()

    cur = conn.cursor()
    cur.executemany('insert into tb_prog (PROG_ID, BRDCST_STTN_ID, GENRE_GB, PROG_TITLE, DESCR, IMG_FILE_PATH, BRDCST_PROGMNG_WEEK_DT, BRDCST_PROGMNG_START_TM, BRDCST_PROGMNG_END_TM, BRDCST_PROGMNG_TMP_YN, PROG_START_DT, PROG_END_DT, PROG_REF_CD, DSPL_ST_CD, RGST_DT, UPD_DT, RGSTR_ID, UPDR_ID, PROG_AUD_RATNG_TITLE, EX_REF_URL, COLLECT_YN, PROG_TITLE_ALIAS, DSPL_ST_PR, ACTORS, GENRE_ENT_LARGE, GENRE_ENT_SMALL, SVC_SRT, PROG_NO_SPACE_NAME) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', rows)
    conn.commit()

    end_time = time.time()
    logging.info(cur.rowcount, 'po_prog -> tb_prog 데이터 전송 배치가 완료되었습니다. 소요시간: ', (end_time - start_time), '초')

    cur.close()
  except Exception as e:
    logging.error(e)

def selectList(conn):
  try:
    cur = conn.cursor()
    cur.execute('select * from tb_prog where RGST_DT is not null order by RGST_DT desc')
    rows = cur.fetchall()
    cur.close()
    
    return rows
  except Exception as e:
    logging.error(e)