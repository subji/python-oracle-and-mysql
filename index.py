#-*- coding: utf-8 -*-

import to_db
import from_db
import logging

logging.basicConfig(
  filename='sometrend-media-rank-batch.log', 
  filemode='a', 
  level=logging.DEBUG, 
  format='%(asctime)s [%(process)d] - %(filename)s - %(funcName)s - %(levelname)s - %(message)s (line: %(lineno)d)')

def compare(from_list, to_list):
  try:
    remain = (len(from_list) - len(to_list)) + 1
    add_list = from_list[0:remain]

    logging.info('새로 추가될 row 개수: ', remain)
    logging.info('새로 추가될 rows: ', add_list)

    return add_list
  except Exception as e:
    logging.error(e)

if __name__ == '__main__':
  try:
    logging.info('출발지, 도착지 DB 커넥션 가져오는 중...')
    to_conn = to_db.getConnection()
    from_conn = from_db.getConnection()
    logging.info('출발지, 도착지 DB 커넥션 생성 완료!')

    logging.info('출발지, 도착지 Prog 테이블 데이터 가져오는 중...')
    to_prog = to_db.selectList(to_conn)
    from_prog = from_db.selectList(from_conn)
    logging.info('출발지, 도착지 Prog 테이블 데이터 목록 조회 완료!')

    logging.info('출발지, 도착지 Row 비교 하는 중...')
    add_list = compare(from_prog, to_prog)
    logging.info('출발지, 도착지 Row 비교 완료!')

    # logging.info('출발지 -> 도착지 데이터 추가하는 중...')
    # to_db.save(to_conn, add_list)
    # logging.info('출발지 -> 도착지 데이터 추가 완료!')

    logging.info('출발지, 도착지 DB 커넥션 종료하는 중...')
    to_conn.close()
    from_conn.close()
    logging.info('출발지, 도착지 DB 커넥션 종료 완료!')
  except Exception as e:
    to_conn.close()
    from_conn.close()
    logging.error(e)