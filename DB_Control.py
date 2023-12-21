import cx_Oracle
import pandas as pd

# DB 연결 함수
# uesername : 아이디, password : 암호, dsn : 접속주소
def db_conn(username, password, dsn) :
    try :
        conn = cx_Oracle.connect(username, password, dsn)
        cur = conn.cursor()
        print("DB 접속 성공")
        
        # 생성한 연결/커서 객체를 반환
        return conn, cur
    except cx_Oracle.DatabaseError as e:
        print(e)

# DB 연결 해제 함수
# cur : 커서 객체, conn : 연결 객체
def db_disconn(conn, cur) :
    cur.close()
    conn.close()
    print("DB 연결 해제")

# 데이터 입력 함수
# t_name : 테이블 명, input_data : 저장할 데이터의 딕셔너리 
def insertData(conn, cur, t_name, input_data):
    # input_data = {"code":code, "name":name, "age":age}
    # 딕셔너리의 키 값을 받아서 query 문을 생성
    # 테이블명을 추가
    # insert into test_tbl values (
    query = f"insert into {t_name} values ("
    
    # 각 컬럼명(키값)을 가져와서 배치
    # insert into test_tbl values (:code, :name, :age,
    for key in input_data.keys() :
        query += f":{key},"
        
    # 마지막의 컴머 제거
    # insert into test_tbl values (:code, :name, :age
    query = query.rstrip(",")
    
    # 괄호 닫기
    # insert into test_tbl values (:code, :name, :age)
    query += ")"
    
    # 딕셔너리 값을 할당해서 쿼리문 실행
    try :
        cur.execute(query, input_data)
    
        # 실행 검증 - cursor객체는 실행을 잘 완료하면 0보다 큰 값을 반환
        if cur.rowcount != 0 :
            print("데이터 추가 완료")
        else :     
            print("데이터 추가 실패")
    
        conn.commit()
    
    except cx_Oracle.DatabaseError as e :
        print(e)    

# 전체 데이터 검색 함수
def searchAllData(cur, t_name) :
    query = f"select * from {t_name}"
    
    try :
        cur.execute(query)
        row = cur.fetchall()
        
        return row
    except cx_Oracle.DatabaseError as e :
        print(e)   

# 한개 데이터 검색 함수
# s_key : 컬럼명, s_value : 컬럼값 -> code = "A100"
def searchData(cur, t_name, s_key, s_value) :
    query = f"select * from {t_name} where {s_key}='{s_value}'"
    
    try :
        cur.execute(query)
        row = cur.fetchone()
        
        return row
    except cx_Oracle.DatabaseError as e :
        print(e)

# 데이터 수정 함수
def updateData(conn, cur, t_name, s_key, s_value, input_data) :
    # update arome_tbl set 
    query = f"update {t_name} set "

    # update arome_tbl set name=:name,age=:age, 
    for key in col_dic.keys() :
        query += f"{key}=:{key},"
    
    # update arome_tbl set name=:name,age=:age
    query = query.rstrip(",")
    
    # update arome_tbl set name=:name,age=:age where code='code값';
    query += f" where {s_key}='{s_value}'"

    try :
        cur.execute(query, input_data)
        conn.commit()
        
        if cur.rowcount != 0 :
            print("변경 완료")
        else :     
            print("변경 실패")
    except cx_Oracle.DatabaseError as e :
        print(e)

# 데이터 삭제 함수
def deleteData(conn, cur, t_name, s_key, s_value) :
    query = f"delete from {t_name} where {s_key}='{s_value}'" 
    
    try :
        cur.execute(query)
        conn.commit()
         
        if cur.rowcount != 0 :
            print("삭제 완료")
        else :
            print("삭제 실패")
    except cx_Oracle.DatabaseError as e:
        print(e)
