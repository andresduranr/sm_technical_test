import pymysql

class Connection:
    def __init__(self,server, user, password, database):
        self.db = pymysql.connect(host = server, user = user,  password = password, database = database)
        self.cursor = self.db.cursor()
        print("Succesfully Connected to Mysql")     


    def execute_query(self, sql):
        self.cursor.execute(sql)
        return self.cursor
    
    def close_connection(self):
        self.db.close()
        print("Disconnected from MySql")

    def commit(self):
        self.db.commit()
        return
    def rollback(self):
        self.db.rollback()
        return