import petl as etl
import os
from Database.Connection import Connection
import numpy as np

I_RAW_FILE_PATH = '/csv/RAW_interactions.csv'
R_RAW_FILE_PATH = '/csv/RAW_recipes.csv' 
R_PREP_FILE_PATH ='/csv/PP_recipes.csv'


class RecipeFileManager:
    
    def __init__(self):
        print("Instanciating Recipes File Manager")
        self.conn = Connection('192.168.1.3', 'sourcemeriuser','holamundo123', 'sm_technicaltest') 

    
    def extract_from_csv(self):
        self.table_name = 'recipes'
        try:
            rpf = os.getcwd()+R_RAW_FILE_PATH
            ppf = os.getcwd()+R_PREP_FILE_PATH
            self.raw = etl.fromcsv(rpf,delimiter=",",encoding='latin-1')
            self.pp = etl.fromcsv(ppf,delimiter=",",encoding='latin-1')
            self.merged_table = etl.join(self.raw,self.pp, key = 'id')
            
        except Exception as e:
            print(e)
          
    def transform_data(self):
        
        # Delete unnecesary fields (just the fields needed on database table recipes)
        self.merged_table = etl.cut(self.merged_table, 1,0,2,3,4,9,17,7,11,6) 
        
        print(f"Rows on merged recipes table : {etl.nrows(self.merged_table)}")

        #Once fields has been parsed, must be converted to data type expected in the Database
        try:
            self.merged_table = etl.convert(self.merged_table,0,int)
        except Exception as e:
            print(e)
            return False
        try:
            self.merged_table = etl.convert(self.merged_table,2,int)
        except Exception as e:
            print(e)
            return False

        try:
            isodate = etl.dateparser('%d/%m/%Y')
            self.merged_table = etl.convert(self.merged_table,4,lambda d : isodate(d))
        except Exception as e:
            print(e)    

        try:
            self.merged_table = etl.convert(self.merged_table,6,int)
        except Exception as e:
            print(e)
            return False
        
        try:
            self.merged_table = etl.convert(self.merged_table,7,int)
        except Exception as e:
            print(e)
            return False
        
        try:
            self.merged_table = etl.convert(self.merged_table,8,int)
        except Exception as e:
            print(e)    
            return False
        #Make the headers and database columns have the same names  
        self.merged_table = etl.rename(self.merged_table,'id', 'recipe_id') 
        
        #Store the nutrition values after validations on recipes 
        self.nutrition_values = etl.cut(self.merged_table,'recipe_id','nutrition')
        # Must get rid off nutrition values on recipe's petl table just before load to DB
        self.merged_table = etl.cutout(self.merged_table,'nutrition')
        return True
    
    def validate_data(self):
        constraints = [
        dict(name='recipeid_', field=0, test=etl.numparser),
        dict(name='minutes_', field=2, test=etl.numparser),
        dict(name='submitted_', field=4, test=etl.dateparser('%d/%m/%Y')),
        dict(name='cal_level_', field=6, test=etl.numparser),
        dict(name='n_steps_', field=7, test= etl.numparser) 
        ]
        
        # some validations to exclude records with invalid values from the load to MySql. 
        p = self.merged_table
        problems = etl.validate(p, constraints=constraints)
        print(f"Rows with validation errors : {etl.nrows(problems)}") 
        if etl.nrows(problems) > 0:
            return False
        
        return True

    
    def load_to_db(self, db_table_name):
        try:
            sql = 'SET SQL_MODE=ANSI_QUOTES'
            cursor = self.conn.execute_query(sql)
            petl_table = self.merged_table
            if db_table_name == 'nutrition_recipe':
                petl_table = self.nutrition_values
            
            etl.todb(petl_table, cursor, db_table_name)
            print("{} Data Loaded to Database".format(db_table_name))
        except Exception as e:
            print(e)
            self.conn.rollback()
        finally:
            self.conn.close_connection()

    
class InteractionFileManager:
        
    def __init__(self):
        print("Creating Interactions File Manager")
        self.conn = Connection('192.168.1.3', 'sourcemeriuser','holamundo123', 'sm_technicaltest') 
    
    def get_recipes_db(self):
        try:
            sql = 'SET SQL_MODE=ANSI_QUOTES'
            cursor = self.conn.db
            sql = 'SELECT recipe_id FROM recipes'
            loaded_recipes = etl.fromdb(cursor,sql)
        except Exception as e:
            print(e)
        
        return loaded_recipes


    def extract_from_csv(self):
        self.table_name = 'interaction'
        rpf = os.getcwd()+I_RAW_FILE_PATH
        try:
            self.raw = etl.fromcsv(rpf,delimiter=",",encoding='latin-1')
        except Exception as e:
            print(e)
          
    def validate_data(self):
        constraints = [
        dict(name='userid_', field=0, test=etl.numparser),
        dict(name='recipeid', field=1, test=etl.numparser),
        dict(name='date_', field=2, test=etl.dateparser('%Y-%m-%d')),
        dict(name='rating_', field=3, test=etl.numparser) 
        ]
        
        # some validations to exclude records with invalid values from the load to MySql. 
        p = self.raw
        problems = etl.validate(p, constraints=constraints)
        print(f"Rows with validation errors : {etl.nrows(problems)}") 
        if etl.nrows(problems) > 0:
            return False
        
        return True


    def transform_data(self):
        #Once fields has been parsed, must be converted to data type expected in the Database
        try:
            self.raw = etl.convert(self.raw,0,int)
        except Exception as e:
            print(e)
            return False
        try:
            self.raw = etl.convert(self.raw,1,int)
        except Exception as e:
            print(e)
            return False

        try:
            isodate = etl.dateparser('%Y-%m-%d')
            self.raw = etl.convert(self.raw,2,lambda d : isodate(d))
        except Exception as e:
            print(e)    

        try:
            self.raw = etl.convert(self.raw,3,int)
        except Exception as e:
            print(e)
            return False
        
        self.pp = self.get_recipes_db()
        self.merged_table = etl.join(self.raw,self.pp, key = 'recipe_id')
           
        print(f"Valid rows on Interaction table : {etl.nrows(self.merged_table)}")

        self.merged_table = etl.rename(self.merged_table,'date', 'i_date') 

        return True
    
    
    def load_to_db(self, db_table_name):
        try:
            sql = 'SET SQL_MODE=ANSI_QUOTES'
            cursor = self.conn.execute_query(sql)
            petl_table = self.merged_table 
            
            etl.todb(petl_table, cursor, db_table_name)
            print("Interactions Data Loaded to Database")
        except Exception as e:
            print(e)
            self.conn.rollback()
        finally:
            self.conn.close_connection()