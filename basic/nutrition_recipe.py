import petl as etl
from Database.Connection import Connection

class NutritionRecipe:
    def __init__(self):
        self.table_name = 'nutrition_recipe'
        self.conn = Connection('192.168.1.3', 'sourcemeriuser','holamundo123', 'sm_technicaltest') 
    
    def extract_nutrition_values(self, nutrition_values):
        print("Extracting data of nutrition values")
        self.nutrition_values = nutrition_values  
        self.nutrition_values = etl.convert(self.nutrition_values,'nutrition',lambda x: x.split(','))
        self.nutrition_values = etl.unpack(self.nutrition_values,'nutrition',['n_cals','tot_fat_pdv','sugar_pdv','sodium_pdv','protein_pdv','sat_fat_pdv','carbs_pdv'])
        
    
    def transform_nutrition_values(self):
        print("Transforming data of nutrition values")
        
        aux_nrp = self.nutrition_values
        # Couldn't make a proper transformation of these fields 
        try:
            aux_nrp = etl.convert(aux_nrp,'n_cals',float)
        except Exception  as e:
            aux_nrp = etl.convert(aux_nrp,'n_cals',0)
            print(e)
        try:
            aux_nrp = etl.convert(aux_nrp,'carbs_pdv',float)
        except Exception  as e:
            aux_nrp = etl.convert(aux_nrp,'carbs_pdv',0)
            print(e)
        
        aux_nrp = etl.convert(aux_nrp, ('tot_fat_pdv','sugar_pdv','sodium_pdv','protein_pdv','sat_fat_pdv'), float)    
        self.nutrition_values = aux_nrp    

    def load_to_db(self, db_table_name):
        try:
            sql = 'SET SQL_MODE=ANSI_QUOTES'
            cursor = self.conn.execute_query(sql)
            petl_table = self.nutrition_values
            etl.todb(petl_table, cursor, db_table_name)
            print("{} Data Loaded to Database".format(db_table_name))
        except Exception as e:
            print(e)
            self.conn.rollback()
        finally:
            self.conn.close_connection()    

        
