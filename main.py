import os
from file_management.Csv_Manager import InteractionFileManager, RecipeFileManager
from basic.nutrition_recipe import NutritionRecipe


def etl_recipes():
    rf = RecipeFileManager()
    rf.extract_from_csv()
    validation = rf.validate_data()
    if validation:
        transform = rf.transform_data()
    if transform:
        rf.load_to_db(rf.table_name)
    
    rnf = NutritionRecipe() 
    rnf.extract_nutrition_values(rf.nutrition_values)
    rnf.transform_nutrition_values()
    rnf.load_to_db(rnf.table_name)

    

def etl_interactions():
    ifm = InteractionFileManager()
    ifm.extract_from_csv()        
    validation = ifm.validate_data()
    if validation:
       transform = ifm.transform_data()
    if transform:
        ifm.load_to_db(ifm.table_name)

etl_recipes()
etl_interactions()
