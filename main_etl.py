# -*- coding: utf-8 -*-
"""
Created on Wed Aug 30 10:17:48 2023

@author: Francisco Daniel Lugo, Data Analyst
"""

###############################################################################
#################### Importación de las librerías #############################
###############################################################################

import pandas as pd
from sqlalchemy import MetaData, Table, Column, Integer, String, ForeignKey,\
    create_engine, DateTime, Float, inspect
from sqlalchemy.orm import sessionmaker
import datetime as dt
import numpy as np
import os

###############################################################################
################ Variables establecidas por el usuario ########################
###############################################################################

def user_variables():
    
    # Se pide el nombre de la base de datos, para la cual se crearán tablas e ingestarán registros
    print("*"*150)
    input_base = input("\nPor favor, ingrese el nombre de la base de datos creada en PostgreSQL: \n")
    print("\n")
    print("*"*150)
    principal_path = input("\nPor favor, ingrese la ruta a la carpeta principal (Señalada en la documentación del proyecto): \n")

    input_path_transaction_table = principal_path + "/Ins"
    input_path_historic_data = principal_path + "/Ins/1. Recursos Est. Entes DPB 2017-2023.xlsx"
    output_path_hist = principal_path + "/Outs"
    print("\n")
    print("*"*150)
    return input_base, input_path_transaction_table, input_path_historic_data, output_path_hist


###############################################################################
############## ETL para la tabla transaccional principal ######################
###############################################################################

def etl_for_main_table(df):
    
    
    
    # vamos a crear un df aparte, solamente para el acumulado, que contenga todas las categorías posibles para segmentar el gasto adecuado:
    segmentaciones = ["SECTOR","ESTATUS","ORIGEN","DESCRIPCION_FF","CC","ENTE",
                      "NOMBRE_DEL_ORGANISMO","COG","FF","DPB","PERIOD_YEAR",
                      "SEGMENT1","SEGMENT2","SEGMENT4","SEGMENT5","SEGMENT7",
                      "SEGMENT8","SEGMENT9","cierre"]
    
    # Generamos un dataframe dónde haga un melt de las columnas de gasto, ejemplo: IMPORTE, EJERCIDO_ACUM, AUTORIZADO, ADECUACIONES, MODIFICADO
    df_melt_tipo_gasto = \
        df.melt(id_vars = segmentaciones,
            value_vars = ["IMPORTE","EJERCIDO_ACUM","AUTORIZADO","ADECUACIONES","MODIFICADO"],
            value_name = "Gasto",
            var_name = "Tipo_de_gasto"
            )
    
    # Tratado de los datos para una correcta funcionalidad de las columnas, dónde se separan tipos de datos y se especifican columnas 
    
    df_melt_tipo_gasto["ESTATUS"] = df_melt_tipo_gasto["ESTATUS"].astype(str) # Cambiamos a tipo object
    # Esta línea de código es utilizada para corregir los errores en el dashboard, según la retroalimentación de Riebeling (Presupuesto B)
    df_melt_tipo_gasto.loc[(df_melt_tipo_gasto.ESTATUS == "EJERCIDO") &
                            (df_melt_tipo_gasto.Tipo_de_gasto == "EJERCIDO_ACUM") & 
                            (df_melt_tipo_gasto.DESCRIPCION_FF.str.startswith("RAMO 33 FAM")),
                            "Gasto"] = np.nan

    df_melt_tipo_gasto["Fecha ejercido"] = df_melt_tipo_gasto["ESTATUS"].copy() # Creamos una columna específica para el mes
        
    correct_dtypes_dict = {
        "ADECUACIONES":None,
        "AUTORIZADO":None,
        "EJERCIDO":None,
        "MODIFICADO":None
            }
    
    df_melt_tipo_gasto["Fecha ejercido"] = df_melt_tipo_gasto["Fecha ejercido"].replace(correct_dtypes_dict)    
    df_melt_tipo_gasto['Fecha ejercido'] = df_melt_tipo_gasto['Fecha ejercido'].replace("0",None)
    df_melt_tipo_gasto.loc[df_melt_tipo_gasto['Fecha ejercido'].notna(),"Fecha ejercido"] = "1/"+ df_melt_tipo_gasto['Fecha ejercido'] + "/" + df_melt_tipo_gasto["PERIOD_YEAR"].astype(str)
    df_melt_tipo_gasto['Fecha ejercido'] = pd.to_datetime(df_melt_tipo_gasto['Fecha ejercido'], format="%d/%m/%Y")
    df_melt_tipo_gasto.drop(columns = ["ESTATUS"], inplace=True)
    
    return df_melt_tipo_gasto
###############################################################################
##### Manejo de una segunda tabla la cual servirá para las adecuaciones #######
###############################################################################

def second_fact_table(df):
    
    
    adecuado_columnas = ["ADEC_ENE","ADEC_FEB","ADEC_MAR","ADEC_ABR","ADEC_MAY","ADEC_JUN","ADEC_JUL","ADEC_AGO",
                         "ADEC_SEP","ADEC_OCT","ADEC_NOV","ADEC_DIC"]
    
    # vamos a crear un df aparte, solamente para el acumulado, que contenga todas las categorías posibles para segmentar el gasto adecuado:
    segmentaciones = ["SECTOR","ESTATUS","ORIGEN","DESCRIPCION_FF","CC","ENTE",
                      "NOMBRE_DEL_ORGANISMO","COG","FF","DPB","PERIOD_YEAR",
                      "SEGMENT1","SEGMENT2","SEGMENT4","SEGMENT5","SEGMENT7",
                      "SEGMENT8","SEGMENT9","cierre"]
    
    df_melted_adecuado_por_mes = \
            df.melt(id_vars = segmentaciones,
            value_vars = adecuado_columnas,
            var_name = "ADECUADO_MES",
            value_name = "MONTO_ADECUADO")
    
    # Creación de la columna fecha
    dicts_to_replace_in_adecuado = {
        'ADEC_ENE':"1-1",
     'ADEC_FEB':"2-1",
     'ADEC_MAR':"3-1",
     'ADEC_ABR':"4-1",
     'ADEC_MAY':"5-1",
     'ADEC_JUN':"6-1",
     'ADEC_JUL':"7-1",
     'ADEC_AGO':"8-1",
     'ADEC_SEP':"9-1",
     'ADEC_OCT':"10-1",
     'ADEC_NOV':"11-1",
     'ADEC_DIC':"12-1"
    }
    
    # Creación de la columna fecha:
    df_melted_adecuado_por_mes["FECHA ADECUADO"] = df_melted_adecuado_por_mes["ADECUADO_MES"].replace(dicts_to_replace_in_adecuado)
    df_melted_adecuado_por_mes["FECHA ADECUADO"] = df_melted_adecuado_por_mes["PERIOD_YEAR"].astype(str) + "-" \
                                                    + df_melted_adecuado_por_mes["FECHA ADECUADO"]
    df_melted_adecuado_por_mes["FECHA ADECUADO"] = pd.to_datetime(df_melted_adecuado_por_mes["FECHA ADECUADO"], format="%Y-%m-%d")
    
    # Dropeamos las columnas que no necesitamos:
    df_melted_adecuado_por_mes = df_melted_adecuado_por_mes.drop(columns=["ADECUADO_MES","ESTATUS"])
    
    return df_melted_adecuado_por_mes

###############################################################################
######## Creación de las tablas dimensionales para el modelo de datos #########
###############################################################################

def dim_tables_creation(df):
    # Creación de centro de costo:
    centro_de_costo = df.groupby(["NOMBRE_DEL_ORGANISMO"],as_index=False )\
        .agg({"ENTE":"unique","CC":"unique"})
        
    # Unpack the lists in "ENTE" and "CC" columns
    centro_de_costo['ENTE'] = centro_de_costo['ENTE'].apply(lambda x: ', '.join(x))
    centro_de_costo['CC'] = centro_de_costo['CC'].apply(lambda x: ', '.join(x))
    
    # Creación de fuente de financiamiento:
    
    fuente_de_financiamiento = df.groupby("FF")["DESCRIPCION_FF"].unique().reset_index()
    def clear_fuente_financiamiento(row):
        cleaned_items = []
        for item in row:
            if item not in [None, "nan", np.nan]:
                cleaned_items.append(item)
        return cleaned_items

    fuente_de_financiamiento["DESCRIPCION_FF"] = fuente_de_financiamiento["DESCRIPCION_FF"].apply(clear_fuente_financiamiento)
    fuente_de_financiamiento["DESCRIPCION_FF"] = fuente_de_financiamiento["DESCRIPCION_FF"].apply(lambda lista: ", ".join(lista))
    
    
    # Creación de una función que nos ayudará a crear tablas dimensionales:
    
    def dimensional_creator(data,column):
        
        """
        This function creates a dimensional table according
        to the column specified"""
        
        list_unique = data[column].unique()
        until = len(list_unique) + 1
        dict_aux = {column:id for column,id in zip(list_unique, np.arange(1, until, 1))}
        dim_df = pd.DataFrame().from_dict(dict_aux.items())
        dim_df.columns=[column,f"ID_{column}"]
        
        return dim_df
    
    
        
    # Creación las demás tablas dimensionales:
    
    dim_origen = dimensional_creator(df, "ORIGEN")   
    dim_DPB = dimensional_creator(df,"DPB")
    dim_SECTOR = dimensional_creator(df,"SECTOR")
    dim_Tipo_de_gasto = dimensional_creator(df,"Tipo_de_gasto")
    
    return centro_de_costo, fuente_de_financiamiento, dim_origen, dim_DPB, dim_SECTOR, dim_Tipo_de_gasto



###############################################################################
###################### Mapeo de las tablas dimensionales ######################
###############################################################################

def dimensional_map(df_melt_tipo_gasto, df_melted_adecuado_por_mes, dim_origen,
                    dim_DPB, dim_SECTOR, dim_Tipo_de_gasto):
    
    
    #Para mapear centro de costo y fuente de financiamiento basta con eliminar las columnas nombre del organismo y ente:
    df_melt_tipo_gasto = df_melt_tipo_gasto.drop(columns=['NOMBRE_DEL_ORGANISMO','ENTE','DESCRIPCION_FF'])
    df_melted_adecuado_por_mes = df_melted_adecuado_por_mes.drop(columns=['NOMBRE_DEL_ORGANISMO','ENTE','DESCRIPCION_FF'])
    
    # Creación de una función que nos ayuda a mapear las llaves en la tabla principal:
        
    def to_map_dimensional(dim_df, column, main_df):
        
        to_map_dict = {i:j for i,j in zip(dim_df[column], dim_df[f"ID_{column}"])}
        main_df[column] = main_df[column].map(to_map_dict)
        print(f"{column} se ha mapeado correctamente en la tabla de hechos.\n")
    
    # Mapeamos la primera tabla de hechos:
        
    to_map_dimensional(dim_origen, "ORIGEN", df_melt_tipo_gasto)
    to_map_dimensional(dim_DPB, "DPB", df_melt_tipo_gasto)
    to_map_dimensional(dim_SECTOR, "SECTOR", df_melt_tipo_gasto)
    to_map_dimensional(dim_Tipo_de_gasto, "Tipo_de_gasto", df_melt_tipo_gasto)
    
    
    # Se mapeará también la otra tabla de hechos:
    
    #Para mapear centro de costo y fuente de financiamiento
    # basta con eliminar las columnas nombre del organismo y ente:
    
    
    to_map_dimensional(dim_origen, "ORIGEN", df_melted_adecuado_por_mes)
    to_map_dimensional(dim_DPB, "DPB", df_melted_adecuado_por_mes)
    to_map_dimensional(dim_SECTOR, "SECTOR", df_melted_adecuado_por_mes)
    
    
    return df_melt_tipo_gasto, df_melted_adecuado_por_mes


###############################################################################
##################### Creación de la base de datos en SQL #####################
###############################################################################

def database_creation(input_base, dim_origen, dim_DPB, dim_SECTOR, dim_Tipo_de_gasto,
                      centro_de_costo, fuente_de_financiamiento, df_melt_tipo_gasto,
                      df_melted_adecuado_por_mes):
    
    Engine = create_engine(f"postgresql+psycopg2://postgres:fdlr1719@localhost:5433/{input_base}")
    Metadata = MetaData(bind = Engine)
    Session = sessionmaker(bind = Engine)
    session = Session()
    
    # Create an inspector to check if the table exists
    inspector = inspect(Engine)
    
    
    
    
    ############################################################ Creación de origen
    # Creación del objeto de la clase Table para dim_origen
    
    dim_origen.columns = ['origen', 'id_origen']
    
    dim_origen_SQL = Table(
        "dim_origen",
        Metadata,
        Column("id_origen", Integer, primary_key=True),
        Column("origen", String(15)),
        extend_existing=True
        )
    
    if not inspector.has_table("dim_origen"): #Si no existe la tabla, la crea
        # Create the table
        dim_origen_SQL.create(Engine)
    else: # Si existe la tabla, la dropea y la crea
        print("Table 'dim_origen' already exists and it's gonna be created again.")
        Engine.execute("TRUNCATE TABLE dim_origen CASCADE;")
        #dim_origen_SQL.create(Engine)
    
    # Ahora insertamos los datos en la tabla con pandas to sql
    dim_origen.to_sql("dim_origen", con=Engine, if_exists="append", index=False)
    
    
    ####################################################### Creación de dim_DPB
    
    dim_DPB.columns = ['DPB', 'id_DPB']
    
    # Creación del objeto de la clase Table para dim_DPB
    dim_DPB_SQL = Table(
        "dim_DPB",
        Metadata,
        Column("id_DPB", Integer, primary_key=True, unique=True),
        Column("DPB", String(50)),
        extend_existing=True
        )
    
    
    if inspector.has_table("dim_DPB"):# Si existe la borra
        
        print("Existe la tabla dim_DPB y se truncará")
        
        Engine.execute('TRUNCATE TABLE public."dim_DPB" CASCADE;')
        
    else:
        
        dim_DPB_SQL.create(Engine)

        
    
    
    # Ahora insertamos los datos en la tabla con pandas to sql
    dim_DPB.to_sql("dim_DPB", con=Engine, if_exists="append", index=False)
    
    
    #################################################### Creación de dim_sector
    
    # Cambiamos el nombre de las columnas
    dim_SECTOR.columns = ['sector', 'id_sector']
    dim_SECTOR.sector = dim_SECTOR.sector.str.capitalize()
    # Creación del objeto de la clase Table para dim_SECTOR
    dim_SECTOR_SQL = Table(
        "dim_sector",
        Metadata,
        Column("id_sector", Integer, primary_key=True),
        Column("sector", String(50)),
        )
    
    if not inspector.has_table("dim_sector"): #Si no existe la tabla, la crea
        # Create the table
        dim_SECTOR_SQL.create(Engine)
    else: # Si existe la tabla, la dropea y la crea
        print("Table 'dim_sector' already exists and it's gonna be created again.")
        Engine.execute("TRUNCATE TABLE dim_sector CASCADE;")
        
    
    # Ahora insertamos los datos en la tabla con pandas to sql
    dim_SECTOR.to_sql("dim_sector", con=Engine, if_exists="append", index=False)
    
    
    ########################################## Creación de tipo de gasto dim
    
    # Cambiamos el nombre de las columnas
    dim_Tipo_de_gasto.columns = ['tipo_de_gasto', 'id_tipo_de_gasto']
    dim_Tipo_de_gasto.tipo_de_gasto = dim_Tipo_de_gasto.tipo_de_gasto.str.capitalize()
    
    # Creación del objeto de la clase Table para dim_Tipo_de_gasto
    dim_Tipo_de_gasto_SQL = Table(
        "dim_tipo_de_gasto",
        Metadata,
        Column("id_tipo_de_gasto", Integer, primary_key=True, unique=True),
        Column("tipo_de_gasto", String(50)),
        extend_existing=True
        )
    
    
    # Check if the table already exists
    if not inspector.has_table("dim_tipo_de_gasto"): #Si no existe la tabla, la crea
        # Create the table
        dim_Tipo_de_gasto_SQL.create(Engine)
    else: # Si existe la tabla, la dropea y la crea
        print("Table 'dim_tipo_de_gasto' already exists and it's gonna be created again.")
        Engine.execute("TRUNCATE TABLE dim_tipo_de_gasto CASCADE;")
        
    
    # Ahora insertamos los datos en la tabla con pandas to sql
    dim_Tipo_de_gasto.to_sql("dim_tipo_de_gasto", con=Engine, if_exists="append", index=False)
    
    
    ########################################### Creación de dim centro de costo
    
    centro_de_costo.columns = ["centro_de_costo","ente","id_centro_de_costo"]
    centro_de_costo["centro_de_costo"] = centro_de_costo["centro_de_costo"].str.title()
    
    
    # Creación del objeto de la clase Table para centro_de_costo
    centro_de_costo_SQL = Table(
        "dim_centro_de_costo",
        Metadata,
        Column("id_centro_de_costo", String(20), primary_key=True, unique=True),
        Column("centro_de_costo", String(200)),
        Column("ente", String(20)),
        extend_existing=True
        )
    
    
    # Check if the table already exists
    if not inspector.has_table("dim_centro_de_costo"): #Si no existe la tabla, la crea
        # Create the table
        centro_de_costo_SQL.create(Engine)
    else: # Si existe la tabla, la dropea y la crea
        print("Table 'dim_centro_de_costo' already exists and it's gonna be created again.")
        Engine.execute("TRUNCATE TABLE dim_centro_de_costo CASCADE;")
        
    
    
    # Ahora insertamos los datos en la tabla con pandas to sql
    centro_de_costo.to_sql("dim_centro_de_costo", con=Engine, index=False, if_exists='append')
    
    
    ########################################### Creación de dim fuente de financiamiento
    
    
    fuente_de_financiamiento.columns = ['id_fuente_de_financiamiento', 'fuente_de_financiamiento']
    fuente_de_financiamiento["fuente_de_financiamiento"] = fuente_de_financiamiento["fuente_de_financiamiento"].str.title()
    
    
    # Creación del objeto de la clase Table para fuente_de_financiamiento
    fuente_de_financiamiento_SQL = Table(
        "dim_fuente_de_financiamiento",
        Metadata,
        Column("id_fuente_de_financiamiento", String(20), primary_key=True),
        Column("fuente_de_financiamiento", String(300)),
        extend_existing=True
        )
    
    
    # Check if the table already exists
    if not inspector.has_table("dim_fuente_de_financiamiento"):
        # Create the table
        fuente_de_financiamiento_SQL.create(Engine)
    else:
        Engine.execute("TRUNCATE TABLE dim_fuente_de_financiamiento CASCADE;")
        print("Table 'dim_fuente_de_financiamiento' already exists.")
    
    
    # Ahora insertamos los datos en la tabla con pandas to sql
    fuente_de_financiamiento.to_sql("dim_fuente_de_financiamiento", con=Engine, if_exists="append", index=False)
    
    #########################################################
    ######################################################### creación de fact_tipo_gasto
    #########################################################
    
    df_melt_tipo_gasto.columns = [i.lower() for i in df_melt_tipo_gasto.columns] # Cambiamos los nombres de las columnas a minusculas
    
    df_melt_tipo_gasto.rename(columns={"ff":"id_fuente_de_financiamiento",
                                       "cc":"id_centro_de_costo",
                                       "origen":"id_origen",
                                       "dpb":"id_dpb",
                                       "sector":"id_sector",
                                       "tipo_de_gasto":"id_tipo_de_gasto",
                                       "fecha ejercido":"fecha_ejercido"}, inplace=True)
    pd.set_option("display.max_columns",None)
    

    
    # Creación del objeto de la clase Table para tipo_gasto
    tipo_gasto_SQL = Table(
        "fact_tipo_gasto",
        Metadata,
        Column('id_centro_de_costo', String(20), ForeignKey("dim_centro_de_costo.id_centro_de_costo")),
        Column('cog',Integer),
        Column('id_dpb',Integer, ForeignKey("dim_DPB.id_DPB")),
        Column('fecha_ejercido',DateTime()),
        Column('id_fuente_de_financiamiento', String(20), ForeignKey("dim_fuente_de_financiamiento.id_fuente_de_financiamiento")),
        Column('gasto',Float()),
        Column('id_origen',Integer, ForeignKey("dim_origen.id_origen")),
        Column('period_year',Integer),
        Column('id_sector',Integer, ForeignKey("dim_sector.id_sector")),
        Column('segment1',Integer),
        Column('segment2',Integer),
        Column('segment4',String(100)),
        Column('segment5',Integer),
        Column('segment7',String(100)),
        Column('segment8',Integer),
        Column('segment9',Integer),
        Column('id_tipo_de_gasto',Integer, ForeignKey("dim_tipo_de_gasto.id_tipo_de_gasto")),
        Column('cierre',String(25)),
        extend_existing=True
        )
    
    # Check if the table already exists
    if not inspector.has_table("fact_tipo_gasto"):
        # Create the table
        tipo_gasto_SQL.create(Engine)
    else:
        Engine.execute("TRUNCATE TABLE fact_tipo_gasto CASCADE;")
        print("Table 'fact_tipo_gasto' already exists.")
        
    
    # Ahora insertamos los datos en la tabla con pandas to sql
    print(df_melt_tipo_gasto.columns)
    correct_order_tipo_gasto = ['id_centro_de_costo','cog','id_dpb','fecha_ejercido','id_fuente_de_financiamiento','gasto',
                                'id_origen','period_year','id_sector','segment1','segment2','segment4','segment5','segment7',
                                'segment8','segment9','id_tipo_de_gasto','cierre']

    df_melt_tipo_gasto = df_melt_tipo_gasto[correct_order_tipo_gasto]
    print(df_melt_tipo_gasto.info())
    

    print(df_melt_tipo_gasto.head())
    df_melt_tipo_gasto.to_sql("fact_tipo_gasto", con=Engine, if_exists="append", index=False)
    
    
    
    ######################################################### creación de fact_adecuado
    
    df_melted_adecuado_por_mes.columns = [i.lower() for i in df_melted_adecuado_por_mes.columns] # Cambiamos los nombres de las columnas a minusculas
    
    df_melted_adecuado_por_mes.rename(columns={"ff":"id_fuente_de_financiamiento",
                                       "cc":"id_centro_de_costo",
                                       "origen":"id_origen",
                                       "dpb":"id_dpb",
                                       "sector":"id_sector",
                                       "tipo_de_gasto":"id_tipo_de_gasto",
                                       "fecha adecuado":"fecha_adecuado"}, inplace=True)
    
    # Creación del objeto de la clase Table para tipo_gasto
    fact_adecuado_SQL = Table(
        "fact_adecuado",
        Metadata,
        Column('id_centro_de_costo',String(10), ForeignKey("dim_centro_de_costo.id_centro_de_costo")),
        Column('cog',Integer()),
        Column('id_dpb',Integer(), ForeignKey("dim_DPB.id_DPB")),
        Column('fecha_adecuado',DateTime()),
        Column('id_fuente_de_financiamiento',String(20), ForeignKey("dim_fuente_de_financiamiento.id_fuente_de_financiamiento")),
        Column('monto_adecuado',Float()),
        Column('id_origen',Integer(), ForeignKey("dim_origen.id_origen")),
        Column('period_year',Integer()),
        Column('id_sector',Integer(), ForeignKey("dim_sector.id_sector")),
        Column('segment1',Integer()),
        Column('segment2',Integer()),
        Column('segment4',String(100)),
        Column('segment5',Integer()),
        Column('segment7',String(100)),
        Column('segment8',Integer()),
        Column('segment9',Integer()),
        Column('cierre',String(25)),
        extend_existing=True
        )
    
    
    # Check if the table already exists
    if not inspector.has_table("fact_adecuado"): #Si no existe la tabla, la crea
        # Create the table
        fact_adecuado_SQL.create(Engine)
    else: # Si existe la tabla, la dropea y la crea
        print("Table 'fact_adecuado' already exists and it's gonna be created again.")
        Engine.execute("TRUNCATE TABLE fact_adecuado CASCADE;")
    
    # Ahora insertamos los datos en la tabla con pandas to sql
    correct_order = ['id_centro_de_costo','cog','id_dpb','fecha_adecuado','id_fuente_de_financiamiento','monto_adecuado',
                    'id_origen','period_year','id_sector','segment1', 'segment2','segment4','segment5','segment7','segment8',
                    'segment9','cierre']
    
    df_melted_adecuado_por_mes = df_melted_adecuado_por_mes[correct_order]
    df_melted_adecuado_por_mes.to_sql("fact_adecuado", con=Engine, if_exists="append", index=False)
    
    
    session.close()
    print("\n¡Se han insertado todos los registros a la base de datos!\n")

# * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
# * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
# * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
# -------------ETL para los datos históricos: Se tienen solamente datos estatales----------------
# * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
# * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
# * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *

def etl_historic_data(input_path_historic_data, output_path_hist):
    
    # Cargamos el excel
    df = pd.read_excel(input_path_historic_data, sheet_name='TD')
    # Creamos una series dónde llenaremos los espacios vacíos con años, esto para mapear correctamente cada columna con su año
    years_series = df.iloc[13].fillna(method='ffill').reset_index(drop=True)
    
    # En la siguiente línea obtenemos una series con los títulos adecuados para cada columna:
    header_series = df.iloc[14].reset_index(drop=True)
    # Combinamos nuestras dos series en una sóla, que será usada cómo título adecuado
    header_and_years = [f"{header} {year}" 
                        if not (pd.isna(year)) # Si no está nulo el año
                        else header for header,year in zip(header_series, years_series) ] # De lo contrario sólo agrega el año
    
    # Se asignan las columnas y se establece el correcto inicio de los registros
    
    df.columns = header_and_years
    df = df.iloc[15:,:] # Los registros comienzan a partir del quinceavo renglón
    
    # Y ahora se cambia el formato de los datos:
    df_melt = df.melt(id_vars=["CC","ENTE"], var_name="Tipo de gasto", value_name="Monto")
    
    # Se dividen las columnas:
    df_melt[["Tipo de gasto","Año"]] = df_melt["Tipo de gasto"].str.split(" ", expand=True)
    # Se reemplazan los puntos para poder convertir a tipo númerico:
    df_melt["Año"] = df_melt["Año"].str.replace(".0","", regex=False)
    df_melt["Año"] = df_melt["Año"].str.strip()
    # Se cambia el tipo de dato
    df_melt["Año"] = df_melt["Año"].astype(int)
    df_melt["Monto"] = df_melt["Monto"].astype(float)
    
    
    df_melt[df_melt["ENTE"].isna()] # Se encontró que para los ENTES nulos, el CC es 'Total general'
    df_melt = df_melt[df_melt["CC"] != "Total general"] #Se eliminan los ENTES nulos
    
    df_melt.columns = ["Centro de costo (historico)",
                       "Ente (historico)","Tipo de gasto (historico)",
                       "Monto (historico)","Año (historico"]
    

########################################### En esta parte del código, se carga otra hoja del archivo de excel
########################################### para mapear los comentarios correspondientes a cada movimiento.
    
    # Se carga el dataset de la hoja con los comentarios
    df_base = pd.read_excel(input_path_historic_data, sheet_name="Base", header=5)
    
    # Se crea una columna que permitirá mapear los comentarios con sus registros correspondientes en df_melt
    # Se crea para ambas bases, df_melt & df_base.
    df_base["clave"] = df_base["ENTE"] + "-" + df_base["AÑO"].astype(str) + "-" + df_base["CONCEPTO"]
    df_melt["clave"] = df_melt["Ente (historico)"] + "-" + df_melt["Año (historico"].astype(str) + "-" + df_melt["Tipo de gasto (historico)"]
    #Nos quedamos solamente con las columnas de interés
    df_base = df_base[["COMENTARIO","clave"]].copy()
    
    # Creamos un nuevo dataframe apartir de estos dos, que incluye los comentarios para cada adecuación
    df_merged = df_melt.merge(df_base, on="clave", how="outer", indicator=True)
    # Eliminamos las columnas que no nos interesan
    df_merged.drop(columns=["clave","_merge"], inplace=True)
    
    
    df_merged.to_csv(output_path_hist + "\historico_presupuestoB.csv",
                    index=False,
                    encoding="utf-8")
    
    
if __name__ == "__main__":
    
    # Almacenamos las variables definidas por el usuario:
    input_base, input_path_transaction_table, input_path_historic_data, output_path_hist = user_variables()
    
    # Se creará la tabla principal de tipo de gasto:

    filenames_list = [file for file in os.listdir(input_path_transaction_table) if file.endswith(".csv")] # Creamos una lista de los archivos que únicamente son CSV's
    dataframes_list = []

    for file in filenames_list:
        # Creamos una columna extra, que nos indicará que cierre estamos consultando
        df = pd.read_csv(input_path_transaction_table + "/" + file)

        for month in ["enero","febrero","marzo","abril","mayo","junio","julio","agosto","septiembre","octubre","noviembre","diciembre"]:
            if month in file.lower():
                df["cierre"] = df["PERIOD_YEAR"].astype(str) + "-" + month # Creación de la columna extra
                dataframes_list.append(df)

    df = pd.concat(dataframes_list)


    # Operaciones ETL para la tabla principal
    df_melt_tipo_gasto = etl_for_main_table(df)
    
    # Operaciones ETL para la segunda tabla de hechos "ADECUADO"
    df_melted_adecuado_por_mes = second_fact_table(df)
    
    # Create dimension tables
    centro_de_costo, fuente_de_financiamiento, dim_origen, dim_DPB, dim_SECTOR, dim_Tipo_de_gasto = dim_tables_creation(df_melt_tipo_gasto)
    
    # Map dimensions
    df_melt_tipo_gasto, df_melted_adecuado_por_mes = dimensional_map(df_melt_tipo_gasto, df_melted_adecuado_por_mes, dim_origen,
                        dim_DPB, dim_SECTOR, dim_Tipo_de_gasto)
    
    
    database_creation(input_base, dim_origen, dim_DPB, dim_SECTOR, dim_Tipo_de_gasto,
                          centro_de_costo, fuente_de_financiamiento, df_melt_tipo_gasto,
                          df_melted_adecuado_por_mes)
    
    # ETL operations on historic data
    etl_historic_data(input_path_historic_data, output_path_hist)

    
    
