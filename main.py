from fastapi import FastAPI, Query, File, UploadFile
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from io import StringIO
import time
from sqlalchemy import create_engine
app = FastAPI()


db_params = {
    "host": "dpg-cl0kfci37rbc73cabd2g-a.oregon-postgres.render.com",
    "port": 5432,
    "user": "dados_0h9o_user",
    "password": "AehMovNWjcBiBTm5J8GPUcCwkpR7gFpl",
    "database": "dados_0h9o",
}

table_name = "dados_csv"

colunas = ['data_abertura',
 'situacao_cadastral',
 'razao_social',
 'nome_fantasia',
 'cnpj',
 'cnpj_mei',
 'cnpj_raiz',
 'atividade_principal_codigo',
 'atividade_principal_descricao',
 'contato_telefonico_0_completo',
 'contato_telefonico_0_tipo',
 'contato_telefonico_0_ddd',
 'contato_telefonico_0_numero',
 'contato_email_0_email',
 'contato_email_0_dominio',
 'codigo_natureza_juridica',
 'descricao_natureza_juridica',
 'logradouro',
 'numero',
 'complemento',
 'bairro',
 'cep',
 'municipio',
 'uf',
 'capital_social',
 'ibge_latitude',
 'ibge_longitude',
 'ibge_codigo_uf',
 'ibge_codigo_municipio',
 'quadro_societario_0_qualificacao',
 'quadro_societario_0_nome']

@app.get("/data/{data}")
async def por_data(data: str, page: int = Query(1, description="Número da página")):
    # Calculate the start and end index based on the page and items per page
    per_page = 500
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)

    # Create a cursor with RealDictCursor to return data as dictionaries
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    # Query the data based on the data and index range
    query = f"SELECT * FROM {table_name} WHERE data_yyyy_mm_dd = '{data}' LIMIT {per_page} OFFSET {start_idx}"
    print(query)
    cursor.execute(query)

    result = cursor.fetchall()

    cursor.close()
    conn.close()

    if result:
        return result
    else:
        return {"message": "Nenhum dado encontrado para a data fornecida."}


