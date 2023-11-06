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
 ' situacao_cadastral',
 ' razao_social',
 ' nome_fantasia',
 ' cnpj',
 ' cnpj_mei',
 ' cnpj_raiz',
 ' atividade_principal_codigo',
 ' atividade_principal_descricao',
 ' contato_telefonico_0_completo',
 ' contato_telefonico_0_tipo',
 ' contato_telefonico_0_ddd',
 ' contato_telefonico_0_numero',
 ' contato_email_0_email',
 ' contato_email_0_dominio',
 ' codigo_natureza_juridica',
 ' descricao_natureza_juridica',
 ' logradouro',
 ' numero',
 ' complemento',
 'bairro',
 ' cep',
 ' municipio',
 ' uf',
 ' capital_social',
 ' ibge_latitude',
 ' ibge_longitude',
 ' ibge_codigo_uf',
 ' ibge_codigo_municipio',
 'quadro_societario_0_qualificacao',
 ' quadro_societario_0_nome',
 ' data_yyyy_mm_dd']

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



@app.post("/upload-csv/")
async def upload_csv(file: UploadFile):

    try:
        if not file.filename.endswith(".csv"):
            return {"message": "O arquivo deve ser um arquivo CSV."}

        # Lê o arquivo CSV e converte-o em um DataFrame do pandas
        print(time.asctime())
        print("lendo arquivo.")
        csv_content = await file.read()
        df = pd.read_csv(StringIO(csv_content.decode('latin1')), sep=";",  encoding="latin1", decimal=",")



        print("arquivo lido com sucesso.")

        print(time.asctime())
        print("inserindo as datas")
        df["data_abertura"] = pd.to_datetime(df["data_abertura"], format='%d/%m/%Y')
        df['data_yyyy_mm_dd'] = df["data_abertura"].dt.strftime("%Y%m%d")
        print("datas inseridas com sucesso.")
        print(time.asctime())
        # Connect to the PostgreSQL database
        db_url = f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"

        # Crie uma conexão com o banco de dados
        engine = create_engine(db_url)

        print("inserindo os dados no banco")
        df.to_sql(table_name,engine, if_exists='append', index = False)
        print("dados inseridos com sucesso.")
        print(time.asctime())
        #captura os ultimos 30 dias, e elimina duplicada
        print("obtendo os ultimos 45 dias.")

        mantem_45_dias = """
        DELETE FROM dados_csv
        WHERE data_abertura < current_date - interval '45 days';
        """
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute(mantem_45_dias)
        conn.commit()
        cur.close()
        conn.close()

        print("os ultimos 45 dias obtidos com sucesso.")
        print(time.asctime())

        remove_duplicatas = '''
        DELETE FROM dados_csv
        WHERE (razao_social, cnpj, cnpj_raiz, contato_email_0_email) IN (
    SELECT razao_social, cnpj, cnpj_raiz, contato_email_0_email
    FROM dados_csv
    GROUP BY razao_social, cnpj, cnpj_raiz, contato_email_0_email
    HAVING COUNT(*) > 1
        );
        '''
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute(remove_duplicatas)
        conn.commit()
        cur.close()
        conn.close()

        print("duplicatas removidas com sucesso.")
        print(time.asctime())

        print("rotina finalizada com sucesso.")
        return {'message':'upload do csv realizado com sucesso.'}

    except Exception as e:
        return{'message': 'erro ao subir o arquivo', 'erro': e}