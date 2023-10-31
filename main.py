from fastapi import FastAPI, File, UploadFile, Query
import pandas as pd
from io import StringIO
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine

#dados de conexao

host="dpg-cl0kfci37rbc73cabd2g-a.oregon-postgres.render.com"
port='5432'
user="dados_0h9o_user"
password="AehMovNWjcBiBTm5J8GPUcCwkpR7gFpl"
database="dados_0h9o"

table_name = "dados_csv"

colunas = ['data_abertura','situacao_cadastral', 'razao_social', 'nome_fantasia', 'cnpj', 'cnpj_mei', 'cnpj_raiz', 'atividade_principal_codigo', 'atividade_principal_descricao',
           'contato_telefonico_0_completo', 'contato_telefonico_0_tipo', 'contato_telefonico_0_ddd', 'contato_telefonico_0_numero', 'contato_email_0_email', 'contato_email_0_dominio', 'codigo_natureza_juridica', 'descricao_natureza_juridica', 'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'municipio', 'uf', 'capital_social', 'ibge_latitude', 'ibge_longitude', 'ibge_codigo_uf', 'ibge_codigo_municipio', 'quadro_societario_0_qualificacao',
           'quadro_societario_0_nome']

db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"



app = FastAPI()


@app.get("/data/{data}")
async def por_data(data: str, page: int = Query(1, description="Número da página")):
    try:
        # Calcular o índice de início e fim com base na página e itens por página
        per_page = 500
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page

        # Conecte-se ao banco de dados SQLite
        conn = create_engine(db_url)

        # Consulte os dados com base na data de abertura e a faixa de índices
        query = f"SELECT * FROM {table_name} WHERE data_yyyy_mm_dd = '{data}' LIMIT {per_page} OFFSET {start_idx}"
        print(query)
        result = pd.read_sql_query(query, conn)
        print(f"result {len(result)}")


        if not result.empty:
            data = result.to_dict(orient='records')
            return data
        else:
            return {"message": "Nenhum dado encontrado para a data fornecida."}
    except Exception as e:
        return {"message:": e}
@app.post("/upload-csv/")
async def upload_csv(file: UploadFile):

    # Verifica se o arquivo é um CSV
    try:
        if file.filename.endswith('.csv'):

            # Lê o arquivo CSV em um DataFrame
            content = await file.read()
            content_str = content.decode('latin1')
            new_df = pd.read_csv(StringIO(content_str), sep=";", decimal=",", encoding="latin1", dtype='str', usecols=colunas   )
            new_df["data_yyyy_mm_dd"] = pd.to_datetime(new_df['data_abertura'], format='%d/%m/%Y').dt.strftime('%Y%m%d')

            # Adiciona os novos dados à tabela no banco de dados
            conn = sqlite3.connect(db_name)
            new_df.to_sql(table_name, conn, if_exists="append", index=False)

            # Remova registros duplicados com base na coluna cnpj_raiz, mantendo o último
            query = f'''
                DELETE FROM {table_name}
                WHERE rowid NOT IN (
                    SELECT MAX(rowid) FROM {table_name}
                    GROUP BY cnpj_raiz
                )
            '''

            conn.execute(query)
            conn.commit()
            conn.close()

            return {"message": "Arquivo CSV carregado com sucesso e dados salvos no banco de dados."}
        else:
            return {"message": "O arquivo fornecido não é um arquivo CSV."}
    except Exception as e:
        return {"erro": e}
