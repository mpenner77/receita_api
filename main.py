
from fastapi import FastAPI, Query, Depends, HTTPException, Header
from psycopg2.extras import RealDictCursor
from typing import List
from typing import Optional

app = FastAPI()

# Substitua os valores abaixo pelos seus parâmetros de banco de dados
db_params = {
    "host": "dpg-cl0kfci37rbc73cabd2g-a.oregon-postgres.render.com",
    "port": 5432,
    "user": "dados_0h9o_user",
    "password": "AehMovNWjcBiBTm5J8GPUcCwkpR7gFpl",
    "database": "dados_0h9o",
}

table_name = "dados_csv"

valid_tokens = ['2d3f27b14b4a4a60a33ea218a53fbc6c',
 '3c79d0a8d6e846d4a7a8dbd60f66c1bd',
 'f8e5e41a13e443de9e049f96e79f1f7d',
 '1b2492f7157b4c179a0cb92e3d6b9ef1',
 '41ef2a9cb52b4b689ac5bb5b62d34ad2',
 '4b07f5871a4b4e7c9d57ab548e504d4e',
 'a6f77b0cb10a4decae9f9b9a1bf30fbb',
 '3ff8138fc6304f28ba938ab3e66658e8',
 '6b2ef0e087cd45be8c1c51864cf00eab',
 'c5d37d32bc8046349a073bc4b4510ac0',
 'd8fc1939a4d1463698f4c2d65eb9dd5a',
 'd271e8ef285944418f8c0e28fc75da4c',
 '6df2798a0f8e4d9a9f99e5b8d98f994c',
 'ee8de2c4e6d84e3cbf16e6564cc35ef5',
 'f03e96d144b3404f80c89e3ff047a0cc',
 '3a8391e8334646239c0e7a6142cf195d',
 'abf0ce88a7de4b1d9a2c0a1b3bbbf1ae',
 '29299d699a174168a739bd35cda92da5',
 '8894e031be114a76b8a4063302e811a7',
 '7e7b9b189ad048349b4b52bc4f4c7962',
 'aa38d9de7f5b4f7d8d047b4e4e4ecf22',
 '94361437c74d4a68b3c7d82a83c63502',
 'b80020a637144a42aeb640a4d929ad8b',
 'beed3c44c9b5405db5bb924ff8b45d5f',
 '59c66e2f81b746a18c164b2389918bc7',
 '60181e5b051243de9abf4e7f8cd66d7b',
 '1b695c0d36d643a097c2e58a34083326',
 'a0da16673bbf49bf8ceab14b89b433b0',
 '30e3cfbe4e524056b72d5881eb8d93f3',
 '19935d4e6b7547ae8c0d0d9a86b8cc8c']


def verify_token(authorization: Optional[str] = Header(None, description="Token de Autorização")):
    if authorization and authorization not in valid_tokens:
        raise HTTPException(status_code=401, detail="Token de Autorização Inválido")

@app.get("/data/{data}")
async def por_data(data: str, page: int = Query(1, description="Número da página"), token: List[str] = Depends(verify_token)):
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
