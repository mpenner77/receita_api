from fastapi import FastAPI, Query, Depends, HTTPException, Header
import psycopg2
from psycopg2.extras import RealDictCursor
from tokens_validos import listar_tokens_bd

app = FastAPI()



db_params = {
    "host": "dpg-cl0kfci37rbc73cabd2g-a.oregon-postgres.render.com",
    "port": 5432,
    "user": "dados_0h9o_user",
    "password": "AehMovNWjcBiBTm5J8GPUcCwkpR7gFpl",
    "database": "dados_0h9o",
}

table_name = "dados_csv"


def verify_token(authorization: str = Header(..., description="Token de Autorização")):
    valid_tokens = (listar_tokens_bd())
    if authorization not in valid_tokens:
        raise HTTPException(status_code=401, detail="Token de Autorização Inválido")



@app.get("/data/{data}")
async def por_data(data: str, page: int = Query(1, description="Número da página"), token: [str] = Depends(verify_token)):
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
