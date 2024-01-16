import psycopg2


db_params = {
    "host": "dpg-cl0kfci37rbc73cabd2g-a.oregon-postgres.render.com",
    "port": 5432,
    "user": "dados_0h9o_user",
    "password": "AehMovNWjcBiBTm5J8GPUcCwkpR7gFpl",
    "database": "dados_0h9o"
}

def conectar_bd():
    try:
        connection = psycopg2.connect(**db_params)
        return connection
    except Exception as e:
         return None

def listar_tokens_bd():
    connection = conectar_bd()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT  token  FROM usuarios")
            data = cursor.fetchall()
            token_list = [row[0] for row in data]
            return token_list
        except Exception as e:
            print(f"Erro ao obter dados do banco de dados:\n{e}")
        finally:
            connection.close()




