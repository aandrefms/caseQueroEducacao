import requests
import pandas as pd
import re
import psycopg2
import construir_database

# Request do arquivo json
response = requests.get('http://dataeng.quero.com:5000/caged-data')
data = response.json()

# Pandas Dataframe para realizar algumas transformações
data = data['caged']
df = pd.DataFrame.from_dict(data)


# Remover virgulas na coluna salário
def remover_virgulas(text):
    text = re.sub(r'[^\w\s.]','',text, re.UNICODE)
    return text


df['salario'] = df['salario'].apply(lambda x: remover_virgulas(x))

# Transformar as colunas para tipo numérico
df = df.apply(pd.to_numeric, errors='ignore')

# Dividir o dataframe em 2, para entao utilizar em um snow flakes schema
data = df.drop(['regiao', 'uf'], axis=1)
data_regiao = df[['municipio','uf','regiao']]
data_regiao = data_regiao.drop_duplicates(subset=['municipio'])

# Transformar novamente para dicionário
data_dic = data.to_dict('records')
data_regiao_dic = data_regiao.to_dict('records')

# Nome para as colunas
colunas_funcionario = list(data_dic[0].keys())
colunas_regiao = list(data_regiao_dic[0].keys())
colunas_regiao[0] = 'id_municipio'

# Executar função para criar database e tabelas (se encontra no arquivo construir_database.py)
# Trocar os parametros para 'False' em caso de database e/ou tabelas já criados
# Trocar o valor do parametro senha
construir_database.criar_database(senha='manchester00',controle_db=False, controle_tabela=True)


# Função para inserir os dados no dataframe criado
def inserir_dados(nome_tabela, nome_colunas, dados):
    sql_string = 'INSERT INTO {} '.format(nome_tabela)
    sql_string += "(" + ', '.join(nome_colunas) + ")\nVALUES "
    
    # Iterar pelos dados
    for i, item in enumerate(dados):
        
        values = []
        for col_names, val in item.items():
            
            # A string do Postgres devem estar inseridas em aspas simples
            if type(val) == str:
                val = val.replace("'", "''")
                val = "'" + val + "'"
            
            values += [str(val)]
        
        # Usar o join na lista 'values' e cercar com parêntesis
        sql_string += "(" + ', '.join(values) + "),\n"
    
    # Remover a ultima virgula e terminar a frase com ';'
    sql_string = sql_string[:-2] + ";"
    
    # Conectar ao database
    conn = psycopg2.connect(
        database="case", user='postgres', password='manchester00', host='127.0.0.1', port='5432'
    )
    
    cursor = conn.cursor()
    try:
        cursor.execute(sql_string)
        # Terminar comunicação com o database
        cursor.close()
        # Executar as alterações
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print(f'Dados inseridos na tabela {nome_tabela} com sucesso!')


# Chamar a função inserir dados para adicionar valores as tabelas 'regiao' e 'funcionario'
# A tabela 'regiao' receberá as colunas 'municipio', 'uf' e 'regiao'
inserir_dados('regiao', colunas_regiao, data_regiao_dic)
inserir_dados('funcionario', colunas_funcionario, data_dic)
