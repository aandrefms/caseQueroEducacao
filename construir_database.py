import psycopg2


def criar_database(senha):
    # Estabelecendo conexao ao database postgres
    conn = psycopg2.connect(
       database="postgres", user='postgres', password=senha, host='127.0.0.1', port= '5432'
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Verificar a Existencia do database
    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'case'")
    exists = cursor.fetchone()
    
    # Se nao existir, criar o database
    if not exists:
        cursor.execute('CREATE DATABASE case')
    
    # Conectando ao database criado
    conn1 = psycopg2.connect(
        database="case", user='postgres', password=senha, host='127.0.0.1', port='5432'
    )
    conn1.autocommit = True
    cursor1 = conn1.cursor()
    
    # Linha de comando SQL para criar as tabelas 'regiao' e 'funcionario'
    sql = \
        ('''
        CREATE TABLE IF NOT EXISTS regiao(
        id_municipio BIGSERIAL PRIMARY KEY,
        uf INT NOT NULL,
        regiao INT NOT NULL
        )
        ''', '''
        CREATE TABLE IF NOT EXISTS funcionario(
        id_funcionario BIGSERIAL NOT NULL PRIMARY KEY,
        categoria INT NOT NULL,
        cbo2002_ocupacao INT NOT NULL,
        competencia INT NOT NULL,
        fonte INT NOT NULL,
        grau_de_instrucao INT NOT NULL,
        horas_contratuais INT NOT NULL,
        id INT NOT NULL,
        idade INT NOT NULL,
        ind_trab_intermitente INT NOT NULL,
        ind_trab_parcial INT NOT NULL,
        indicador_aprendiz INT NOT NULL,
        municipio INT NOT NULL REFERENCES regiao (id_municipio),
        raca_cor INT NOT NULL,
        salario FLOAT NOT NULL,
        saldo_movimentacao INT NOT NULL,
        secao VARCHAR(1) NOT NULL,
        sexo INT NOT NULL,
        subclasse INT NOT NULL,
        tam_estab_jan INT NOT NULL,
        tipo_de_deficiencia INT NOT NULL,
        tipo_empregador INT NOT NULL,
        tipo_estabelecimento INT NOT NULL,
        tipo_movimentacao INT NOT NULL
        )''')
    
    # Executar o comando SQL
    try:
        for command in sql:
            cursor1.execute(command)
            
        # Encerrar comunicação com o banco de dados
        cursor1.close()
        
        # Executar as alterações
        conn1.commit()
        print('Database e tabelas criados com sucesso!')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn1.close()
