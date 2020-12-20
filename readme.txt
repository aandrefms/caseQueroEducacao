Case realizado para Quero Educação;

Verificar os requerimentos (bibliotercas necessárias) no arquivo requirements.txt

- Iniciar e executar o arquivo main.py, inserindo a senha para o database.
- Para realizar alterações no nome, host ou port do database, editar o arquivo 'construir_database.py'.
- O script entao verificará a existência do database e das tabelas, e caso não existam, as criará.

Algumas informações sobre Banco de dados criado:
- O banco relacional utilizado foi o PostgreSQL;
- O banco de dados foi criado tendo como base o esquema floco de neve;
    - A única relação direta que encontrei entre as colunas foi: municipio - uf - regiao;
    - Portanto, criei uma tabela separada contendo esses elementos, afim de diminuir a redundância;
