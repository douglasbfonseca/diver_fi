## ETL de diversificação dos fundos de investimento

### Resumo
Projeto que visa extrair, transformar e carregar os dados de diversificação de fundos de investimento.

A fonte de dados é o portal de dados abertos da CVM (https://dados.cvm.gov.br/)

### Rodando a pipeline
kedro run

python -m kedro run

Mais instrucoes de run em instrucoes.txt

### Bibliotecas necessárias
Todos as bibliotecas necessárias encontram-se no Pipfile, são elas:

Kedro; Pandas; psycopg2; sqlalchemy

### Output
São 2 os outputs:
- Um arquivo .csv agregado no diretório data/agg
- Três Tabelas SQL, já normalizadas, no DB destino, df_cnpj , df_percentual, df_one_hot

df_cnpj : contém o CNPJ e a Denominacao Social dos fundos. (1 CNPJ por linha)

df_percentual: contém o percentual de cada ativo por fundo. (Mais de 1 CNPJ por linha)

df_one_hot: contém a mesma informação do df_percentual, porém na formatação OneHotEncoder, com os ativos transformados em colunas. (1 CNPJ por linha)

### PostegreSQL Data Base
O servidor do DB deve ser adcionado em parameters.yml -> credentials

As Queries para joins e constraints das tabelas encontram-se em docs.

### Extract e Extract_12
O padrão do projeto é coletar mensalmente os dados.

O node Extract por ser substituído na pipeline por Extract_12 para uma coleta anual.

