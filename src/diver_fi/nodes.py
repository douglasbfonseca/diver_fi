import pandas as pd
import requests
from zipfile import ZipFile
from unicodedata import normalize
from kedro.extras.datasets.pandas import SQLTableDataSet
from typing import Any, Dict, Tuple


def extract(parameters: Dict[str, str]) -> pd.DataFrame:
    """
    Funcao principal de extracao, define e chama download_zip_file e csv_reader.
    Concatena os DataFrames e escreve 'df_all' em 'data/agg'.
    
    Args:
        parameters: Paramentros 'encoding', 'url' e 'path' definidos em parameters.yml.
    Returns:
        df_all: Pandas DataFrame agregado.
    """
    def download_zip_file(parameters: Dict[str, str]) -> None:
        """
        Faz o dowoload e extrai os dados .zip vindos da fonte https:// e os escreve no path.

        Args:
            parameters: Paramentros "url" e "path" definidos em parameters.yml.
        Returns:
            None.
        """
        file = requests.get(parameters["url"])
        if file.status_code == requests.codes.OK:
            with open(parameters["path"], "wb") as cda_fi:
                cda_fi.write(file.content)
            file = ZipFile(parameters["path"], "r")
            file.extractall(path="data/raw")
            file.close()

    def csv_reader(i:int, encoding:str, sep: str) -> pd.DataFrame:
        """
        Le os dados .csv e retorna um Pandas DataFrame.

        Args:
            i: Iterador.
            encoding: enconding do arquivo .csv
            sep: separador do arquivo .csv
        Returns:
            df: Pandas DataFrame.
        """
        year = 2022
        month = 12
        path = "data/raw/cda_fi_BLC_" + str(i) + "_" + str(year) + str(month) + ".csv"
        df = pd.read_csv(path, encoding = encoding, sep = sep)
        return df

    download_zip_file(parameters)
    df_all = pd.concat([csv_reader(i,parameters["encoding"], parameters["sep"]) for i in range(1,9)], ignore_index=True) 
    df_all.to_csv('data/agg/cda_fi_BLC_agregate_202212.csv', encoding='utf-8') #Arquivo agregado!
    return df_all

def transform(df_all: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Transforma o DataFrame agregado em 3 DataFrames distintos.

    Args:
        df_all: Pandas DataFrame agregado.
    Returns:
        df_cnpj: DataFrame contento o CNPJ e a Denominacao Social dos fundos.
        df_percentual: DataFrame contendo o percentual de cada ativo por fundo.
        df_one_hot: DataFrame na formatacao ONEHOTENCODER, com os ativos transformados em colunas.
    """
    # inputando fundo com denominação social NaN - somente o "45.908.231/0001-36" neste caso
    df_all["DENOM_SOCIAL"] = df_all["DENOM_SOCIAL"].fillna("Sem denominacao social NaN")
    # Pegando cnpj e denominação social
    df_cnpj = df_all.groupby(by=["CNPJ_FUNDO", "DENOM_SOCIAL"]).aggregate("sum").reset_index()[["CNPJ_FUNDO", "DENOM_SOCIAL"]]

    # Calculando o valor total (em vl_fundo) e percentual dos ativos nos fundos
    # Tirando colunas indesejáveis
    vl_fundo = pd.DataFrame(df_all.groupby(by="CNPJ_FUNDO").aggregate("sum")["VL_MERC_POS_FINAL"]).reset_index()
    df_all = pd.merge(df_all, vl_fundo, how = 'inner', on = 'CNPJ_FUNDO')
    df_all["PERCENTUAL_ATIVO"] = df_all["VL_MERC_POS_FINAL_x"]/df_all["VL_MERC_POS_FINAL_y"]
    df_all = df_all[["CNPJ_FUNDO","TP_ATIVO","VL_MERC_POS_FINAL_x", "VL_MERC_POS_FINAL_y","PERCENTUAL_ATIVO"]]
    df_all.columns = ["CNPJ_FUNDO","TP_ATIVO","VL_MERC_POS_FINAL", "VL_MERC_FUNDO","PERCENTUAL_ATIVO"]
    df_all = df_all.groupby(by=["CNPJ_FUNDO", "TP_ATIVO"]).aggregate("sum").reset_index()
    df_all = df_all.drop(columns=["VL_MERC_POS_FINAL", "VL_MERC_FUNDO"])

    #Separando df com os percentuais
    df_percentual = df_all.copy()

    #Pivotada a fim de conseguir o dataframe no formato ONEHOTENCODER
    df_one_hot= pd.pivot_table(df_all, index = ["CNPJ_FUNDO"], columns = ["TP_ATIVO"], values = "PERCENTUAL_ATIVO").reset_index().fillna(0)

    return df_cnpj , df_percentual, df_one_hot

def load(df_cnpj: pd.DataFrame, df_percentual: pd.DataFrame, df_one_hot: pd.DataFrame, parameters: Dict[str, Any]) -> None:
    """
    Funcao principal de carregamento.
    Define e chama as funcoes write_df_to_db e standardizer para carregar os DataFrames no banco de dados PostgreSQL.

    Args:
        parameters: Parametro "credentials" definido em parameters.yml.
        df_cnpj: DataFrame contento o CNPJ e a Denominacao Social dos fundos.
        df_percentual: DataFrame contendo o percentual de cada ativo por fundo.
        df_one_hot: DataFrame na formatacao ONEHOTENCODER, com os ativos transformados em colunas.
    Returns:
        None
    """
    def write_df_to_db(df: pd.DataFrame, table_name: str, credentials: Dict[str, str]) -> None:
        """
        Carrega a tabela no DB
        
        Args:
            df: Pandas DataFrame
            table_name: nome da tabela
            credentials: dicionario com as credenciais do servidor e do DB
        Return:
            None
        """
        data_set = SQLTableDataSet(table_name=table_name,
                               credentials=credentials)

        data_set.save(df)

    def standardizer(elemento: str) -> str:
        """
        Retira acentos, dois pontos, barras e demais carateres indesejaveis nas colunas das tabelas no DB.
        Padroniza em letras minusculas.

        Args:
            elemento: string fora do padrao
        Return:
            elemento: string dentro do padrao
        """
        elemento = normalize('NFKD', elemento).encode('ASCII','ignore').decode('ASCII').lower()
        elemento = elemento.replace(":","_")
        elemento = elemento.replace("/","_")
        return elemento

    df_cnpj.columns = ["id_cnpj", "denom_social"]
    df_percentual.columns = ["id_cnpj", "nome_ativo", "percentual_ativo"]
    df_one_hot.columns = [standardizer(elemento) for elemento in df_one_hot.columns.to_list()]
    df_one_hot = df_one_hot.rename(columns={'cnpj_fundo': 'id_cnpj'})
    write_df_to_db(df_cnpj, "tb_cnpj_fundos", parameters["credentials"])
    write_df_to_db(df_percentual, "tb_percentual",parameters["credentials"])
    write_df_to_db(df_one_hot, "tb_one_hot", parameters["credentials"])

def extract_12(parameters: Dict[str, str]) -> pd.DataFrame:
    """
    Funcao principal de extracao para os 12 meses. 
    Define e chama download_zip_file_12 e csv_reader_12.
    Concatena os DataFrames e escreve 'df_all' em 'data/agg'.
    
    Args:
        parameters: Paramentros 'url' e 'path' definidos em parameters.yml.
    Returns:
        df_all: Pandas DataFrame agregado.
    """
    def download_zip_file_12(i: int, parameters: Dict[str, str]) -> None:
        """
        Faz o dowoload e extrai os dados .zip vindos da fonte https:// e os escreve no path.

        Args:
            i: Iterador
            parameters: Paramentros "url" e "path" definidos em parameters.yml.
        Returns:
            None.
        """
        if i < 10:
            new_url = parameters["url"][:-6] + '0' + str(i) + parameters["url"][-4:]
        else:
            new_url = parameters["url"][:-6] + str(i) + parameters["url"][-4:]
        
        new_path = parameters["path"][:14] + new_url[-17:]
        
        file = requests.get(new_url)
        if file.status_code == requests.codes.OK:
            with open(new_path, "wb") as cda_fi:
                cda_fi.write(file.content)
            file = ZipFile(new_path, "r")
            file.extractall(path="data/raw")
            file.close()

    def csv_reader_12(i:int, month: int, encoding: str, sep: str) -> pd.DataFrame:
        """
        Le os dados .csv e retorna um Pandas DataFrame.

        Args:
            i: Iterador.
            month: mês do arquivo .csv
            encoding: enconding do arquivo .csv
            sep: separador do arquivo .csv
        Returns:
            df: Pandas DataFrame.
        """
        year = 2022
        month = '0' + str(month) if month < 10 else str(month)
        path = "data/raw/cda_fi_BLC_" + str(i) + "_" + str(year) + month + ".csv"
        df = pd.read_csv(path, encoding = encoding, sep = sep)
        return df

    for i in range(1,13):
        download_zip_file_12(i,parameters)

    df_all = pd.concat([csv_reader_12(i, month, parameters["encoding"], parameters["sep"]) for i in range(1,9) for month in range(1,13)], ignore_index=True)
    df_all.to_csv('data/agg/cda_fi_BLC_agregate_202212.csv', encoding='utf-8')
    df_all = df_all[["CNPJ_FUNDO", "DENOM_SOCIAL", "TP_ATIVO","VL_MERC_POS_FINAL"]]
    return df_all

