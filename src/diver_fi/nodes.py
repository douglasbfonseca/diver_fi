import pandas as pd
import requests
from zipfile import ZipFile
from unicodedata import normalize
from kedro.extras.datasets.pandas import SQLTableDataSet


def extract(parameters):
    """
    Extrai os dados da fonte
    """
    def download_zip_file(parameters):
        file = requests.get(parameters["url"])
        if file.status_code == requests.codes.OK:
            with open(parameters["path"], "wb") as cda_fi:
                cda_fi.write(file.content)
            file = ZipFile(parameters["path"], "r")
            file.extractall(path="data/raw")
            file.close()

    def csv_reader(i:int):
        year = 2022
        month = 12
        path = "data/raw/cda_fi_BLC_" + str(i) + "_" + str(year) + str(month) + ".csv"
        df = pd.read_csv(path, encoding = "ISO-8859-1", sep = ";")
        return df

    download_zip_file(parameters)
    df_all = pd.concat([csv_reader(i) for i in range(1,9)], ignore_index=True) #Arquivo agregado!
    df_all.to_csv('data/agg/cda_fi_BLC_agregate_202212.csv', encoding='utf-8')
    return df_all

def transform(df_all):
    """
    Transforma os dados
    """
    # Pegando cnpj e denominação social
    df_cnpj = df_all.groupby(by=["CNPJ_FUNDO", "DENOM_SOCIAL"]).aggregate("sum").reset_index()[["CNPJ_FUNDO", "DENOM_SOCIAL"]]

    # Calculando percentual de ativos nos fundos e tirando colunas indesejáveis
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

def load(df_cnpj, df_percentual, df_one_hot, parameters):
    """
    Carrega os dataframes no banco de dados PostgreSQL
    """
    def write_df_to_db(df, table_name, credentials):
        data_set = SQLTableDataSet(table_name=table_name,
                               credentials=credentials)

        data_set.save(df)

    def standardizer(elemento):
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



