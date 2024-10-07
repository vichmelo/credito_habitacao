import os
import pandas as pd
import logging
import tkinter as tk
from tkinter import filedialog
from datetime import datetime

def configurar_logging(log_file="consolidacao_base_imparidade.log"):
    """
    Configura o sistema de logging para registrar mensagens em um arquivo e na saída padrão.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode="w"),
            logging.StreamHandler()
        ]
    )

def selecionar_diretorio():
    """
    Abre uma caixa de diálogo para selecionar o diretório de entrada.

    Retorna:
    - str: Caminho do diretório selecionado ou None se nenhum diretório for selecionado.
    """
    root = tk.Tk()
    root.withdraw()  # Oculta a janela principal do Tkinter
    diretorio_selecionado = filedialog.askdirectory(title="Selecione o diretório de entrada")
    if not diretorio_selecionado:
        logging.warning("Nenhum diretório foi selecionado.")
    else:
        logging.info(f"Diretório selecionado: {diretorio_selecionado}")
    return diretorio_selecionado

def carregar_arquivos_da_pasta(diretorio):
    """
    Carrega todos os arquivos Excel que seguem o padrão 'Imparidade_credito_*.xlsx' dentro do diretório.

    Parâmetros:
    - diretorio (str): Caminho da pasta contendo os arquivos Excel.

    Retorna:
    - list: Lista de caminhos dos arquivos encontrados.
    """
    arquivos = [
        os.path.join(diretorio, arquivo) 
        for arquivo in os.listdir(diretorio) 
        if arquivo.startswith("Imparidade_credito_") and arquivo.endswith(".xlsx")
    ]
    if not arquivos:
        logging.warning("Nenhum arquivo encontrado no diretório especificado.")
    return arquivos

def extrair_data_do_nome(arquivo):
    """
    Extrai o ano e mês do nome do arquivo e retorna uma data no formato '01/MM/YYYY'.

    Parâmetros:
    - arquivo (str): Nome do arquivo no formato 'Imparidade_credito_YYYYMM.xlsx'.

    Retorna:
    - datetime: Data no formato '01/MM/YYYY'.
    - None: Se a data não puder ser extraída.
    """
    try:
        partes_nome = arquivo.split('_')
        data_str = partes_nome[-1].replace(".xlsx", "")  # Assume que a última parte do nome contém 'YYYYMM'
        ano = data_str[:4]
        mes = data_str[4:6]
        data = datetime.strptime(f'01/{mes}/{ano}', '%d/%m/%Y')
        return data
    except ValueError as ve:
        logging.error(f"Erro ao converter data do arquivo {arquivo}: {ve}")
    except Exception as e:
        logging.exception(f"Erro inesperado ao extrair data do arquivo {arquivo}: {e}")
    return None

def verificar_sheet_existente(arquivo, sheet_name):
    """
    Verifica se a sheet especificada existe no arquivo Excel.

    Parâmetros:
    - arquivo (str): Caminho do arquivo Excel.
    - sheet_name (str): Nome da planilha a verificar.

    Retorna:
    - bool: Retorna True se a planilha existir, False caso contrário.
    """
    try:
        sheets = pd.ExcelFile(arquivo).sheet_names
        if sheet_name in sheets:
            logging.info(f"Planilha '{sheet_name}' encontrada no arquivo {arquivo}.")
            return True
        else:
            logging.warning(f"Planilha '{sheet_name}' não encontrada no arquivo {arquivo}.")
            return False
    except Exception as e:
        logging.error(f"Erro ao verificar planilhas do arquivo {arquivo}: {e}")
        return False

def transformar_e_filtrar_dataframe(df, data):
    """
    Aplica as transformações e filtros necessários ao DataFrame.

    Parâmetros:
    - df (pd.DataFrame): O DataFrame carregado.
    - data (str): A data extraída do nome do arquivo a ser inserida como coluna.

    Retorna:
    - pd.DataFrame: DataFrame transformado e filtrado.
    """
    try:
        # Filtrar as linhas onde a coluna 'Produto' for igual a 'Mortgage Loan'
        df_filtrado = df[df['Produto'] == 'Mortgage Loan'][['CREDIT_FACILITY_ID_CODE', 'Dos quais:Imparidade On-balance']]

        # Adicionar a coluna 'DATA' com a data extraída do nome do arquivo
        df_filtrado['DATA'] = data

        logging.info(f"DataFrame transformado com sucesso para a data: {data}")
        return df_filtrado
    except KeyError as ke:
        logging.error(f"Erro ao transformar DataFrame: Coluna não encontrada - {ke}")
        raise
    except Exception as e:
        logging.exception(f"Erro inesperado ao transformar DataFrame: {e}")
        raise

def salvar_dataframe_consolidado(df, output_directory):
    """
    Salva o DataFrame consolidado em arquivos Feather e CSV.

    Parâmetros:
    - df (pd.DataFrame): O DataFrame consolidado.
    - output_directory (str): O caminho do diretório onde salvar os arquivos.
    """
    nome_arquivo = "Consolidado_Imparidade_Credito"
    
    output_path_feather = os.path.join(output_directory, f"{nome_arquivo}.feather")
    output_path_csv = os.path.join(output_directory, f"{nome_arquivo}.csv")
    
    try:
        df.to_feather(output_path_feather)
        df.to_csv(output_path_csv, encoding='UTF-8-sig', index=False)
        logging.info(f"Arquivos consolidados e salvos com sucesso: {output_path_feather} e {output_path_csv}")
    except Exception as e:
        logging.error(f"Erro ao salvar os arquivos: {e}")
        raise

def main():
    configurar_logging()

    # Abrir diálogo para selecionar o diretório de entrada
    diretorio_entrada = selecionar_diretorio()
    if not diretorio_entrada:
        logging.warning("Nenhum diretório de entrada foi selecionado. O programa será encerrado.")
        return

    # Definir o diretório de saída
    diretorio_saida = "C:/Users/1502553/CTT - Correios de Portugal/Planeamento e Controlo - PCG_MIS/20. Project/Analytics/07. PBI Crédito Hipotecário/Projeto Crédito Hipotecário/01. Dados Consolidados/05. Imparidade"  

    # Verificar e criar o diretório de saída se não existir
    os.makedirs(diretorio_saida, exist_ok=True)
    
    # Carregar todos os arquivos da pasta
    arquivos = carregar_arquivos_da_pasta(diretorio_entrada)
    
    if arquivos:
        lista_dataframes = []
        for arquivo in arquivos:
            try:
                # Verificar se a sheet "Base_Com Overlay" existe no arquivo
                if not verificar_sheet_existente(arquivo, "Base_Com Overlay"):
                    continue

                # Extrair a data do nome do arquivo
                data = extrair_data_do_nome(arquivo)
                
                # Carregar o arquivo XLSX em um DataFrame, utilizando a sheet correta "Base_Com Overlay"
                df = pd.read_excel(arquivo, sheet_name="Base_Com Overlay", header=3)  # Especificando a planilha
                logging.info(f"Arquivo carregado com sucesso: {arquivo}")
                
                # Transformar e filtrar o DataFrame
                df_transformado = transformar_e_filtrar_dataframe(df, data)
                
                # Adicionar o DataFrame transformado à lista
                lista_dataframes.append(df_transformado)
            except Exception as e:
                logging.error(f"Erro ao processar o arquivo {arquivo}: {e}")
        
        # Consolidar todos os DataFrames em um único DataFrame
        if lista_dataframes:
            df_consolidado = pd.concat(lista_dataframes, ignore_index=True)
            
            # Salvar o DataFrame consolidado
            salvar_dataframe_consolidado(df_consolidado, diretorio_saida)
        else:
            logging.warning("Nenhum DataFrame foi consolidado.")
    else:
        logging.warning("Nenhum arquivo para processar.")

if __name__ == "__main__":
    main()
