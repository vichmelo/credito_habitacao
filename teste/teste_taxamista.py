import os
import pandas as pd
import tkinter as tk
from tkinter import filedialog
import logging
from datetime import datetime

def configurar_logging(log_file="consolidacao_base_contabilidade.log"):
    """
    Configura o sistema de logging para registrar mensagens em um arquivo e na saída padrão.

    Parâmetros:
    - log_file (str): Nome do arquivo de log. O padrão é "consolidacao_ods.log".
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode="w"),
            logging.StreamHandler()
        ]
    )

def selecionar_pasta():
    """
    Abre uma caixa de diálogo para selecionar uma pasta e retorna o caminho da pasta selecionada.

    Retorna:
    - str: Caminho da pasta selecionada ou None se nenhuma pasta for selecionada.
    """
    root = tk.Tk()
    root.withdraw()  # Oculta a janela principal do Tkinter
    pasta_selecionada = filedialog.askdirectory(title="Selecione a pasta com os arquivos Excel")
    if not pasta_selecionada:
        logging.warning("Nenhuma pasta foi selecionada.")
    return pasta_selecionada


def extrair_data_do_nome(arquivo):
    """
    Extrai a data do nome do arquivo no formato '2024_05 - Carteira Crédito Habitação'.

    Parâmetros:
    - arquivo (str): Nome do arquivo.

    Retorna:
    - datetime: Data extraída do nome do arquivo, sempre com o dia como 01.
    - None: Se a data não puder ser extraída.
    """
    try:
        data_str = arquivo.split(' - ')[0]
        data = datetime.strptime(data_str, "%Y_%m")
        return data
    except ValueError as ve:
        logging.error(f"Erro ao converter data do arquivo {arquivo}: {ve}")
    except Exception as e:
        logging.exception(f"Erro inesperado ao extrair data do arquivo {arquivo}: {e}")
    return None

def consolidacao_taxas_mistas(diretorio, sheet_name):
    """
    Consolida arquivos Excel com o padrão 'Carteira Crédito Habitação' em um único DataFrame para a sheet Taxas Mistas.

    Parâmetros:
    - diretorio (str): Caminho para o diretório onde os arquivos estão localizados.
    - sheet_name (str): Nome da planilha a ser consolidada (Taxas Mistas).

    Retorna:
    - pd.DataFrame: DataFrame consolidado com todos os dados da planilha especificada.
    """
    lista_dfs = []
    contador_arquivos = 0

    for arquivo in os.listdir(diretorio):
        if 'Carteira Crédito Habitação' in arquivo and arquivo.endswith(('.xlsx', '.xls', '.xlsb')):
            caminho_arquivo = os.path.join(diretorio, arquivo)
            
            try:
                data = extrair_data_do_nome(arquivo)
                
                if not data:
                    logging.warning(f"Data não extraída para o arquivo {arquivo}. Arquivo ignorado.")
                    continue

                df = pd.read_excel(caminho_arquivo, sheet_name=sheet_name, header=2)
                df['Data'] = data
                
                lista_dfs.append(df)
                contador_arquivos += 1
                logging.info(f"Arquivo consolidado com sucesso: {caminho_arquivo}")
            except Exception as e:
                logging.exception(f"Erro inesperado ao processar o arquivo {caminho_arquivo}: {e}")
    
    if lista_dfs:
        df_consolidado = pd.concat(lista_dfs, ignore_index=True)
        logging.info(f"Total de arquivos consolidados para {sheet_name}: {contador_arquivos}")
        return df_consolidado
    else:
        logging.warning(f"Nenhum arquivo foi consolidado para {sheet_name}.")
        return pd.DataFrame()
    

def main():
    configurar_logging()
    
    diretorio = selecionar_pasta()
    
    if not diretorio:
        logging.error("Processo encerrado: Nenhuma pasta foi selecionada.")
        return
    
    # Dicionários separados para cada planilha
    dfs_consolidado_taxas_mistas = {}

    # Tentativa de consolidação para a planilha "Carteira"
    try:
        df_consolidado_tx = consolidacao_taxas_mistas(diretorio, "Teste")
        if not df_consolidado_tx.empty:
            dfs_consolidado_taxas_mistas["Taxas Mistas"] = df_consolidado_tx
            logging.info("Consolidação concluída para a planilha: TaxasMistas")
        else:
            logging.warning("Nenhum dado consolidado para a planilha: Taxas Mistas")
    except Exception as e:
        logging.error(f"Erro ao consolidar a planilha 'Taxas Mistas': {e}")

    # Definindo diretório de saída e salvando os DataFrames consolidados em CSV
    output_directory = "C:/Users/1502553/CTT - Correios de Portugal/Planeamento e Controlo - PCG_MIS/20. Project/Analytics/07. PBI Crédito Hipotecário/Projeto Crédito Hipotecário/01. Dados Consolidados/02. Contabilidade"
    os.makedirs(output_directory, exist_ok=True)
    
    # Salvando os DataFrames da planilha "Taxas Mistas" em CSV
    for sheet_name, df in dfs_consolidado_taxas_mistas.items():
        output_path_csv = os.path.join(output_directory, f"{sheet_name}_Contabilidade.csv")
        try:
            df.to_csv(output_path_csv, encoding='UTF-8-sig', index=False)
            logging.info(f"Arquivo salvo: {output_path_csv}")
        except Exception as e:
            logging.error(f"Erro ao salvar o arquivo {output_path_csv}: {e}")

if __name__ == "__main__":
    main()