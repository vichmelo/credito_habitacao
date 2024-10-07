import os
import pandas as pd
import tkinter as tk
from tkinter import filedialog
from datetime import datetime
import logging

def configurar_logging(log_file="consolidacao_contabilidade.log"):
    """
    Configura o logging para registrar mensagens no arquivo e no console.
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
    Abre a janela para selecionar uma pasta e retorna o caminho selecionado.
    """
    root = tk.Tk()
    root.withdraw()  # Oculta a janela principal
    pasta_selecionada = filedialog.askdirectory(title="Selecione a pasta com os arquivos Excel")
    if not pasta_selecionada:
        logging.warning("Nenhuma pasta foi selecionada.")
    else:
        logging.info(f"Pasta selecionada: {pasta_selecionada}")
    return pasta_selecionada

def extrair_data_do_nome(arquivo):
    """
    Extrai a data do nome do arquivo.
    """
    try:
        data_str = arquivo.split(' - ')[0]
        data = datetime.strptime(data_str, "%Y_%m")
        return data
    except Exception as e:
        logging.exception(f"Erro ao extrair data do arquivo '{arquivo}': {e}")
        return None

def remover_linhas_invalidas(df):
    """
    Remove colunas e linhas inválidas do DataFrame.
    """
    try:
        colunas_invalidas = ['…', '….1', '….2']
        df = df.drop(columns=[col for col in colunas_invalidas if col in df.columns], errors='ignore')

        if 'N. Contrato' in df.columns:
            index_invalid = df[df['N. Contrato'] == '…'].index
            if not index_invalid.empty:
                df = df.loc[:index_invalid[0] - 1].copy()
            df = df[df['N. Contrato'].notna() & df['N. Contrato'].str.strip().astype(bool)]
        return df
    except Exception as e:
        logging.exception(f"Erro ao remover linhas inválidas: {e}")
        return df

def consolidacao_contabilidade(diretorio, sheet_name):
    """
    Consolida os arquivos Excel da 'Carteira' no diretório.
    """
    lista_dfs = []
    contador_arquivos = 0

    for arquivo in os.listdir(diretorio):
        if 'Carteira Crédito Habitação' in arquivo and arquivo.endswith(('.xlsx', '.xls', '.xlsb')):
            caminho_arquivo = os.path.join(diretorio, arquivo)
            try:
                logging.info(f"Processando arquivo: {arquivo}")
                data = extrair_data_do_nome(arquivo)
                if not data:
                    logging.warning(f"Data não encontrada no arquivo: {arquivo}")
                    continue

                df = pd.read_excel(caminho_arquivo, sheet_name=sheet_name, header=4)
                df = remover_linhas_invalidas(df)
                df['Data'] = data
                lista_dfs.append(df)
                contador_arquivos += 1
            except Exception as e:
                logging.exception(f"Erro ao processar o arquivo '{arquivo}': {e}")
    
    if lista_dfs:
        df_consolidado = pd.concat(lista_dfs, ignore_index=True)
        logging.info(f"Total de arquivos consolidados: {contador_arquivos}")
        return df_consolidado
    else:
        logging.warning("Nenhum arquivo válido foi consolidado.")
        return pd.DataFrame()

def consolidacao_taxas_mistas(diretorio, sheet_name):
    """
    Consolida os arquivos Excel de 'Taxas Mistas' no diretório.
    """
    lista_dfs = []
    contador_arquivos = 0

    for arquivo in os.listdir(diretorio):
        if 'Carteira Crédito Habitação' in arquivo and arquivo.endswith(('.xlsx', '.xls', '.xlsb')):
            caminho_arquivo = os.path.join(diretorio, arquivo)
            try:
                logging.info(f"Processando Taxas Mistas para o arquivo: {arquivo}")
                data = extrair_data_do_nome(arquivo)
                if not data:
                    logging.warning(f"Data não encontrada no arquivo: {arquivo}")
                    continue

                logging.info(f"Tentando leitura com header=1 para o arquivo: {arquivo}")
                df = pd.read_excel(caminho_arquivo, sheet_name=sheet_name, header=1)
                df['Data'] = data

                if 'N. Contrato' in df.columns and 'ADJ' in df.columns:
                    df = df.rename(columns={'N. Contrato': 'Contrato', 'ADJ': 'B/S Adj D Cred C P&L'})
                    df = df[['Contrato', 'Data', 'B/S Adj D Cred C P&L']]
                    df['Contrato'] = df['Contrato'].fillna(0).astype(str).str.replace('.0', '')

                else:
                    logging.info(f"Tentando leitura com header=2 para o arquivo: {arquivo}")
                    df = pd.read_excel(caminho_arquivo, sheet_name=sheet_name, header=2)
                    df['Data'] = data
                    df = df.rename(columns={
                        'N. Contrato': 'Contrato', 
                        'B/S Adj\nD Cred / C P&L': 'B/S Adj D Cred C P&L', 
                        'B/S Adj\nD P&L / C B/S': 'B/S Adj D P&L / C B/S'
                    })
                    df = df[['Contrato', 'Data', 'B/S Adj D Cred C P&L', 'B/S Adj D P&L / C B/S']]
                    df['Contrato'] = df['Contrato'].fillna(0).astype(str).str.replace('.0', '')

                lista_dfs.append(df)
                contador_arquivos += 1

            except Exception as e:
                logging.exception(f"Erro ao processar Taxas Mistas no arquivo '{arquivo}': {e}")
    
    if lista_dfs:
        logging.info(f"Consolidando {contador_arquivos} arquivos de Taxas Mistas.")
        df_consolidado = pd.concat(lista_dfs, ignore_index=True)
        return df_consolidado
    else:
        logging.warning("Nenhum arquivo válido foi consolidado para Taxas Mistas.")
        return pd.DataFrame()

def consolidacao_protocolo(diretorio, sheet_name):
    """
    Consolida os arquivos Excel de 'Protocolos CH' no diretório.
    """
    lista_dfs = []
    contador_arquivos = 0

    for arquivo in os.listdir(diretorio):
        if 'Carteira Crédito Habitação' in arquivo and arquivo.endswith(('.xlsx', '.xls', '.xlsb')):
            caminho_arquivo = os.path.join(diretorio, arquivo)
            try:
                logging.info(f"Processando Protocolos CH para o arquivo: {arquivo}")
                data = extrair_data_do_nome(arquivo)
                if not data:
                    logging.warning(f"Data não encontrada no arquivo: {arquivo}")
                    continue

                df = pd.read_excel(caminho_arquivo, sheet_name=sheet_name, header=2)
                df['Data'] = data
                df = df[['ID_Contrato', 'Data', 'P&L - NIM']]
                lista_dfs.append(df)
                contador_arquivos += 1
            except Exception as e:
                logging.exception(f"Erro ao processar Protocolos CH no arquivo '{arquivo}': {e}")
    
    if lista_dfs:
        df_consolidado = pd.concat(lista_dfs, ignore_index=True)
        logging.info(f"Total de arquivos de Protocolos CH consolidados: {contador_arquivos}")
        return df_consolidado
    else:
        logging.warning("Nenhum arquivo válido foi consolidado para Protocolos CH.")
        return pd.DataFrame()

def consolidacao_contabilidade_geral(carteira_dict: dict, taxas_mistas_dict: dict, protocolo_dict: dict):
    """
    Consolida os DataFrames de Carteira, Taxas Mistas e Protocolos CH.
    """
    try:
        carteira_df = carteira_dict.get('Carteira')
        taxas_mistas_df = taxas_mistas_dict.get('Taxas Mistas')
        protocolo_df = protocolo_dict.get('Protocolos CH')

        taxas_mistas_df['Contrato'] = taxas_mistas_df['Contrato'].astype(str)
        protocolo_df['ID_Contrato'] = protocolo_df['ID_Contrato'].astype(str)

        if carteira_df is None or taxas_mistas_df is None:
            logging.error("Dados da Carteira ou Taxas Mistas não estão disponíveis.")
            return pd.DataFrame()

        merged_taxas_mistas = pd.merge(
            carteira_df, 
            taxas_mistas_df, 
            left_on=['N. Contrato', 'Data'], 
            right_on=['Contrato', 'Data'], 
            how='left'
        )

        if protocolo_df is not None:
            merged_protocolo = pd.merge(
                merged_taxas_mistas, 
                protocolo_df, 
                left_on=['N. Contrato', 'Data'], 
                right_on=['ID_Contrato', 'Data'], 
                how='left'
            )

        return merged_protocolo

    except Exception as e:
        logging.exception(f"Erro ao consolidar Contabilidade Geral: {e}")
        return pd.DataFrame()

def main():
    configurar_logging()

    diretorio = selecionar_pasta()
    
    if not diretorio:
        logging.error("Nenhum diretório foi selecionado.")
        return
    
    dfs_consolidado_carteira = {}
    dfs_consolidado_taxas_mistas = {}
    dfs_consolidado_protocolos_ch = {}
    dfs_consolidado_contabilidade = {}

    try:
        df_consolidado_cart = consolidacao_contabilidade(diretorio, "Carteira")
        if not df_consolidado_cart.empty:
            dfs_consolidado_carteira["Carteira"] = df_consolidado_cart
    except Exception as e:
        logging.exception(f"Erro ao consolidar Carteira: {e}")

    try:
        df_consolidado_tx = consolidacao_taxas_mistas(diretorio, "TaxasMistas")
        if not df_consolidado_tx.empty:
            dfs_consolidado_taxas_mistas["Taxas Mistas"] = df_consolidado_tx
    except Exception as e:
        logging.exception(f"Erro ao consolidar Taxas Mistas: {e}")

    try:
        df_consolidado_prot = consolidacao_protocolo(diretorio, "ProtocolosCH")
        if not df_consolidado_prot.empty:
            dfs_consolidado_protocolos_ch["Protocolos CH"] = df_consolidado_prot
    except Exception as e:
        logging.exception(f"Erro ao consolidar Protocolos CH: {e}")

    try:
        df_consolidado_contabilidade = consolidacao_contabilidade_geral(dfs_consolidado_carteira, dfs_consolidado_taxas_mistas, dfs_consolidado_protocolos_ch)
        if not df_consolidado_contabilidade.empty:
            dfs_consolidado_contabilidade["Contabilidade"] = df_consolidado_contabilidade
    except Exception as e:
        logging.exception(f"Erro ao consolidar Contabilidade Geral: {e}")

    output_directory = "C:/Users/1502553/CTT - Correios de Portugal/Planeamento e Controlo - PCG_MIS/20. Project/Analytics/07. PBI Crédito Hipotecário/Projeto Crédito Hipotecário/01. Dados Consolidados/02. Contabilidade"
    os.makedirs(output_directory, exist_ok=True)

    for sheet_name, df in dfs_consolidado_carteira.items():
        try:
            output_path_csv = os.path.join(output_directory, f"{sheet_name}_Contabilidade.csv")
            df.to_csv(output_path_csv, encoding='UTF-8-sig', index=False)
            logging.info(f"CSV da Carteira salvo com sucesso: {output_path_csv}")
        except Exception as e:
            logging.exception(f"Erro ao salvar CSV da Carteira: {e}")

    for sheet_name, df in dfs_consolidado_taxas_mistas.items():
        try:
            output_path_csv = os.path.join(output_directory, f"{sheet_name}_Contabilidade.csv")
            df.to_csv(output_path_csv, encoding='UTF-8-sig', index=False)
            logging.info(f"CSV de Taxas Mistas salvo com sucesso: {output_path_csv}")
        except Exception as e:
            logging.exception(f"Erro ao salvar CSV de Taxas Mistas: {e}")

    for sheet_name, df in dfs_consolidado_protocolos_ch.items():
        try:
            output_path_csv = os.path.join(output_directory, f"{sheet_name}_Contabilidade.csv")
            df.to_csv(output_path_csv, encoding='UTF-8-sig', index=False)
            logging.info(f"CSV de Protocolos CH salvo com sucesso: {output_path_csv}")
        except Exception as e:
            logging.exception(f"Erro ao salvar CSV de Protocolos CH: {e}")

    for sheet_name, df in dfs_consolidado_contabilidade.items():
        try:
            output_path_csv = os.path.join(output_directory, "Contabilidade.csv")
            df.to_csv(output_path_csv, encoding='UTF-8-sig', index=False)
            logging.info(f"CSV da Contabilidade Geral salvo com sucesso: {output_path_csv}")
        except Exception as e:
            logging.exception(f"Erro ao salvar CSV da Contabilidade Geral: {e}")

if __name__ == "__main__":
    main()
