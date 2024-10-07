import os
import pandas as pd
import tkinter as tk
from tkinter import filedialog
import logging
from datetime import datetime

def configurar_logging(log_file="consolidacao_gl_extract_mes.log"):
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

def selecionar_pasta():
    """
    Abre a janela para selecionar uma pasta e retorna o caminho da pasta selecionada.
    """
    root = tk.Tk()
    root.withdraw()  # Oculta a janela principal do Tkinter
    pasta_selecionada = filedialog.askdirectory(title="Selecione a pasta com os arquivos Excel")
    if not pasta_selecionada:
        logging.warning("Nenhuma pasta foi selecionada.")
    else:
        logging.info(f"Pasta selecionada: {pasta_selecionada}")
    return pasta_selecionada

def extrair_data_do_diretorio(diretorio):
    """
    Extrai o ano e mês do nome do diretório e retorna uma data no formato '01/MM/YYYY'.
    """
    try:
        ano, mes = os.path.basename(diretorio).split('.')
        data = datetime.strptime(f'01/{mes}/{ano}', '%d/%m/%Y')
        return data
    except ValueError as ve:
        logging.error(f"Erro ao converter data do diretório {diretorio}: {ve}")
    except Exception as e:
        logging.exception(f"Erro inesperado ao extrair data do diretório {diretorio}: {e}")
    return None

def consolidacao_gl_extract(diretorio_final):
    """
    Consolida todas as sheets que contenham 'GL_Extract' no nome em um único DataFrame.
    """
    lista_dfs = []
    contador_arquivos = 0

    # Extrai a data do diretório
    data = extrair_data_do_diretorio(diretorio_final)
    if data is None:
        return pd.DataFrame(), None

    # Colunas a serem mantidas
    colunas_desejadas = [
        'NOMINAL_CODE', 'NOMINAL_CODE Description', 'SAP_AMOUNT', 
        'EVENTO', 'EVENTO DESCRITIVO', 'PROCESSO', 'CONTRATO'
    ]
    
    # NOMINAL_CODE permitidos
    nominal_codes_permitidos = [
        7904001080, 8001001082, 8001001081, 8001001083, 8001001084, 8001001085,
        6701001081, 6701001082, 6701001083, 6701001084, 6701001085, 6701001086,
        6701001087, 6701001088, 6701001090, 6701001091, 6701001089, 6701001092, 
        6701004000, 8138840000, 8138843000, 8138841010, 8138831000, 8138830000,
        8138833000, 8138842000, 7904001081
    ]
    
    # Processa os arquivos que começam com 'Rec_CH_' e terminam em Excel
    for arquivo in os.listdir(diretorio_final):
        if arquivo.startswith('Rec_CH_') and arquivo.endswith(('.xlsx', '.xls', '.xlsb')):
            caminho_arquivo = os.path.join(diretorio_final, arquivo)
            
            try:
                # Obtém os nomes das sheets do arquivo Excel
                todas_sheets = pd.ExcelFile(caminho_arquivo).sheet_names
                
                # Filtra as sheets que contêm 'GL_Extract'
                sheets_filtradas = [sheet for sheet in todas_sheets if 'GL_Extract' in sheet]
                
                # Lê e processa as sheets filtradas
                for sheet_name in sheets_filtradas:
                    df = pd.read_excel(caminho_arquivo, sheet_name=sheet_name)
                    
                    # Seleciona as colunas desejadas
                    df = df[colunas_desejadas]
                    
                    # Filtra os NOMINAL_CODE permitidos
                    df = df[df['NOMINAL_CODE'].isin(nominal_codes_permitidos)]
                    
                    # Adiciona a coluna 'DATA' baseada no nome do diretório
                    df['DATA'] = data.strftime('%d/%m/%Y')

                    # Converte a coluna 'CONTRATO' para string
                    df['CONTRATO'] = df['CONTRATO'].astype(str)
                    
                    lista_dfs.append(df)
                    contador_arquivos += 1
                    logging.info(f"Arquivo consolidado com sucesso: {caminho_arquivo} - Sheet: {sheet_name}")
            except KeyError as ke:
                logging.error(f"Erro de chave ao processar {caminho_arquivo} (Sheet: {sheet_name}): {ke}")
            except Exception as e:
                logging.exception(f"Erro inesperado ao processar {caminho_arquivo} (Sheet: {sheet_name}): {e}")
    
    if lista_dfs:
        df_consolidado = pd.concat(lista_dfs, ignore_index=True)
        logging.info(f"Total de arquivos consolidados: {contador_arquivos}")
        return df_consolidado, data
    else:
        logging.warning(f"Nenhum arquivo foi consolidado.")
        return pd.DataFrame(), None

def main():
    configurar_logging()
    
    diretorio_final = selecionar_pasta()
    
    if not diretorio_final:
        logging.error("Processo encerrado: Nenhuma pasta foi selecionada.")
        return
    
    # Consolida os arquivos para as sheets que contêm 'GL_Extract'
    df_consolidado, data_diretorio = consolidacao_gl_extract(diretorio_final)
    
    if not df_consolidado.empty and data_diretorio is not None:
        ano_mes = data_diretorio.strftime('%Y.%m')
        nome_arquivo = f"GL_Extract_{ano_mes}"
        
        output_directory = "C:/Users/1502553/CTT - Correios de Portugal/Planeamento e Controlo - PCG_MIS/20. Project/Analytics/07. PBI Crédito Hipotecário/Projeto Crédito Hipotecário/01. Dados Consolidados/03. GL Extract"
        os.makedirs(output_directory, exist_ok=True)
        
        output_path_feather = os.path.join(output_directory, f"{nome_arquivo}.feather")
        output_path_csv = os.path.join(output_directory, f"{nome_arquivo}.csv")

        try:
            df_consolidado.to_feather(output_path_feather)
            df_consolidado.to_csv(output_path_csv, encoding='UTF-8-sig', index=False)
            logging.info(f"Arquivos salvos: {output_path_feather} e {output_path_csv}")
        except Exception as e:
            logging.error(f"Erro ao salvar os arquivos: {e}")
    else:
        logging.warning("Nenhum dado consolidado disponível para salvar.")

if __name__ == "__main__":
    main()
