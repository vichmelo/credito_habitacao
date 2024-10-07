import os
import pandas as pd
import tkinter as tk
from tkinter import filedialog
import logging

def configurar_logging():
    """
    Configura o sistema de logging para registrar mensagens no console e em um arquivo de log.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("processamento_balancete.log", mode="w"),
            logging.StreamHandler()
        ]
    )

def selecionar_arquivo():
    """
    Abre uma caixa de diálogo para selecionar um arquivo e retorna o caminho do arquivo.
    """
    root = tk.Tk()
    root.withdraw()  # Oculta a janela principal do Tkinter
    arquivo_selecionado = filedialog.askopenfilename(
        title="Selecione o arquivo de entrada",
        filetypes=[("CSV files", "*.csv")]
    )
    
    if arquivo_selecionado:
        logging.info(f"Arquivo selecionado: {arquivo_selecionado}")
    else:
        logging.warning("Nenhum arquivo foi selecionado.")
    
    return arquivo_selecionado

def transformar_dataframe(df):
    """
    Aplica transformações no DataFrame. Esta função contém placeholders que 
    podem ser customizados conforme necessário.
    
    Parâmetros:
    - df (DataFrame): O DataFrame de entrada.
    
    Retorna:
    - DataFrame transformado.
    """
    try:
        logging.info("Iniciando as transformações no DataFrame.")
        
        df = df.copy()      
        df = df.drop(columns={'∆', '2024-06-30 00:00:00.1', 'Unnamed: 5'})  
    
        logging.info("Transformações aplicadas com sucesso.")
        return df
    except KeyError as e:
        logging.error(f"Erro de chave durante a transformação: {e}")
        raise
    except Exception as e:
        logging.exception(f"Erro inesperado durante a transformação: {e}")
        raise

def salvar_dataframe(df, output_directory):
    """
    Salva o DataFrame transformado em arquivos Feather e CSV.
    
    Parâmetros:
    - df (DataFrame): O DataFrame consolidado e transformado.
    - output_directory (str): O caminho do diretório onde salvar os arquivos.
    """
    try:
        nome_arquivo = "Balancete"
    
        # Caminhos para salvar os arquivos
        output_path_feather = os.path.join(output_directory, f"{nome_arquivo}.feather")
        output_path_csv = os.path.join(output_directory, f"{nome_arquivo}.csv")
        
        # Salvar o DataFrame em Feather e CSV
        df.to_feather(output_path_feather)
        df.to_csv(output_path_csv, encoding='UTF-8-sig', index=False)
        
        logging.info(f"Arquivos transformados e salvos: {output_path_feather} e {output_path_csv}")
    except Exception as e:
        logging.error(f"Erro ao salvar os arquivos: {e}")
        raise

def main():
    # Configurar o logging
    configurar_logging()
    
    # Selecionar o arquivo de origem (Feather ou CSV)
    arquivo_selecionado = selecionar_arquivo()
    
    if arquivo_selecionado:
        # Ler o arquivo selecionado
        try:
            if arquivo_selecionado.endswith('.feather'):
                df = pd.read_feather(arquivo_selecionado)
            elif arquivo_selecionado.endswith('.csv'):
                df = pd.read_csv(arquivo_selecionado, encoding='UTF-8-sig')
            logging.info(f"Arquivo lido com sucesso: {arquivo_selecionado}")
        except Exception as e:
            logging.error(f"Erro ao ler o arquivo: {e}")
            return
        
        # Aplicar as transformações desejadas no DataFrame
        try:
            df_transformado = transformar_dataframe(df)
        except Exception as e:
            logging.error(f"Erro ao transformar o DataFrame: {e}")
            return
        
        # Definir manualmente o diretório de saída
        output_directory = "C:/Users/1502553/CTT - Correios de Portugal/Planeamento e Controlo - PCG_MIS/20. Project/Analytics/07. PBI Crédito Hipotecário/Projeto Crédito Hipotecário/02. Dados Processados/06. Balancete" 

        if output_directory:
            # Salvar o DataFrame transformado
            try:
                salvar_dataframe(df_transformado, output_directory)
            except Exception as e:
                logging.error(f"Erro ao salvar o DataFrame transformado: {e}")
        else:
            logging.warning("Nenhum diretório de saída definido.")
    else:
        logging.warning("Nenhum arquivo de origem selecionado.")

if __name__ == "__main__":
    main()
