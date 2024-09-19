import os
import pandas as pd
import tkinter as tk
from tkinter import filedialog
import logging

def configurar_logging(log_file="consolidacao_gl_extract_ano.log"):
    """
    Configura o sistema de logging para registrar mensagens em um arquivo e na saída padrão.

    Parâmetros:
    - log_file (str): Nome do arquivo de log. O padrão é "consolidacao_gl_extract_ytd.log".
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
    Abre uma caixa de diálogo para selecionar um diretório e retorna o caminho do diretório selecionado.

    Retorna:
    - str: Caminho do diretório selecionado ou None se nenhum diretório for selecionado.
    """
    root = tk.Tk()
    root.withdraw()  # Oculta a janela principal do Tkinter
    diretorio_selecionado = filedialog.askdirectory(title="Selecione o diretório contendo os arquivos Feather")
    if not diretorio_selecionado:
        logging.warning("Nenhum diretório foi selecionado.")
    return diretorio_selecionado

def consolidar_arquivos_feather(diretorio):
    """
    Consolida todos os arquivos Feather no formato 'GL_Extract_AAAA.MM.feather' em um único DataFrame.

    Parâmetros:
    - diretorio (str): Caminho para o diretório contendo os arquivos Feather.

    Retorna:
    - pd.DataFrame: DataFrame consolidado.
    """
    arquivos_feather = [f for f in os.listdir(diretorio) if f.startswith("GL_Extract_") and f.endswith(".feather")]
    
    if not arquivos_feather:
        logging.error("Nenhum arquivo Feather com o padrão 'GL_Extract_AAAA.MM.feather' foi encontrado no diretório.")
        raise FileNotFoundError("Nenhum arquivo Feather encontrado no diretório.")

    dataframes = []
    
    for arquivo in arquivos_feather:
        caminho_arquivo = os.path.join(diretorio, arquivo)
        try:
            df = pd.read_feather(caminho_arquivo)
            dataframes.append(df)
            logging.info(f"Arquivo {arquivo} carregado com sucesso.")
        except Exception as e:
            logging.error(f"Erro ao carregar o arquivo {arquivo}: {e}")
            raise

    # Concatenar todos os DataFrames em um único DataFrame consolidado
    df_consolidado = pd.concat(dataframes, ignore_index=True)
    logging.info("Consolidação dos arquivos Feather concluída.")
    
    return df_consolidado

def salvar_dataframe(df, output_directory):
    """
    Salva o DataFrame consolidado em arquivos Feather e CSV.

    Parâmetros:
    - df (pd.DataFrame): O DataFrame consolidado.
    - output_directory (str): O caminho do diretório onde salvar os arquivos.
    """
    nome_arquivo = "GL_Extract_Consolidado"
    
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
    
    diretorio_selecionado = selecionar_diretorio()
    
    if diretorio_selecionado:
        try:
            df_consolidado = consolidar_arquivos_feather(diretorio_selecionado)
            
            salvar_dataframe(df_consolidado, diretorio_selecionado)
        except Exception as e:
            logging.error(f"Erro ao consolidar ou processar os arquivos: {e}")
    else:
        logging.warning("Nenhum diretório selecionado.")

if __name__ == "__main__":
    main()
