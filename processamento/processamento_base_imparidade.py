import os
import pandas as pd
import tkinter as tk
from tkinter import filedialog
import logging

def configurar_logging():
    """Configura o sistema de logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("processamento_imparidade.log", mode="w"),
            logging.StreamHandler()
        ]
    )

def selecionar_arquivo():
    """Abre uma caixa de diálogo para selecionar um arquivo e retorna o caminho do arquivo."""
    root = tk.Tk()
    root.withdraw()  # Oculta a janela principal do Tkinter
    arquivo_selecionado = filedialog.askopenfilename(
        title="Selecione o arquivo consolidado",
        filetypes=[("Feather files", "*.feather"), ("CSV files", "*.csv")]
    )
    
    if arquivo_selecionado:
        logging.info(f"Arquivo selecionado: {arquivo_selecionado}")
    else:
        logging.warning("Nenhum arquivo foi selecionado.")
    
    return arquivo_selecionado

def transformar_dataframe(df):
    """
    Aplica transformações no DataFrame consolidado.
    
    Parâmetros:
    - df (DataFrame): O DataFrame consolidado.
    
    Retorna:
    - DataFrame transformado.
    """
    try:
        logging.info("Iniciando as transformações no DataFrame.")
        
        # Formatar a coluna 'DATA' e remover duplicatas
        df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce').dt.strftime('%d/%m/%Y')
        df = df.drop_duplicates()
        
        logging.info("Transformações aplicadas com sucesso.")
        return df
    except KeyError as e:
        logging.error(f"Erro na transformação: Coluna não encontrada - {e}")
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
        nome_arquivo = "Consolidado_Imparidade_Credito"
        
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
        
        # Aplicar as transformações desejadas
        try:
            df_transformado = transformar_dataframe(df)
        except Exception as e:
            logging.error(f"Erro durante a transformação: {e}")
            return
        
        # Definir manualmente o diretório de saída
        output_directory = "C:/Users/1502553/CTT - Correios de Portugal/Planeamento e Controlo - PCG_MIS/20. Project/Analytics/07. PBI Crédito Hipotecário/Projeto Crédito Hipotecário/02. Dados Processados/05. Imparidade"
        
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
