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
            logging.FileHandler("processamento_gl_extract.log", mode="w"),
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
    Aplica transformações no DataFrame consolidado e realiza novos incrementos.
    
    Parâmetros:
    - df (DataFrame): O DataFrame consolidado.
    
    Retorna:
    - DataFrame transformado.
    """
    try:
        logging.info("Iniciando as transformações no DataFrame.")

        # Filtrando valores indesejados
        df = df[df['EVENTO'] != 'CH207']

        # Verificar se 'SAP_AMOUNT' já é numérico. Se não for, aplicar a conversão.
        if df['SAP_AMOUNT'].dtype != 'float64' and df['SAP_AMOUNT'].dtype != 'int64':
            # Convertendo a coluna 'SAP_AMOUNT' para string e substituindo vírgulas por pontos
            df['SAP_AMOUNT'] = df['SAP_AMOUNT'].astype(str).str.replace(',', '.').astype(float)
        else:
            df['SAP_AMOUNT'] = df['SAP_AMOUNT'].astype(float)

        df['SAP_AMOUNT'] = df['SAP_AMOUNT'].fillna(0) 

        # Criando pivot table
        df_pivot = df.pivot_table(
            index=['CONTRATO', 'DATA', 'PROCESSO'], 
            columns='NOMINAL_CODE', 
            values='SAP_AMOUNT', 
            aggfunc='sum'
        ).fillna(0).reset_index()

        # Resetar o índice para obter um DataFrame plano
        df_pivot = df_pivot.reset_index(drop=True)

        # Adiciona '_GL' ao final dos nomes das colunas
        df_pivot.columns = [str(col) + '_GL' for col in df_pivot.columns]

        # Ajuste nas colunas renomeadas
        df_pivot.columns = df_pivot.columns.str.replace('.0', '', regex=False)

        # Renomear colunas específicas para os nomes desejados
        df_pivot = df_pivot.rename(columns={'PROCESSO_GL': 'N. Proposta', 'CONTRATO_GL': 'IDCONTRATOCH', 'DATA_GL': 'DATA'})

        # --- Incremento: Filtrar contratos válidos (diferente de '-') ---
        df_com_contrato = df_pivot[df_pivot['IDCONTRATOCH'] != '-']

        # Definir a função de agregação para cada coluna
        df_grouped = df_com_contrato.groupby(['IDCONTRATOCH', 'DATA'], as_index=False).agg({
            'N. Proposta': 'first',   # Mantém a primeira proposta
            '6701001081_GL': 'sum',
            '6701001082_GL': 'sum',
            '6701001083_GL': 'sum',
            '6701001084_GL': 'sum',   
            '6701001085_GL': 'sum',
            '6701001086_GL': 'sum',
            '6701001087_GL': 'sum',
            '6701001089_GL': 'sum', 
            '6701001090_GL': 'sum',
            '6701001091_GL': 'sum',
            '6701001092_GL': 'sum',
            '7904001080_GL': 'sum',
            '7904001081_GL': 'sum',
            '8001001081_GL': 'sum',
            '8001001082_GL': 'sum',
            '8001001083_GL': 'sum',
            '8001001084_GL': 'sum',
            '8001001085_GL': 'sum',
            '8138831000_GL': 'sum',
            '8138830000_GL': 'sum',  
            '8138833000_GL': 'sum',
            '8138840000_GL': 'sum',
            '8138842000_GL': 'sum',
            '8138843000_GL': 'sum',
        })

        # --- Filtrar contratos sem informação (IDCONTRATOCH == '-') ---
        df_sem_contrato = df_pivot[df_pivot['IDCONTRATOCH'] == '-']

        # --- Concatenar os dataframes com e sem contrato ---
        df_final = pd.concat([df_grouped, df_sem_contrato]).drop_duplicates().reset_index(drop=True)

        logging.info("Transformações aplicadas com sucesso.")
        return df_final

    except Exception as e:
        logging.error(f"Erro durante a transformação do DataFrame: {e}")
        raise

def salvar_dataframe(df, output_directory):
    """
    Salva o DataFrame transformado em arquivos Feather e CSV.
    
    Parâmetros:
    - df (DataFrame): O DataFrame consolidado e transformado.
    - output_directory (str): O caminho do diretório onde salvar os arquivos.
    """
    try:
        nome_arquivo = "GL_Extract_Processado"
        
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
            logging.error(f"Erro durante a transformação do DataFrame: {e}")
            return
        
        # Definir manualmente o diretório de saída
        output_directory = "C:/Users/1502553/CTT - Correios de Portugal/Planeamento e Controlo - PCG_MIS/20. Project/Analytics/07. PBI Crédito Hipotecário/Projeto Crédito Hipotecário/02. Dados Processados/03. GL Extract"
        
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
