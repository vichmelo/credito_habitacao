import os
import pandas as pd
import tkinter as tk
from tkinter import filedialog
import logging

def configurar_logging(log_file="consolidacao_balancete.log"):
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
    Abre uma caixa de diálogo para selecionar uma pasta e retorna o caminho da pasta selecionada.
    """
    root = tk.Tk()
    root.withdraw()  # Oculta a janela principal do Tkinter
    pasta_selecionada = filedialog.askdirectory(title="Selecione a pasta com os arquivos Excel")
    if not pasta_selecionada:
        logging.warning("Nenhuma pasta foi selecionada.")
    return pasta_selecionada

def consolidacao_bancoctt(diretorio):
    """
    Consolida arquivos Excel em um único DataFrame, adicionando uma coluna 'data' baseada na quarta coluna,
    e renomeando a quarta coluna para 'Valor'.

    Parâmetros:
    - diretorio (str): Caminho para o diretório onde os arquivos estão localizados.

    Retorna:
    - pd.DataFrame: DataFrame consolidado com todos os dados da planilha "Balancete".
    """
    lista_dfs = []
    contador_arquivos = 0  # Contador de arquivos consolidados

    for arquivo in os.listdir(diretorio):
        if arquivo.endswith(('.xlsx', '.xls', '.xlsb')):
            caminho_arquivo = os.path.join(diretorio, arquivo)
            
            try:
                # Lendo a planilha 'Balancete' e configurando header na linha 3 (índice 2)
                df = pd.read_excel(caminho_arquivo, sheet_name="Balancete", header=2)
                
                # Identificar a quarta coluna (índice 3)
                coluna_data = df.columns[3]  # A quarta coluna é sempre a de índice 3
                
                # Criar a coluna 'data' e preencher com o nome da quarta coluna
                df['Data'] = coluna_data
                
                # Renomear a quarta coluna para 'Valor'
                df.rename(columns={coluna_data: 'Valor'}, inplace=True)
                
                # Filtrar linhas onde a coluna 'Acct Type' não é vazia (nem NaN)
                df_filtrado = df[df['Acct Type'].notna() & (df['Acct Type'] != '')]
                
                # Se houver dados após o filtro, adiciona à lista
                if not df_filtrado.empty:
                    lista_dfs.append(df_filtrado)
                    contador_arquivos += 1
                    logging.info(f"Arquivo consolidado com sucesso: {caminho_arquivo}")
                else:
                    logging.warning(f"Nenhuma linha válida em 'Acct Type' no arquivo: {caminho_arquivo}")
                    
            except Exception as e:
                logging.exception(f"Erro ao processar o arquivo {caminho_arquivo}: {e}")
    
    if lista_dfs:
        df_consolidado = pd.concat(lista_dfs, ignore_index=True)
        logging.info(f"Total de arquivos consolidados: {contador_arquivos}")
        return df_consolidado
    else:
        logging.warning(f"Nenhum arquivo foi consolidado.")
        return pd.DataFrame()

def main():
    configurar_logging()
    
    diretorio = selecionar_pasta()
    
    if not diretorio:
        logging.error("Processo encerrado: Nenhuma pasta foi selecionada.")
        return
    
    # Consolidação dos arquivos na planilha 'Balancete'
    df_consolidado = consolidacao_bancoctt(diretorio)
    
    if not df_consolidado.empty:
        output_directory = "C:/Users/1502553/CTT - Correios de Portugal/Planeamento e Controlo - PCG_MIS/20. Project/Analytics/07. PBI Crédito Hipotecário/Projeto Crédito Hipotecário/01. Dados Consolidados/06. Balancete"
        os.makedirs(output_directory, exist_ok=True)
        output_path = os.path.join(output_directory, "Balancete_consolidado.csv")
        
        try:
            df_consolidado.to_csv(output_path, index=False)
            logging.info(f"Arquivo salvo: {output_path}")
        except Exception as e:
            logging.error(f"Erro ao salvar o arquivo {output_path}: {e}")
    else:
        logging.warning("Nenhum dado foi consolidado.")

if __name__ == "__main__":
    main()
