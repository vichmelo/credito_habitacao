import os
import pandas as pd
import tkinter as tk
from tkinter import filedialog
import logging

def configurar_logging(log_file="consolidacao_balancete.log"):
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
    Abre uma janela para o usuário selecionar uma pasta e retorna o caminho selecionado.
    """
    root = tk.Tk()
    root.withdraw()  # Oculta a janela principal do Tkinter
    pasta_selecionada = filedialog.askdirectory(title="Selecione a pasta com os arquivos Excel")
    if not pasta_selecionada:
        logging.warning("Nenhuma pasta foi selecionada.")
    else:
        logging.info(f"Pasta selecionada: {pasta_selecionada}")
    return pasta_selecionada

def consolidacao_bancoctt(diretorio):
    """
    Consolida arquivos Excel da planilha 'Balancete', cria coluna 'Data' e renomeia a quarta coluna como 'Valor'.
    """
    lista_dfs = []
    contador_arquivos = 0  # Contador de arquivos consolidados

    for arquivo in os.listdir(diretorio):
        if arquivo.endswith(('.xlsx', '.xls', '.xlsb')):
            caminho_arquivo = os.path.join(diretorio, arquivo)
            
            try:
                # Lê a planilha 'Balancete' com header na linha 3
                df = pd.read_excel(caminho_arquivo, sheet_name="Balancete", header=2)
                
                # A quarta coluna é usada como 'Data' e renomeada para 'Valor'
                coluna_data = df.columns[3]
                df['Data'] = coluna_data
                df.rename(columns={coluna_data: 'Valor'}, inplace=True)
                
                # Filtra as linhas onde 'Acct Type' não é vazia
                df_filtrado = df[df['Acct Type'].notna() & (df['Acct Type'] != '')]
                
                if not df_filtrado.empty:
                    lista_dfs.append(df_filtrado)
                    contador_arquivos += 1
                    logging.info(f"Arquivo consolidado: {caminho_arquivo}")
                else:
                    logging.warning(f"Nenhuma linha válida em 'Acct Type' no arquivo: {caminho_arquivo}")
                    
            except Exception as e:
                logging.exception(f"Erro ao processar o arquivo {caminho_arquivo}: {e}")
    
    if lista_dfs:
        df_consolidado = pd.concat(lista_dfs, ignore_index=True)
        logging.info(f"Total de arquivos consolidados: {contador_arquivos}")
        return df_consolidado
    else:
        logging.warning("Nenhum arquivo foi consolidado.")
        return pd.DataFrame()

def main():
    configurar_logging()
    
    diretorio = selecionar_pasta()
    
    if not diretorio:
        logging.error("Processo encerrado: Nenhuma pasta foi selecionada.")
        return
    
    # Consolida arquivos da pasta selecionada
    df_consolidado = consolidacao_bancoctt(diretorio)
    
    if not df_consolidado.empty:
        output_directory = "C:/Users/1502553/CTT - Correios de Portugal/Planeamento e Controlo - PCG_MIS/20. Project/Analytics/07. PBI Crédito Hipotecário/Projeto Crédito Hipotecário/01. Dados Consolidados/06. Balancete"
        os.makedirs(output_directory, exist_ok=True)
        output_path = os.path.join(output_directory, "Balancete_consolidado.csv")
        
        try:
            df_consolidado.to_csv(output_path, index=False)
            logging.info(f"Arquivo salvo com sucesso: {output_path}")
        except Exception as e:
            logging.error(f"Erro ao salvar o arquivo {output_path}: {e}")
    else:
        logging.warning("Nenhum dado foi consolidado, arquivo não salvo.")

if __name__ == "__main__":
    main()
