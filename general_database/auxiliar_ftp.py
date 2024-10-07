import pandas as pd
import os
import logging

def configurar_logging(log_file="transformacao_ftp_historico.log"):
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

def transformar_ftp_historico(diretorio, file_name):
    """
    Realiza as transformações necessárias no DataFrame da planilha FTP Completo.
    
    Parâmetros:
    - diretorio (str): Caminho do diretório onde o arquivo Excel está localizado.
    - file_name (str): Nome do arquivo Excel a ser carregado.
    
    Retorna:
    - pd.DataFrame: DataFrame transformado.
    """
    try:
        caminho_arquivo = os.path.join(diretorio, file_name)
        logging.info(f"Carregando o arquivo: {caminho_arquivo}")
        
        # Carregar a planilha 'FTP Completo' do arquivo Excel
        ftp_historico = pd.read_excel(caminho_arquivo, sheet_name='FTP Completo', engine='openpyxl')
        logging.info(f"Arquivo carregado com sucesso: {file_name}")

        # Criando pivot table (unpivot)
        df_unpivot = pd.melt(ftp_historico, id_vars=['FTP'], var_name='Intervalo', value_name='Valor')
      
        df_unpivot['ID_FTP'] = df_unpivot['FTP'].astype(str).str.cat(df_unpivot['Intervalo'].astype(str), sep='_')
        
        return df_unpivot
    
    except FileNotFoundError as fnf_error:
        logging.error(f"Arquivo não encontrado: {fnf_error}")
        raise
    except Exception as e:
        logging.exception(f"Erro inesperado ao carregar o arquivo {file_name}: {e}")
        raise

def transformar_swap_historico(diretorio, file_name):
    """
    Realiza as transformações necessárias no DataFrame da planilha SWAP Completo.
    
    Parâmetros:
    - diretorio (str): Caminho do diretório onde o arquivo Excel está localizado.
    - file_name (str): Nome do arquivo Excel a ser carregado.
    
    Retorna:
    - pd.DataFrame: DataFrame transformado.
    """
    try:
        caminho_arquivo = os.path.join(diretorio, file_name)
        logging.info(f"Carregando o arquivo: {caminho_arquivo}")
        
        # Carregar a planilha 'SWAP Completo' do arquivo Excel
        swap_historico = pd.read_excel(caminho_arquivo, sheet_name='SWAP Completo', engine='openpyxl')
        logging.info(f"Arquivo carregado com sucesso: {file_name}")

        # Criando pivot table (unpivot)
        df_unpivot = pd.melt(swap_historico, id_vars=['SWAP'], var_name='Intervalo', value_name='Valor')
      
        df_unpivot['ID_SWAP'] = df_unpivot['SWAP'].astype(str).str.cat(df_unpivot['Intervalo'].astype(str), sep='_')
        
        return df_unpivot
    
    except FileNotFoundError as fnf_error:
        logging.error(f"Arquivo não encontrado: {fnf_error}")
        raise
    except Exception as e:
        logging.exception(f"Erro inesperado ao carregar o arquivo {file_name}: {e}")
        raise

def main():
    """
    Função principal para carregar e transformar o FTP e SWAP Tesouraria.
    """
    configurar_logging()
    
    diretorio_input = r'C:\Users\1502553\CTT - Correios de Portugal\Planeamento e Controlo - PCG_MIS\20. Project\Analytics\07. PBI Crédito Hipotecário\Projeto Crédito Hipotecário\00. Fontes de Dados\04. General Database'
    diretorio_output = r'C:\Users\1502553\CTT - Correios de Portugal\Planeamento e Controlo - PCG_MIS\20. Project\Analytics\07. PBI Crédito Hipotecário\Projeto Crédito Hipotecário\02. Dados Processados\04. General Database'
    
    file_name = 'FTP_SWAP Tesouraria.xlsx'
    
    try:
        # Carregar e aplicar transformações no FTP Histórico
        ftp_historico_df = transformar_ftp_historico(diretorio_input, file_name)
        
        # Verificar se o DataFrame foi carregado com sucesso
        if ftp_historico_df is not None:
            output_file = os.path.join(diretorio_output, 'FTP_Histórico_transformado.csv')
            ftp_historico_df.to_csv(output_file, index=False)
            logging.info(f"Arquivo CSV salvo com sucesso em: {output_file}")
        
        # Carregar e aplicar transformações no SWAP Histórico
        swap_historico_df = transformar_swap_historico(diretorio_input, file_name)
        
        # Verificar se o DataFrame foi carregado com sucesso
        if swap_historico_df is not None:
            output_file = os.path.join(diretorio_output, 'SWAP_Histórico_transformado.csv')
            swap_historico_df.to_csv(output_file, index=False)
            logging.info(f"Arquivo CSV salvo com sucesso em: {output_file}")
        
        logging.info("Transformação do FTP e SWAP Histórico concluída com sucesso.")
        return ftp_historico_df, swap_historico_df
    
    except Exception as e:
        logging.error(f"Erro ao processar os arquivos: {e}")
        return None

if __name__ == "__main__":
    main()
