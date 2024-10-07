import os
import pandas as pd
import logging

def configurar_logging(log_file="processamento_contabilidade.log"):
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

def transform_carteira(carteira):
    """
    Aplica transformações no DataFrame da carteira de crédito.

    Parâmetros:
    - carteira (pd.DataFrame): DataFrame original da carteira.

    Retorna:
    - pd.DataFrame: DataFrame transformado.
    """
    try:
        logging.info("Iniciando as transformações na carteira.")
        
        # Renomeia colunas específicas
        carteira = carteira.rename(columns={
            'AMT Juro Corrido\nOn-BS': 'AMT Juro Corrido On-BS', 
            'AMT Juro Vencido\nOn-BS': 'AMT Juro Vencido On-BS', 
            'AMT Juro Vencido\nOff-BS': 'AMT Juro Vencido Off-BS'
        })

        # Conversão de colunas para tipos de dados apropriados
        carteira['N. Proposta'] = carteira['N. Proposta'].astype(int)
        carteira['AMT Empréstimo Original'] = pd.to_numeric(carteira['AMT Empréstimo Original'])
        carteira['AMT Capital Utilizado'] = pd.to_numeric(carteira['AMT Capital Utilizado'])
        carteira['AMT Capital em Dívida'] = pd.to_numeric(carteira['AMT Capital em Dívida'])
        carteira['AMT Capital Vincendo'] = pd.to_numeric(carteira['AMT Capital Vincendo'])
        carteira['AMT Capital Vencido'] = pd.to_numeric(carteira['AMT Capital Vencido'])
        carteira['AMT Juro Corrido On-BS'] = pd.to_numeric(carteira['AMT Juro Corrido On-BS'])
        carteira['AMT Juro Vencido On-BS'] = pd.to_numeric(carteira['AMT Juro Vencido On-BS'])
        carteira['B/S Adj D Cred C P&L'] = pd.to_numeric(carteira['B/S Adj D Cred C P&L'])
        carteira['P&L - NIM'] = pd.to_numeric(carteira['P&L - NIM'])

        # Ordenação por contrato e data
        carteira = carteira.sort_values(by=['N. Contrato', 'Data'])

        # Cálculo de diferenças entre meses anteriores
        carteira['AMT Juro Corrido On-BS'] = carteira['AMT Juro Corrido On-BS'].fillna(0)
        carteira['AMT Juro Corrido On-BS_M-1'] = carteira.groupby('N. Contrato')['AMT Juro Corrido On-BS'].shift().fillna(0)
        carteira['AMT Juro Corrido Dif'] = carteira['AMT Juro Corrido On-BS_M-1'] - carteira['AMT Juro Corrido On-BS']  

        carteira['AMT Juro Vencido On-BS'] = carteira['AMT Juro Vencido On-BS'].fillna(0)
        carteira['AMT Juro Vencido On-BS_M-1'] = carteira.groupby('N. Contrato')['AMT Juro Vencido On-BS'].shift().fillna(0)
        carteira['AMT Juro Vencido Dif'] =  carteira['AMT Juro Vencido On-BS_M-1'] - carteira['AMT Juro Vencido On-BS']

        carteira['B/S Adj D Cred C P&L'] = carteira['B/S Adj D Cred C P&L'].fillna(0)
        carteira['B/S Adj D Cred C P&L_M-1'] = carteira.groupby('N. Contrato')['B/S Adj D Cred C P&L'].shift().fillna(0)
        carteira['B/S Adj D Cred C P&L_Dif'] = carteira['B/S Adj D Cred C P&L_M-1'] - carteira['B/S Adj D Cred C P&L']

        carteira['P&L - NIM'] = carteira['P&L - NIM'].fillna(0)
        carteira['P&L - NIM_M-1'] = carteira.groupby('N. Contrato')['P&L - NIM'].shift().fillna(0)
        carteira['P&L - NIM_Dif'] =  carteira['P&L - NIM_M-1'] - carteira['P&L - NIM']

        carteira['B/S Adj D P&L / C B/S'] = carteira['B/S Adj D P&L / C B/S'].fillna(0)
        carteira['B/S Adj D P&L / C B/S_M-1'] = carteira.groupby('N. Contrato')['B/S Adj D P&L / C B/S'].shift().fillna(0)
        carteira['B/S Adj D P&L / C B/S_Dif'] = carteira['B/S Adj D P&L / C B/S'] - carteira['B/S Adj D P&L / C B/S_M-1']

        # Seleciona e organiza as colunas de interesse
        carteira_df = carteira[['N. Proposta', 'N. Contrato', 'Data', 'Branch ID', 'Conta Cliente', 
                                'DT Escritura', 'AMT Empréstimo Original', 'AMT Capital Utilizado', 'AMT Capital em Dívida',
                                'AMT Capital Vincendo', 'AMT Capital Vencido', 'AMT Juro Corrido', 'AMT Juro Vencido', 
                                'AMT Juro Mora Vencido', 'AMT Liquidação Antec Acum', 'AMT Avaliação Colateral', 'AMT Escriturado', 
                                'AMT Juro Corrido On-BS', 'AMT Juro Vencido On-BS', 'AMT Juro Vencido Off-BS', 
                                'AMT Juro Corrido On-BS_M-1', 'AMT Juro Corrido Dif', 'B/S Adj D Cred C P&L', 
                                'B/S Adj D Cred C P&L_M-1', 'P&L - NIM', 'P&L - NIM_M-1', 'AMT Juro Vencido Dif', 
                                'B/S Adj D Cred C P&L_Dif', 'P&L - NIM_Dif', 'B/S Adj D P&L / C B/S', 
                                'B/S Adj D P&L / C B/S_M-1', 'B/S Adj D P&L / C B/S_Dif']]
        
        logging.info("Transformação da carteira concluída com sucesso.")
        return carteira_df
    except KeyError as ke:
        logging.error(f"Erro ao transformar DataFrame: Coluna não encontrada - {ke}")
        raise
    except Exception as e:
        logging.exception(f"Erro inesperado ao transformar DataFrame: {e}")
        raise

def main():
    # Configurar o logging
    configurar_logging()
    
    # Diretório de entrada e saída
    diretorio_carteira = r'C:\Users\1502553\CTT - Correios de Portugal\Planeamento e Controlo - PCG_MIS\20. Project\Analytics\07. PBI Crédito Hipotecário\Projeto Crédito Hipotecário\01. Dados Consolidados\02. Contabilidade'
    output_dir = r'C:\Users\1502553\CTT - Correios de Portugal\Planeamento e Controlo - PCG_MIS\20. Project\Analytics\07. PBI Crédito Hipotecário\Projeto Crédito Hipotecário\02. Dados Processados\02. Contabilidade'
    os.makedirs(output_dir, exist_ok=True)

    try:
        # Processamento da Carteira
        carteira_path = os.path.join(diretorio_carteira, 'Contabilidade.csv')
        if os.path.exists(carteira_path):
            carteira = pd.read_csv(carteira_path)
            logging.info(f"Arquivo Carteira carregado com sucesso: {carteira_path}")
            
            # Aplicar as transformações na carteira
            carteira_df = transform_carteira(carteira)
            
            # Salvar o arquivo transformado
            output_carteira = os.path.join(output_dir, 'Carteira_Contabilidade_Transformado.csv')
            carteira_df.to_csv(output_carteira, index=False, encoding='UTF-8-sig')
            logging.info(f"Arquivo transformado e salvo em: {output_carteira}")
        else:
            logging.error(f"Arquivo Carteira não encontrado: {carteira_path}")
    except Exception as e:
        logging.exception(f"Erro inesperado no processo: {e}")

if __name__ == "__main__":
    main()
