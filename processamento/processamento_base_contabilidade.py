import pandas as pd
import os
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
        # Renomeia colunas específicas
        carteira = carteira.rename(columns={
            'AMT Juro Corrido\nOn-BS': 'AMT Juro Corrido On-BS', 
            'AMT Juro Vencido\nOn-BS': 'AMT Juro Vencido On-BS', 
            'AMT Juro Vencido\nOff-BS': 'AMT Juro Vencido Off-BS'
        })

        carteira_df = carteira.copy()

        # Conversão de colunas para tipos de dados apropriados
        carteira_df['N. Proposta'] = carteira_df['N. Proposta'].astype(int)
        carteira_df['AMT Empréstimo Original'] = pd.to_numeric(carteira_df['AMT Empréstimo Original'])
        carteira_df['AMT Capital Utilizado'] = pd.to_numeric(carteira_df['AMT Capital Utilizado'])
        carteira_df['AMT Capital em Dívida'] = pd.to_numeric(carteira_df['AMT Capital em Dívida'])
        carteira_df['AMT Capital Vincendo'] = pd.to_numeric(carteira_df['AMT Capital Vincendo'])
        carteira_df['AMT Capital Vencido'] = pd.to_numeric(carteira_df['AMT Capital Vencido'])
        carteira_df['AMT Juro Corrido On-BS'] = pd.to_numeric(carteira_df['AMT Juro Corrido On-BS'])
        carteira_df['AMT Juro Vencido On-BS'] = pd.to_numeric(carteira_df['AMT Juro Vencido On-BS'])
        carteira_df['B/S Adj D Cred C P&L'] = pd.to_numeric(carteira_df['B/S Adj D Cred C P&L'])
        carteira_df['P&L - NIM'] = pd.to_numeric(carteira_df['P&L - NIM'])

        carteira_df = carteira_df.sort_values(by=['N. Contrato', 'Data'])

        carteira_df['AMT Juro Corrido On-BS'] = carteira_df['AMT Juro Corrido On-BS'].fillna(0)
        carteira_df['AMT Juro Corrido On-BS_M-1'] = carteira_df.groupby('N. Contrato')['AMT Juro Corrido On-BS'].shift().fillna(0)
        carteira_df['AMT Juro Corrido Dif'] = carteira_df['AMT Juro Corrido On-BS_M-1'] - carteira_df['AMT Juro Corrido On-BS']  

        carteira_df['AMT Juro Vencido On-BS'] = carteira_df['AMT Juro Vencido On-BS'].fillna(0)
        carteira_df['AMT Juro Vencido On-BS_M-1'] = carteira_df.groupby('N. Contrato')['AMT Juro Vencido On-BS'].shift().fillna(0)
        carteira_df['AMT Juro Vencido Dif'] =  carteira_df['AMT Juro Vencido On-BS_M-1'] - carteira_df['AMT Juro Vencido On-BS']

        carteira_df['B/S Adj D Cred C P&L'] = carteira_df['B/S Adj D Cred C P&L'].fillna(0)
        carteira_df['B/S Adj D Cred C P&L_M-1'] = carteira_df.groupby('N. Contrato')['B/S Adj D Cred C P&L'].shift().fillna(0)
        carteira_df['B/S Adj D Cred C P&L_Dif'] = carteira_df['B/S Adj D Cred C P&L_M-1'] - carteira_df['B/S Adj D Cred C P&L']

        carteira_df['P&L - NIM'] = carteira_df['P&L - NIM'].fillna(0)
        carteira_df['P&L - NIM_M-1'] = carteira_df.groupby('N. Contrato')['P&L - NIM'].shift().fillna(0)
        carteira_df['P&L - NIM_Dif'] =  carteira_df['P&L - NIM_M-1'] - carteira_df['P&L - NIM']

        carteira_df['B/S Adj D P&L / C B/S'] = carteira_df['B/S Adj D P&L / C B/S'].fillna(0)
        carteira_df['B/S Adj D P&L / C B/S_M-1'] = carteira_df.groupby('N. Contrato')['B/S Adj D P&L / C B/S'].shift().fillna(0)
        carteira_df['B/S Adj D P&L / C B/S_Dif'] = carteira_df['B/S Adj D P&L / C B/S'] - carteira_df['B/S Adj D P&L / C B/S_M-1']
 
        carteira_df = carteira_df[['N. Proposta', 'N. Contrato', 'Data', 'Branch ID', 'Conta Cliente', 
                                   'DT Escritura' , 'AMT Empréstimo Original', 'AMT Capital Utilizado', 'AMT Capital em Dívida',
                                   'AMT Capital Vincendo', 'AMT Capital Vencido', 'AMT Juro Corrido', 'AMT Juro Vencido', 
                                   'AMT Juro Mora Vencido', 'AMT Liquidação Antec Acum', 'AMT Avaliação Colateral', 'AMT Escriturado', 
                                   'AMT Juro Corrido On-BS', 'AMT Juro Vencido On-BS', 'AMT Juro Vencido Off-BS' , 'AMT Com.Dif #3400001000',
                                   'AMT Com.Dif #3400001100', 'AMT Com.Dif #3400001200', 'AMT Com.Dif #3400001300', 'AMT Com.Dif #3400001400',
                                   'AMT Com.Dif #3400001500', 'AMT Com.Dif #3400001600', 'AMT Com.Dif #3400001700', 'AMT Com.Dif #3400001800',
                                   'AMT Com.Dif #3400001900', 'AMT Com.Dif #3400002000', 'AMT Com.Dif #3400002100', 'AMT Com.Dif #3400003000',
                                   'AMT Com.Dif #3400003001', 'AMT Com.Dif #3400004000', 'AMT Com.Dif #5301001000', 'AMT Com.Dif #5301001100',
                                   'AMT Com.Dif #5301001200', 'AMT Com.Dif #5301001300', 'AMT Com.Dif #5301001400', 'AMT Juro Corrido On-BS_M-1',
                                   'AMT Juro Corrido Dif', 'B/S Adj D Cred C P&L', 'B/S Adj D Cred C P&L_M-1', 'P&L - NIM', 'P&L - NIM_M-1',
                                   'AMT Juro Vencido Dif', 'B/S Adj D Cred C P&L_Dif', 'P&L - NIM_Dif', 'B/S Adj D P&L / C B/S', 'B/S Adj D P&L / C B/S_M-1', 
                                   'B/S Adj D P&L / C B/S_Dif']]

        
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
    
    # Diretórios de entrada separados para cada arquivo
    diretorio_carteira = r'C:\Users\1502553\CTT - Correios de Portugal\Planeamento e Controlo - PCG_MIS\20. Project\Analytics\07. PBI Crédito Hipotecário\Projeto Crédito Hipotecário\01. Dados Consolidados\02. Contabilidade'

    # Diretório de saída (o mesmo para todos os arquivos transformados)
    output_dir = r'C:\Users\1502553\CTT - Correios de Portugal\Planeamento e Controlo - PCG_MIS\20. Project\Analytics\07. PBI Crédito Hipotecário\Projeto Crédito Hipotecário\02. Dados Processados\02. Contabilidade'
    os.makedirs(output_dir, exist_ok=True)

    try:
        # Processamento da Carteira
        carteira_path = os.path.join(diretorio_carteira, 'Contabilidade.csv')
        if os.path.exists(carteira_path):
            carteira = pd.read_csv(carteira_path)
            logging.info(f"Arquivo Carteira carregado com sucesso: {carteira_path}")
            carteira_df = transform_carteira(carteira)
            output_carteira = os.path.join(output_dir, 'Carteira_Contabilidade_Transformado.csv')
            carteira_df.to_csv(output_carteira, index=False, encoding='UTF-8-sig')
            logging.info(f"Arquivo transformado e salvo em: {output_carteira}")
        else:
            logging.error(f"Arquivo Carteira não encontrado: {carteira_path}")

    except Exception as e:
        logging.exception(f"Erro inesperado no processo: {e}")

if __name__ == "__main__":
    main()
