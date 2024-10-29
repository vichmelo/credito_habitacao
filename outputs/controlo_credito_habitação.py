import pandas as pd
import numpy as np
from pathlib import Path
import sys
import os

# Adicione aqui o caminho correto para a importação
app_path = Path(__file__).parents[1]
sys.path.append(str(app_path))

from general_database.general_database import id_tipo_taxa_juro, id_fin_cred, id_fin_prod, protocolo
from general_database.taxa_swap import taxa_swap

def transform_controlo_ch(base_informatica_df: pd.DataFrame, base_contabilidade_df: pd.DataFrame, gl_extract_df: pd.DataFrame, base_imparidade: pd.DataFrame, ftp_historico: pd.DataFrame, swap_historico: pd.DataFrame):
    """
    Função que realiza o cruzamento dos dados.
    
    Parâmetros:
    base_informatica_df, base_contabilidade_df, gl_extract_df: DataFrames do pandas contendo os dados a serem cruzados.
    
    Retorna:
    Um DataFrame resultante do cruzamento das três fontes de dados.
    """
    # Substitui '-' por nada, efetivamente removendo esse caractere
    gl_extract_df['IDCONTRATOCH'] = gl_extract_df['IDCONTRATOCH'].astype(str)
    gl_extract_df['IDCONTRATOCH'] = gl_extract_df['IDCONTRATOCH'].str.replace('-', '0')

    # Opcional: lidar com valores NaN, por exemplo, preenchendo com um valor padrão ou removendo
    gl_extract_df['IDCONTRATOCH'] = gl_extract_df['IDCONTRATOCH'].astype('Int64')

    merged_ctb = pd.merge(base_informatica_df, base_contabilidade_df, left_on=['IDCONTRATOCH', 'DATA'], right_on=['N. Contrato', 'Data'], how='outer', indicator=False)
    merged_ctb = merged_ctb.drop(columns={'N. Contrato', 'Data'})

    # Converter a coluna 'DATA' para o tipo datetime
    merged_ctb['DATA'] = pd.to_datetime(merged_ctb['DATA'], format='%Y-%m-%d')
    merged_ctb['DATAFORMALIZACAO'] = pd.to_datetime(merged_ctb['DATAFORMALIZACAO'])
    merged_ctb['DATAULTIMAREVISAO'] = pd.to_datetime(merged_ctb['DATAULTIMAREVISAO'])
    
    # Criar a nova coluna 'DATAMESANTERIOR', ajustando diretamente para o último dia do mês anterior
    merged_ctb['DATAMESANTERIORSOURCE'] = merged_ctb['DATA'] - pd.offsets.MonthEnd(1)
    merged_ctb['DATAMESANTERIORFORMALIZAÇÃO'] = merged_ctb['DATAFORMALIZACAO'] - pd.offsets.MonthEnd(1)
    merged_ctb['DATAREVISÃO'] = merged_ctb['DATAULTIMAREVISAO'] + pd.offsets.MonthEnd(0) 
    merged_ctb['DATAFTPSWAP'] = merged_ctb['DATAREVISÃO'].fillna(merged_ctb['DATAMESANTERIORFORMALIZAÇÃO'])
    
    # Limitar essa condição a taxa variavel, vamos buscar a primeira posição. 
    merged_ctb['DATAMESANTERIORFINAL'] = np.where(merged_ctb['TAXA'] == 'Variável', merged_ctb['DATAMESANTERIORFORMALIZAÇÃO'], merged_ctb['DATAFTPSWAP'])

    # Pros mistos ou fixos buscar a última data 
    merged_ctb['DATA'] = pd.to_datetime(merged_ctb['DATA'])
    merged_ctb['DATA'] = merged_ctb['DATA'].dt.strftime('%d/%m/%Y')    

    # Fill NaN values with an empty string, then convert to string and concatenate
    merged_ctb['ID_FTP'] = merged_ctb['DATAMESANTERIORFINAL'].astype(str).str.cat(
        merged_ctb['MATURIDADE BUCKETS_x'].astype(str), sep='_'
    )

    merged_ctb['DURATION'] = merged_ctb['DURATION'].fillna(0)

    merged_ctb['DURATION'] = merged_ctb['DURATION'].astype(int)

    merged_ctb['ID_SWAP'] = (
        merged_ctb['DATAFTPSWAP'].fillna('').astype(str) + '_' +
        merged_ctb['MATURIDADE BUCKETS_x'].fillna('').astype(str)
    )
    
    gl_extract_df = gl_extract_df.rename(columns={'IDCONTRATOCH':'IDCONTRATOCH_GL', 'DATA':'DATA_GL'})
        
    merged_gl = pd.merge(merged_ctb, gl_extract_df, left_on=['IDCONTRATOCH','DATA'], right_on=['IDCONTRATOCH_GL','DATA_GL'], how='outer', indicator=False)

    merged_gl['DATA'] = merged_gl['DATA'].fillna(merged_gl['DATA_GL'])

    # Criando a coluna 'ANGARIADOR' baseada nas condições
    # A quantidade total de contratos bate, porém a abertura entre 'Rede Lojas' e 'Imobiliárias' não bate devido a falta de histórico
    merged_gl['ANGARIADOR'] = np.where((merged_gl['AMT Com.Dif #3400001600'] > 0) | 
                                        (merged_gl['6701001087_GL'] < 0), 
                                        'Imobiliária', 'Loja')
    
    merged_gl['ANGARIADOR'] = np.where(merged_gl['IDCONTRATOCH'] == 0,
                                        None, 
                                        merged_gl['ANGARIADOR'])

    # Função personalizada para determinar o valor final de 'ANGARIADOR' para cada contrato
    def determinar_angariador(grupo):
        if 'Imobiliária' in grupo['ANGARIADOR'].values:
            return 'Imobiliária'
        else:
            return 'Loja'

    # Agrupando por 'IDCONTRATOCH' e aplicando a função personalizada
    angariador_df = merged_gl.groupby('IDCONTRATOCH').apply(determinar_angariador).reset_index()

    # Renomeando as colunas para o resultado final
    angariador_df.columns = ['IDCONTRATOCH', 'ANGARIADOR']

    merged_ang = pd.merge(merged_gl, angariador_df, on='IDCONTRATOCH', how='left', indicator=False)

    merged_ang['Empresa'] = np.where(((merged_ang['IDTIPOTAXAJURO'].isin([3, 4, 5])) & 
                                    (merged_ang['SPREADATUAL'] <= 0.05) & pd.isna(merged_ang['Empresa'])), 
                                'Protocolo CH', 
                                merged_ang['Empresa'])
    
    # Lista das colunas que você deseja somar
    colunas_para_somar_balanço = [
        'AMT Capital em Dívida', 'AMT Juro Corrido On-BS', 'AMT Juro Vencido On-BS',
        'AMT Com.Dif #5301001100', 'AMT Com.Dif #5301001000', 'AMT Com.Dif #5301001200', 
        'AMT Com.Dif #5301001300', 'AMT Com.Dif #5301001400', 'AMT Com.Dif #3400001000', 
        'AMT Com.Dif #3400001100', 'AMT Com.Dif #3400001200', 'AMT Com.Dif #3400001300', 
        'AMT Com.Dif #3400001400', 'AMT Com.Dif #3400001500', 'AMT Com.Dif #3400001600', 
        'AMT Com.Dif #3400001700', 'AMT Com.Dif #3400001900', 'AMT Com.Dif #3400002000', 
        'AMT Com.Dif #3400002100', 'AMT Com.Dif #3400001800', 'AMT Com.Dif #3400004000', 
    ]

    # Somando as colunas
    merged_ang['Balanço Bruto'] = merged_ang[colunas_para_somar_balanço].sum(axis=1)

    merged_ang['7904001080_GL'] = merged_ang['7904001080_GL'].fillna(0)
    merged_ang['7904001081_GL'] = merged_ang['7904001081_GL'].fillna(0)

    merged_ang['8001001082_GL'] = merged_ang['8001001082_GL'].fillna(0) * -1 
    merged_ang['8001001081_GL'] = merged_ang['8001001081_GL'].fillna(0) * -1 
    merged_ang['8001001083_GL'] = merged_ang['8001001083_GL'].fillna(0) * -1 
    merged_ang['8001001084_GL'] = merged_ang['8001001084_GL'].fillna(0) * -1 
    merged_ang['8001001085_GL'] = merged_ang['8001001085_GL'].fillna(0) * -1 
    merged_ang['6701001081_GL'] = merged_ang['6701001081_GL'].fillna(0) * -1  
    merged_ang['6701001082_GL'] = merged_ang['6701001082_GL'].fillna(0) * -1  
    merged_ang['6701001083_GL'] = merged_ang['6701001083_GL'].fillna(0) * -1  
    merged_ang['6701001084_GL'] = merged_ang['6701001084_GL'].fillna(0) * -1  
    merged_ang['6701001085_GL'] = merged_ang['6701001085_GL'].fillna(0) * -1  
    merged_ang['6701001086_GL'] = merged_ang['6701001086_GL'].fillna(0) * -1  
    merged_ang['6701001087_GL'] = merged_ang['6701001087_GL'].fillna(0) * -1  
    merged_ang['6701001090_GL'] = merged_ang['6701001090_GL'].fillna(0) * -1 
    merged_ang['6701001091_GL'] = merged_ang['6701001091_GL'].fillna(0) * -1 
    merged_ang['6701001089_GL'] = merged_ang['6701001089_GL'].fillna(0) * -1 
    merged_ang['6701001092_GL'] = merged_ang['6701001092_GL'].fillna(0) * -1 
    merged_ang['8138831000_GL'] = merged_ang['8138831000_GL'].fillna(0) * -1 
    
    merged_ang['8138843000_GL'] = merged_ang['8138843000_GL'].fillna(0) * -1

    merged_ang['JE - Juro'] = merged_ang['7904001080_GL'] + merged_ang['7904001081_GL'] + merged_ang['AMT Juro Corrido Dif'] + merged_ang['AMT Juro Vencido Dif'] + merged_ang['B/S Adj D Cred C P&L_Dif'] + merged_ang['P&L - NIM_Dif'] 

    merged_ang['JE - Juro'] = merged_ang['JE - Juro'] * -1 

    # Lista das colunas que você deseja somar
    colunas_para_somar_juro_efetivo = [
        'JE - Juro', '8001001082_GL', '8001001081_GL', '8001001083_GL', '8001001084_GL',
        '8001001085_GL', '6701001081_GL', '6701001082_GL', '6701001083_GL', '6701001084_GL', 
        '6701001085_GL', '6701001086_GL', '6701001087_GL', '6701001090_GL', '6701001091_GL',
        '6701001089_GL', '6701001092_GL', 'B/S Adj D P&L / C B/S_Dif',
    ]

    # Somando as colunas
    merged_ang['Juro Efetivo'] = merged_ang[colunas_para_somar_juro_efetivo].sum(axis=1)

    merged_ang['NEW_DESCTAXA'] = np.where(pd.isna(merged_ang['Empresa']), merged_ang['DESC_TAXA'], merged_ang['DESC_TAXA'] + '_' + 'protocolo')

    merged_imp = pd.merge(merged_ang, base_imparidade, left_on=['IDCONTRATOCH', 'DATA'], right_on=['CREDIT_FACILITY_ID_CODE', 'DATA'], how='left', indicator=False)

    merged_imp['Dos quais:Imparidade On-balance'] = merged_imp['Dos quais:Imparidade On-balance'].fillna(0)

    merged_imp['Dos quais:Imparidade On-balance'] = merged_imp['Dos quais:Imparidade On-balance'] * -1

    merged_imp['Balanço Líquido'] = merged_imp['Balanço Bruto'] + merged_imp['Dos quais:Imparidade On-balance']

    merged_imp['IDCONTRATOCH'] = np.where(merged_imp['IDCONTRATOCH'] == 0, None, merged_imp['IDCONTRATOCH'])

    # Ensure commas are replaced by periods before numeric conversion
    merged_imp['MONTANTECAPITALUTILIZADO'] = merged_imp['MONTANTECAPITALUTILIZADO'].str.replace(',', '.')

    # Convert to numeric and coerce any remaining issues to NaN
    merged_imp['MONTANTECAPITALUTILIZADO'] = pd.to_numeric(merged_imp['MONTANTECAPITALUTILIZADO'], errors='coerce')

    merged_imp['LTV Exposição'] = merged_imp['AMT Juro Corrido On-BS'] + merged_imp['AMT Juro Vencido On-BS'] + merged_imp['AMT Capital em Dívida']

    merged_imp['LTV'] = (merged_imp['LTV Exposição'] / merged_imp['VALORAVALIACAO']).fillna(0)

    # Identificar onde há uma alteração de taxa "SIM"
    merged_imp['flag_alteracao'] = merged_imp['ALTERACAO_TAXA'] == 'SIM'

    # Criar uma coluna cumulativa dentro de cada grupo de contrato
    merged_imp['DURACAO_NEW_TAXA'] = (
        merged_imp.groupby('IDCONTRATOCH')['flag_alteracao']
        .cumsum()
        .where(merged_imp['flag_alteracao'].cumsum() > 0, 0)  # Manter 0 até encontrar o "SIM"
        .astype(int)  # Converter para inteiros
    )

    merged_ftp = pd.merge(merged_imp, ftp_historico, on='ID_FTP', how='left', indicator=False)

    merged_swap = pd.merge(merged_ftp, swap_historico, on='ID_SWAP', how='left', indicator=False)

    merged_swap['EURIBOR'] = merged_swap['EURIBOR'] / 100

    merged_swap['SPREADATUAL'] = merged_swap['SPREADATUAL'] / 100

    merged_swap['TANATUAL'] = merged_swap['TANATUAL'] / 100



    # criando o df tam stock para mitigar os TAN que são iguais a 0, para esses casos buscaremos a última TAN registrada.
    tan_stock = merged_swap[['IDCONTRATOCH', 'TANATUAL', 'DATA']]
    tan_stock = tan_stock[tan_stock['TANATUAL'] != 0]
    tan_stock = tan_stock.rename(columns={'TANATUAL': 'TANSTOCK'})

    # Converte a coluna DATA para o tipo datetime para que possamos identificar a data mais recente
    tan_stock['DATA'] = pd.to_datetime(tan_stock['DATA'], format='%d/%m/%Y')

    # Agrupa por IDCONTRATOCH e pega a linha com a maior DATA em cada grupo
    tan_stock = tan_stock.loc[tan_stock.groupby('IDCONTRATOCH')['DATA'].idxmax()]

    # Ordena o resultado por IDCONTRATOCH, se necessário
    tan_stock = tan_stock.sort_values(by='IDCONTRATOCH').reset_index(drop=True)

    tan_stock = tan_stock[['IDCONTRATOCH', 'TANSTOCK']]

    merged_tan = pd.merge(merged_swap, tan_stock, how='left', on=['IDCONTRATOCH'], indicator=False) 

    merged_tan['TANSTOCKFTP'] = np.where(merged_tan['TANATUAL'] == 0, merged_tan['TANSTOCK'], merged_tan['TANATUAL'])
    merged_tan['TANSTOCKFTP'] = merged_tan['TANSTOCKFTP'].fillna(0)

    # Define the conditions
    conditions = [
        merged_tan['TAXA'] == 'Variável',
        merged_tan['NEW_DESCTAXA'] == 'Mista_EURIBOR 6M 70% - FIXA_48'
    ]

    # Define the choices based on each condition
    choices = [
        merged_tan['SPREADATUAL'] - merged_tan['Valor_x'],
        merged_tan['SPREADATUAL'] - merged_tan['Valor_x']
    ]

    # Define the default value if none of the conditions are met
    default_value = merged_tan['TANATUAL'] - merged_tan['Valor_y'] - (merged_tan['Valor_x'] + merged_tan['SPREADATUAL'])

    # Apply np.select
    merged_tan['Spread Comercial'] = np.select(conditions, choices, default=default_value).round(40)

    merged_tan['Spread Gestão'] = np.where(merged_tan['NEW_DESCTAXA'] == 'Mista_EURIBOR 6M 70% - FIXA_48', merged_tan['Spread Comercial'] + merged_tan['Valor_x'], merged_tan['SPREADATUAL'])


    # Define the conditions
    conditions = [
        merged_tan['TAXA'] == 'Variável',
        merged_tan['NEW_DESCTAXA'] == 'Mista_EURIBOR 6M 70% - FIXA_48'
    ]

    # Define the choices for each condition
    choices = [
        merged_tan['EURIBOR'],
        merged_tan['TANATUAL'] - merged_tan['Spread Gestão']
    ]

    # Define the default value if none of the conditions are met
    default_value = merged_tan['Valor_y']

    # Apply np.select
    merged_tan['Indexante'] = np.select(conditions, choices, default=default_value)
    merged_tan['Indexante'] = merged_tan['Indexante'].round(40)

    final_df = merged_tan.sort_values('IDCONTRATOCH')

    final_df = final_df.drop(columns={'DT_INFORMATION_y', 'N. Proposta_y', 'DURACAOTXFIXA_y', 'MATURIDADE BUCKETS_y'})

    final_df = final_df.rename(columns={'DT_INFORMATION_x':'DT_INFORMATION', 'N. Proposta_x':'N. Proposta', 'Dos quais:Imparidade On-balance': 'Imparidade On-balance', 'ANGARIADOR_x':'ANGARIADOR', 'Valor_y':'Taxa SWAP', 'Valor_x':'Taxa FTP'})
        
    final_df['Taxa FTP'] = final_df['Taxa FTP'].round(40)

    final_df['DATAMESANTERIORFINAL'] = pd.to_datetime(final_df['DATAMESANTERIORFINAL'])

    final_df['Unique ID SWAP'] = (
    final_df['DATAMESANTERIORFINAL'].astype(str) + 
    final_df['MATURIDADE BUCKETS_x'].astype(str) + 
    final_df['DURACAOTXFIXA_x'].astype(str) +
    final_df['DESC_TAXA'].astype(str) + 
    final_df['TANATUAL'].astype(str)
)
    
    final_df['Unique ID SWAP'] = final_df['Unique ID SWAP'].str.replace('NaTnannannannan', '') 

    final_df_swap = pd.merge(final_df, taxa_swap, how='left', on='Unique ID SWAP', indicator=True)

    final_df_swap['Teste Adqueação FTP'] = final_df_swap['TANATUAL_x'] - final_df_swap['Indexante'] - final_df_swap['Spread Comercial'] - final_df_swap['Taxa FTP']
    final_df_swap['Teste Adqueação FTP'] = final_df_swap['Teste Adqueação FTP'].round(15)

    final_df_swap['ContemJuro'] = np.where(final_df_swap['Juro Efetivo'] == 0, 'Não', 'Sim')

    final_df_swap['Teste Adqueação FTP'] = np.where(final_df_swap['ContemJuro'] == 'Não', 0, final_df_swap['Teste Adqueação FTP'])
    final_df_swap['Teste Adqueação FTP'] = final_df_swap['Teste Adqueação FTP'].round(4)

    final_df_swap['Indexante_Juro'] = final_df_swap['JE - Juro']*final_df_swap['Indexante']/final_df_swap['TANATUAL_x']
    final_df_swap['Spread_Juro'] = final_df_swap['JE - Juro']*final_df_swap['SPREADATUAL']/final_df_swap['TANATUAL_x']
    final_df_swap['Spread Comercial_Juro'] = final_df_swap['JE - Juro']*final_df_swap['Spread Comercial']/final_df_swap['TANATUAL_x']
    final_df_swap['Prémio de Liquidez_Juro'] = final_df_swap['JE - Juro']*final_df_swap['Taxa FTP']/final_df_swap['TANATUAL_x']
    final_df_swap['Margem Tesouraria_Juro'] = final_df_swap['Indexante_Juro'] + final_df_swap['Prémio de Liquidez_Juro']
    final_df_swap['Margem Comercial_Juro'] = final_df_swap['Spread Comercial_Juro']
    final_df_swap['Teste_Calculo'] = final_df_swap['Indexante_Juro'] + final_df_swap['Spread Comercial_Juro'] + final_df_swap['Prémio de Liquidez_Juro'] - final_df_swap['JE - Juro']
    final_df_swap['Teste_CalculoII'] = final_df_swap['Margem Tesouraria_Juro'] + final_df_swap['Margem Comercial_Juro'] - final_df_swap['JE - Juro']

    return final_df_swap 

def main():
    # Selecionar a pasta de processamento e a de saída
    processing_dir = Path(r'C:\Users\1502553\CTT - Correios de Portugal\Planeamento e Controlo - PCG_MIS\20. Project\Analytics\07. PBI Crédito Hipotecário\Projeto Crédito Hipotecário\02. Dados Processados')
    output_dir = Path('C:/Users/1502553/CTT - Correios de Portugal/Planeamento e Controlo - PCG_MIS/20. Project/Analytics/07. PBI Crédito Hipotecário/Projeto Crédito Hipotecário/03. Outputs')

    # Verificar se o diretório de processamento existe
    if processing_dir.exists():
        # Verificar se os arquivos de entrada existem
        base_informatica_path = processing_dir / '01. ODS/Base Informatica.csv'
        base_contabilidade_path = processing_dir / '02. Contabilidade/Carteira_Contabilidade_Transformado.csv'
        gl_extract_path = processing_dir / '03. GL Extract/GL_Extract_Processado.csv'
        base_imparidade_path = processing_dir / '05. Imparidade/Consolidado_Imparidade_Credito.csv'
        ftp_historico_path = processing_dir / '04. General Database/FTP_Histórico_transformado.csv'
        swap_historico_path = processing_dir / '04. General Database/SWAP_Histórico_transformado.csv'

        if not all([base_informatica_path.exists(), base_contabilidade_path.exists(), gl_extract_path.exists(), base_imparidade_path.exists()]):
            print("Um ou mais arquivos de entrada não foram encontrados.")
            return

        # Leitura dos arquivos CSV
        base_informatica = pd.read_csv(base_informatica_path, encoding='Latin-1')
        base_contabilidade = pd.read_csv(base_contabilidade_path, encoding='utf-8-sig')
        gl_extract = pd.read_csv(gl_extract_path, encoding='utf-8-sig')
        base_imparidade = pd.read_csv(base_imparidade_path, encoding='utf-8-sig')
        ftp_historico = pd.read_csv(ftp_historico_path, encoding='utf-8-sig')
        swap_historico = pd.read_csv(swap_historico_path, encoding='utf-8-sig')

        # Realizar a transformação e o cruzamento dos dados
        controlo_ch_df = transform_controlo_ch(base_informatica, base_contabilidade, gl_extract, base_imparidade, ftp_historico, swap_historico)

        # Verificar se o diretório de saída existe, se não, criar
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Gerar os arquivos de saída
        output_file = Path(output_dir / 'Controlo Crédito Habitação_0924_teste.csv')
        controlo_ch_df.to_csv(output_file, index=False, encoding='utf-8-sig')

        print(f"Arquivo gerado com sucesso em: {output_file}")
    else:
        print(f"O diretório {processing_dir} não existe.")

if __name__ == "__main__":
    main()
