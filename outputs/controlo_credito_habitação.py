import pandas as pd
import numpy as np
from pathlib import Path
import sys
import os

# Adicione aqui o caminho correto para a importação
app_path = Path(__file__).parents[1]
sys.path.append(str(app_path))

from general_database.general_database import id_tipo_taxa_juro, id_fin_cred, id_fin_prod, protocolo

def transform_controlo_ch(base_informatica_df: pd.DataFrame, base_contabilidade_df: pd.DataFrame, gl_extract_df: pd.DataFrame, base_imparidade: pd.DataFrame):
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

    merged_ctb['DATA'] = pd.to_datetime(merged_ctb['DATA'])
    merged_ctb['DATA'] = merged_ctb['DATA'].dt.strftime('%d/%m/%Y')    

    gl_extract_df = gl_extract_df.rename(columns={'IDCONTRATOCH':'IDCONTRATOCH_GL', 'DATA':'DATA_GL'})
        
    merged_gl = pd.merge(merged_ctb, gl_extract_df, left_on=['IDCONTRATOCH','DATA'], right_on=['IDCONTRATOCH_GL','DATA_GL'], how='left', indicator=False)

    # Criando a coluna 'ANGARIADOR' baseada nas condições
    # A quantidade total de contratos bate, porém a abertura entre 'Rede Lojas' e 'Imobiliárias' não bate devido a falta de histórico
    merged_gl['ANGARIADOR'] = np.where((merged_gl['AMT Com.Dif #3400001600'] > 0) | 
                                        (merged_gl['6701001087_GL'] > 0), 
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
                                    (merged_ang['SPREADATUAL'] == 0) & pd.isna(merged_ang['Empresa'])), 
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

    final_df = merged_imp.sort_values('IDCONTRATOCH')

    final_df = final_df.drop(columns={'DT_INFORMATION_y', 'index_GL', 'N. Proposta_y'})

    final_df = final_df.rename(columns={'DT_INFORMATION_x':'DT_INFORMATION', 'N. Proposta_x':'N. Proposta', 'Dos quais:Imparidade On-balance': 'Imparidade On-balance', 'ANGARIADOR_x':'ANGARIADOR'})
        
    return final_df

def main():
    # Selecionar a pasta de processamento e a de saída
    processing_dir = Path(r'C:\Users\1502553\CTT - Correios de Portugal\Planeamento e Controlo - PCG_MIS\20. Project\Analytics\07. PBI Crédito Hipotecário\Projeto Crédito Hipotecário\02. Dados Processados')
    output_dir = Path(r'C:\Users\1502553\CTT - Correios de Portugal\Planeamento e Controlo - PCG_MIS\20. Project\Analytics\07. PBI Crédito Hipotecário\Projeto Crédito Hipotecário\03. Outputs')

    # Verificar se o diretório de processamento existe
    if processing_dir.exists():
        # Verificar se os arquivos de entrada existem
        base_informatica_path = processing_dir / '01. ODS/Base Informatica.csv'
        base_contabilidade_path = processing_dir / '02. Contabilidade/Carteira_Contabilidade_Transformado.csv'
        gl_extract_path = processing_dir / '03. GL Extract/GL_Extract_Processado.csv'
        base_imparidade_path = processing_dir / '05. Imparidade/Consolidado_Imparidade_Credito.csv'

        if not all([base_informatica_path.exists(), base_contabilidade_path.exists(), gl_extract_path.exists(), base_imparidade_path.exists()]):
            print("Um ou mais arquivos de entrada não foram encontrados.")
            return

        # Leitura dos arquivos CSV
        base_informatica = pd.read_csv(base_informatica_path, encoding='Latin-1')
        base_contabilidade = pd.read_csv(base_contabilidade_path, encoding='utf-8-sig')
        gl_extract = pd.read_csv(gl_extract_path, encoding='utf-8-sig')
        base_imparidade = pd.read_csv(base_imparidade_path, encoding='utf-8-sig')

        # Realizar a transformação e o cruzamento dos dados
        controlo_ch_df = transform_controlo_ch(base_informatica, base_contabilidade, gl_extract, base_imparidade)

        # Verificar se o diretório de saída existe, se não, criar
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Gerar os arquivos de saída
        output_file = output_dir / 'Controlo Crédito Habitação_v4.1.csv'
        controlo_ch_df.to_csv(output_file, index=False, encoding='utf-8-sig')

        print(f"Arquivo gerado com sucesso em: {output_file}")
    else:
        print(f"O diretório {processing_dir} não existe.")

if __name__ == "__main__":
    main()
