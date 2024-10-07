import pandas as pd
import numpy as np
import chardet as ch
from datetime import datetime
from pathlib import Path
import sys
import os
import logging

# Configuração do caminho do aplicativo
app_path = Path(__file__).parents[1]
sys.path.append(str(app_path))

from general_database.general_database import id_tipo_taxa_juro, id_fin_cred, id_fin_prod, protocolo, duration

def configurar_logging(log_file="processamento_base_informatica.log"):
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

def detect_encoding(dataframe):
    """
    Detecta o encoding de um DataFrame convertido em string CSV temporária.
    """
    try:
        csv_data = dataframe.to_csv(index=False).encode('utf-8')
        result = ch.detect(csv_data)
        encoding = result['encoding']
        confidence = result['confidence']
        
        logging.info(f"Encoding detectado: {encoding} com confiança de {confidence*100:.2f}%")
        return encoding
    except Exception as e:
        logging.error("Erro ao detectar encoding.")
        raise

def transform_ods_contrato(ods_contrato, id_tipo_taxa_juro, id_fin_cred, id_fin_prod, protocolo):
    """
    Aplica transformações ao DataFrame 'ods_contrato' e realiza merges com outras tabelas.
    """
    try:
        ods_contrato_df = ods_contrato.copy()

        # Seleção e conversão de colunas
        ods_contrato_df = ods_contrato_df[[
            'DT_INFORMATION', 'IDCONTRATOCH', 'DATAFORMALIZACAO', 'DATAVENCIMENTO', 'IDTIPOTAXAJURO',
            'IDPROTOCOLO', 'DURACAOTXFIXA', 'DATAULTIMAREVISAO', 'CODIGOBALCAO', 'MONTANTEORIGINALEMPRESTIMO', 'MONTANTECAPITALUTILIZADO',
            'IDFINALIDADEPRODUTO', 'IDFINALIDADECREDITO', 'PRAZOCONTRATADO', 'TANATUAL', 'SPREADATUAL', 'CAPITALEMDIVIDA',
            'DATA'
        ]]

        # Convertemos para os tipos adequados
        ods_contrato_df['IDTIPOTAXAJURO'] = ods_contrato_df['IDTIPOTAXAJURO'].astype(int)
        ods_contrato_df['CODIGOBALCAO'] = ods_contrato_df['CODIGOBALCAO'].astype(int)
        ods_contrato_df['IDFINALIDADEPRODUTO'] = ods_contrato_df['IDFINALIDADEPRODUTO'].astype(int)
        ods_contrato_df['IDFINALIDADECREDITO'] = ods_contrato_df['IDFINALIDADECREDITO'].astype(int)
        ods_contrato_df['PRAZOCONTRATADO'] = ods_contrato_df['PRAZOCONTRATADO'].astype(int)
        ods_contrato_df['MONTANTEORIGINALEMPRESTIMO'] = pd.to_numeric(ods_contrato_df['MONTANTEORIGINALEMPRESTIMO'])
        ods_contrato_df['MONTANTECAPITALUTILIZADO'] = pd.to_numeric(ods_contrato_df['MONTANTECAPITALUTILIZADO'])
        ods_contrato_df['CAPITALEMDIVIDA'] = pd.to_numeric(ods_contrato_df['CAPITALEMDIVIDA'])

        # Adicionando novas colunas e realizando cálculos
        ods_contrato_df['PRAZOCONTRATADOANOS'] = np.floor(ods_contrato_df['PRAZOCONTRATADO'] / 12)
        ods_contrato_df = ods_contrato_df.sort_values(by=['IDCONTRATOCH', 'DATA'])
        ods_contrato_df['IDTIPOTAXAJURO_M-1'] = ods_contrato_df.groupby('IDCONTRATOCH')['IDTIPOTAXAJURO'].shift().fillna(method='bfill')
        ods_contrato_df['ALTERACAO_TAXA'] = np.where(ods_contrato_df['IDTIPOTAXAJURO_M-1'] != ods_contrato_df['IDTIPOTAXAJURO'], 'SIM', 'NÃO')

        ods_contrato_df['MONTANTEUTILIZADO_M-1'] = ods_contrato_df.groupby('IDCONTRATOCH')['MONTANTECAPITALUTILIZADO'].shift().fillna(0)
        ods_contrato_df['PRODUÇÃO'] = ods_contrato_df['MONTANTECAPITALUTILIZADO'] - ods_contrato_df['MONTANTEUTILIZADO_M-1']

        # Aplicando correções
        ods_contrato_df.loc[ods_contrato_df['IDTIPOTAXAJURO'] == 80, 'DURACAOTXFIXA'] = 30

        # Aplicando lógica condicional para buckets de maturidade
        conditions = [
            (ods_contrato_df['PRAZOCONTRATADOANOS'] <= 25),
            (ods_contrato_df['PRAZOCONTRATADOANOS'] <= 30),
            (ods_contrato_df['PRAZOCONTRATADOANOS'] <= 35),
            (ods_contrato_df['PRAZOCONTRATADOANOS'] <= 100)
        ]
        values = [25, 30, 35, 40]
        ods_contrato_df['MATURIDADE BUCKETS'] = np.select(conditions, values, default=0)

        ods_contrato_df['MATURIDADE BUCKETS'] = ods_contrato_df['MATURIDADE BUCKETS'].astype(str)
        ods_contrato_df['DURACAOTXFIXA'] = ods_contrato_df['DURACAOTXFIXA'].astype(str)

        ods_contrato_df['ID_DURATION'] = ods_contrato_df['DURACAOTXFIXA'] + ods_contrato_df['MATURIDADE BUCKETS']

        # Mesclando com outros DataFrames
        merged_df = pd.merge(ods_contrato_df, id_tipo_taxa_juro, on='IDTIPOTAXAJURO', how='left')
        merged_df = pd.merge(merged_df, id_fin_cred, on='IDFINALIDADECREDITO', how='left')
        merged_df = pd.merge(merged_df, id_fin_prod, on='IDFINALIDADEPRODUTO', how='left')
        merged_df = pd.merge(merged_df, protocolo, on='IDPROTOCOLO', how='left')
        merged_df = pd.merge(merged_df, duration, on='ID_DURATION', how='left')

        merged_df['EURIBOR'] = np.where(merged_df['TAXA'] == 'Variável', merged_df['TANATUAL'] - merged_df['SPREADATUAL'], 0)

        # Formatando os números
        cols_to_format = ['MONTANTEORIGINALEMPRESTIMO', 'MONTANTECAPITALUTILIZADO', 'MONTANTEUTILIZADO_M-1', 'PRODUÇÃO']
        for col in cols_to_format:
            merged_df[col] = merged_df[col].apply(lambda x: f"{x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', ''))

        logging.info("Transformação do DataFrame ods_contrato concluída com sucesso.")
        return merged_df
    except Exception as e:
        logging.exception("Erro ao transformar o DataFrame ods_contrato")
        raise

def transform_ods_interveniente(ods_interveniente):
    """
    Aplica transformações específicas ao DataFrame 'ods_interveniente'.
    """
    try:
        ods_interveniente_df = ods_interveniente[['DT_INFORMATION', 'IDCONTRATOCH', 'NIFINTERVENIENTE', 'IDTIPOINTERVENIENTE', 'DATA']].copy()
        ods_interveniente_df = ods_interveniente_df[ods_interveniente_df['IDTIPOINTERVENIENTE'] == 2]
        ods_interveniente_df['NIFINTERVENIENTE'] = ods_interveniente_df['NIFINTERVENIENTE'].str.replace('PT', '')
        ods_interveniente_df = ods_interveniente_df.drop_duplicates()

        logging.info("Transformação do DataFrame ods_interveniente concluída com sucesso.")
        return ods_interveniente_df
    except Exception as e:
        logging.exception("Erro ao transformar o DataFrame ods_interveniente")
        raise

def transform_ods_avaliacao(ods_avaliacao):
    """
    Aplica transformações específicas ao DataFrame 'ods_avaliacao'.
    """
    try:
        ods_avaliacao_df = ods_avaliacao[['IDCONTRATOCH', 'DT_INFORMATION', 'DTAVALIACAO', 'VALORAVALIACAO', 'DATA']].copy()
        ods_avaliacao_df['DTAVALIACAO'] = pd.to_datetime(ods_avaliacao_df['DTAVALIACAO'], errors='coerce')
        ods_avaliacao_df = ods_avaliacao_df.dropna(subset=['DTAVALIACAO'])
        ods_avaliacao_df = ods_avaliacao_df.sort_values(by=['IDCONTRATOCH', 'DATA', 'DTAVALIACAO'])

        grouped = ods_avaliacao_df.groupby(['IDCONTRATOCH', 'DATA'])
        result = grouped.apply(lambda x: x.nsmallest(1, 'DTAVALIACAO') if len(x) > 1 else x).reset_index(drop=True)
        result['DTAVALIACAO'] = result['DTAVALIACAO'].dt.strftime('%d/%m/%Y')

        logging.info("Transformação do DataFrame ods_avaliacao concluída com sucesso.")
        return result
    except Exception as e:
        logging.exception("Erro ao transformar o DataFrame ods_avaliacao")
        raise

def transform_ods_operacao(ods_operacao):
    """
    Aplica transformações específicas ao DataFrame 'ods_operacao'.
    """
    try:
        ods_operacao_df = ods_operacao[['IDCONTRATOCH', 'MNTOPERACAO', 'DATA']].copy()
        ods_operacao_df['MNTOPERACAO'] = pd.to_numeric(ods_operacao_df['MNTOPERACAO'])
        grouped = ods_operacao_df.groupby(['IDCONTRATOCH', 'DATA']).agg({'MNTOPERACAO': 'sum'}).reset_index()

        logging.info("Transformação do DataFrame ods_operacao concluída com sucesso.")
        return grouped
    except Exception as e:
        logging.exception("Erro ao transformar o DataFrame ods_operacao")
        raise

def merged_base_informatica(ods_contrato_df, ods_interveniente_df, ods_avaliacao_df, ods_operacao_df):
    """
    Realiza a fusão dos DataFrames transformados para criar a base final de informática.
    """
    try:
        merged_df = pd.merge(ods_contrato_df, ods_interveniente_df, on=['IDCONTRATOCH', 'DATA'], how='left')
        merged_df = pd.merge(merged_df, ods_avaliacao_df, on=['IDCONTRATOCH', 'DATA'], how='left')
        merged_df = pd.merge(merged_df, ods_operacao_df, on=['IDCONTRATOCH', 'DATA'], how='left')

        merged_df['DT_INFORMATION'] = pd.to_datetime(merged_df['DT_INFORMATION_x']).dt.strftime('%d/%m/%Y')
        merged_df['DATAFORMALIZACAO'] = pd.to_datetime(merged_df['DATAFORMALIZACAO']).dt.strftime('%d/%m/%Y')
        merged_df['DATAVENCIMENTO'] = pd.to_datetime(merged_df['DATAVENCIMENTO']).dt.strftime('%d/%m/%Y')

        merged_df['CAPITALAMORTIZADO'] = merged_df['MNTOPERACAO'].fillna(0)
        merged_df['CAPITALAMORTIZADO_M-1'] = merged_df.groupby('IDCONTRATOCH')['CAPITALAMORTIZADO'].shift().fillna(0)
        merged_df['AMORTIZACAOMES'] = merged_df['CAPITALAMORTIZADO'] - merged_df['CAPITALAMORTIZADO_M-1']

        conditions = [
            merged_df['CAPITALAMORTIZADO'] == 0,
            (merged_df['CAPITALAMORTIZADO'] > 0) & (merged_df['CAPITALEMDIVIDA'] == 0),
            (merged_df['CAPITALAMORTIZADO'] > 0) & (merged_df['CAPITALEMDIVIDA'] > 0)
        ]
        results = [None, 'TOTAL', 'PARCIAL']
        merged_df['TIPOAMORTIZACAO'] = np.select(conditions, results, default=None)

        merged_df['CAPITALEMDIVIDA'] = pd.to_numeric(merged_df['CAPITALEMDIVIDA'])
        merged_df['CAPITALAMORTIZADO'] = pd.to_numeric(merged_df['CAPITALAMORTIZADO'])
        merged_df['AMORTIZACAOMES'] = pd.to_numeric(merged_df['AMORTIZACAOMES'])

        cols_to_format = ['CAPITALEMDIVIDA', 'CAPITALAMORTIZADO', 'AMORTIZACAOMES']
        for col in cols_to_format:
            merged_df[col] = merged_df[col].apply(lambda x: f"{x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', ''))

        logging.info("Fusão dos DataFrames para base final de informática concluída com sucesso.")
        return merged_df
    except Exception as e:
        logging.exception("Erro ao mesclar os DataFrames para criar a base final de informática")
        raise

def main():
    configurar_logging()

    diretorio = r'C:\Users\1502553\CTT - Correios de Portugal\Planeamento e Controlo - PCG_MIS\20. Project\Analytics\07. PBI Crédito Hipotecário\Projeto Crédito Hipotecário\01. Dados Consolidados\01. ODS'
    
    try:
        if os.path.exists(diretorio):
            # Carregar DataFrames
            ods_contrato = pd.read_csv(os.path.join(diretorio, '(1) Entidade CONTRATO_consolidado.csv'))
            ods_interveniente = pd.read_csv(os.path.join(diretorio, '(2) Entidade INTERVENIENTE_consolidado.csv'))
            ods_avaliacao = pd.read_csv(os.path.join(diretorio, '(8) Entidade AVALIACAO_consolidado.csv'))
            ods_operacao = pd.read_csv(os.path.join(diretorio, '(9) Entidade OPERACAO_consolidado.csv'))

            # Aplicar as transformações
            ods_contrato_df = transform_ods_contrato(ods_contrato, id_tipo_taxa_juro, id_fin_cred, id_fin_prod, protocolo)
            ods_interveniente_df = transform_ods_interveniente(ods_interveniente)
            ods_avaliacao_df = transform_ods_avaliacao(ods_avaliacao)
            ods_operacao_df = transform_ods_operacao(ods_operacao)
            
            # Fazer a fusão final
            merged_df = merged_base_informatica(ods_contrato_df, ods_interveniente_df, ods_avaliacao_df, ods_operacao_df)

            # Salvar os arquivos
            output_dir = r'C:\Users\1502553\CTT - Correios de Portugal\Planeamento e Controlo - PCG_MIS\20. Project\Analytics\07. PBI Crédito Hipotecário\Projeto Crédito Hipotecário\02. Dados Processados\01. ODS'
            os.makedirs(output_dir, exist_ok=True)

            ods_contrato_df.to_csv(os.path.join(output_dir, 'Contrato.csv'), index=False, encoding='Latin-1')
            ods_interveniente_df.to_csv(os.path.join(output_dir, 'Interveniente.csv'), index=False, encoding='Latin-1')
            ods_avaliacao_df.to_csv(os.path.join(output_dir, 'Avaliacao.csv'), index=False, encoding='Latin-1')
            ods_operacao_df.to_csv(os.path.join(output_dir, 'Operacao.csv'), index=False, encoding='Latin-1')
            merged_df.to_csv(os.path.join(output_dir, 'Base Informatica.csv'), index=False, encoding='Latin-1')

            logging.info("Arquivos CSV gerados com sucesso.")
        else:
            logging.error(f"O diretório {diretorio} não existe.")
    except FileNotFoundError as fnf_error:
        logging.error(f"Erro ao carregar os arquivos: {fnf_error}")
    except Exception as e:
        logging.exception(f"Erro inesperado no processo: {e}")

if __name__ == "__main__":
    main()
