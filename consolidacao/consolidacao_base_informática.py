import os
import pandas as pd
import tkinter as tk
from tkinter import filedialog
import logging
from datetime import datetime

def configurar_logging(log_file="consolidacao_base_informatica.log"):
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
    root.withdraw()
    pasta_selecionada = filedialog.askdirectory(title="Selecione a pasta com os arquivos Excel")
    if not pasta_selecionada:
        logging.warning("Nenhuma pasta foi selecionada.")
    return pasta_selecionada

def extrair_data_do_nome(arquivo):
    """
    Extrai o ano e mês do nome do arquivo e retorna uma data no formato '01/MM/YYYY'.
    """
    try:
        partes_nome = arquivo.split('_')
        data_str = partes_nome[-1]  # Assume que a última parte do nome contém 'YYYYMM'
        ano = data_str[:4]
        mes = data_str[4:6]
        data = datetime.strptime(f'01/{mes}/{ano}', '%d/%m/%Y')
        return data
    except ValueError as ve:
        logging.error(f"Erro ao converter data do arquivo {arquivo}: {ve}")
    except Exception as e:
        logging.exception(f"Erro inesperado ao extrair data do arquivo {arquivo}: {e}")
    return None

def get_expected_columns(sheet_name):
    """
    Retorna a lista de colunas esperadas para a respectiva sheet_name.
    """
    columns_map = {
        "(1) Entidade CONTRATO": [
            "DT_INFORMATION", "IDCONTRATOCH", "MOEDA", "IDCREDITOHABASSOC", "CATEGORIACREDITO",
            "IDGRUPOCONTRATOS", "IDCONTADOESSENCE", "CODIGOBALCAO", "IDTIPOPAGAMENTO",
            "MONTANTEORIGINALEMPRESTIMO", "MONTANTECAPITALUTILIZADO", "BULLETPERCENT",
            "BULLETVALUE", "PRAZOCONTRATADO", "DATAFORMALIZACAO", "DATAVENCIMENTO", 
            "DATALIQTOTAL", "IDREGIME", "IDFAMILIA", "IDSUBFAMILIA", "IDFINALIDADEPRODUTO", 
            "IDFINALIDADECREDITO", "IDPRODUTO", "IDTIPOPRESTACAO", "IDMODALIDADESREEMBOLSO",
            "IDTIPOTAXAJURO", "IDTIPOTAXAJUROINICIOCONTRATO", "DURACAOTXFIXA", "BASEJURO",
            "TAXAJUROBASE", "TANINICIO", "TANATUAL", "TAEINICIO", "TAEATUAL", "TAEGINICIO",
            "TAEGATUAL", "TAERATUAL", "SPREADPRECARIO", "DESCONTOCOMERCIAL", "SPREADBASE",
            "DESCONTOPRODUTOSINICIOCONTRATO", "TAXAJUROMORA", "SPREADATUAL", "TAXAJUROMINIMA",
            "TAXAJUROMAXIMA", "DATAPRIMEIRAREVISAO", "DATAPROXIMAREVISAO", "DATAULTIMAREVISAO",
            "DESCONTOREGIMEBONIF", "CAPITALVINCENDO", "CAPITALEMDIVIDA", 
            "SALDOMONTANTETOTALEMDIVIDA", "CAPITALVENCIDO", "JUROVENCIDO", "JUROMORAVENCIDO",
            "MONTANTESPENDENTESALOCACAO", "NUMERODIASINCUMPRIMENTO", "DATAPRIMEIROINCUMPRIMENTO",
            "DATAULTIMOINCUMPRIMENTO", "MONTANTEPRIMEIRAPRESTACAO", "DATAOPERACAO",
            "IDESTADODOEMPRESTIMO", "DATAESTADOEMPRESTIMO", "DATAPROCESSOJUDICIAL",
            "DATAPOSSEFISICA", "RACIOVALORSOBREEMPRESTIMO", "CODIGOANGARIADOR",
            "DATAREEMBOLSOANTECIPADO", "TIPODEREEMBOLSOANTECIPADO", "MONTANTEREEMBOLSOANTECIPADO",
            "IDMOTIVOREEMBOLSO", "TAXAPENPAGANTECIPADO", "CAPITALDIVIDARENEGOCIADA", 
            "DATARENEGOCIACAO", "INICIOPRAZOCARENCIAJUROSRENEGOCIACAO", 
            "FIMPRAZOCARENCIAJUROSRENEGOCIACAO", "INICIOPRAZOCARENCIACAPITALRENEGOCIACAO",
            "FIMPRAZOCARENCIACAPITALRENEGOCIACAO", "INICIOPRAZOSUSPENSAOPAGAMENTOSRENEGOCIACAO",
            "FIMPRAZOSUSPENSAOPAGAMENTOSRENEGOCIACAO", "VARSPREAD", "VARPRAZOCONTRATO",
            "VARPRAZOCARENCIAJUROS", "VARPRAZOCARENCIACAPITAL", "VARSUSPENSAOPAGAMENTO",
            "VARBULLET", "TIPORENEGOCIACAO", "DT_EXECUTION", "DATAAPROVACAO", "NIFANGARIADOR",
            "IDPROTOCOLO", "PRAZOUTILIZACAO", "DATAINICIOUTILIZACAO", "DATAFIMUTILIZACAO",
            "TAEGINICIOSCS", "TAEGATUALSCS", "TAEGENQUADRACCS", "TAEGENQUADRASCS",
            "TAEGFORMACCS", "TAEGFORMASCS", "TAEGMAX20CCS", "TAEGMAX20SCS", "TAEGMAX20INICIALCCS",
            "TAEGMAX20INICIALSCS", "TAEGMax20EnquadraCCS", "TAEGMax20EnquadraSCS",
            "MTICInicialCCS", "MTICinicialSCS", "MTICEnqudraCCS", "MTICEnquadraSCS",
            "MTICFormaCCS", "MTICFormaSCS", "MTICMax20InicialCCS", "MTICMax20InicialSCS",
            "MTICMax20EnquadraCCS", "MTICMax20EnquadraSCS", "MTICMaximo20FormaCCS",
            "MTICMax20FormaSCS", "RENDIMENTOSINICIAIS", "ANOREFRENDINICIAIS", "DATARENDINICIAIS",
            "DATA"
        ],
        "(2) Entidade INTERVENIENTE": [
            "DT_INFORMATION", "IDCONTRATOCH", "IDPARTY", "IDTIPOINTERVENIENTE",
            "NIFINTERVENIENTE", "RENDANUALBRUTO", "ANORENDANUALBRUTO", "TIPOACTREND", 
            "DATAULTIMAACT", "DT_EXECUTION", "NOME", "IDGENERO", "IDSITPROF", "IDHABLITERARIAS",
            "IDPAISRESIDENCIA", "IDNACIONALIDADE", "DATANASCIMENTO", "AGREGFAMILIAR", 
            "NUMDOCIDENT", "TIPODOCID", "DATAEMISSAODOC", "DATAVALIDADEDOC", "IDPAISEMISSOR", 
            "DATA"
        ],
        "(8) Entidade AVALIACAO": [
            "DT_INFORMATION", "IDIMOVEL", "IDAVALIACAO", "IDTIPOAVAL", "IDSUBTIPOAVAL", 
            "DTAVALIACAO", "VALORAVALIACAO", "VALORVENDAFORCADA", "SOCIEDADEAVALIACAO", 
            "NUMERODEREGISTOSOCIEDADEDEAVALIACAO", "NIFENTIDADEAVALIACAO", "AVALIADOR", 
            "NUMERODEREGISTODOAVALIADOR", "NIFAVALIADOR", "DT_EXECUTION", 
            "VALORAVALIACAOANTESOBRAS", "IDFINALIDADEAVALIACAO", "IDCONTRATOCH", "DATA"
        ],
        "(9) Entidade OPERACAO": [
            "DT_INFORMATION", "IDOPERACAO", "IDCONTRATOCH", "TIPOOPERACAO", "SUBTIPOOPERACAO",
            "MOTIVOOPERACAO", "DATAOPERACAO", "DT_EXECUTION", "MNTOPERACAO", "DATA"
        ]
    }
    return columns_map.get(sheet_name)

def consolidacao_ods(diretorio, sheet_name):
    """
    Consolida arquivos Excel com o padrão 'CH_Extract_ODS' em um único DataFrame.
    """
    lista_dfs = []
    contador_arquivos = 0

    expected_columns = get_expected_columns(sheet_name)

    for arquivo in os.listdir(diretorio):
        if 'CH_Extract_ODS' in arquivo and arquivo.endswith(('.xlsx', '.xls', '.xlsb')):
            caminho_arquivo = os.path.join(diretorio, arquivo)
            
            try:
                df = pd.read_excel(caminho_arquivo, sheet_name=sheet_name)
                data = extrair_data_do_nome(arquivo)
                if not data:
                    logging.warning(f"Data não extraída para o arquivo {arquivo}. Arquivo ignorado.")
                    continue

                df['DATA'] = data

                # Reordenar as colunas conforme esperado
                if expected_columns:
                    for col in expected_columns:
                        if col not in df.columns:
                            df[col] = None  # Adiciona colunas faltantes com valor nulo
                    df = df[expected_columns]  # Reordena as colunas conforme a lista

                lista_dfs.append(df)
                contador_arquivos += 1
                logging.info(f"Arquivo consolidado com sucesso: {caminho_arquivo}")
            except ValueError as ve:
                logging.warning(f"Planilha '{sheet_name}' não encontrada no arquivo {caminho_arquivo}. Pulando este arquivo.")
            except Exception as e:
                logging.exception(f"Erro inesperado ao processar o arquivo {caminho_arquivo}: {e}")
    
    if lista_dfs:
        df_consolidado = pd.concat(lista_dfs, ignore_index=True)
        logging.info(f"Total de arquivos consolidados para {sheet_name}: {contador_arquivos}")
        return df_consolidado
    else:
        logging.warning(f"Nenhum arquivo foi consolidado para {sheet_name}.")
        return pd.DataFrame()

def main():
    configurar_logging()
    
    diretorio = selecionar_pasta()
    
    if not diretorio:
        logging.error("Processo encerrado: Nenhuma pasta foi selecionada.")
        return
    
    sheets = ["(1) Entidade CONTRATO", "(2) Entidade INTERVENIENTE", "(8) Entidade AVALIACAO", "(9) Entidade OPERACAO"]
    dfs_consolidados = {}

    for sheet_name in sheets:
        df_consolidado = consolidacao_ods(diretorio, sheet_name)
        if not df_consolidado.empty:
            dfs_consolidados[sheet_name] = df_consolidado
            logging.info(f"Consolidação concluída para a planilha: {sheet_name}")
        else:
            logging.warning(f"Nenhum dado consolidado para a planilha: {sheet_name}")
    
    output_directory = "C:/Users/1502553/CTT - Correios de Portugal/Planeamento e Controlo - PCG_MIS/20. Project/Analytics/07. PBI Crédito Hipotecário/Projeto Crédito Hipotecário/01. Dados Consolidados/01. ODS"
    os.makedirs(output_directory, exist_ok=True)
    
    for sheet_name, df in dfs_consolidados.items():
        output_path = os.path.join(output_directory, f"{sheet_name}_consolidado.csv")
        try:
            df.to_csv(output_path, index=False)
            logging.info(f"Arquivo salvo: {output_path}")
        except Exception as e:
            logging.error(f"Erro ao salvar o arquivo {output_path}: {e}")

if __name__ == "__main__":
    main()
