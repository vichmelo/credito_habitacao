import pandas as pd
import os

diretorio = r'C:\Users\1502553\CTT - Correios de Portugal\Planeamento e Controlo - PCG_MIS\20. Project\Analytics\07. PBI Crédito Hipotecário\Projeto Crédito Hipotecário\00. Fontes de Dados\04. General Database'
file_name = 'General Database.xlsx'

# Leitura das planilhas do Excel
id_tipo_taxa_juro = pd.read_excel(os.path.join(diretorio, file_name), sheet_name='IDTIPOTAXAJURO', engine='openpyxl')
id_tipo_taxa_juro = id_tipo_taxa_juro.rename(columns={'TIPO2': 'TAXA','TIPO': 'DESC_TAXA','PERIODICIDADE(Meses)': 'MESES TAXA'})

id_fin_prod = pd.read_excel(os.path.join(diretorio, file_name), sheet_name='IDFINALIDADEPRODUTO', engine='openpyxl')

id_fin_cred = pd.read_excel(os.path.join(diretorio, file_name), sheet_name='IDFINALIDADECREDITO', engine='openpyxl')

duration = pd.read_excel(os.path.join(diretorio, file_name), sheet_name='DURATION', engine='openpyxl')
duration['ID_DURATION'] = duration['ID_DURATION'].astype(str)

profiletaxa = pd.read_excel(os.path.join(diretorio, file_name), sheet_name='PROFILETAXA', engine='openpyxl')

perfilprepagamento = pd.read_excel(os.path.join(diretorio, file_name), sheet_name='PERFILPREPAGAMENTO', engine='openpyxl')

protocolo = pd.read_excel(os.path.join(diretorio, file_name), sheet_name='PROTOCOLO', engine='openpyxl')




