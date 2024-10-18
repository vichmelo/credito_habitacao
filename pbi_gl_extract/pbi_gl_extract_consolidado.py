import os
import pandas as pd

ano = input("Qual o ano e mês do ficheiro que deseja importar? (formato yyyy.mm)")
path = 'C:/Users/1502553/CTT - Correios de Portugal/Planeamento e Controlo - PCG_MIS/20. Project\Analytics/07. PBI Crédito Hipotecário/Projeto Crédito Hipotecário/00. Fontes de Dados/03. GL Extract/' + ano
files = os.listdir(path)
df = pd.DataFrame()

print(files)

# Criar um endereço para cada ficheiro que está na pasta
files_xlsb = [path + '\\' + f for f in files if f[-4:] == 'xlsb']

# Identificar o nome dos ficheiros que estão a ser lidos no Python
print('Nome de todos os ficheiros lidos:', files)

def read_xlsb_sheets(file_path, sheet_prefix):
    with pd.ExcelFile(file_path, engine="pyxlsb") as xlsb:
        df = pd.DataFrame()
        
        for sheet_name in xlsb.sheet_names:
            if sheet_name.startswith(sheet_prefix):
                sheet_data = xlsb.parse(sheet_name)
                sheet_data['Sheet'] = sheet_name  # Adiciona uma coluna com o nome da sheet
                print(sheet_name, '-', file_path)
                df = pd.concat([df, sheet_data], ignore_index=True)
    return df

# Lista de caminhos dos arquivos XLSB
files_xlsb = [path + '\\' + f for f in files if f[-4:] == 'xlsb']
sheet_prefix = "MPA_GL_Extract" ############################ VERIFICAR SE O NOME DA SHEET É ESTE OU OUTRO #######################
##sheet_prefix = "GL_Extract"

df = pd.DataFrame()

# Itere sobre os arquivos
for file_path in files_xlsb:
    file_name = os.path.basename(file_path)
    # Extrai a parte específica desejada do nome do arquivo
    source_column_value = file_name.split('.')[0]
    source_column_value = source_column_value.split('\\')[-1]
    file_data = read_xlsb_sheets(file_path, sheet_prefix)
    file_data['Source'] = source_column_value  # Adiciona uma coluna 'Source' ao DataFrame
    df = pd.concat([df, file_data], ignore_index=True)

df = df.drop(columns = ['TRANSACTION_ID', 'OPERATION_ID', 'OP_CHARGEABLE', 'TRANSACTION_DATE', 'VALUE_DATE', 'CURRENCY', 'EXCHANGE_RATE', 'DEBIT_CREDIT_FLAG', 'CUSTOMER_CODE',
                        'CUSTOMER_BANK_ACCOUNT', 'NOMINAL_CODE', 'AMOUNT', 'TAX_CODE', 'TOTAL_AMOUNT', 'REFERENCE', 'BRANCH_CODE', 'DESCRIPTION', '5488434000', '5488434001', 'SOURCE', 'PROCESS', 'CONTRACT',
                        'EVENT', 'DESCRIPTION', 'TRANSACTION_ID', 'TRANSACTION_DATE', 'VALUE_DATE', 'AMOUNT', 'DESCRIPTION.1', 'TRANSACTION_ID.1', 'TRANSACTION_DATE.1', 'VALUE_DATE.1', 'AMOUNT.1', '…', '….1'])

# Caminho base para salvar os arquivos
base_path = r'C:\Users\1502553\CTT - Correios de Portugal\Planeamento e Controlo - PCG_MIS\20. Project\Analytics\08. Rec_CH\Output Python'

# Nome do arquivo a ser salvo
output_file_json = f"{base_path}/GL_EXTRACT_" + ano + ".json"

# Perguntar ao usuário se tem certeza que deseja salvar o arquivo
confirmacao = input(f"Tem certeza que deseja salvar o arquivo '{output_file_json}'? (S/N): ").strip().lower()

if confirmacao == 's':
    # Salvar o DataFrame como arquivo JSON
    df.to_json(output_file_json, orient='records', lines=True)
    print(f"Arquivo JSON salvo em: {output_file_json}")
elif confirmacao == 'n':
    print("Arquivo não foi salvo.")
else:
    print("Opção inválida. Arquivo não foi salvo.")
