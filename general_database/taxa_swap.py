import pandas as pd
import numpy as np

taxa_swap = pd.read_excel('C:/Users/1502553/CTT - Correios de Portugal/Planeamento e Controlo - PCG_MIS/20. Project/Analytics/07. PBI Crédito Hipotecário/Motor SWAP_Teste_Inputs.xlsx', sheet_name='BD_NEW')

taxa_swap['TANATUAL'] = taxa_swap['TANATUAL'].round(4)

taxa_swap['Unique ID SWAP'] = (
    taxa_swap['DATAMESANTERIORFINAL'].astype(str) + 
    taxa_swap['MATURIDADE'].astype(str) + 
    taxa_swap['DURACAOTXFIXA'].astype(str) +
    taxa_swap['DESC_TAXA'].astype(str) + 
    taxa_swap['TANATUAL'].astype(str))

taxa_swap.to_csv('C:/Users/1502553/CTT - Correios de Portugal/Planeamento e Controlo - PCG_MIS/20. Project/Analytics/07. PBI Crédito Hipotecário/Motor SWAP_Teste_Inputs.csv')