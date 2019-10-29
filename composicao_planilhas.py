from tkinter import filedialog
import pandas as pd
import glob
import os
import matplotlib.pyplot as plt

df_final = pd.DataFrame()
dirname = filedialog.askdirectory(initialdir="/",title='Selecione o diretório das Unidades Consumidoras')
print(dirname)

for filename in glob.glob(os.path.join(dirname, '*.csv')):
    print(filename)

    try:
        df_csv = pd.read_csv(filename, encoding='utf8', sep=';', decimal=',')
        df_final = df_final.append(df_csv)
    except Exception as e:
        print('\tArquivo não inserido: {} - erro:{} '.format(filename, e) + '\n')

# Exporta os dados que estão nas planilhas
# df_final.to_csv(nome_arq, sep=';', header=True, index=False, decimal=',', encoding='utf8')

df_final.set_index('data_analise', inplace=True)
df_final.groupby('esp_cd')['count'].plot(legend=True)
#df_final[['data_analise', 'count', 'esp_cd']].plot()
plt.show()
