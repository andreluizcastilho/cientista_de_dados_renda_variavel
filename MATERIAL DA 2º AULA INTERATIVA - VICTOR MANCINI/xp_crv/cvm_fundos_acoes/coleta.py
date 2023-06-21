import pandas as pd

import io
import requests_html
import zipfile


def coleta_dados_fundos_cvm(dt_ref):
    
    url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{dt_ref}.zip"
    nome_arquivo = f"inf_diario_fi_{dt_ref}.csv"
    
    r = requests_html.HTMLSession().get(url)
    
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    zf = zf.open(nome_arquivo)
    list_ = [x.decode().split(';') for x in zf.readlines()]
    
    df_fundos = pd.DataFrame(list_[1:], columns=list_[0])
   
    for column in df_fundos.columns:
        df_fundos = df_fundos.rename(
            {
                column: column.split()[0]
            },
            axis="columns"
        )
        
    df_fundos["NR_COTST"] = [x.split()[0] for x in df_fundos["NR_COTST"]]

    df_fundos.to_csv(f"cvm_fundos_acoes/fundos/inf_diario_fi_{dt_ref}.csv")