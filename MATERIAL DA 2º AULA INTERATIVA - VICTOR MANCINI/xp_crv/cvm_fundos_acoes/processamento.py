import pandas as pd


def processar_fundo(list_dt_ref, cnpj_fundo):
    
    df_fundo_acoes = pd.DataFrame()
    for dt_ref in list_dt_ref:
        df_fundos = pd.read_csv(f'cvm_fundos_acoes/fundos_download/inf_diario_fi_{dt_ref}.csv')
        df_temp = df_fundos[df_fundos["CNPJ_FUNDO"] == cnpj_fundo]
        df_fundo_acoes = pd.concat([df_fundo_acoes, df_temp])
        
    df_fundo_acoes = df_fundo_acoes.sort_values(by="DT_COMPTC", ascending=True)
    df_fundo_acoes["pct_change"] = df_fundo_acoes["VL_QUOTA"].pct_change()
    df_fundo_acoes = df_fundo_acoes.reset_index(drop=True)
    df_fundo_acoes = df_fundo_acoes.rename(
        {
            "DT_COMPTC": "Date"
        },
        axis="columns"
    )
    df_fundo_acoes.index = pd.to_datetime(df_fundo_acoes["Date"])
    df_fundo_acoes.index = [x.strftime("%Y-%m-%d") for x in df_fundo_acoes.index]
    
    return df_fundo_acoes