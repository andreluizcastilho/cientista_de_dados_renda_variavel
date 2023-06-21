from cvm_fundos_acoes.coleta import coleta_dados_fundos_cvm
from cvm_fundos_acoes.armazenamento import uploadToBlobStorage, downloadFromBlobStorage, uploadToPostgreSQL
from cvm_fundos_acoes.processamento import processar_fundo

from dotenv import load_dotenv

import fire
import os

import pandas as pd


class CLI:
    
    def __init__(self):
        
        load_dotenv(".env")
        
        self.connection_string = os.getenv('connection_string')
        self.container_name = os.getenv('container_name')
        self.list_dt_ref = [
            "202303",
            "202302",
            # "202301",
            # "202212",
            # "202211",
            # "202210",
            # "202209",
            # "202208",
            # "202207",
            # "202206",
            # "202205",
            # "202204",
            # "202203",
        ]
    
    def run_fundos_cvm(self):
        
        try:            
            for dt_ref in self.list_dt_ref:
                coleta_dados_fundos_cvm(dt_ref)
                uploadToBlobStorage(
                    self.connection_string, 
                    self.container_name, 
                    f'cvm_fundos_acoes/fundos/inf_diario_fi_{dt_ref}.csv', 
                    f'inf_diario_fi_{dt_ref}.csv'
                )
                
        except Exception as e:
            raise
        

    def run_download_fundos_cvm(self):
        
        try:
            
            for dt_ref in self.list_dt_ref:
                downloadFromBlobStorage(
                    self.connection_string, 
                    self.container_name, 
                    f'cvm_fundos_acoes/fundos_download/inf_diario_fi_{dt_ref}.csv', 
                    f'inf_diario_fi_{dt_ref}.csv'
                )
            
            # df_fundo_acoes = processar_fundo(self.list_dt_ref, "11.145.320/0001-56")
            # uploadToPostgreSQL(df_fundo_acoes)
            
        except Exception as e:
            raise
    

if __name__ == "__main__":
    
    fire.Fire(CLI)            