from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine


def uploadToBlobStorage(connection_string, container_name, file_path, file_name):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data)
        print(f"Uploaded {file_name}.")


def downloadFromBlobStorage(connection_string, container_name, file_path, file_name):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
    with open(file_path, "wb") as f:
        data = blob_client.download_blob()
        data.readinto(f)        


def uploadToPostgreSQL(df_fundo_acoes):    
    con = create_engine('postgresql+psycopg2://postgres:pwd@localhost:5432/dados_mercado')    
    df_fundo_acoes.to_sql(
        name="fundos_acoes",
        con=con,
        if_exists='replace',
        index=False
    )