from google.cloud import bigquery
import os

CREDENTIALS_PATH = "Removi_as_minhas_credenciais_por_segurança"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

# Inicializar o cliente BigQuery autenticado
bq_client = bigquery.Client()


QUERY_DICIONARIO = """
EXPORT DATA 
  OPTIONS (
    uri='gs://gcp-default-bucket-jose/raw/dicionario/dicionario_*.parquet',
    format='PARQUET',
    compression='SNAPPY',
    overwrite=true
  )
AS 
SELECT 
    id_tabela,
    chave,
    nome_coluna,
    valor,
    cobertura_temporal
FROM `basedosdados.br_inep_saeb.dicionario`
WHERE nome_coluna IN (
    'sexo', 'raca_cor', 'faixa_etaria', 'rede', 'localizacao', 'escola_publica',
    'turno', 'serie', 'disciplina', 'presenca', 'escolaridade_mae', 'escolaridade_pai',
    'possui_necessidade_especial', 'pessoa_acompanha_vida_escolar',
    'responsaveis_comparecem_reuniao_pais', 'responsaveis_incentivam_estudos',
    'possui_internet', 'possui_computador', 'pretensao_futura', 'pandemia_apoio_familia'
);

"""



def export_data():
    """Exporta para formato parquet no GCS"""
    try:
        # Exporta dados traduzidos
        query_job = bq_client.query(QUERY_DICIONARIO)
        query_job.result()
        print("Exportação dicionario concluída")

    except Exception as e:
        print(f"Erro durante a exportação: {e}")
        raise

if __name__ == "__main__":
    export_data()