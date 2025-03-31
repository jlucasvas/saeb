from google.cloud import bigquery
import os

CREDENTIALS_PATH = "Removi_as_minhas_credenciais_por_segurança"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

# Inicializar o cliente BigQuery autenticado
bq_client = bigquery.Client()

QUERY_ALUNOS = """
EXPORT DATA 
  OPTIONS (
    uri='gs://gcp-default-bucket-jose/raw/aluno_ef_9ano/aluno_ef_9ano_*.parquet',
    format='PARQUET',
    compression='SNAPPY',
    overwrite=true
  )
AS 
SELECT 
    id_aluno, id_escola, ano, sigla_uf, id_municipio,
    proficiencia, erro_padrao, desempenho_aluno, nivel_inse, faltas_aula,
    sexo, raca_cor, faixa_etaria, rede, localizacao, escola_publica,
    turno, serie, disciplina, presenca, escolaridade_mae, escolaridade_pai,
    possui_necessidade_especial, pessoa_acompanha_vida_escolar,
    responsaveis_comparecem_reuniao_pais, responsaveis_incentivam_estudos,
    possui_internet, possui_computador, pretensao_futura, pandemia_apoio_familia
FROM `basedosdados.br_inep_saeb.aluno_ef_9ano`
WHERE ano BETWEEN 1995 AND 2022
AND proficiencia IS NOT NULL AND desempenho_aluno IS NOT NULL

"""



def export_data():
    """Exporta para formato parquet no GCS"""
    try:
        # Exporta dados traduzidos
        query_job = bq_client.query(QUERY_ALUNOS)
        query_job.result()
        print("Exportação aluno_ef_9ano concluída")

    except Exception as e:
        print(f"Erro durante a exportação: {e}")
        raise

if __name__ == "__main__":
    export_data()