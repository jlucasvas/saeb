SELECT
    pandemia_apoio_familia AS apoio_original,
    nivel_apoio_familiar,
    ROW_NUMBER() OVER() AS sk_apoio_familiar
FROM (
    SELECT DISTINCT
        pandemia_apoio_familia,
        nivel_apoio_familiar
    FROM {{ ref('stg_alunos') }}
)