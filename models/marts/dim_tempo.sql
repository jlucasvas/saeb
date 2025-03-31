SELECT
    ano AS sk_tempo,
    ano,
    CASE 
        WHEN ano = 2021 THEN 'Pós-pandemia'
        WHEN ano < 2021 THEN 'Pré-pandemia'
        ELSE 'Outros'
    END AS periodo_pandemia
FROM (
    SELECT DISTINCT ano FROM {{ ref('stg_alunos') }}
)