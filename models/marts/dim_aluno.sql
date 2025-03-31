SELECT
    id_aluno,
    sexo,
    raca_cor,
    faixa_etaria,
    possui_necessidade_especial,
    escolaridade_mae,
    escolaridade_pai,
    pessoa_acompanha_vida_escolar,
    responsaveis_comparecem_reuniao_pais,
    responsaveis_incentivam_estudos,
    possui_internet,
    possui_computador,
    pretensao_futura,
    -- Cria uma chave surrogate para ser usado por exemplo como campo de incrementalidade
    ROW_NUMBER() OVER() AS sk_aluno
FROM (
    SELECT DISTINCT
        id_aluno,
        sexo,
        raca_cor,
        faixa_etaria,
        possui_necessidade_especial,
        escolaridade_mae,
        escolaridade_pai,
        pessoa_acompanha_vida_escolar,
        responsaveis_comparecem_reuniao_pais,
        responsaveis_incentivam_estudos,
        possui_internet,
        possui_computador,
        pretensao_futura
    FROM {{ ref('stg_alunos') }}
)