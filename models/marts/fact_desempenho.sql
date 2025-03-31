SELECT
    da.sk_desempenho,
    de.sk_escola,
    da.id_aluno,
    dt.sk_tempo,
    daf.sk_apoio_familiar,
    da.proficiencia,
    da.erro_padrao,
    da.desempenho_aluno,
    da.nivel_inse,
    da.faltas_aula,
    da.turno,
    da.serie,
    da.disciplina,
    da.presenca
FROM {{ ref('stg_alunos') }} da
JOIN {{ ref('dim_escola') }} de ON da.id_escola = de.id_escola
JOIN {{ ref('dim_tempo') }} dt ON da.ano = dt.ano
JOIN {{ ref('dim_apoio_familiar') }} daf ON da.nivel_apoio_familiar = daf.nivel_apoio_familiar