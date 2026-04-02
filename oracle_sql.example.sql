SELECT tff.DATA_FATURAMENTO,
       tff.DATA_SAIDA,
       tff.CODIGO_CLIENTE,
       tff.CODIGO_FILIAL,
       tff.CHAVE_NFE,
       tff.NUMERO_TRANSACAO_VENDA,
       tff.CODIGO_ENDERECO_ENTREGA,
       tff.ENDERECO_DESTINO,
       tff.ENDERECO_ORIGEM,
       tff.NUMERO_NOTA,
       tff.NUMERO_CARGA,
       tff.DATA_FECHAMENTO,
       tff.CHAVE_CTE,
       tff.CIDADE_DESTINO,
       tff.CIDADE_ORIGEM,
        tff.UF
  FROM villa_origem_destino_notas_taff tff
 WHERE tff.DATA_FATURAMENTO >= TO_DATE(:data_inicio, 'YYYY-MM-DD')
   AND tff.DATA_FATURAMENTO < TO_DATE(:data_fim, 'YYYY-MM-DD') + 1
   AND (:uf IS NULL OR tff.UF = :uf)
   AND (:codigo_filial IS NULL OR tff.CODIGO_FILIAL = :codigo_filial)
   AND (:codigo_cliente IS NULL OR tff.CODIGO_CLIENTE = :codigo_cliente)
   AND (:cidade_origem IS NULL OR UPPER(tff.CIDADE_ORIGEM) = UPPER(:cidade_origem))
   AND (:cidade_destino IS NULL OR UPPER(tff.CIDADE_DESTINO) = UPPER(:cidade_destino))
   AND tff.ENDERECO_ORIGEM IS NOT NULL
   AND tff.ENDERECO_DESTINO IS NOT NULL
   AND tff.CIDADE_ORIGEM IS NOT NULL
   AND tff.CIDADE_DESTINO IS NOT NULL
   AND tff.UF IS NOT NULL
 ORDER BY tff.DATA_FATURAMENTO DESC, tff.NUMERO_CARGA, tff.NUMERO_NOTA