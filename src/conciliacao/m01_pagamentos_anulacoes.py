"""
GUIRRA SOLUTION — Módulo 1: Conciliação de Pagamentos e Anulações
Cruza BASE_INTERNA (vendas/capturas/cancelamentos) x THIRD_PARTY
Chave primária: NSU | Secundária: PAYMENT_ID x ORDER_ID
"""

import pandas as pd
import numpy as np
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from utils import (get_logger, STATUS, carregar_base_interna,
                   carregar_third_party, normalizar_nsu)

log = get_logger("guirra.m01_pagamentos")

TOLERANCIA_VALOR   = 0.02   # R$ — tolerância de centavo para match
TOLERANCIA_DIAS    = 2       # Dias — tolerância de data (D+1/D+2)
MOVIMENTOS_ESCOPO  = ["venda", "captura", "cancelamento"]


def executar(df_bi: pd.DataFrame = None, df_tp: pd.DataFrame = None) -> dict:
    """
    Executa a conciliação de Pagamentos e Anulações.
    Retorna dict com DataFrames: resultado, conciliados, pendentes, sem_par, excluidos
    """
    log.info("=" * 60)
    log.info("M01 — Conciliação de Pagamentos e Anulações")
    log.info("=" * 60)

    # ── Carregar bases ────────────────────────────────────────────
    if df_bi is None:
        df_bi = carregar_base_interna(deduplicar_nsu=True)
    if df_tp is None:
        df_tp = carregar_third_party(agrupar_por_nsu=True)

    # ── Filtrar movimentos em escopo ──────────────────────────────
    bi = df_bi[df_bi["TIPO_MOVIMENTO"].isin(MOVIMENTOS_ESCOPO)].copy()
    tp = df_tp[df_tp["TIPO_MOVIMENTO"].isin(MOVIMENTOS_ESCOPO)].copy()
    log.info(f"BI em escopo: {len(bi):,} | TP em escopo: {len(tp):,}")

    # ── Separar negadas (não chegam à adquirente) ─────────────────
    negadas = bi[bi["STATUS_OPERACIONAL"] == "negada"].copy()
    negadas["STATUS_CONCILIACAO"] = STATUS["EXCLUIDO_NEGADA"]
    negadas["MOTIVO_EXCLUSAO"]    = "Transação negada — não processada pela adquirente"
    bi_ativas = bi[bi["STATUS_OPERACIONAL"] != "negada"].copy()
    log.info(f"Negadas excluídas: {len(negadas):,} | BI ativas: {len(bi_ativas):,}")

    # ── Match por NSU (chave primária) ────────────────────────────
    tp_lookup = tp.set_index("NSU_KEY")[
        ["VALOR_BRUTO", "TIPO_MOVIMENTO", "STATUS", "DATA_NORM",
         "DATA_CAPTURA_NORM", "BANDEIRA", "QTDE_PARCELAS",
         "CANAL_VENDA", "MODO_ENTRADA", "AUTH_CODE", "MERCHANT_NUM_ADQ"]
    ].add_prefix("TP_").rename_axis("NSU_KEY")

    bi_merged = bi_ativas.join(tp_lookup, on="NSU_KEY", how="left")

    # ── Classificar matches ───────────────────────────────────────
    has_tp     = bi_merged["TP_VALOR_BRUTO"].notna()
    valor_ok   = (bi_merged["VALOR_BRUTO"] - bi_merged["TP_VALOR_BRUTO"]).abs() <= TOLERANCIA_VALOR
    tipo_ok    = bi_merged["TIPO_MOVIMENTO"] == bi_merged["TP_TIPO_MOVIMENTO"]
    bandeira_ok= bi_merged["BANDEIRA"] == bi_merged["TP_BANDEIRA"]

    # Conciliado: tem par + valor ok
    mask_conc  = has_tp & valor_ok
    # Divergente: tem par mas valor diferente
    mask_div   = has_tp & ~valor_ok
    # Sem par
    mask_sp    = ~has_tp

    bi_merged["STATUS_CONCILIACAO"] = np.where(
        mask_conc, STATUS["CONCILIADO"],
        np.where(mask_div, STATUS["DIVERGENTE"], STATUS["SEM_PAR"])
    )

    # Enriquecer com diagnóstico
    bi_merged["DIFF_VALOR"] = (
        bi_merged["VALOR_BRUTO"] - bi_merged["TP_VALOR_BRUTO"]
    ).round(2)
    bi_merged["FLAG_TIPO_DIVERGE"]    = ~tipo_ok & has_tp
    bi_merged["FLAG_BANDEIRA_DIVERGE"] = ~bandeira_ok & has_tp

    # Autorizadas sem captura (Hipótese H4 da Fase 1)
    expiradas_mask = (
        (bi_merged["STATUS_CONCILIACAO"] == STATUS["SEM_PAR"]) &
        (bi_merged["STATUS_OPERACIONAL"] == "autorizada") &
        (bi_merged["TIPO_MOVIMENTO"].isin(["venda", "captura"]))
    )
    bi_merged.loc[expiradas_mask, "STATUS_CONCILIACAO"] = STATUS["EXCLUIDO_EXPIRADA"]
    bi_merged.loc[expiradas_mask, "MOTIVO_EXCLUSAO"] = (
        "Autorizada sem captura — possível expiração D+2"
    )

    # ── Estatísticas ──────────────────────────────────────────────
    total       = len(bi_merged)
    n_conc      = (bi_merged["STATUS_CONCILIACAO"] == STATUS["CONCILIADO"]).sum()
    n_div       = (bi_merged["STATUS_CONCILIACAO"] == STATUS["DIVERGENTE"]).sum()
    n_sp        = (bi_merged["STATUS_CONCILIACAO"] == STATUS["SEM_PAR"]).sum()
    n_exp       = (bi_merged["STATUS_CONCILIACAO"] == STATUS["EXCLUIDO_EXPIRADA"]).sum()
    n_neg       = len(negadas)

    vlr_conc    = bi_merged.loc[bi_merged["STATUS_CONCILIACAO"]==STATUS["CONCILIADO"], "VALOR_BRUTO"].abs().sum()
    vlr_div     = bi_merged.loc[bi_merged["STATUS_CONCILIACAO"]==STATUS["DIVERGENTE"], "VALOR_BRUTO"].abs().sum()
    vlr_sp      = bi_merged.loc[bi_merged["STATUS_CONCILIACAO"]==STATUS["SEM_PAR"], "VALOR_BRUTO"].abs().sum()

    taxa_conc   = n_conc / total * 100 if total > 0 else 0

    log.info(f"RESULTADO M01:")
    log.info(f"  CONCILIADO  : {n_conc:>6,} registros | R$ {vlr_conc:>12,.2f}")
    log.info(f"  DIVERGENTE  : {n_div:>6,} registros | R$ {vlr_div:>12,.2f}")
    log.info(f"  SEM_PAR     : {n_sp:>6,}  registros | R$ {vlr_sp:>12,.2f}")
    log.info(f"  EXPIRADAS   : {n_exp:>6,} registros")
    log.info(f"  NEGADAS     : {n_neg:>6,} registros")
    log.info(f"  Taxa Conciliação: {taxa_conc:.1f}%")

    resultado = pd.concat([bi_merged, negadas], ignore_index=True)
    resultado["MODULO"] = "M01_PAGAMENTOS_ANULACOES"

    return {
        "resultado":    resultado,
        "conciliados":  bi_merged[bi_merged["STATUS_CONCILIACAO"] == STATUS["CONCILIADO"]],
        "divergentes":  bi_merged[bi_merged["STATUS_CONCILIACAO"] == STATUS["DIVERGENTE"]],
        "sem_par":      bi_merged[bi_merged["STATUS_CONCILIACAO"] == STATUS["SEM_PAR"]],
        "excluidos":    pd.concat([negadas,
                                   bi_merged[bi_merged["STATUS_CONCILIACAO"] == STATUS["EXCLUIDO_EXPIRADA"]]]),
        "kpis": {
            "total": total, "conciliados": n_conc, "divergentes": n_div,
            "sem_par": n_sp, "expiradas": n_exp, "negadas": n_neg,
            "taxa_conciliacao_pct": round(taxa_conc, 2),
            "valor_conciliado": round(vlr_conc, 2),
            "valor_divergente": round(vlr_div, 2),
            "valor_sem_par": round(vlr_sp, 2),
        }
    }


if __name__ == "__main__":
    r = executar()
    log.info("Concluído M01.")
