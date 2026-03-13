"""
GUIRRA SOLUTION — Módulos 2 a 11: Conciliações Especializadas
M02: Reembolsos Totais e Parciais
M03: Chargebacks Notificados / Alertas Antecipados
M04: Gestão de Disputas e Reclamações
M05: Chargebacks Debitados
M06: Taxas, Impostos e Controle de Custos
M07: Saldo Operacional (Fluxo de Caixa)
M08: Conciliação de Parcelamento
M09: Conciliação de Antecipações
M10: Conciliação de Recebíveis Líquidos
M11: Pagamentos / Remessas para Comércios
"""

import pandas as pd
import numpy as np
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from utils import (get_logger, STATUS, carregar_base_interna,
                   carregar_base_interna_debitos, carregar_third_party,
                   carregar_debitos_third_party, normalizar_nsu)
from m00_dicionario_custos import (calcular_mdr_df, get_mdr,
                                    TARIFAS, IMPOSTOS)

log = get_logger("guirra.modulos")

TOLERANCIA = 0.02

# ═══════════════════════════════════════════════════════════════════════════════
# M02 — REEMBOLSOS TOTAIS E PARCIAIS
# ═══════════════════════════════════════════════════════════════════════════════
def m02_reembolsos(df_bid: pd.DataFrame = None,
                   df_dtp: pd.DataFrame = None) -> dict:
    log.info("M02 — Conciliação de Reembolsos Totais e Parciais")

    if df_bid is None: df_bid = carregar_base_interna_debitos()
    if df_dtp is None: df_dtp = carregar_debitos_third_party()

    # BID: refunds internos | DTP: refund_parcial da adquirente
    bid = df_bid[df_bid["TIPO_MOVIMENTO"] == "refund"].copy()
    dtp = df_dtp[df_dtp["TIPO_MOVIMENTO"] == "refund_parcial"].copy()

    # Agrupar DTP por NSU (soma parcelas de reembolso)
    dtp_grp = dtp.groupby("NSU_KEY").agg(
        VALOR_DTP_TOTAL  = ("VALOR_BRUTO", "sum"),
        QTD_PARCELAS_DTP = ("PARCELA_NUM", "count"),
        VALOR_PARCELA_DTP= ("VALOR_PARCELA", "sum"),
        STATUS_DTP       = ("STATUS", "first"),
        BANDEIRA_DTP     = ("BANDEIRA", "first"),
    ).reset_index()

    # Agrupar BID por NSU
    bid_grp = bid.groupby("NSU_KEY").agg(
        VALOR_BID_TOTAL  = ("VALOR_BRUTO", "sum"),
        STATUS_BID       = ("STATUS_OPERACIONAL", "first"),
        BANDEIRA_BID     = ("BANDEIRA", "first"),
        PAYMENT_ID       = ("PAYMENT_ID", "first"),
        MEIO_PAGAMENTO   = ("MEIO_PAGAMENTO", "first"),
    ).reset_index()

    merged = bid_grp.merge(dtp_grp, on="NSU_KEY", how="outer", indicator=True)

    def classify(row):
        src = row["_merge"]
        if src == "left_only":  return STATUS["SEM_PAR"]
        if src == "right_only": return STATUS["SEM_PAR"]
        diff = abs(row["VALOR_BID_TOTAL"] - row["VALOR_DTP_TOTAL"])
        if diff <= TOLERANCIA: return STATUS["CONCILIADO"]
        return STATUS["DIVERGENTE"]

    merged["STATUS_CONCILIACAO"] = merged.apply(classify, axis=1)
    merged["DIFF_VALOR"] = (merged["VALOR_BID_TOTAL"] - merged["VALOR_DTP_TOTAL"]).round(2)

    # Flag de anomalia: refund com valor negativo
    merged["FLAG_VALOR_NEGATIVO_BID"] = merged["VALOR_BID_TOTAL"] < 0
    merged["FLAG_VALOR_NEGATIVO_DTP"] = merged["VALOR_DTP_TOTAL"] < 0

    total   = len(merged)
    n_conc  = (merged["STATUS_CONCILIACAO"] == STATUS["CONCILIADO"]).sum()
    n_div   = (merged["STATUS_CONCILIACAO"] == STATUS["DIVERGENTE"]).sum()
    n_sp    = (merged["STATUS_CONCILIACAO"] == STATUS["SEM_PAR"]).sum()

    log.info(f"  CONCILIADO: {n_conc:,} | DIVERGENTE: {n_div:,} | SEM_PAR: {n_sp:,}")
    merged["MODULO"] = "M02_REEMBOLSOS"
    return {"resultado": merged,
            "kpis": {"total": total, "conciliados": n_conc, "divergentes": n_div,
                     "sem_par": n_sp, "taxa_conciliacao_pct": round(n_conc/total*100,2) if total else 0,
                     "valor_reembolsado": round(merged.loc[merged["STATUS_CONCILIACAO"]==STATUS["CONCILIADO"],"VALOR_BID_TOTAL"].abs().sum(),2)}}


# ═══════════════════════════════════════════════════════════════════════════════
# M03 — CHARGEBACKS NOTIFICADOS / ALERTAS ANTECIPADOS
# ═══════════════════════════════════════════════════════════════════════════════
def m03_chargebacks_notificados(df_bi: pd.DataFrame = None,
                                 df_tp: pd.DataFrame = None) -> dict:
    log.info("M03 — Chargebacks Notificados / Alertas Antecipados")

    if df_bi is None: df_bi = carregar_base_interna(deduplicar_nsu=False)
    if df_tp is None: df_tp = carregar_third_party(agrupar_por_nsu=False)

    # Filtrar chargebacks
    bi_cb = df_bi[df_bi["TIPO_MOVIMENTO"] == "chargeback"].copy()
    tp_cb = df_tp[df_tp["TIPO_MOVIMENTO"] == "chargeback"].copy()

    log.info(f"  Chargebacks BI: {len(bi_cb):,} | TP: {len(tp_cb):,}")

    # Cruzar por NSU
    tp_cb_lkp = tp_cb.set_index("NSU_KEY")[
        ["VALOR_BRUTO","STATUS","BANDEIRA","DATA_NORM","CANAL_VENDA"]
    ].add_prefix("TP_").rename_axis("NSU_KEY")

    merged = bi_cb.join(tp_cb_lkp, on="NSU_KEY", how="left")

    has_tp  = merged["TP_VALOR_BRUTO"].notna()
    val_ok  = (merged["VALOR_BRUTO"].abs() - merged["TP_VALOR_BRUTO"].abs()) \
                .abs() <= TOLERANCIA

    merged["STATUS_CONCILIACAO"] = np.where(
        has_tp & val_ok, STATUS["CONCILIADO"],
        np.where(has_tp, STATUS["DIVERGENTE"], STATUS["SEM_PAR"])
    )
    merged["DIFF_VALOR"] = (merged["VALOR_BRUTO"].abs() - merged["TP_VALOR_BRUTO"].abs()).round(2)

    # Prazo de contestação (regra bandeiras: 45 dias corridos)
    merged["DATA_NORM_DT"] = pd.to_datetime(merged["DATA_NORM"], errors="coerce", utc=True)
    hoje = pd.Timestamp.now(tz="UTC")
    merged["DIAS_DESDE_CB"] = (hoje - merged["DATA_NORM_DT"]).dt.days
    merged["FLAG_PRAZO_VENCIDO"] = merged["DIAS_DESDE_CB"] > 45
    merged["FLAG_PRAZO_URGENTE"] = (merged["DIAS_DESDE_CB"] >= 30) & (~merged["FLAG_PRAZO_VENCIDO"])

    # Aging bucket
    def aging_bucket(dias):
        if pd.isnull(dias): return "Sem data"
        if dias <= 15: return "0–15 dias"
        if dias <= 30: return "16–30 dias"
        if dias <= 45: return "31–45 dias (Urgente)"
        return "> 45 dias (Vencido)"
    merged["AGING_BUCKET"] = merged["DIAS_DESDE_CB"].apply(aging_bucket)

    n_conc = (merged["STATUS_CONCILIACAO"] == STATUS["CONCILIADO"]).sum()
    n_div  = (merged["STATUS_CONCILIACAO"] == STATUS["DIVERGENTE"]).sum()
    n_sp   = (merged["STATUS_CONCILIACAO"] == STATUS["SEM_PAR"]).sum()
    n_venc = merged["FLAG_PRAZO_VENCIDO"].sum()
    n_urg  = merged["FLAG_PRAZO_URGENTE"].sum()
    vlr_cb = merged["VALOR_BRUTO"].abs().sum()

    log.info(f"  CONCILIADO: {n_conc:,} | DIVERGENTE: {n_div:,} | SEM_PAR: {n_sp:,}")
    log.info(f"  Prazos vencidos: {n_venc:,} | Urgentes: {n_urg:,}")
    log.info(f"  Valor total em chargebacks: R$ {vlr_cb:,.2f}")

    merged["MODULO"] = "M03_CB_NOTIFICADOS"
    return {"resultado": merged,
            "kpis": {"total": len(merged), "conciliados": n_conc, "divergentes": n_div,
                     "sem_par": n_sp, "prazo_vencido": int(n_venc), "prazo_urgente": int(n_urg),
                     "valor_total_cb": round(vlr_cb, 2),
                     "taxa_conciliacao_pct": round(n_conc/len(merged)*100,2) if len(merged) else 0}}


# ═══════════════════════════════════════════════════════════════════════════════
# M04 — GESTÃO DE DISPUTAS E RECLAMAÇÕES
# ═══════════════════════════════════════════════════════════════════════════════
def m04_disputas(df_bi: pd.DataFrame = None) -> dict:
    log.info("M04 — Gestão de Disputas e Reclamações")

    if df_bi is None: df_bi = carregar_base_interna(deduplicar_nsu=False)

    # Representações = contestações de chargeback
    disputas = df_bi[df_bi["TIPO_MOVIMENTO"].isin(["chargeback","representacao"])].copy()

    disputas["DATA_NORM_DT"] = pd.to_datetime(disputas["DATA_NORM"], errors="coerce", utc=True)
    hoje = pd.Timestamp.now(tz="UTC")
    disputas["DIAS_ABERTO"] = (hoje - disputas["DATA_NORM_DT"]).dt.days

    def status_disputa(row):
        if row["TIPO_MOVIMENTO"] == "representacao":
            return "Representação Enviada"
        if row["STATUS_OPERACIONAL"] == "cancelada":
            return "Encerrado — Favor Cliente"
        if row.get("DIAS_ABERTO", 999) > 45:
            return "Prazo Vencido — Risco de Perda"
        if row.get("DIAS_ABERTO", 999) >= 30:
            return "Urgente — Ação Necessária"
        return "Em Disputa — Dentro do Prazo"

    disputas["STATUS_DISPUTA"] = disputas.apply(status_disputa, axis=1)
    disputas["VALOR_EM_RISCO"]  = disputas["VALOR_BRUTO"].abs()

    # Pipeline de status
    pipeline = disputas.groupby("STATUS_DISPUTA").agg(
        QUANTIDADE=("NSU", "count"),
        VALOR_TOTAL=("VALOR_EM_RISCO", "sum"),
    ).reset_index()

    vlr_risco = disputas["VALOR_EM_RISCO"].sum()
    n_repr    = (disputas["TIPO_MOVIMENTO"] == "representacao").sum()
    n_cb      = (disputas["TIPO_MOVIMENTO"] == "chargeback").sum()

    log.info(f"  Chargebacks: {n_cb:,} | Representações: {n_repr:,}")
    log.info(f"  Valor total em risco: R$ {vlr_risco:,.2f}")

    disputas["MODULO"] = "M04_DISPUTAS"
    return {"resultado": disputas, "pipeline": pipeline,
            "kpis": {"total_cb": int(n_cb), "total_repr": int(n_repr),
                     "valor_em_risco": round(vlr_risco, 2),
                     "prazo_vencido": int((disputas["STATUS_DISPUTA"]=="Prazo Vencido — Risco de Perda").sum()),
                     "urgentes": int((disputas["STATUS_DISPUTA"]=="Urgente — Ação Necessária").sum())}}


# ═══════════════════════════════════════════════════════════════════════════════
# M05 — CHARGEBACKS DEBITADOS
# ═══════════════════════════════════════════════════════════════════════════════
def m05_chargebacks_debitados(df_bi: pd.DataFrame = None,
                               df_tp: pd.DataFrame = None) -> dict:
    log.info("M05 — Conciliação de Chargebacks Debitados")

    if df_bi is None: df_bi = carregar_base_interna(deduplicar_nsu=False)
    if df_tp is None: df_tp = carregar_third_party(agrupar_por_nsu=False)

    bi_cb = df_bi[df_bi["TIPO_MOVIMENTO"] == "chargeback"].copy()
    tp_cb = df_tp[df_tp["TIPO_MOVIMENTO"] == "chargeback"].copy()

    # Verificar se débito foi efetivado (status capturada/liquidada na TP)
    tp_cb_deb = tp_cb[tp_cb["STATUS"].isin(["capturada","liquidada_prevista"])].copy()

    tp_lkp = tp_cb_deb.set_index("NSU_KEY")[
        ["VALOR_BRUTO","STATUS","DATA_NORM","BANDEIRA"]
    ].add_prefix("TP_DEB_").rename_axis("NSU_KEY")

    merged = bi_cb.join(tp_lkp, on="NSU_KEY", how="left")
    has_deb = merged["TP_DEB_VALOR_BRUTO"].notna()
    val_ok  = (merged["VALOR_BRUTO"].abs() - merged["TP_DEB_VALOR_BRUTO"].abs()).abs() <= TOLERANCIA

    merged["STATUS_CONCILIACAO"] = np.where(
        has_deb & val_ok, STATUS["CONCILIADO"],
        np.where(has_deb, STATUS["DIVERGENTE"], STATUS["PENDENTE"])
    )
    merged["VALOR_DEBITADO"]  = merged["TP_DEB_VALOR_BRUTO"].abs()
    merged["VALOR_CB_BI"]     = merged["VALOR_BRUTO"].abs()
    merged["DIFF_DEBITO"]     = (merged["VALOR_CB_BI"] - merged["VALOR_DEBITADO"]).round(2)

    n_conc   = (merged["STATUS_CONCILIACAO"] == STATUS["CONCILIADO"]).sum()
    n_pend   = (merged["STATUS_CONCILIACAO"] == STATUS["PENDENTE"]).sum()
    n_div    = (merged["STATUS_CONCILIACAO"] == STATUS["DIVERGENTE"]).sum()
    vlr_deb  = merged["VALOR_DEBITADO"].sum()

    log.info(f"  CONCILIADO: {n_conc:,} | PENDENTE: {n_pend:,} | DIVERGENTE: {n_div:,}")
    log.info(f"  Valor total debitado: R$ {vlr_deb:,.2f}")

    merged["MODULO"] = "M05_CB_DEBITADOS"
    return {"resultado": merged,
            "kpis": {"total": len(merged), "conciliados": n_conc,
                     "pendentes": n_pend, "divergentes": n_div,
                     "valor_debitado": round(vlr_deb,2),
                     "taxa_conciliacao_pct": round(n_conc/len(merged)*100,2) if len(merged) else 0}}


# ═══════════════════════════════════════════════════════════════════════════════
# M06 — TAXAS, IMPOSTOS E CONTROLE DE CUSTOS
# ═══════════════════════════════════════════════════════════════════════════════
def m06_taxas_custos(df_tp: pd.DataFrame = None) -> dict:
    log.info("M06 — Gestão de Taxas, Impostos e Controle de Custos")

    if df_tp is None: df_tp = carregar_third_party(agrupar_por_nsu=True)

    # Apenas vendas/capturas (base de cálculo do MDR)
    vendas = df_tp[
        df_tp["TIPO_MOVIMENTO"].isin(["venda","captura"]) &
        (df_tp["VALOR_BRUTO"] > 0)
    ].copy()

    # Calcular MDR contratado
    vendas = calcular_mdr_df(vendas)

    # Simular MDR cobrado: pequena variação aleatória seed-fixada para reprodutibilidade
    rng = np.random.default_rng(seed=42)
    noise = rng.normal(0, 0.08, len(vendas))
    vendas["MDR_COBRADO_PCT"]    = (vendas["MDR_CONTRATADO_PCT"] + noise).clip(0).round(4)
    vendas["VALOR_MDR_COBRADO"]  = (vendas["VALOR_BRUTO"] * vendas["MDR_COBRADO_PCT"] / 100).round(2)
    vendas["DIFF_MDR_VALOR"]     = (vendas["VALOR_MDR_COBRADO"] - vendas["VALOR_MDR_CONTRATADO"]).round(2)
    vendas["DIFF_MDR_PCT"]       = (vendas["MDR_COBRADO_PCT"] - vendas["MDR_CONTRATADO_PCT"]).round(4)
    vendas["FLAG_DIVERGENCIA_MDR"] = vendas["DIFF_MDR_VALOR"].abs() > 0.05

    # Tarifa por transação
    vendas["TARIFA_TRANSACAO"]   = TARIFAS["TARIFA_TRANSACAO"]
    vendas["TARIFA_GATEWAY"]     = TARIFAS["TARIFA_GATEWAY"]

    # Impostos sobre MDR cobrado
    vendas["PIS_COFINS"]  = (vendas["VALOR_MDR_COBRADO"] *
                              (IMPOSTOS["PIS"] + IMPOSTOS["COFINS"]) / 100).round(2)
    vendas["ISS"]         = (vendas["VALOR_MDR_COBRADO"] * IMPOSTOS["ISS"] / 100).round(2)
    vendas["CUSTO_TOTAL"] = (vendas["VALOR_MDR_COBRADO"]
                             + vendas["TARIFA_TRANSACAO"]
                             + vendas["TARIFA_GATEWAY"]
                             + vendas["PIS_COFINS"]
                             + vendas["ISS"]).round(2)

    # Resumo por bandeira
    resumo_bandeira = vendas.groupby("BANDEIRA").agg(
        QTD_TX          = ("NSU_KEY", "count"),
        TPV             = ("VALOR_BRUTO", "sum"),
        MDR_CONTRATADO  = ("VALOR_MDR_CONTRATADO", "sum"),
        MDR_COBRADO     = ("VALOR_MDR_COBRADO", "sum"),
        DIFF_MDR        = ("DIFF_MDR_VALOR", "sum"),
        CUSTO_TOTAL     = ("CUSTO_TOTAL", "sum"),
        N_DIVERGENCIAS  = ("FLAG_DIVERGENCIA_MDR", "sum"),
    ).round(2).reset_index()
    resumo_bandeira["MDR_MEDIO_CONTRATADO_PCT"] = (
        resumo_bandeira["MDR_CONTRATADO"] / resumo_bandeira["TPV"] * 100
    ).round(4)
    resumo_bandeira["MDR_MEDIO_COBRADO_PCT"] = (
        resumo_bandeira["MDR_COBRADO"] / resumo_bandeira["TPV"] * 100
    ).round(4)

    total_tpv   = vendas["VALOR_BRUTO"].sum()
    total_mdr_c = vendas["VALOR_MDR_CONTRATADO"].sum()
    total_mdr_r = vendas["VALOR_MDR_COBRADO"].sum()
    total_diff  = vendas["DIFF_MDR_VALOR"].sum()
    total_custo = vendas["CUSTO_TOTAL"].sum()
    n_diverg    = vendas["FLAG_DIVERGENCIA_MDR"].sum()

    log.info(f"  TPV base: R$ {total_tpv:,.2f}")
    log.info(f"  MDR Contratado: R$ {total_mdr_c:,.2f} | MDR Cobrado: R$ {total_mdr_r:,.2f}")
    log.info(f"  Diferença MDR: R$ {total_diff:,.2f} | Divergências: {n_diverg:,}")
    log.info(f"  Custo total operação: R$ {total_custo:,.2f}")

    vendas["MODULO"] = "M06_TAXAS_CUSTOS"
    return {"resultado": vendas, "resumo_bandeira": resumo_bandeira,
            "kpis": {"tpv": round(total_tpv,2), "mdr_contratado": round(total_mdr_c,2),
                     "mdr_cobrado": round(total_mdr_r,2), "diff_mdr": round(total_diff,2),
                     "custo_total": round(total_custo,2), "n_divergencias_mdr": int(n_diverg),
                     "custo_pct_tpv": round(total_custo/total_tpv*100,2) if total_tpv else 0}}


# ═══════════════════════════════════════════════════════════════════════════════
# M07 — SALDO OPERACIONAL (FLUXO DE CAIXA)
# ═══════════════════════════════════════════════════════════════════════════════
def m07_fluxo_caixa(df_bi: pd.DataFrame = None,
                    df_bid: pd.DataFrame = None,
                    df_tp: pd.DataFrame = None) -> dict:
    log.info("M07 — Gestão de Saldo Operacional / Fluxo de Caixa")

    if df_bi  is None: df_bi  = carregar_base_interna()
    if df_bid is None: df_bid = carregar_base_interna_debitos()
    if df_tp  is None: df_tp  = carregar_third_party(agrupar_por_nsu=False)

    # Entradas: vendas positivas
    entradas = df_bi[
        df_bi["TIPO_MOVIMENTO"].isin(["venda","captura"]) &
        (df_bi["VALOR_BRUTO"] > 0) &
        (df_bi["STATUS_OPERACIONAL"].isin(["capturada","liquidada_prevista"]))
    ].copy()
    entradas["DATA_D"] = entradas["DATA_ARQ_NORM"].dt.floor("D")
    entradas["TIPO_FLUXO"] = "ENTRADA"

    # Saídas: chargebacks, refunds, cancelamentos
    saidas_cb  = df_bi[df_bi["TIPO_MOVIMENTO"] == "chargeback"].copy()
    saidas_ref = df_bid.copy()
    saidas_cb["DATA_D"]  = saidas_cb["DATA_ARQ_NORM"].dt.floor("D")
    saidas_ref["DATA_D"] = saidas_ref["DATA_NORM"].dt.floor("D")
    saidas_cb["TIPO_FLUXO"]  = "SAIDA_CHARGEBACK"
    saidas_ref["TIPO_FLUXO"] = "SAIDA_REEMBOLSO"

    # Consolidar por dia
    def agg_dia(df, tipo):
        if "DATA_D" not in df.columns: return pd.DataFrame()
        g = df.groupby("DATA_D").agg(
            VALOR=("VALOR_BRUTO", lambda x: x.abs().sum()),
            QTD=("VALOR_BRUTO", "count")
        ).reset_index()
        g["TIPO_FLUXO"] = tipo
        return g

    fluxo = pd.concat([
        agg_dia(entradas, "ENTRADA"),
        agg_dia(saidas_cb, "SAIDA_CHARGEBACK"),
        agg_dia(saidas_ref, "SAIDA_REEMBOLSO"),
    ], ignore_index=True)

    # Pivot diário
    pivot = fluxo.pivot_table(
        index="DATA_D", columns="TIPO_FLUXO", values="VALOR", aggfunc="sum"
    ).fillna(0).reset_index()

    for col in ["ENTRADA","SAIDA_CHARGEBACK","SAIDA_REEMBOLSO"]:
        if col not in pivot.columns: pivot[col] = 0

    pivot["SALDO_BRUTO_DIA"]   = pivot["ENTRADA"] - pivot["SAIDA_CHARGEBACK"] - pivot["SAIDA_REEMBOLSO"]
    pivot["SALDO_ACUMULADO"]   = pivot["SALDO_BRUTO_DIA"].cumsum()
    pivot = pivot.sort_values("DATA_D")

    total_entrada = pivot["ENTRADA"].sum()
    total_saida   = (pivot["SAIDA_CHARGEBACK"] + pivot["SAIDA_REEMBOLSO"]).sum()
    saldo_final   = pivot["SALDO_ACUMULADO"].iloc[-1] if len(pivot) > 0 else 0

    log.info(f"  Total Entradas: R$ {total_entrada:,.2f}")
    log.info(f"  Total Saídas:   R$ {total_saida:,.2f}")
    log.info(f"  Saldo Final:    R$ {saldo_final:,.2f}")

    pivot["MODULO"] = "M07_FLUXO_CAIXA"
    return {"resultado": pivot, "fluxo_detalhado": fluxo,
            "kpis": {"total_entrada": round(total_entrada,2),
                     "total_saida": round(total_saida,2),
                     "saldo_final": round(saldo_final,2),
                     "dias_negativo": int((pivot["SALDO_BRUTO_DIA"] < 0).sum())}}


# ═══════════════════════════════════════════════════════════════════════════════
# M08 — CONCILIAÇÃO DE PARCELAMENTO
# ═══════════════════════════════════════════════════════════════════════════════
def m08_parcelamento(df_bi: pd.DataFrame = None,
                     df_tp: pd.DataFrame = None) -> dict:
    log.info("M08 — Conciliação de Parcelamento")

    if df_bi is None: df_bi = carregar_base_interna()
    if df_tp is None: df_tp = carregar_third_party(agrupar_por_nsu=False)

    # Transações parceladas (QTDE_PARCELAS > 1)
    bi_parc = df_bi[df_bi["QTDE_PARCELAS"] > 1].copy()
    tp_parc = df_tp[df_tp["QTDE_PARCELAS"] > 1].copy()

    log.info(f"  BI parceladas: {len(bi_parc):,} | TP parcelas: {len(tp_parc):,}")

    # Agrupar TP por NSU: verificar completude das parcelas
    tp_agg = tp_parc.groupby("NSU_KEY").agg(
        QTD_PARCELAS_ESPERADAS = ("QTDE_PARCELAS", "first"),
        QTD_PARCELAS_RECEBIDAS = ("PARCELA_NUM", "count"),
        VALOR_TOTAL_TP         = ("VALOR_BRUTO", "first"),
        VALOR_PARCELAS_SOMA    = ("VALOR_BRUTO", "sum"),
        PARCELA_MAX            = ("PARCELA_NUM", "max"),
    ).reset_index()
    tp_agg["FLAG_PARCELAS_INCOMPLETAS"] = (
        tp_agg["QTD_PARCELAS_RECEBIDAS"] < tp_agg["QTD_PARCELAS_ESPERADAS"]
    )

    # Merge com BI
    bi_parc_grp = bi_parc.groupby("NSU_KEY").agg(
        VALOR_BI          = ("VALOR_BRUTO", "first"),
        QTDE_PARC_BI      = ("QTDE_PARCELAS", "first"),
        BANDEIRA          = ("BANDEIRA", "first"),
        TIPO_OPERACION    = ("TIPO_OPERACION", "first"),
        STATUS_OPERACIONAL= ("STATUS_OPERACIONAL", "first"),
    ).reset_index()

    merged = bi_parc_grp.merge(tp_agg, on="NSU_KEY", how="outer", indicator=True)

    def classify(row):
        if row["_merge"] == "left_only":  return STATUS["SEM_PAR"]
        if row["_merge"] == "right_only": return STATUS["SEM_PAR"]
        if row["FLAG_PARCELAS_INCOMPLETAS"]: return STATUS["PENDENTE"]
        diff = abs(row["VALOR_BI"] - row["VALOR_TOTAL_TP"])
        return STATUS["CONCILIADO"] if diff <= TOLERANCIA else STATUS["DIVERGENTE"]

    merged["STATUS_CONCILIACAO"] = merged.apply(classify, axis=1)
    merged["DIFF_VALOR"] = (merged["VALOR_BI"] - merged["VALOR_TOTAL_TP"]).round(2)

    n_conc  = (merged["STATUS_CONCILIACAO"] == STATUS["CONCILIADO"]).sum()
    n_pend  = (merged["STATUS_CONCILIACAO"] == STATUS["PENDENTE"]).sum()
    n_div   = (merged["STATUS_CONCILIACAO"] == STATUS["DIVERGENTE"]).sum()
    n_inc   = merged["FLAG_PARCELAS_INCOMPLETAS"].sum() if "FLAG_PARCELAS_INCOMPLETAS" in merged.columns else 0

    log.info(f"  CONCILIADO: {n_conc:,} | PENDENTE: {n_pend:,} | DIVERGENTE: {n_div:,}")
    log.info(f"  Parcelas incompletas: {n_inc:,}")

    merged["MODULO"] = "M08_PARCELAMENTO"
    return {"resultado": merged,
            "kpis": {"total": len(merged), "conciliados": n_conc, "pendentes": n_pend,
                     "divergentes": n_div, "parcelas_incompletas": int(n_inc),
                     "taxa_conciliacao_pct": round(n_conc/len(merged)*100,2) if len(merged) else 0}}


# ═══════════════════════════════════════════════════════════════════════════════
# M09 — CONCILIAÇÃO DE ANTECIPAÇÕES
# ═══════════════════════════════════════════════════════════════════════════════
def m09_antecipacoes(df_tp: pd.DataFrame = None) -> dict:
    log.info("M09 — Conciliação de Antecipações")

    if df_tp is None: df_tp = carregar_third_party(agrupar_por_nsu=False)

    # Transações parceladas: base de cálculo para antecipação
    tp_parc = df_tp[
        (df_tp["QTDE_PARCELAS"] > 1) &
        (df_tp["TIPO_MOVIMENTO"].isin(["venda","captura"])) &
        (df_tp["VALOR_BRUTO"] > 0)
    ].copy()

    # Simular agenda de recebíveis (D+30 por parcela como padrão)
    tp_parc["DATA_CAPTURA_DT"] = pd.to_datetime(tp_parc["DATA_CAPTURA_NORM"], errors="coerce", utc=True)
    tp_parc["DATA_PREVISTA_PARCELA"] = (
        tp_parc["DATA_CAPTURA_DT"] +
        pd.to_timedelta(tp_parc["PARCELA_NUM"] * 30, unit="D")
    )
    hoje = pd.Timestamp.now(tz="UTC")
    tp_parc["DIAS_PARA_LIQUIDACAO"] = (tp_parc["DATA_PREVISTA_PARCELA"] - hoje).dt.days
    tp_parc["FLAG_ANTECIPAVEL"] = tp_parc["DIAS_PARA_LIQUIDACAO"] > 0

    # Calcular custo de antecipação (taxa mensal composta)
    taxa_mes = TARIFAS["TARIFA_ANTECIPACAO_MES"] / 100
    tp_parc["MESES_ANT"] = (tp_parc["DIAS_PARA_LIQUIDACAO"].clip(lower=0) / 30).round(1)
    tp_parc["CUSTO_ANTECIPACAO"] = (
        tp_parc["VALOR_BRUTO"] * (1 - 1 / (1 + taxa_mes) ** tp_parc["MESES_ANT"])
    ).round(2).clip(lower=0)
    tp_parc["VALOR_LIQUIDO_ANT"] = (tp_parc["VALOR_BRUTO"] - tp_parc["CUSTO_ANTECIPACAO"]).round(2)

    antec = tp_parc[tp_parc["FLAG_ANTECIPAVEL"]].copy()
    antec["STATUS_CONCILIACAO"] = STATUS["CONCILIADO"]

    total_ant   = antec["VALOR_BRUTO"].sum()
    custo_ant   = antec["CUSTO_ANTECIPACAO"].sum()
    liquido_ant = antec["VALOR_LIQUIDO_ANT"].sum()

    log.info(f"  Valor antecipável: R$ {total_ant:,.2f}")
    log.info(f"  Custo antecipação: R$ {custo_ant:,.2f}")
    log.info(f"  Valor líquido após antecipação: R$ {liquido_ant:,.2f}")

    antec["MODULO"] = "M09_ANTECIPACOES"
    return {"resultado": antec,
            "kpis": {"valor_antecipavel": round(total_ant,2),
                     "custo_antecipacao": round(custo_ant,2),
                     "valor_liquido_antecipado": round(liquido_ant,2),
                     "taxa_custo_pct": round(custo_ant/total_ant*100,2) if total_ant else 0}}


# ═══════════════════════════════════════════════════════════════════════════════
# M10 — CONCILIAÇÃO DE RECEBÍVEIS LÍQUIDOS
# ═══════════════════════════════════════════════════════════════════════════════
def m10_recebiveis_liquidos(df_tp: pd.DataFrame = None,
                             df_bi: pd.DataFrame = None) -> dict:
    log.info("M10 — Conciliação de Recebíveis Líquidos")

    if df_tp is None: df_tp = carregar_third_party(agrupar_por_nsu=True)
    if df_bi is None: df_bi = carregar_base_interna()

    vendas = df_tp[
        df_tp["TIPO_MOVIMENTO"].isin(["venda","captura"]) &
        (df_tp["VALOR_BRUTO"] > 0)
    ].copy()

    # Calcular MDR
    vendas = calcular_mdr_df(vendas)

    # Chargebacks (débito)
    cb_bi = df_bi[df_bi["TIPO_MOVIMENTO"] == "chargeback"].groupby("NSU_KEY")["VALOR_BRUTO"].sum().abs()
    vendas["VALOR_CHARGEBACK"] = vendas["NSU_KEY"].map(cb_bi).fillna(0)

    # Waterfall: Bruto → MDR → Tarifa → CB → Líquido
    vendas["TARIFA_TX"] = TARIFAS["TARIFA_TRANSACAO"] + TARIFAS["TARIFA_GATEWAY"]
    vendas["IMPOSTOS"]  = (vendas["VALOR_MDR_CONTRATADO"] *
                           (IMPOSTOS["PIS"] + IMPOSTOS["COFINS"] + IMPOSTOS["ISS"]) / 100).round(2)
    vendas["VALOR_LIQUIDO_FINAL"] = (
        vendas["VALOR_BRUTO"]
        - vendas["VALOR_MDR_CONTRATADO"]
        - vendas["TARIFA_TX"]
        - vendas["IMPOSTOS"]
        - vendas["VALOR_CHARGEBACK"]
    ).round(2)
    vendas["STATUS_CONCILIACAO"] = np.where(
        vendas["VALOR_LIQUIDO_FINAL"] >= 0, STATUS["CONCILIADO"], STATUS["ANOMALIA"]
    )

    # Agenda por data
    if "DATA_NORM" in vendas.columns:
        vendas["DATA_D"] = pd.to_datetime(vendas["DATA_NORM"], errors="coerce", utc=True).dt.floor("D")
    else:
        vendas["DATA_D"] = pd.NaT

    agenda = vendas.groupby("DATA_D").agg(
        VALOR_BRUTO          = ("VALOR_BRUTO", "sum"),
        VALOR_MDR            = ("VALOR_MDR_CONTRATADO", "sum"),
        VALOR_CHARGEBACK     = ("VALOR_CHARGEBACK", "sum"),
        IMPOSTOS             = ("IMPOSTOS", "sum"),
        VALOR_LIQUIDO_FINAL  = ("VALOR_LIQUIDO_FINAL", "sum"),
    ).round(2).reset_index()

    total_bruto   = vendas["VALOR_BRUTO"].sum()
    total_mdr     = vendas["VALOR_MDR_CONTRATADO"].sum()
    total_cb      = vendas["VALOR_CHARGEBACK"].sum()
    total_imp     = vendas["IMPOSTOS"].sum()
    total_liquido = vendas["VALOR_LIQUIDO_FINAL"].sum()

    log.info(f"  Bruto:    R$ {total_bruto:,.2f}")
    log.info(f"  (-) MDR:  R$ {total_mdr:,.2f}")
    log.info(f"  (-) CB:   R$ {total_cb:,.2f}")
    log.info(f"  (-) Imp:  R$ {total_imp:,.2f}")
    log.info(f"  Líquido:  R$ {total_liquido:,.2f}")

    vendas["MODULO"] = "M10_RECEBIVEIS_LIQUIDOS"
    return {"resultado": vendas, "agenda": agenda,
            "kpis": {"bruto": round(total_bruto,2), "mdr": round(total_mdr,2),
                     "chargeback": round(total_cb,2), "impostos": round(total_imp,2),
                     "liquido": round(total_liquido,2),
                     "margem_liquida_pct": round(total_liquido/total_bruto*100,2) if total_bruto else 0}}


# ═══════════════════════════════════════════════════════════════════════════════
# M11 — PAGAMENTOS / REMESSAS PARA COMÉRCIOS
# ═══════════════════════════════════════════════════════════════════════════════
def m11_remessas_comercios(df_tp: pd.DataFrame = None) -> dict:
    log.info("M11 — Conciliação de Pagamentos / Remessas para Comércios")

    if df_tp is None: df_tp = carregar_third_party(agrupar_por_nsu=False)

    liquidadas = df_tp[df_tp["STATUS"].isin(["capturada","liquidada_prevista"])].copy()

    # Agrupar por merchant e data de arquivo
    liquidadas["DATA_D"] = pd.to_datetime(
        liquidadas["DATA_GERACAO_ARQ"], errors="coerce", utc=True
    ).dt.floor("D")

    agenda_merchant = liquidadas.groupby(
        ["MERCHANT_NUM_ADQ", "MERCHANT_NOME", "DATA_D"]
    ).agg(
        QTD_TX          = ("NSU_KEY", "count"),
        VALOR_BRUTO     = ("VALOR_BRUTO", "sum"),
    ).round(2).reset_index()
    agenda_merchant["STATUS_CONCILIACAO"] = STATUS["CONCILIADO"]

    total_remessas    = agenda_merchant["VALOR_BRUTO"].sum()
    n_merchants       = agenda_merchant["MERCHANT_NUM_ADQ"].nunique()
    n_dias            = agenda_merchant["DATA_D"].nunique()

    # Top 10 comércios
    top10 = (agenda_merchant.groupby(["MERCHANT_NUM_ADQ","MERCHANT_NOME"])
             .agg(VALOR_TOTAL=("VALOR_BRUTO","sum"), QTD_TX=("QTD_TX","sum"))
             .sort_values("VALOR_TOTAL", ascending=False)
             .head(10).reset_index())

    log.info(f"  Total remessas: R$ {total_remessas:,.2f}")
    log.info(f"  Comércios ativos: {n_merchants:,} | Dias de liquidação: {n_dias:,}")

    agenda_merchant["MODULO"] = "M11_REMESSAS"
    return {"resultado": agenda_merchant, "top10_merchants": top10,
            "kpis": {"total_remessas": round(total_remessas,2),
                     "n_merchants": int(n_merchants), "n_dias": int(n_dias)}}
