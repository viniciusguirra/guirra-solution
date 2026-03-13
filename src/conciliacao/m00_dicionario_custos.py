"""
GUIRRA SOLUTION — Módulo 0: Dicionário de Custos Contratados (MDR)
Tabela de referência de taxas MDR, tarifas e impostos da adquirente XZ
Baseado em padrões de mercado brasileiro (BACEN, bandeiras internacionais)
"""

import pandas as pd
import numpy as np
from utils import get_logger

log = get_logger("guirra.m00_mdr")

# ─── TABELA MDR POR BANDEIRA, MODALIDADE E PARCELAS ──────────────────────────
# Fonte: Padrão de mercado adquirente XZ — Referência ABECS/BACEN 2025
# Nota: Para ambiente de estudo, valores são estimativas de mercado.
#       Em produção, substituir pelos valores do contrato vigente.

MDR_TABLE = [
    # Débito
    {"BANDEIRA": "Visa",       "TIPO_OPERACION": "débito",  "PARCELA_MIN": 1, "PARCELA_MAX": 1,  "MDR_PCT": 1.49},
    {"BANDEIRA": "Mastercard", "TIPO_OPERACION": "débito",  "PARCELA_MIN": 1, "PARCELA_MAX": 1,  "MDR_PCT": 1.49},
    {"BANDEIRA": "Elo",        "TIPO_OPERACION": "débito",  "PARCELA_MIN": 1, "PARCELA_MAX": 1,  "MDR_PCT": 1.55},
    {"BANDEIRA": "Amex",       "TIPO_OPERACION": "débito",  "PARCELA_MIN": 1, "PARCELA_MAX": 1,  "MDR_PCT": 2.10},
    # Crédito à vista (1x)
    {"BANDEIRA": "Visa",       "TIPO_OPERACION": "crédito", "PARCELA_MIN": 1, "PARCELA_MAX": 1,  "MDR_PCT": 2.39},
    {"BANDEIRA": "Mastercard", "TIPO_OPERACION": "crédito", "PARCELA_MIN": 1, "PARCELA_MAX": 1,  "MDR_PCT": 2.39},
    {"BANDEIRA": "Elo",        "TIPO_OPERACION": "crédito", "PARCELA_MIN": 1, "PARCELA_MAX": 1,  "MDR_PCT": 2.45},
    {"BANDEIRA": "Amex",       "TIPO_OPERACION": "crédito", "PARCELA_MIN": 1, "PARCELA_MAX": 1,  "MDR_PCT": 2.99},
    # Crédito parcelado 2–6x
    {"BANDEIRA": "Visa",       "TIPO_OPERACION": "crédito", "PARCELA_MIN": 2, "PARCELA_MAX": 6,  "MDR_PCT": 3.19},
    {"BANDEIRA": "Mastercard", "TIPO_OPERACION": "crédito", "PARCELA_MIN": 2, "PARCELA_MAX": 6,  "MDR_PCT": 3.19},
    {"BANDEIRA": "Elo",        "TIPO_OPERACION": "crédito", "PARCELA_MIN": 2, "PARCELA_MAX": 6,  "MDR_PCT": 3.29},
    {"BANDEIRA": "Amex",       "TIPO_OPERACION": "crédito", "PARCELA_MIN": 2, "PARCELA_MAX": 6,  "MDR_PCT": 3.79},
    # Crédito parcelado 7–12x
    {"BANDEIRA": "Visa",       "TIPO_OPERACION": "crédito", "PARCELA_MIN": 7, "PARCELA_MAX": 12, "MDR_PCT": 3.59},
    {"BANDEIRA": "Mastercard", "TIPO_OPERACION": "crédito", "PARCELA_MIN": 7, "PARCELA_MAX": 12, "MDR_PCT": 3.59},
    {"BANDEIRA": "Elo",        "TIPO_OPERACION": "crédito", "PARCELA_MIN": 7, "PARCELA_MAX": 12, "MDR_PCT": 3.69},
    {"BANDEIRA": "Amex",       "TIPO_OPERACION": "crédito", "PARCELA_MIN": 7, "PARCELA_MAX": 12, "MDR_PCT": 4.19},
]

# ─── TARIFAS ADICIONAIS ───────────────────────────────────────────────────────
TARIFAS = {
    "TARIFA_TRANSACAO":         0.09,   # R$ por transação capturada
    "TARIFA_CHARGEBACK_NOTIF":  30.00,  # R$ por chargeback notificado
    "TARIFA_CHARGEBACK_DEBITO": 0.00,   # Já embutida no débito do chargeback
    "TARIFA_ANTECIPACAO_MES":   1.99,   # % a.m. sobre valor antecipado (padrão)
    "TARIFA_GATEWAY":           0.04,   # R$ por transação (gateway de roteamento)
    "TARIFA_MENSALIDADE":       69.90,  # R$ fixo/mês por terminal ativo
}

# ─── IMPOSTOS ─────────────────────────────────────────────────────────────────
IMPOSTOS = {
    "PIS":    0.65,   # % sobre receita financeira (regime não-cumulativo)
    "COFINS": 4.00,   # % sobre receita financeira (regime não-cumulativo)
    "ISS":    2.00,   # % sobre serviços de processamento (varia por município)
    "IRPJ_CSLL_ESTIMADO": 0.00,  # Calculado na apuração, não por transação
}

# ─── MOTIVOS DE CHARGEBACK (VISA/MASTERCARD) ──────────────────────────────────
CB_REASON_CODES = {
    # Visa
    "10.1": "EMV Liability Shift — Counterfeit",
    "10.2": "EMV Liability Shift — Lost/Stolen",
    "10.3": "Other Fraud — Card-Present",
    "10.4": "Other Fraud — Card-Absent",
    "10.5": "Visa Fraud Monitoring Program",
    "11.1": "Card Recovery Bulletin",
    "11.2": "Declined Authorization",
    "11.3": "No Authorization",
    "12.1": "Late Presentment",
    "12.2": "Incorrect Transaction Code",
    "12.3": "Incorrect Currency",
    "12.4": "Incorrect Account Number",
    "12.5": "Incorrect Amount",
    "12.6": "Duplicate Processing / Paid by Other Means",
    "12.7": "Invalid Data",
    "13.1": "Merchandise/Services Not Received",
    "13.2": "Cancelled Recurring",
    "13.3": "Not as Described or Defective",
    "13.4": "Counterfeit Merchandise",
    "13.5": "Misrepresentation",
    "13.6": "Credit Not Processed",
    "13.7": "Cancelled Merchandise",
    "13.8": "Original Credit Transaction Not Accepted",
    "13.9": "Non-Receipt of Cash / Load Transaction Value",
    # Mastercard
    "4807":  "Warning Bulletin File",
    "4808":  "Authorization-Related Chargeback",
    "4812":  "Account Number Not On File",
    "4831":  "Transaction Amount Differs",
    "4834":  "Duplicate Processing",
    "4837":  "No Cardholder Authorization",
    "4840":  "Fraudulent Processing of Transactions",
    "4841":  "Cancelled Recurring or Digital Goods Transactions",
    "4842":  "Late Presentment",
    "4853":  "Cardholder Dispute",
    "4855":  "Goods or Services Not Provided",
    "4859":  "Services Not Rendered",
    "4860":  "Credit Not Processed",
    "4863":  "Cardholder Does Not Recognize — Potential Fraud",
    "4870":  "Chip Liability Shift",
    "4871":  "Chip/PIN Liability Shift",
    # Elo
    "ELO-4831": "Valor divergente",
    "ELO-4853": "Disputa do portador",
    "ELO-4855": "Produto/Serviço não entregue",
    # Amex
    "A01": "Charge Amount Exceeds Authorization Amount",
    "A02": "No Valid Authorization",
    "A08": "Authorization Approval Expired",
    "C02": "Credit Not Processed",
    "C04": "Goods/Services Returned or Refused",
    "C05": "Goods/Services Cancelled",
    "C08": "Goods/Services Not Received or Only Partially Received",
    "C14": "Paid by Other Means",
    "C18": "\"No Show\" or CARDeposit Cancelled",
    "C28": "Cancelled Recurring Billing",
    "C31": "Goods/Services Not as Described",
    "FR2": "Fraud Full Recourse Program",
    "FR4": "Immediate Chargeback Program",
    "FR6": "Partial Immediate Chargeback Program",
    "M10": "Vehicle Rental — Capital Damages",
    "M49": "Vehicle Rental — Theft or Loss of Use",
    "P01": "Unassigned Card Number",
    "P03": "Credit Processed as Charge",
    "P04": "Charge Processed as Credit",
    "P05": "Incorrect Charge Amount",
    "P07": "Late Submission",
    "P08": "Duplicate Charge",
    "P22": "Non-Matching Card Number",
    "P23": "Currency Discrepancy",
}

# ─── FUNÇÕES UTILITÁRIAS ──────────────────────────────────────────────────────
def get_mdr(bandeira: str, tipo_operacion: str, qtde_parcelas: int) -> float:
    """Retorna o MDR (%) contratado para a combinação informada."""
    bandeira = str(bandeira).strip()
    tipo_operacion = str(tipo_operacion).strip()
    qtde_parcelas = int(qtde_parcelas) if not pd.isnull(qtde_parcelas) else 1
    for row in MDR_TABLE:
        if (row["BANDEIRA"] == bandeira and
            row["TIPO_OPERACION"] == tipo_operacion and
            row["PARCELA_MIN"] <= qtde_parcelas <= row["PARCELA_MAX"]):
            return row["MDR_PCT"]
    log.warning(f"MDR não encontrado: {bandeira} | {tipo_operacion} | {qtde_parcelas}x — usando 2.99% default")
    return 2.99

def calcular_mdr_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica o MDR contratado linha a linha e calcula:
      - MDR_CONTRATADO_PCT  : taxa percentual contratada
      - VALOR_MDR_CONTRATADO: valor do MDR em R$
      - VALOR_LIQUIDO_MDR   : valor bruto - MDR
    """
    df = df.copy()
    df["MDR_CONTRATADO_PCT"] = df.apply(
        lambda r: get_mdr(r.get("BANDEIRA",""), r.get("TIPO_OPERACION",""), r.get("QTDE_PARCELAS", 1)),
        axis=1
    )
    vb = df["VALOR_BRUTO"].abs()
    df["VALOR_MDR_CONTRATADO"] = (vb * df["MDR_CONTRATADO_PCT"] / 100).round(2)
    df["VALOR_LIQUIDO_MDR"]    = (vb - df["VALOR_MDR_CONTRATADO"]).round(2)
    return df

def get_df_mdr() -> pd.DataFrame:
    return pd.DataFrame(MDR_TABLE)

def get_df_tarifas() -> pd.DataFrame:
    return pd.DataFrame([
        {"TIPO": k, "VALOR": v, "UNIDADE": "R$" if "TARIFA" in k else "%"}
        for k, v in TARIFAS.items()
    ])

def get_df_impostos() -> pd.DataFrame:
    return pd.DataFrame([
        {"IMPOSTO": k, "ALIQUOTA_PCT": v}
        for k, v in IMPOSTOS.items()
    ])

def get_df_cb_reasons() -> pd.DataFrame:
    return pd.DataFrame([
        {"CODIGO": k, "DESCRICAO": v}
        for k, v in CB_REASON_CODES.items()
    ])

if __name__ == "__main__":
    log.info("=== Dicionário de Custos MDR — Guirra Solution ===")
    log.info(f"MDR Visa Crédito 1x: {get_mdr('Visa','crédito',1)}%")
    log.info(f"MDR Mastercard Crédito 6x: {get_mdr('Mastercard','crédito',6)}%")
    log.info(f"MDR Elo Débito: {get_mdr('Elo','débito',1)}%")
    log.info(f"MDR Amex Crédito 12x: {get_mdr('Amex','crédito',12)}%")
    log.info(f"Tarifa transação: R$ {TARIFAS['TARIFA_TRANSACAO']}")
    log.info(f"Tarifa chargeback: R$ {TARIFAS['TARIFA_CHARGEBACK_NOTIF']}")
