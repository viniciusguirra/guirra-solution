"""
GUIRRA SOLUTION — Módulo de Utilitários Comuns
Normalização de dados, parsing de datas, logs e helpers compartilhados
"""

import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime

# ─── LOGGING ──────────────────────────────────────────────────────────────────
def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        fmt = logging.Formatter('[%(asctime)s] %(name)s | %(levelname)s | %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S')
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        logger.addHandler(ch)
    return logger

log = get_logger("guirra.utils")


# ─── NORMALIZAÇÃO DE DATAS ────────────────────────────────────────────────────
# Fase 1 identificou 4 formatos distintos na BASE_INTERNA:
#   1. ISO-8601   : 2025-01-07T02:42:58Z   (40%)
#   2. BR sem hora: 26/01/2025             (20%)
#   3. Dash+hora  : 05-03-2025 22:35       (20%)
#   4. Space+seg  : 2025-03-11 00:13:33    (20%)

_DATE_FORMATS = [
    "%Y-%m-%dT%H:%M:%SZ",    # ISO-8601 com Z
    "%Y-%m-%dT%H:%M:%S",     # ISO-8601 sem Z
    "%Y-%m-%d %H:%M:%S%z",   # ISO com tz offset
    "%Y-%m-%d %H:%M:%S",     # Espaço com segundos
    "%Y-%m-%d %H:%M",        # Espaço sem segundos
    "%d/%m/%Y %H:%M:%S",     # BR com hora
    "%d/%m/%Y",               # BR sem hora
    "%d-%m-%Y %H:%M",        # Dash com hora
    "%d-%m-%Y",               # Dash sem hora
]

def normalizar_data(valor) -> pd.Timestamp | None:
    """Converte qualquer formato de data suportado para pd.Timestamp UTC."""
    if pd.isnull(valor) or valor == "" or valor != valor:
        return None
    if isinstance(valor, (pd.Timestamp, datetime)):
        ts = pd.Timestamp(valor)
        return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
    s = str(valor).strip()
    # Tentar pandas primeiro (robusto para ISO)
    try:
        ts = pd.to_datetime(s, utc=True)
        return ts
    except Exception:
        pass
    # Tentar formatos manuais
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            return pd.Timestamp(dt, tz="UTC")
        except ValueError:
            continue
    log.warning(f"Data não reconhecida: '{s}'")
    return None

def normalizar_coluna_data(series: pd.Series) -> pd.Series:
    """Aplica normalizar_data em toda uma coluna."""
    return series.apply(normalizar_data)


# ─── NORMALIZAÇÃO DE NSU ──────────────────────────────────────────────────────
def normalizar_nsu(series: pd.Series) -> pd.Series:
    """NSU: strip, sem zeros à esquerda, string."""
    return series.astype(str).str.strip().str.lstrip("0").replace("", "0")


# ─── NORMALIZAÇÃO DE VALORES ──────────────────────────────────────────────────
def normalizar_valor(series: pd.Series) -> pd.Series:
    """Garante float64 com 2 casas decimais."""
    return pd.to_numeric(series, errors="coerce").round(2)


# ─── DEDUPLICAÇÃO ─────────────────────────────────────────────────────────────
def deduplicar(df: pd.DataFrame, chave: str, criterio: str = "first",
               log_label: str = "") -> pd.DataFrame:
    """
    Remove duplicatas por chave. Registra quantas foram removidas.
    criterio: 'first' (mantém primeiro) | 'last' | False (marca todas)
    """
    total_antes = len(df)
    df_dedup = df.drop_duplicates(subset=[chave], keep=criterio).copy()
    removidos = total_antes - len(df_dedup)
    if removidos > 0:
        log.warning(f"[{log_label}] Deduplicação: {removidos} registros removidos "
                    f"por NSU duplicado ({total_antes} → {len(df_dedup)})")
    return df_dedup


# ─── STATUS DE CONCILIAÇÃO ────────────────────────────────────────────────────
STATUS = {
    "CONCILIADO":          "CONCILIADO",
    "PENDENTE":            "PENDENTE",
    "DIVERGENTE":          "DIVERGENTE",
    "SEM_PAR":             "SEM_PAR",
    "EXCLUIDO_NEGADA":     "EXCLUIDO_NEGADA",
    "EXCLUIDO_EXPIRADA":   "EXCLUIDO_EXPIRADA",
    "ANOMALIA":            "ANOMALIA",
}

# ─── CARREGAMENTO PADRONIZADO ─────────────────────────────────────────────────
# Caminho da pasta data/ relativo à raiz do projeto
# Funciona em qualquer computador automaticamente
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")

def carregar_base_interna(deduplicar_nsu: bool = True) -> pd.DataFrame:
    path = os.path.join(DATA_DIR, "BASE_INTERNA.csv")
    df = pd.read_csv(path, low_memory=False)
    df["NSU_KEY"] = normalizar_nsu(df["NSU"].astype(str))
    df["VALOR_BRUTO"] = normalizar_valor(df["VALOR_BRUTO"])
    df["DATA_NORM"] = normalizar_coluna_data(df["DATA_HORA_TRANSACAO"])
    df["DATA_ARQ_NORM"] = normalizar_coluna_data(df["DATA_ARQUIVO_ORIGEM"])
    if deduplicar_nsu:
        df = deduplicar(df, "NSU_KEY", log_label="BASE_INTERNA")
    log.info(f"BASE_INTERNA carregada: {len(df):,} registros")
    return df

def carregar_base_interna_debitos() -> pd.DataFrame:
    path = os.path.join(DATA_DIR, "BASE_INTERNA_DEBITOS.csv")
    df = pd.read_csv(path, low_memory=False)
    df["NSU_KEY"] = normalizar_nsu(df["NSU"].astype(str))
    df["VALOR_BRUTO"] = normalizar_valor(df["VALOR_BRUTO"])
    df["VALOR_PARCELA"] = normalizar_valor(df["VALOR_PARCELA"])
    df["DATA_NORM"] = normalizar_coluna_data(df["DATA_HORA_TRANSACAO"])
    log.info(f"BASE_INTERNA_DEBITOS carregada: {len(df):,} registros")
    return df

def carregar_third_party(agrupar_por_nsu: bool = False) -> pd.DataFrame:
    path = os.path.join(DATA_DIR, "THIRD_PARTY.csv")
    df = pd.read_csv(path, low_memory=False)
    df["NSU_KEY"] = normalizar_nsu(df["NSU"].astype(str))
    df["VALOR_BRUTO"] = normalizar_valor(df["VALOR_BRUTO"])
    df["DATA_NORM"] = normalizar_coluna_data(df["DATA_HORA_AUTORIZACAO"])
    df["DATA_CAPTURA_NORM"] = normalizar_coluna_data(df["DATA_HORA_CAPTURA"])
    if agrupar_por_nsu:
        # Agrupar parcelas: cabeçalho da transação (primeira linha por NSU)
        df = df.sort_values(["NSU_KEY", "PARCELA_NUM"])
        df_head = df.groupby("NSU_KEY").first().reset_index()
        df_soma = df.groupby("NSU_KEY")["VALOR_BRUTO"].sum().reset_index()
        df_soma.columns = ["NSU_KEY", "VALOR_BRUTO_TOTAL"]
        df_head = df_head.merge(df_soma, on="NSU_KEY", how="left")
        log.info(f"THIRD_PARTY agrupado por NSU: {len(df_head):,} transações únicas")
        return df_head
    log.info(f"THIRD_PARTY carregada: {len(df):,} registros")
    return df

def carregar_debitos_third_party() -> pd.DataFrame:
    path = os.path.join(DATA_DIR, "DEBITOS_THIRD_PARTY.csv")
    df = pd.read_csv(path, low_memory=False)
    df["NSU_KEY"] = normalizar_nsu(df["NSU"].astype(str))
    df["VALOR_BRUTO"] = normalizar_valor(df["VALOR_BRUTO"])
    df["VALOR_PARCELA"] = normalizar_valor(df["VALOR_PARCELA"])
    df["DATA_NORM"] = normalizar_coluna_data(df["DATA_HORA_AUTORIZACAO"])
    log.info(f"DEBITOS_THIRD_PARTY carregada: {len(df):,} registros")
    return df
