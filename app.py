import streamlit as st
import os
import json
import requests
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
import pandas as pd
import io

# Teste de leitura das chaves do secrets (pode remover depois)
st.write("🔐 Chaves disponíveis no st.secrets:", list(st.secrets.keys()))

# ---------------------- CONFIGURAÇÕES ----------------------
openai_key = st.secrets["OPENAI_API_KEY"]
zapi_user = st.secrets["ZAPI_USER"]
zapi_token = st.secrets["ZAPI_TOKEN"]
zapi_client_token = st.secrets["ZAPI_CLIENT_TOKEN"]
zapi_phone = st.secrets["ZAPI_PHONE"]
gpt_model = "gpt-4"
gpt_url = "https://api.openai.com/v1/chat/completions"

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=["https://www.googleapis.com/auth/drive"]
)
drive_service = build("drive", "v3", credentials=credentials)

pastas_ids = {
    "tabela_preco_convenios": "1FwE_Qdjv6ERsL3Dt9dUr4xrIbxzYxWkt",
    "tabela_custos_exames": "1GAoelILXNIMplq_g-jZkd9Gc_ebUFxT3",
    "producao_diaria_geral": "1yEba6h_GLE2NCFKPwq3U7JpDL503lm1z",
    "saida_gpt": "1prkf5SBpNc09LaE7R3_RY6ipsjkXwWqU"
}

# ---------------------- FUNÇÕES ----------------------

def buscar_csv_mais_recente(pasta_id):
    resultados = drive_service.files().list(
        q=f"'{pasta_id}' in parents and mimeType='text/csv'",
        orderBy='modifiedTime desc',
        fields='files(id, name)',
        pageSize=1
    ).execute()
    arquivos = resultados.get('files', [])
    if not arquivos:
        return None
    file_id = arquivos[0]['id']
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return pd.read_csv(fh, encoding='latin1', sep=';')

def tratar_valores_numericos(df, colunas):
    for col in colunas:
        df[col] = df[col].astype(str).str.replace('.', '', regex=False)
        df[col] = df[col].str.replace(',', '.', regex=False)
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def enviar_ao_gpt(dados_json):
    mensagens = [
        {"role": "system", "content": "Você é um analista financeiro especializado em laboratórios de análises clínicas."},
        {"role": "user", "content": f"Analise os seguintes dados financeiros e gere insights estratégicos:\n\n{json.dumps(dados_json)}"}
    ]
    resposta = requests.post(
        gpt_url,
        headers={"Authorization": f"Bearer {openai_key}"},
        json={"model": gpt_model, "messages": mensagens}
    )
    return resposta.json()["choices"][0]["message"]["content"]

def enviar_zapi(mensagem):
    url = f"https://api.z-api.io/instances/{zapi_user}/token/{zapi_token}/send-text"
    payload = {
        "phone": zapi_phone,
        "message": mensagem,
        "clientToken": zapi_client_token
    }
    headers = {
        "Content-Type": "application/json"
    }
    resposta = requests.post(url, json=payload, headers=headers)
    if resposta.status_code == 200:
        st.success("✅ Mensagem enviada com sucesso via Z-API.")
    else:
        st.error(f"❌ Erro ao enviar mensagem via Z-API: {resposta.status_code}")
        st.text(resposta.text)

# ---------------------- INTERFACE ----------------------

st.set_page_config(page_title="Análise Financeira LJP", layout="centered")
st.title("📊 Análise Financeira - Laboratório João Paulo")

col1, col2 = st.columns(2)
periodo = col1.date_input("📅 Data da Análise", datetime.today())
tipo_analise = col2.selectbox("📂 Tipo de Análise", ["Completa", "Por Unidade", "Por Convênio"])

if st.button("▶️ Rodar Análise"):
    st.info("🔄 Lendo planilhas do Google Drive...")
    dados = {}
    for chave, id_pasta in pastas_ids.items():
        df = buscar_csv_mais_recente(id_pasta)
        if df is not None:
            colunas_numericas = [
                col for col in df.columns if any(p in col.lower()
                for p in ['valor', 'fat', 'bruto', 'líquido', 'desconto', 'receita', 'custo'])
            ]
            df = tratar_valores_numericos(df, colunas_numericas)
            dados[chave] = df.to_dict(orient='records')

    st.success("✅ Dados carregados. Enviando ao GPT...")
    resposta = enviar_ao_gpt(dados)
    st.text_area("📄 Resposta do GPT", resposta, height=300)

    # Salvar TXT no Drive
    nome_arquivo = f"analise_{periodo.strftime('%Y-%m-%d')}.txt"
    conteudo = io.BytesIO(resposta.encode())
    media = MediaIoBaseUpload(conteudo, mimetype='text/plain')
    drive_service.files().create(
        body={"name": nome_arquivo, "parents": [pastas_ids['saida_gpt']]},
        media_body=media,
        fields="id"
    ).execute()

    # WhatsApp
    resumo = "📬 Análise concluída com sucesso! Acesse o relatório completo no Google Drive."
    enviar_zapi(resumo)
    st.success("📦 Resultado enviado ao WhatsApp!")
