import streamlit as st
import os
import json
import requests
from datetime import datetime
import pandas as pd
import io

# Importações para Google Drive API
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# ---------------------- CONFIGURAÇÕES GERAIS ----------------------
st.set_page_config(page_title="Analista Financeiro Interativo - LJP", layout="centered")
st.title("📊 Analista Financeiro Interativo - Laboratório João Paulo")

# Credenciais e chaves obtidas via st.secrets
openai_key = st.secrets["OPENAI_API_KEY"]
zapi_user = st.secrets["ZAPI_USER"]
zapi_token = st.secrets["ZAPI_TOKEN"]
zapi_client_token = st.secrets["ZAPI_CLIENT_TOKEN"]
zapi_phone = st.secrets["ZAPI_PHONE"]

# Configurar credenciais do Google Cloud
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=["https://www.googleapis.com/auth/drive"]
)
drive_service = build("drive", "v3", credentials=credentials)

# ---------------------- IDS DAS PASTAS NO GOOGLE DRIVE ----------------------
pastas_ids = {
    "tabela_preco_convenios": "1FwE_Qdjv6ERsL3Dt9dUr4xrIbxzYxWkt",
    "tabela_custos_exames": "1GAoelILXNIMplq_g-jZkd9Gc_ebUFxT3",
    "producao_diaria_geral": "1yEba6h_GLE2NCFKPwq3U7JpDL503lm1z",
    "custo_geral": "1G3T4rXWRWBbz1vJrKQji3eM89F9mUbhi",
    "orcamentos_nao_convertidos": "1ANSQ3AKOsZWmCVm3-T2XclJNMnEkkWKR",
    "fidelidade": "19UkxnrFSY78PBVyeqyj5hmDtBXNN_OPC",
    "extratos_bancarios": "1FQV_1uXYy7jfp8LpZ0eRCjfLrRF9r5ba",
    "saida_gpt": "1prkf5SBpNc09LaE7R3_RY6ipsjkXwWqU",
    "planilha_mestre_saida": "ID_PLANILHA_MESTRA",  # Substitua pelo ID correto
    "exames_por_convenio": "1GIPM_lJ08RvBl3OW3YWKG2TaVob_xi0a",
    "exames_por_unidade": "1G1NjpOLs--NY-43MSAHOFw0FtRxqjhdj",
    "prazo_medio": "1Fzi9SVkQhUktCdjMCTD3Jmy1n8VWepEs",
    "estrategia_marketing": "1ipVIwAvtshUzZQp3wwZPCuAxZk862faq",
    "tributos_ljp": "1LOz99HHvH-VlRernKDuPmxeHEIfxJNFw",
    "tributos_lmg": "1CW28OATRpL7xK-IZwqOyQ47nRoi1wchK",
    "producao_diaria_por_convenio": "1ZxqhaYPYrzKq3iVV32TfoxRGKmvEA2Ci",
    "meta_mes": "1EJseWmWfhpPM0FQgnfQdYgabQ-xNbXPa"
}

# ---------------------- FUNÇÕES DE LEITURA DOS ARQUIVOS ----------------------
def buscar_csv_mais_recente(pasta_id):
    """
    Busca o arquivo CSV mais recente na pasta especificada.
    Nota: Para arquivos que englobam períodos maiores, o período é informado manualmente.
    """
    resultados = drive_service.files().list(
        q=f"'{pasta_id}' in parents and mimeType='text/csv'",
        orderBy='modifiedTime desc',
        fields='files(id, name)',
        pageSize=10
    ).execute()
    arquivos = resultados.get('files', [])
    if not arquivos:
        return None
    # Retorna o primeiro arquivo da lista (pode ser refinado conforme necessidade)
    file_id = arquivos[0]['id']
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    try:
        df = pd.read_csv(fh, encoding='latin1', sep=';')
        # Remover linhas que contenham a palavra "total" (ignorando duplicidade)
        df = df[~df.apply(lambda row: row.astype(str).str.lower().str.contains("total").any(), axis=1)]
    except Exception as e:
        st.error(f"Erro ao ler CSV: {e}")
        return None
    return df

def buscar_planilha_com_data(pasta_id, data_ref):
    """
    Busca um arquivo CSV na pasta cujo nome contenha a data de referência (formato YYYY-MM-DD).
    Se encontrado, retorna o DataFrame; caso contrário, retorna None.
    """
    resultados = drive_service.files().list(
        q=f"'{pasta_id}' in parents and mimeType='text/csv'",
        fields='files(id, name)',
        pageSize=10
    ).execute()
    arquivos = resultados.get('files', [])
    data_str = data_ref.strftime('%Y-%m-%d')
    for arq in arquivos:
        if data_str in arq['name']:
            file_id = arq['id']
            request = drive_service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            fh.seek(0)
            try:
                df = pd.read_csv(fh, encoding='latin1', sep=';')
                df = df[~df.apply(lambda row: row.astype(str).str.lower().str.contains("total").any(), axis=1)]
            except Exception as e:
                st.error(f"Erro ao ler CSV com data: {e}")
                return None
            return df
    return None
# ---------------------- FUNÇÕES DE PROCESSAMENTO ----------------------
def tratar_valores_numericos(df, colunas):
    """
    Converte as colunas especificadas para formato numérico.
    Garante que os separadores de milhar e decimal sejam tratados corretamente.
    """
    for col in colunas:
        df[col] = df[col].astype(str).str.replace('.', '', regex=False)
        df[col] = df[col].str.replace(',', '.', regex=False)
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

# ---------------------- FUNÇÕES PARA COMUNICAÇÃO COM GPT ----------------------
def enviar_ao_gpt(dados_json, periodo, opcoes_analise):
    """
    Envia os dados e parâmetros para a API do GPT com um prompt estruturado para análise estratégica.
    """
    prompt = (
        f"Você é o CFO do Laboratório João Paulo. Os dados a seguir referem-se ao período de {periodo['inicio']} a {periodo['fim']}.\n"
        f"Considere as seguintes regras e indicadores:\n"
        f"- Rentabilidade geral, por unidade e por convênio (tanto teórica quanto real).\n"
        f"- EBITDA, margem de contribuição, margem operacional e DRE.\n"
        f"- Ponto de equilíbrio real, calculado com base na média ponderada do preço de venda por exame, "
        f"para determinar quantos exames são necessários para cobrir os custos fixos.\n"
        f"- Ticket médio para convênios e particulares, % médio de desconto e taxa média de conversão.\n"
        f"- Fluxo de caixa projetado, real, necessidade de capital de giro, ciclo financeiro e índice de liquidez.\n"
        f"- Elasticidade do preço, analisando o impacto de variações de desconto na conversão e receita.\n"
        f"- Indicadores de volume: número de exames, número de pacientes, número de orçamentos gerados e perda financeira acumulada.\n"
        f"- Análise dos dados de marketing para gerar insights estratégicos (ROI, projeções de aumento de receita) – esses dados são complementares.\n"
        f"- Considerar as diferenças tributárias: LJP (Lucro Presumido) e LMG (Simples Nacional), onde os tributos incidem sobre o preço faturado.\n"
        f"- Caso especial da Unidade Domiciliar VIP: incluir 10% de comissão médica e R$60 por paciente para coleta.\n\n"
        f"Os dados recebidos estão organizados em diferentes planilhas (produção, custos, exames por convênio e unidade, prazo médio, "
        f"marketing, tributos, meta mensal, etc.).\n"
        f"Análises solicitadas: {', '.join(opcoes_analise)}.\n\n"
        f"Apresente um relatório estratégico com insights e projeções, explicando quais unidades ou convênios estão lucrativos ou não, "
        f"o que pode ser ajustado e as oportunidades de melhoria."
    )
    mensagens = [
        {"role": "system", "content": "Você é um consultor financeiro e estratégico para o Laboratório João Paulo."},
        {"role": "user", "content": prompt}
    ]
    payload = {
        "model": "gpt-4",
        "messages": mensagens
    }
    resposta = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {openai_key}"},
        json=payload
    )
    try:
        conteudo = resposta.json()["choices"][0]["message"]["content"]
    except Exception as e:
        conteudo = f"Erro na resposta do GPT: {e}"
    return conteudo

# ---------------------- FUNÇÕES PARA ENVIO VIA ZAPI (WhatsApp) ----------------------
def enviar_zapi(mensagem):
    """
    Envia mensagem via ZAPI para WhatsApp usando o formato e headers exigidos.
    """
    url = f"https://api.z-api.io/instances/{zapi_user}/token/{zapi_token}/send-text"
    headers = {
        "Content-Type": "application/json",
        "client-token": zapi_client_token
    }
    payload = {
        "phone": zapi_phone,
        "message": mensagem
    }
    try:
        resp = requests.post(url, headers=headers, json=payload)
        return resp.json()
    except Exception as e:
        st.error(f"Erro ao enviar mensagem via ZAPI: {e}")
        return None
# ---------------------- INTERFACE DO USUÁRIO ----------------------
st.sidebar.header("Configurações da Análise")

# Seleção do período manualmente
data_inicio = st.sidebar.date_input("📅 Data Inicial da Análise", datetime.today())
data_fim = st.sidebar.date_input("📅 Data Final da Análise", datetime.today())

# Seleção dos tipos de análise desejadas
st.sidebar.subheader("Selecione as Análises Desejadas")
opcoes_analise = []
if st.sidebar.checkbox("Rentabilidade (Geral, Unidade, Convênio)"):
    opcoes_analise.append("Rentabilidade")
if st.sidebar.checkbox("EBITDA"):
    opcoes_analise.append("EBITDA")
if st.sidebar.checkbox("Margem de Contribuição/Operacional"):
    opcoes_analise.append("Margem")
if st.sidebar.checkbox("Ponto de Equilíbrio"):
    opcoes_analise.append("Ponto de Equilíbrio")
if st.sidebar.checkbox("Ticket Médio (Convênio e Particular)"):
    opcoes_analise.append("Ticket Médio")
if st.sidebar.checkbox("% Médio de Desconto"):
    opcoes_analise.append("Desconto Médio")
if st.sidebar.checkbox("Taxa Média de Conversão"):
    opcoes_analise.append("Taxa de Conversão")
if st.sidebar.checkbox("Fluxo de Caixa (Projetado e Real)"):
    opcoes_analise.append("Fluxo de Caixa")
if st.sidebar.checkbox("Capital de Giro, Ciclo Financeiro e Liquidez"):
    opcoes_analise.append("Indicadores Financeiros")
if st.sidebar.checkbox("Elasticidade do Preço"):
    opcoes_analise.append("Elasticidade")
if st.sidebar.checkbox("Indicadores de Volume (Exames, Pacientes, Orçamentos)"):
    opcoes_analise.append("Volume e Conversão")
if st.sidebar.checkbox("Análise de Marketing"):
    opcoes_analise.append("Marketing")
if st.sidebar.checkbox("Meta Mensal (Diferença para bater a meta)"):
    opcoes_analise.append("Meta Mensal")

# Botão para iniciar a análise
if st.button("▶️ Rodar Análise"):
    st.info("🔄 Carregando dados do Google Drive...")
    
    dados = {}
    
    # Produção diária geral
    df_producao_geral = buscar_csv_mais_recente(pastas_ids["producao_diaria_geral"])
    if df_producao_geral is not None:
        colunas_numericas = [col for col in df_producao_geral.columns if any(p in col.lower() for p in ['valor', 'custo', 'fatur'])]
        df_producao_geral = tratar_valores_numericos(df_producao_geral, colunas_numericas)
        dados["producao_geral"] = df_producao_geral.to_dict(orient='records')
    else:
        st.warning("Produção diária geral não encontrada.")
    
    # Planilha de custo geral (que possui data)
    df_custo_geral = buscar_planilha_com_data(pastas_ids["custo_geral"], data_inicio)
    if df_custo_geral is not None:
        colunas_numericas = [col for col in df_custo_geral.columns if any(p in col.lower() for p in ['custo', 'valor', 'despesa'])]
        df_custo_geral = tratar_valores_numericos(df_custo_geral, colunas_numericas)
        dados["custo_geral"] = df_custo_geral.to_dict(orient='records')
    else:
        st.warning("Planilha de custo geral não encontrada.")
    
    # Orcamentos não convertidos (diária)
    df_orcamentos = buscar_csv_mais_recente(pastas_ids["orcamentos_nao_convertidos"])
    if df_orcamentos is not None:
        colunas_numericas = [col for col in df_orcamentos.columns if any(p in col.lower() for p in ['valor', 'desconto', 'orc'])]
        df_orcamentos = tratar_valores_numericos(df_orcamentos, colunas_numericas)
        dados["orcamentos"] = df_orcamentos.to_dict(orient='records')
    else:
        st.warning("Orçamentos não convertidos não encontrados.")
    
    # Fidelidade (atualizada semanal/quinzenal)
    df_fidelidade = buscar_csv_mais_recente(pastas_ids["fidelidade"])
    if df_fidelidade is not None:
        colunas_numericas = [col for col in df_fidelidade.columns if any(p in col.lower() for p in ['frequencia', 'qtd'])]
        df_fidelidade = tratar_valores_numericos(df_fidelidade, colunas_numericas)
        dados["fidelidade"] = df_fidelidade.to_dict(orient='records')
    else:
        st.warning("Planilha de fidelidade não encontrada.")
    
    # Extratos bancários (semanais)
    df_extratos = buscar_csv_mais_recente(pastas_ids["extratos_bancarios"])
    if df_extratos is not None:
        colunas_numericas = [col for col in df_extratos.columns if any(p in col.lower() for p in ['valor', 'saldo', 'mov'])]
        df_extratos = tratar_valores_numericos(df_extratos, colunas_numericas)
        dados["extratos"] = df_extratos.to_dict(orient='records')
    else:
        st.warning("Extratos bancários não encontrados.")
    
    # Exames por convênio
    df_exames_conv = buscar_csv_mais_recente(pastas_ids["exames_por_convenio"])
    if df_exames_conv is not None:
        colunas_numericas = [col for col in df_exames_conv.columns if any(p in col.lower() for p in ['quant', 'valor', 'fatur'])]
        df_exames_conv = tratar_valores_numericos(df_exames_conv, colunas_numericas)
        dados["exames_conv"] = df_exames_conv.to_dict(orient='records')
    else:
        st.warning("Planilha de exames por convênio não encontrada.")
    
    # Exames por unidade
    df_exames_unid = buscar_csv_mais_recente(pastas_ids["exames_por_unidade"])
    if df_exames_unid is not None:
        colunas_numericas = [col for col in df_exames_unid.columns if any(p in col.lower() for p in ['quant', 'valor', 'fatur'])]
        df_exames_unid = tratar_valores_numericos(df_exames_unid, colunas_numericas)
        dados["exames_unid"] = df_exames_unid.to_dict(orient='records')
    else:
        st.warning("Planilha de exames por unidade não encontrada.")
    
    # Prazo médio de recebimento
    df_pmr = buscar_csv_mais_recente(pastas_ids["prazo_medio"])
    if df_pmr is not None:
        colunas_numericas = [col for col in df_pmr.columns if "pmr" in col.lower()]
        df_pmr = tratar_valores_numericos(df_pmr, colunas_numericas)
        dados["pmr"] = df_pmr.to_dict(orient='records')
    else:
        st.warning("Planilha de prazo médio de recebimento não encontrada.")
    
    # Estratégia de Marketing
    df_marketing = buscar_csv_mais_recente(pastas_ids["estrategia_marketing"])
    if df_marketing is not None:
        colunas_numericas = [col for col in df_marketing.columns if any(p in col.lower() for p in ['valor', 'roi'])]
        df_marketing = tratar_valores_numericos(df_marketing, colunas_numericas)
        dados["marketing"] = df_marketing.to_dict(orient='records')
    else:
        st.warning("Planilha de estratégia de marketing não encontrada.")
    
    # Tributos LJP e LMG
    df_tributos_ljp = buscar_csv_mais_recente(pastas_ids["tributos_ljp"])
    if df_tributos_ljp is not None:
        colunas_numericas = [col for col in df_tributos_ljp.columns if any(p in col.lower() for p in ['base', 'aliq'])]
        df_tributos_ljp = tratar_valores_numericos(df_tributos_ljp, colunas_numericas)
        dados["tributos_ljp"] = df_tributos_ljp.to_dict(orient='records')
    else:
        st.warning("Planilha de tributos LJP não encontrada.")
    
    df_tributos_lmg = buscar_csv_mais_recente(pastas_ids["tributos_lmg"])
    if df_tributos_lmg is not None:
        colunas_numericas = [col for col in df_tributos_lmg.columns if any(p in col.lower() for p in ['base', 'aliq'])]
        df_tributos_lmg = tratar_valores_numericos(df_tributos_lmg, colunas_numericas)
        dados["tributos_lmg"] = df_tributos_lmg.to_dict(orient='records')
    else:
        st.warning("Planilha de tributos LMG não encontrada.")
    
    # Produção diária por convênio
    df_producao_conv = buscar_csv_mais_recente(pastas_ids["producao_diaria_por_convenio"])
    if df_producao_conv is not None:
        colunas_numericas = [col for col in df_producao_conv.columns if any(p in col.lower() for p in ['quant', 'valor', 'fatur'])]
        df_producao_conv = tratar_valores_numericos(df_producao_conv, colunas_numericas)
        dados["producao_conv"] = df_producao_conv.to_dict(orient='records')
    else:
        st.warning("Planilha de produção diária por convênio não encontrada.")
    
    # Meta do mês
    df_meta = buscar_csv_mais_recente(pastas_ids["meta_mes"])
    if df_meta is not None:
        colunas_numericas = [col for col in df_meta.columns if any(p in col.lower() for p in ['meta', 'valor'])]
        df_meta = tratar_valores_numericos(df_meta, colunas_numericas)
        dados["meta_mes"] = df_meta.to_dict(orient='records')
    else:
        st.warning("Planilha de meta do mês não encontrada.")
    
    # Define o período da análise
    periodo = {
        "inicio": data_inicio.strftime('%Y-%m-%d'),
        "fim": data_fim.strftime('%Y-%m-%d')
    }
    
    st.success("✅ Dados carregados. Enviando dados para análise estratégica...")
    
    # ---------------------- ENVIO AO GPT ----------------------
    resposta_gpt = enviar_ao_gpt(dados, periodo, opcoes_analise)
    
    st.subheader("📄 Relatório Estratégico Gerado")
    st.text_area("Relatório GPT", resposta_gpt, height=300)
# ---------------------- SALVANDO O RELATÓRIO NO DRIVE ----------------------
nome_arquivo = f"analise_{periodo['inicio']}_a_{periodo['fim']}.txt"
conteudo_relatorio = resposta_gpt  # Relatório com comentários estratégicos

conteudo_bytes = io.BytesIO(conteudo_relatorio.encode('utf-8'))
media_upload = MediaIoBaseUpload(conteudo_bytes, mimetype='text/plain')
try:
    drive_service.files().create(
        body={"name": nome_arquivo, "parents": [pastas_ids['saida_gpt']]},
        media_body=media_upload,
        fields="id"
    ).execute()
    st.success("📁 Relatório salvo no Google Drive.")
except Exception as e:
    st.error(f"Erro ao salvar relatório: {e}")

# ---------------------- ATUALIZANDO AS PLANILHAS DE SAÍDA ----------------------
# Exemplo: Criar planilha de indicadores consolidados (vários indicadores por período).
# Aqui simulamos os indicadores; na prática, você deverá implementar as fórmulas específicas.

indicadores_consolidados = {
    "Data_Analise": [f"{periodo['inicio']} a {periodo['fim']}"],
    "Rentabilidade_Geral": [0.15],
    "EBITDA": [12000],
    "Margem_Contribuicao": [0.30],
    "PontoEquilibrio_Geral": [8000],
    "PontoEquilibrio_PorExame": [round(8000 /  (130), 2)],  # Exemplo: número de exames para cobrir custos fixos, usando ticket médio de 130
    "TicketMedio_Conv": [130],
    "TicketMedio_Part": [150],
    "FluxoCaixa_Proj": [5000],
    "CapitalGiro": [3000],
    "CicloFinanceiro": [45],
    "IndiceLiquidez": [1.8],
    "ElasticidadePreco": [0.85],
    "DescontoMedio": [0.10],
    "TaxaConversao": [0.25],
    "NumExames_Conv": [250],
    "NumExames_Part": [180],
    "NumPacientes_Conv": [200],
    "NumPacientes_Part": [150],
    "NumOrcamentos": [80],
    "PerdaFinanceira": [2500],
    "Meta_Atingida": [False],
    "FaltanteMeta": [3500]
}
df_indicadores = pd.DataFrame(indicadores_consolidados)

# Atualização da planilha de indicadores consolidados
csv_buffer = io.StringIO()
df_indicadores.to_csv(csv_buffer, index=False)
conteudo_csv = io.BytesIO(csv_buffer.getvalue().encode('utf-8'))
media_upload_csv = MediaIoBaseUpload(conteudo_csv, mimetype='text/csv')
try:
    drive_service.files().create(
        body={"name": f"indicadores_consolidados_{periodo['inicio']}_a_{periodo['fim']}.csv", "parents": [pastas_ids['planilha_mestre_saida']]},
        media_body=media_upload_csv,
        fields="id"
    ).execute()
    st.success("📊 Planilha de indicadores consolidada atualizada.")
except Exception as e:
    st.error(f"Erro ao atualizar planilha mestre: {e}")

# ---------------------- ENVIO VIA ZAPI (WhatsApp) ----------------------
resumo_whatsapp = (
    f"📊 Relatório Financeiro LJP\n\n"
    f"Período: {periodo['inicio']} a {periodo['fim']}\n"
    f"Rentabilidade Geral: 15%\n"
    f"EBITDA: R$12.000\n"
    f"Fluxo de Caixa Projetado: R$5.000\n"
    f"Meta: Faltam R$3.500 para bater a meta\n"
    f"...\n\n"
    f"Verifique o relatório completo no Drive."
)
envio_zapi = enviar_zapi(resumo_whatsapp)
if envio_zapi is not None:
    st.success("📬 Resumo enviado via WhatsApp!")
else:
    st.error("Erro ao enviar resumo via WhatsApp.")
