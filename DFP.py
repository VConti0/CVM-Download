# ----------------------------
# VERIFICAÇÃO E INSTALAÇÃO DE DEPENDÊNCIAS
# ----------------------------
import sys
import subprocess
import os

# Lista de pacotes necessários
required_packages = ['pandas', 'requests']

print("Verificando dependências...")
for package in required_packages:
    try:
        # Tenta importar o pacote
        __import__(package)
    except ImportError:
        # Se falhar, instala o pacote usando o pip do interpretador atual
        print(f"A dependência '{package}' não foi encontrada. Instalando...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        except subprocess.CalledProcessError as e:
            print(f"ERRO: Falha ao instalar '{package}'.")
            print("Por favor, instale manualmente usando: pip install", package)
            input("Pressione ENTER para encerrar...")
            sys.exit(1) # Encerra o script com erro

print("Todas as dependências estão satisfeitas.\n")
# ----------------------------
# FIM DA VERIFICAÇÃO
# ----------------------------

import os
import requests
import zipfile
import glob
import pandas as pd

# ----------------------------
# CONFIGURAÇÕES
# ----------------------------
import os

usuario = os.getlogin()
base_folder = rf"C:\Users\{usuario}\Downloads\cvm"

# Nome inicial da pasta
dest_folder = os.path.join(base_folder, "DFP")

while os.path.exists(dest_folder):
    print(f"A pasta {dest_folder} já existe.")
    nova_pasta = input("Digite um novo nome para a pasta: ").strip()
    dest_folder = os.path.join(base_folder, nova_pasta)

# Cria a pasta
os.makedirs(dest_folder)
print(f"Pasta criada com sucesso em: {dest_folder}")


anos = range(2024, 2026)
tipos_arquivos = ["dfp_cia_aberta_BPA_con", "dfp_cia_aberta_BPA_ind",
                  "dfp_cia_aberta_DRE_con", "dfp_cia_aberta_DRE_ind"]
url_base = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS"

# ----------------------------
# DOWNLOAD E EXTRAÇÃO
# ----------------------------
for ano in anos:
    zip_filename = f"dfp_cia_aberta_{ano}.zip"
    zip_path = os.path.join(dest_folder, zip_filename)
    url = f"{url_base}/{zip_filename}"

    if not os.path.exists(zip_path):
        print(f"⬇️ Baixando {zip_filename}...")
        resp = requests.get(url)
        if resp.status_code == 200:
            with open(zip_path, "wb") as f:
                f.write(resp.content)
            print(f"📥 {zip_filename} baixado com sucesso.")
        else:
            print(f"Erro ao baixar {zip_filename}: {resp.status_code}")
            continue

    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(dest_folder)
        print(f"📂 {zip_filename} extraído.")
    except Exception as e:
        print(f"Erro ao extrair {zip_filename}: {e}")

print("\nTodos os downloads e extrações foram processados.\n")

# ----------------------------
# PROCESSAMENTO DOS ARQUIVOS
# ----------------------------
for tipo in tipos_arquivos:
    print(f"\n⚙️ Iniciando o processamento para {tipo}...")
    dfs = []

    for ano in anos:
        padrao = os.path.join(dest_folder, f"*{tipo}_{ano}.csv")
        arquivos = glob.glob(padrao)
        if not arquivos:
            print(f"Nenhum arquivo encontrado para {tipo} no ano {ano}.")
            continue

        for csv_file in arquivos:
            try:
                df = pd.read_csv(csv_file, encoding="ISO-8859-1", sep=";")
                df.columns = [c.strip().upper() for c in df.columns]

                if "ORDEM_EXERC" in df.columns:
                    df["ORDEM_EXERC"] = df["ORDEM_EXERC"].astype(str).str.strip().str.upper()
                if "CD_CONTA" in df.columns:
                    df["CD_CONTA"] = df["CD_CONTA"].astype(str).str.strip()
                if "DT_REFER" in df.columns:
                    df["DT_REFER"] = pd.to_datetime(df["DT_REFER"], errors="coerce")
                if "CNPJ_CIA" in df.columns:
                    df["CNPJ_CIA"] = df["CNPJ_CIA"].astype(str).str.strip()

                # FILTRO E ORDENAÇÃO
                if tipo.startswith("BPA"):
                    df = df[(df["ORDEM_EXERC"] == "ÚLTIMO") & (df["CD_CONTA"] == "1")]
                elif tipo.startswith("DRE"):
                    df = df[(df["ORDEM_EXERC"] == "ÚLTIMO") & (df["CD_CONTA"] == "3.01")]

                if df.empty:
                    continue

                # TRATAMENTO DE VALORES
                def limpar_valor(x):
                    if pd.isna(x):
                        return None
                    x = str(x).strip()
                    if x.endswith("0000000000"):
                        x = x[:-10]
                    return x

                if "VL_CONTA" in df.columns:
                    df["VALOR_TRATADO"] = df["VL_CONTA"].apply(limpar_valor)
                    df["VALOR_TRATADO"] = pd.to_numeric(df["VALOR_TRATADO"], errors="coerce")

                    if "ESCALA_MOEDA" in df.columns:
                        df["ESCALA_MOEDA"] = df["ESCALA_MOEDA"].astype(str).str.strip().str.upper()

                        def ajustar_escala(row):
                            if row["ESCALA_MOEDA"] == "MIL":
                                return row["VALOR_TRATADO"] * 1000
                            return row["VALOR_TRATADO"]

                        df["VALOR_TRATADO"] = df.apply(ajustar_escala, axis=1)

                dfs.append(df)
                print(f"⚙️ {tipo} {ano} processado com sucesso.")

            except Exception as e:
                print(f"Erro ao processar {csv_file}: {e}")

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)

        # ORDENAÇÃO FINAL
        combined_df = combined_df.sort_values(
            by=["CNPJ_CIA", "DT_REFER", "ORDEM_EXERC"],
            ascending=[True, False, False]
        )

        output_file = os.path.join(dest_folder, f"{tipo}_combinado.csv")
        combined_df.to_csv(output_file, index=False, sep=";", encoding="ISO-8859-1", decimal=",")
        print(f"📁 Arquivo salvo em: {output_file}")
    else:
        print(f"Nenhum dado válido encontrado para {tipo}.")
# ----------------------------
# MENSAGEM FINAL E PAUSA
# ----------------------------
print("\nProcessamento concluído com sucesso!")
input("Pressione ENTER para encerrar...")
    