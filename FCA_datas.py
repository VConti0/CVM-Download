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

usuario = os.getlogin()
base_folder = rf"C:\Users\{usuario}\Downloads\cvm"
dest_folder = os.path.join(base_folder, "FCA")

# Se a pasta existir, pede um novo nome
while os.path.exists(dest_folder):
    print(f"A pasta {dest_folder} já existe.")
    nova_pasta = input("Digite um novo nome para a pasta: ").strip()
    dest_folder = os.path.join(base_folder, nova_pasta)

os.makedirs(dest_folder)
print(f"Pasta criada com sucesso em: {dest_folder}")

anos = range(2020, 2026)
url_base = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/fca/DADOS"

# ----------------------------
# DOWNLOAD E EXTRAÇÃO
# ----------------------------
for ano in anos:
    zip_filename = f"fca_cia_aberta_{ano}.zip"
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

print("\n⚙️ Todos os downloads e extrações foram processados.\n")

# ----------------------------
# PROCESSAMENTO DOS ARQUIVOS CSV
# ----------------------------
dfs = []

for ano in anos:
    padrao = os.path.join(dest_folder, f"fca_cia_aberta_{ano}.csv")
    arquivos = glob.glob(padrao)
    if not arquivos:
        print(f"Nenhum arquivo encontrado para o ano {ano}.")
        continue

    for csv_file in arquivos:
        try:
            df = pd.read_csv(csv_file, encoding="ISO-8859-1", sep=";")
            df.columns = [c.strip().upper() for c in df.columns]

            # Converte datas para datetime
            if "DT_REFER" in df.columns:
                df["DT_REFER"] = pd.to_datetime(df["DT_REFER"], errors="coerce")
            if "DT_RECEB" in df.columns:
                df["DT_RECEB"] = pd.to_datetime(df["DT_RECEB"], errors="coerce")
            if "CNPJ_CIA" in df.columns:
                df["CNPJ_CIA"] = df["CNPJ_CIA"].astype(str).str.strip()

            dfs.append(df)
            print(f"📁 FCA do ano {ano} carregado com sucesso.")

        except Exception as e:
            print(f"Erro ao processar {csv_file}: {e}")

if dfs:
    combined_df = pd.concat(dfs, ignore_index=True)

    # Ordenação final: CNPJ_CIA crescente, DT_REFER decrescente, DT_RECEB decrescente
    combined_df = combined_df.sort_values(
        by=["CNPJ_CIA", "DT_REFER", "DT_RECEB"],
        ascending=[True, False, False]
    )

    output_file = os.path.join(dest_folder, "fca_cia_aberta_combinado.csv")
    combined_df.to_csv(output_file, index=False, sep=";", encoding="ISO-8859-1", decimal=",")
    print(f"\nFCA combinado e ordenado salvo em: {output_file}")
else:
    print("Nenhum FCA CSV foi encontrado para processar.")
# ----------------------------
# MENSAGEM FINAL E PAUSA
# ----------------------------
print("\n✅ Processamento concluído com sucesso!")
input("Pressione ENTER para encerrar...")