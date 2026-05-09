import json
import os

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

path_image = "artifacts/trivy-image.json"
path_fs = "artifacts/trivy-fs.json"
path_config = "artifacts/trivy-config.json"
os.makedirs("artifacts", exist_ok=True)

sns.set_palette("muted")


def count_by_severity(trivy_json):
    severities = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    counts = {s: 0 for s in severities}
    if "Results" in trivy_json:
        for res in trivy_json["Results"]:
            for v in res.get("Vulnerabilities") or []:
                sev = v.get("Severity")
                if sev in counts:
                    counts[sev] += 1
            for m in res.get("Misconfigurations") or []:
                sev = m.get("Severity")
                if sev in counts:
                    counts[sev] += 1
    return counts


def load_json_or_empty(path):
    if os.path.isfile(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


def add_labels(ax):
    for p in ax.patches:
        height = p.get_height()
        if height > 0:
            ax.annotate(f'{int(height)}',
                        (p.get_x() + p.get_width() / 2., height),
                        ha='center', va='bottom',
                        fontsize=9, color='black')


# Tradução para português
traduzir = {
    "LOW": "Baixa",
    "MEDIUM": "Média",
    "HIGH": "Alta",
    "CRITICAL": "Crítica"
}

img = load_json_or_empty(path_image)
fs = load_json_or_empty(path_fs)
cfg = load_json_or_empty(path_config)

sev_img = count_by_severity(img)
sev_fs = count_by_severity(fs)
sev_cfg = count_by_severity(cfg)

df = pd.DataFrame(
    [{"categoria": "imagem", "severidade": k, "contagem": v} for k, v in sev_img.items()] +
    [{"categoria": "fs", "severidade": k, "contagem": v} for k, v in sev_fs.items()] +
    [{"categoria": "config", "severidade": k, "contagem": v} for k, v in sev_cfg.items()]
)
df["severidade"] = df["severidade"].map(traduzir)

plt.figure(figsize=(10, 6))
ax = sns.barplot(data=df, x="categoria", y="contagem", hue="severidade")
add_labels(ax)
plt.title("Vulnerabilidades por categoria e severidade")
plt.ylabel("Quantidade")
plt.xlabel("Categoria")
plt.tight_layout()
plt.savefig("artifacts/epv_categoria_severidade.png")
plt.savefig("artifacts/epv_categoria_severidade.pdf")
plt.close()

df_total = df.groupby("severidade")["contagem"].sum().reset_index()
plt.figure(figsize=(8, 5))
ax = sns.barplot(data=df_total, x="severidade", y="contagem")
add_labels(ax)
plt.title("Vulnerabilidades totais por severidade")
plt.tight_layout()
plt.savefig("artifacts/epv_por_severidade.png")
plt.savefig("artifacts/epv_por_severidade.pdf")
plt.close()

count_image = sum(sev_img[s] for s in ["MEDIUM", "HIGH", "CRITICAL"])
count_fs = sum(sev_fs[s] for s in ["MEDIUM", "HIGH", "CRITICAL"])
count_cfg = sum(sev_cfg[s] for s in ["MEDIUM", "HIGH", "CRITICAL"])

df2 = pd.DataFrame({"categoria": ["imagem", "fs", "config"], "contagem": [count_image, count_fs, count_cfg]})
plt.figure(figsize=(8, 5))
ax = sns.barplot(data=df2, x="categoria", y="contagem")
add_labels(ax)
plt.title("Contagem de vulnerabilidades por categoria (Média/Alta/Crítica)")
plt.tight_layout()
plt.savefig("artifacts/epv_por_categoria.png")
plt.savefig("artifacts/epv_por_categoria.pdf")
plt.close()

total = count_image + count_fs + count_cfg
df3 = pd.DataFrame({"tipo": ["imagem", "fs", "config", "total"], "contagem": [count_image, count_fs, count_cfg, total]})
plt.figure(figsize=(8, 5))
ax = sns.barplot(data=df3, x="tipo", y="contagem")
add_labels(ax)
plt.title("Vulnerabilidades totais + por categoria (Média/Alta/Crítica)")
plt.tight_layout()
plt.savefig("artifacts/epv_total_completo.png")
plt.savefig("artifacts/epv_total_completo.pdf")
plt.close()
