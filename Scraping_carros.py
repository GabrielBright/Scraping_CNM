import pandas as pd
import os
import asyncio
import sys
import logging
import re
from tqdm import tqdm
from playwright.async_api import async_playwright
from time import time

sys.stdout.reconfigure(encoding='utf-8')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ARQUIVO_EXCEL_LINKS = "links_chaves_na_mao_carros.xlsx"
ARQUIVO_PKL_DADOS = "dados_chaves_na_mao_carros.pkl"
ARQUIVO_EXCEL_DADOS = "dados_chaves_na_mao_carros.xlsx"
ARQUIVO_CHECKPOINT = "checkpoint_carros.pkl"

TIMEOUT = 30000
RETRIES = 3
MAX_CONCURRENT = 15

SELETORES_FIPE = {
    "codigo_fipe": [
        "#version-price-fipe > tr:nth-child(1) > td:nth-child(4) > p",
        "#version-price-fipe > tr.versionTemplate-module__qVM2Bq__highlighted > td:nth-child(4) > p",
        "//table[@id='version-price-fipe']//td[position()=4]/p",
    ],
    "preco_fipe": [
        "#version-price-fipe > tr:nth-child(1) > td:nth-child(5) > p > b",
        "#version-price-fipe > tr.versionTemplate-module__qVM2Bq__highlighted > td:nth-child(5) > p > b",
        "//article/section[2]/div/div[3]/div/div[2]/span/span/h2/b",
        "//article/section[2]/div/div[3]/div/div[1]/span/span/h3/b",
    ]
}

async def carregar_links():
    if not os.path.exists(ARQUIVO_EXCEL_LINKS):
        logging.error(f"Arquivo {ARQUIVO_EXCEL_LINKS} não encontrado.")
        return []
    try:
        df = await asyncio.to_thread(pd.read_excel, ARQUIVO_EXCEL_LINKS)
        if "Link" not in df.columns:
            logging.error("Coluna 'Link' não encontrada.")
            return []
        links = df['Link'].dropna().unique().tolist()
        logging.info(f"{len(links)} links únicos carregados.")
        return links
    except Exception as e:
        logging.error(f"Erro ao carregar links: {e}")
        return []

async def extrair_texto(pagina, seletores, default="N/A"):
    for seletor in seletores:
        try:
            s = seletor.strip()
            is_xpath = s.startswith("xpath=") or s.startswith("//") or s.startswith("/")
            locator = (
                pagina.locator(s) if s.startswith("xpath=")
                else pagina.locator(f"xpath={s}" if is_xpath else s)
            ).first

            if await locator.count() > 0:
                try:
                    await locator.scroll_into_view_if_needed(timeout=TIMEOUT)
                except:
                    pass
                await asyncio.sleep(0.2)

                texto = await locator.text_content(timeout=TIMEOUT)
                if texto:
                    texto = texto.replace("\xa0", " ").strip()
                    if texto:
                        return texto
        except:
            continue
    return default

async def extracao_dados(contexto, link, semaphore):
    async with semaphore:
        pagina = await contexto.new_page()
        try:
            for _ in range(RETRIES):
                try:
                    response = await pagina.goto(link, timeout=TIMEOUT, wait_until='domcontentloaded')
                    if not response or response.status != 200:
                        continue

                    # Scroll básico até a tabela aparecer
                    for _ in range(5):
                        if await pagina.locator("#version-price-fipe").count() > 0:
                            break
                        await pagina.evaluate("window.scrollBy(0, 800)")
                        await asyncio.sleep(0.4)

                    seletores = {
                        "Modelo": "main article section.row div.column span p > b",
                        "Versão": [
                            "/html/body/main/article/section[2]/div/div[1]/div[1]/div/span/p/span",
                            "body > main > article > section.row.spacing-4x.space-between.style-module-scss-module___tK7ya__mainSection > div > div.column.spacing-2x.style-module-scss-module___tK7ya__mainSectionVehicle > div.column.style-module-scss-module__7azAOG__vehicleDataWrapper > div > span > p > span",
                            # fallback genérico dentro do wrapper
                            "xpath=(//section[contains(@class,'mainSection')]//div[contains(@class,'vehicleDataWrapper')]//span/p/span)[1]"
                        ],
                        "Preço": [
                            # Caminho “curto” mais comum (b em <p>)
                            "section.row div.column.spacing-2x div.column.style-module__2c1zQG__vehicleDataWrapper div > div > p > b",
                            # Mesma área, mas às vezes vem sem <b>
                            "section.row div.column.spacing-2x div.column.style-module__2c1zQG__vehicleDataWrapper div > div > p",
                            # Caminho “longo” que você reportou
                            "body > main > article > section.row.spacing-4x.space-between.style-module__vnSL7G__mainSection > div > div.column.spacing-2x.style-module__vnSL7G__mainSectionVehicle > div.column.style-module__2c1zQG__vehicleDataWrapper > div > div > p > b",
                            # XPath genérico procurando um <p>/<b> com “R$” perto
                            "//section[contains(@class,'mainSection')]//div[contains(@class,'vehicleDataWrapper')]//p[.//b or text()][contains(., 'R$')]",
                        ],
                        "Localização": [
                            "/html/body/main/article/section[2]/div/div[1]/div[1]/ul/li[1]/b",
                            "body > main > article > section.row.spacing-4x.space-between.style-module-scss-module___tK7ya__mainSection > div > div.column.spacing-2x.style-module-scss-module___tK7ya__mainSectionVehicle > div.column.style-module-scss-module__7azAOG__vehicleDataWrapper > ul > li:nth-child(1) > b",
                            "xpath=(//section[contains(@class,'mainSection')]//div[contains(@class,'vehicleDataWrapper')]//ul/li[1]//b)[1]",
                            # por rótulo, se existir
                            "xpath=//ul/li[.//small[contains(translate(.,'ÂÃÁÀáãâà','AAAAaaaa'),'localiza')]]//b"
                        ],
                        "Ano do Modelo": [
                            "/html/body/main/article/section[2]/div/div[1]/div[1]/ul/li[2]/b",
                            "body > main > article > section.row.spacing-4x.space-between.style-module-scss-module___tK7ya__mainSection > div > div.column.spacing-2x.style-module-scss-module___tK7ya__mainSectionVehicle > div.column.style-module-scss-module__7azAOG__vehicleDataWrapper > ul > li:nth-child(2) > b",
                            "xpath=//ul/li[.//small[contains(., 'Ano') and contains(., 'modelo')]]//b",
                            "xpath=(//section[contains(@class,'mainSection')]//div[contains(@class,'vehicleDataWrapper')]//ul/li[2]//b)[1]"
                        ],
                        "KM": [
                            "/html/body/main/article/section[2]/div/div[1]/div[1]/ul/li[3]/b",
                            "body > main > article > section.row.spacing-4x.space-between.style-module-scss-module___tK7ya__mainSection > div > div.column.spacing-2x.style-module-scss-module___tK7ya__mainSectionVehicle > div.column.style-module-scss-module__7azAOG__vehicleDataWrapper > ul > li:nth-child(3) > b",
                            "xpath=//ul/li[.//small[contains(translate(.,'ÂÃÁÀáãâà','AAAAaaaa'),'km') or contains(.,'Quilometr')]]//b",
                            "xpath=(//section[contains(@class,'mainSection')]//div[contains(@class,'vehicleDataWrapper')]//ul/li[3]//b)[1]"
                        ],
                        "Combustível": [
                            "/html/body/main/article/section[2]/div/div[1]/div[1]/ul/li[4]/b",
                            "body > main > article > section.row.spacing-4x.space-between.style-module-scss-module___tK7ya__mainSection > div > div.column.spacing-2x.style-module-scss-module___tK7ya__mainSectionVehicle > div.column.style-module-scss-module__7azAOG__vehicleDataWrapper > ul > li:nth-child(4) > b",
                            "xpath=//ul/li[.//small[contains(translate(.,'ÂÃÁÀáãâà','AAAAaaaa'),'combust')]]//b",
                            "xpath=(//section[contains(@class,'mainSection')]//div[contains(@class,'vehicleDataWrapper')]//ul/li[4]//b)[1]"
                        ],
                        "Transmissão": [
                            "/html/body/main/article/section[2]/div/div[1]/div[1]/ul/li[5]/b",
                            "body > main > article > section.row.spacing-4x.space-between.style-module-scss-module___tK7ya__mainSection > div > div.column.spacing-2x.style-module-scss-module___tK7ya__mainSectionVehicle > div.column.style-module-scss-module__7azAOG__vehicleDataWrapper > ul > li:nth-child(5) > b",
                            "//ul/li[.//small[contains(., 'Trans') or contains(., 'Câmbio')]]//b"
                        ],
                        "Cor": [
                            "body > main > article > section.row.spacing-4x.space-between.style-module-scss-module___tK7ya__mainSection > div > div.column.spacing-2x.style-module-scss-module___tK7ya__mainSectionVehicle > div.column.style-module-scss-module__7azAOG__vehicleDataWrapper > ul > li:nth-child(7) > b",
                            "/html/body/main/article/section[2]/div/div[1]/div[1]/ul/li[7]/b",
                            "xpath=//ul/li[.//small[contains(translate(.,'ÂÃÁÀáãâà','AAAAaaaa'),'cor')]]//b",
                            "xpath=(//section[contains(@class,'mainSection')]//div[contains(@class,'vehicleDataWrapper')]//ul/li[7]//b)[1]"
                        ],
                        "Anunciante": "aside span span.wrap a span h2 > b"
                    }

                    dados = {}
                    for chave, seletor in seletores.items():
                        lista = seletor if isinstance(seletor, list) else [seletor]
                        dados[chave] = await extrair_texto(pagina, lista)

                    dados["Link"] = link
                    dados["Cidade"] = "Desconhecido"

                    dados["Cor"] = await extrair_texto(pagina, [
                        '/html/body/main/article/section[2]/div/div[1]/div[1]/ul/li[7]/b',
                    ])

                    dados["Código Fipe"] = await extrair_texto(pagina, SELETORES_FIPE["codigo_fipe"])
                    dados["Fipe"] = await extrair_texto(pagina, SELETORES_FIPE["preco_fipe"])

                    dados["Código Fipe"] = await extrair_texto(pagina, SELETORES_FIPE["codigo_fipe"])
                    dados["Fipe"] = await extrair_texto(pagina, SELETORES_FIPE["preco_fipe"])

                    # Conversão robusta do "Preço"
                    preco_raw = dados.get("Preço", "") or ""
                    try:
                        # remove espaços especiais
                        preco_raw = preco_raw.replace("\xa0", " ").strip()

                        # se vier algo como "Preço: R$ 123.456,78", pegamos a parte com R$
                        m = re.search(r"R\$\s*([\d\.\,]+)", preco_raw)
                        if m:
                            preco_num = m.group(1)
                        else:
                            # fallback: pega só dígitos e separadores
                            m2 = re.search(r"([\d\.\,]+)", preco_raw)
                            preco_num = m2.group(1) if m2 else ""

                        if preco_num:
                            dados["Preço"] = float(preco_num.replace(".", "").replace(",", "."))
                        else:
                            dados["Preço"] = "N/A"
                    except:
                        dados["Preço"] = "N/A"

                    if " - " in dados.get("Localização", ""):
                        dados["Cidade"] = dados["Localização"].split(" - ")[0]

                    try:
                        dados["Ano do Modelo"] = int(dados["Ano do Modelo"].split("/")[-1])
                    except:
                        dados["Ano do Modelo"] = "N/A"
                        
                    blindagem = await extrair_texto(pagina, [
                        '/html/body/main/article/section[2]/div/div[2]/span/ul/li[15]',
                        'body > main > article > section.row.spacing-4x.space-between.style-module__vnSL7G__mainSection > div > div.style-module__HWYeja__optionalItemsContainer > span > ul > li:nth-child(14) > p'
                    ], default="")

                    dados["Blindados"] = "S" if "blindado" in blindagem.lower() else "N"
                    
                    return dados
                except:
                    await asyncio.sleep(1)
            return None
        finally:
            await pagina.close()

async def processar_links(links, max_concurrent=MAX_CONCURRENT):
    start_time = time()
    dados_coletados = []
    processed_links = set()

    if os.path.exists(ARQUIVO_CHECKPOINT):
        try:
            dados_coletados = pd.read_pickle(ARQUIVO_CHECKPOINT).to_dict('records')
            processed_links = {d["Link"] for d in dados_coletados}
            links = [link for link in links if link not in processed_links]
            logging.info(f"Checkpoint carregado: {len(dados_coletados)} prontos, {len(links)} restantes.")
        except Exception as e:
            logging.error(f"Erro ao carregar checkpoint: {e}")

    async with async_playwright() as p:
        navegador = await p.chromium.launch(headless=True)
        contexto = await navegador.new_context()
        semaphore = asyncio.Semaphore(max_concurrent)

        for i in range(0, len(links), max_concurrent):
            batch = links[i:i + max_concurrent]
            tarefas = [extracao_dados(contexto, link, semaphore) for link in batch]
            for tarefa in tqdm(asyncio.as_completed(tarefas), total=len(tarefas), desc=f"Lote {i//max_concurrent + 1}"):
                try:
                    resultado = await tarefa
                    if resultado:
                        dados_coletados.append(resultado)
                        if len(dados_coletados) % 1000 == 0:
                            pd.DataFrame(dados_coletados).to_pickle(ARQUIVO_CHECKPOINT)
                            logging.info(f"{len(dados_coletados)} salvos no checkpoint.")
                except Exception as e:
                    logging.error(f"Erro ao processar link: {e}")
            await asyncio.sleep(0.5)

        await contexto.close()
        await navegador.close()

    logging.info(f"Finalizado em {time() - start_time:.2f}s com {len(dados_coletados)} registros.")
    return dados_coletados

async def salvar_dados(dados):
    if not dados:
        logging.warning("Nenhum dado para salvar.")
        return
    df = pd.DataFrame(dados)
    await asyncio.to_thread(df.to_pickle, ARQUIVO_PKL_DADOS)
    logging.info(f"PKL salvo: {ARQUIVO_PKL_DADOS}")

    try:
        if os.path.exists(ARQUIVO_EXCEL_DADOS):
            df_existente = await asyncio.to_thread(pd.read_excel, ARQUIVO_EXCEL_DADOS, engine='openpyxl')
            df_final = pd.concat([df_existente, df], ignore_index=True)
        else:
            df_final = df
        await asyncio.to_thread(df_final.to_excel, ARQUIVO_EXCEL_DADOS, index=False, engine='openpyxl')
        logging.info(f"Excel salvo: {ARQUIVO_EXCEL_DADOS}")
    except Exception as e:
        logging.error(f"Erro ao salvar Excel: {e}")

async def main():
    links = await carregar_links()
    if not links:
        return
    dados = await processar_links(links)
    await salvar_dados(dados)

if __name__ == "__main__":
    asyncio.run(main())
