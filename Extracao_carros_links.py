from playwright.async_api import async_playwright
import asyncio
import os
import pickle
import pandas as pd
import traceback  
from urllib.parse import urljoin

links = [
    ("Carros", "https://www.chavesnamao.com.br/carros-usados/brasil/?filtro=amin:2002", 30000),
    ("Carros", "https://www.chavesnamao.com.br/carros-usados/brasil/?filtro=amin:2002,or:1", 30000),
    ("Carros", "https://www.chavesnamao.com.br/carros-usados/brasil/?filtro=amin:2002,com:[2],or:1", 30000),
    ("Carros", "https://www.chavesnamao.com.br/carros-usados/brasil/?filtro=amin:2002,com:[3],or:1", 30000),
    ("Carros", "https://www.chavesnamao.com.br/carros-usados/brasil/?filtro=amin:2002,com:[8+9+10+1],or:1", 30000),
    ("Carros", "https://www.chavesnamao.com.br/carros-usados/brasil/?filtro=amin:2002,com:[4],or:1", 30000),
    ("Carros", "https://www.chavesnamao.com.br/carros-usados/brasil/?filtro=amin:2002,com:[4],or:0", 30000),
    ("Carros", "https://www.chavesnamao.com.br/carros-usados/rj-rio-de-janeiro/?filtro=amin:2002,com:[4],or:0", 30000),
    ("Carros", "https://www.chavesnamao.com.br/carros-usados/mg-belo-horizonte/?filtro=amin:2002,com:[4],or:0", 30000),
    ("Carros", "https://www.chavesnamao.com.br/carros-usados/sp-campinas/?filtro=amin:2002,com:[4],or:0", 30000),
    ("Carros", "https://www.chavesnamao.com.br/carros-usados/sp-sao-paulo/?filtro=amin:2002,com:[4],or:0", 30000),
    ("Carros", "https://www.chavesnamao.com.br/carros-usados/pr-curitiba/?filtro=amin:2002,com:[4],or:0", 30000),
    ("Carros", "https://www.chavesnamao.com.br/carros/pr-curitiba/volkswagen/?filtro=amin:2002,com:[4],or:0", 30000),
    ("Carros", "https://www.chavesnamao.com.br/carros/pr-curitiba/chevrolet/?filtro=amin:2002,com:[4],or:0", 30000),
    ("Renault", "https://www.chavesnamao.com.br/carros/brasil/renault-sandero/2002/", 30000),
    ("FORD", "https://www.chavesnamao.com.br/carros/brasil/ford/2002/?filtro=or:2", 30000),
    ("São Paulo", "https://www.chavesnamao.com.br/carros-usados/sp-sao-paulo/", 30000),
    ("PCD", "https://www.chavesnamao.com.br/carros-para-pcd/brasil/?&filtro=amin:2002,amax:0,ne:[4],or:0", 30000),
    ("Carro", "https://www.chavesnamao.com.br/carros-usados/brasil/?&filtro=amin:2002,amax:0,or:4", 25000),
    ("JEEP", "https://www.chavesnamao.com.br/carros/brasil/honda/2002/?filtro=or:2", 20900),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-onix/2002/", 27049),
    ("Campinas", "https://www.chavesnamao.com.br/carros-usados/sp-campinas/", 24090),
    ("HONDA", "https://www.chavesnamao.com.br/carros/brasil/honda/2002/?filtro=or:2", 22103),
    ("TOYOTA", "https://www.chavesnamao.com.br/carros/brasil/toyota/2002/?filtro=or:2", 20000),
    ("Rj", "https://www.chavesnamao.com.br/carros-usados/rj-rio-de-janeiro/?filtro=amin:2002,amax:0,or:1", 20000),
    ("São Paulo", "https://www.chavesnamao.com.br/carros-usados/sp-sao-paulo/?&filtro=amin:2002,amax:0,or:1", 20000),
    ("São Paulo", "https://www.chavesnamao.com.br/carros-usados/sp-sao-paulo/?&filtro=amin:2002,amax:0,or:4", 20000),
    ("Porto Alegre", "https://www.chavesnamao.com.br/carros-usados/rs-porto-alegre/", 18008),
    ("Curitiba", "https://www.chavesnamao.com.br/carros-usados/pr-curitiba/?&filtro=amin:2002,amax:0,or:1", 10000),
    ("Curitiba", "https://www.chavesnamao.com.br/carros-usados/pr-curitiba/?&filtro=amin:2002,amax:0,or:2", 10000),
    ("Curitiba", "https://www.chavesnamao.com.br/carros-usados/pr-curitiba/?&filtro=amin:2002,amax:0,or:3", 30000),
    ("Curitiba", "https://www.chavesnamao.com.br/carros-usados/pr-curitiba/?&filtro=amin:2002,amax:0,or:4", 30000),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-toro/2002/", 8409),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-uno/2002/", 5304),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-strada/2002/", 8059),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-argo/2002/", 6806),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-cronos/2002/", 39004),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-fiorino/2002/", 28007),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-pulse/2002/", 23004),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-fastback/2002/", 19002),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-idea/2002/", 10045),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-siena/2002/", 10091),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-grand-siena/2002/", 12000),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-punto/2002/", 11004),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-500/2002/", 11002),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-freemont/2002/", 63000),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-palio-weekend/2002/", 51000),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-doblo/2002/", 50008),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-t-cross/2002/", 10506),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-polo/2002/", 9804),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-gol/2002/", 10160),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-fox/2002/", 7600),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-saveiro/2002/", 7670),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-nivus/2002/", 6770),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-voyage/2002/", 51300),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-amarok/2002/", 41000),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-up/2002/", 43300),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-virtus/2002/", 37003),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-golf/2002/", 25001),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-tiguan/2002/", 26004),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-taos/2002/", 14003),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-spacefox/2002/", 11002),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-kombi/2002/", 4700),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-parati/2002/", 3007),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-tracker/2002/", 15093),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-cruze/2002/", 60004),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-s10/2002/", 56100),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-montana/2002/", 32002),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-prisma/2002/", 40018),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-spin/2002/", 48005),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-classic/2002/", 10043),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-corsa/2002/", 12006),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-cobalt/2002/", 13005),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-celta/2002/", 20024),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-agile/2002/", 9009),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-vectra/2002/", 8005),
    ("HYUNDAI", "https://www.chavesnamao.com.br/carros/brasil/hyundai-hb20/2002/", 12097),
    ("HYUNDAI", "https://www.chavesnamao.com.br/carros/brasil/hyundai-hb20s/2002/", 61007),
    ("HYUNDAI", "https://www.chavesnamao.com.br/carros/brasil/hyundai-creta/2002/", 60005),
    ("HYUNDAI", "https://www.chavesnamao.com.br/carros/brasil/hyundai-tucson/2002/", 30049),
    ("HYUNDAI", "https://www.chavesnamao.com.br/carros/brasil/hyundai-ix35/2002/", 20065),
    ("Franca", "https://www.chavesnamao.com.br/carros-usados/sp-franca/?filtro=amin:2002,amax:0,or:4", 26009),
    ("HYUNDAI", "https://www.chavesnamao.com.br/carros/brasil/hyundai-i30/2002/", 9800),
    ("HYUNDAI", "https://www.chavesnamao.com.br/carros/brasil/hyundai-santa-fe/2002/", 8009),
    ("HYUNDAI", "https://www.chavesnamao.com.br/carros/brasil/hyundai-hr/2002/", 5100),
    ("Citroën", "https://www.chavesnamao.com.br/carros/brasil/citroen/2002/?filtro=or:2", 14800),
    ("Peugeot", "https://www.chavesnamao.com.br/carros/brasil/peugeot/2002/?filtro=or:2", 15000),
    ("Mitsubishi", "https://www.chavesnamao.com.br/carros/brasil/mitsubishi/2002/?filtro=or:2", 11300),
    ("Sorocaba", "https://www.chavesnamao.com.br/carros-usados/sp-sorocaba/", 10009),
    ("Ribeirão Preto", "https://www.chavesnamao.com.br/carros-usados/sp-ribeirao-preto/", 9810),
    ("São José Dos Campos", "https://www.chavesnamao.com.br/carros-usados/sp-sao-jose-dos-campos/", 9970),
    ("São José do Rio Preto", "https://www.chavesnamao.com.br/carros-usados/sp-sao-jose-do-rio-preto/", 28600),
    ("Botucatu", "https://www.chavesnamao.com.br/carros-usados/sp-botucatu/", 29200),
    ("Mogi das Cruzes", "https://www.chavesnamao.com.br/carros-usados/sp-mogi-das-cruzes/", 31600),
    ("Limeira", "https://www.chavesnamao.com.br/carros-usados/sp-limeira/", 31700),
    ("Jundiaí", "https://www.chavesnamao.com.br/carros-usados/sp-jundiai/", 36300),
    ("Guarulhos", "https://www.chavesnamao.com.br/carros-usados/sp-guarulhos/", 37800),
    ("Carro", "https://www.chavesnamao.com.br/carros-7-lugares/brasil/?&filtro=amin:2002,amax:0,ne:[2],or:0", 30000),
    ("Santo André", "https://www.chavesnamao.com.br/carros-usados/sp-santo-andre/", 40300),
    ("São Bernardo do Campo", "https://www.chavesnamao.com.br/carros-usados/sp-sao-bernardo-do-campo/", 50043),
    ("Piracicaba", "https://www.chavesnamao.com.br/carros-usados/sp-piracicaba/", 549),
    ("Santos", "https://www.chavesnamao.com.br/carros-usados/sp-santos/", 6190),
    ("Osasco", "https://www.chavesnamao.com.br/carros-usados/sp-osasco/", 6820),
    ("HYUNDAI", "https://www.chavesnamao.com.br/carros/pr-curitiba/hyundai/2002/", 6002),
    ("AUDI", "https://www.chavesnamao.com.br/carros/brasil/audi/2002/?filtro=or:2", 8000),
    ("BMW", "https://www.chavesnamao.com.br/carros/brasil/bmw/2002/?filtro=or:2", 10000),
    ("CHERY", "https://www.chavesnamao.com.br/carros/brasil/caoa-chery/2002/?filtro=or:2", 5508),
    ("KIA", "https://www.chavesnamao.com.br/carros/brasil/kia/2002/?filtro=or:2", 7140),
    ("LAND ROVER", "https://www.chavesnamao.com.br/carros/brasil/land-rover/2002/?filtro=or:2", 6008),
    ("MERCEDES", "https://www.chavesnamao.com.br/carros/brasil/mercedes-benz/2002/?filtro=or:2", 9000),
    ("Blindados", "https://www.chavesnamao.com.br/carros-usados/brasil/?filtro=op:[239]", 4065),
    ("PORSCHE", "https://www.chavesnamao.com.br/carros/brasil/porsche/2002/?filtro=or:2", 3000),
    ("RAM", "https://www.chavesnamao.com.br/carros/brasil/ram/2002/?filtro=or:2", 2042),
    ("VOLVO", "https://www.chavesnamao.com.br/carros/brasil/volvo/2002/?filtro=or:2", 3308),
    ("SUZUKI", "https://www.chavesnamao.com.br/carros/brasil/suzuki/2002/?filtro=or:2", 1700),
    ("BYD", "https://www.chavesnamao.com.br/carros/brasil/byd/2002/?filtro=or:2", 9100),
    ("MINI", "https://www.chavesnamao.com.br/carros/brasil/mini/2002/?filtro=or:2", 21300),
    ("TROLLER", "https://www.chavesnamao.com.br/carros/brasil/troller/2002/", 10400),
    ("Jaguar", "https://www.chavesnamao.com.br/carros/brasil/jaguar/2002/", 9500),
    ("DODGE", "https://www.chavesnamao.com.br/carros/brasil/dodge/2002/?filtro=or:2", 8500),
    ("Antigos", "https://www.chavesnamao.com.br/carros-antigos/brasil/?filtro=ne:[3],amin:2002,amax:0,or:2", 7008),
    ("JAC", "https://www.chavesnamao.com.br/carros/brasil/jac/2002/", 6400),
    ("GWM", "https://www.chavesnamao.com.br/carros/brasil/gwm/2002/?filtro=or:2", 46000),
    ("SMART", "https://www.chavesnamao.com.br/carros/brasil/smart/2002/", 38000),
    ("Chrysler", "https://www.chavesnamao.com.br/carros/brasil/chrysler/2002/", 35000),
    ("Eletricos", "https://www.chavesnamao.com.br/carros-eletricos/brasil/?&filtro=amin:2002,amax:0,ne:[6],or:0", 38000),
    ("Lexus", "https://www.chavesnamao.com.br/carros/brasil/lexus/2002/", 20005),
    ("LIFAN", "https://www.chavesnamao.com.br/carros/brasil/lifan/2002/", 20002),
    ("Ssangyong", "https://www.chavesnamao.com.br/carros/brasil/ssangyong/2002/", 13000),
    ("Iveco", "https://www.chavesnamao.com.br/carros/brasil/iveco/2002/", 10002),
    ("EEFA", "https://www.chavesnamao.com.br/carros/brasil/effa/2002/", 10001),
    ("TESLA", "https://www.chavesnamao.com.br/carros/brasil/tesla/2002/", 8000),
    ("Shineray", "https://www.chavesnamao.com.br/carros/brasil/shineray/2002/", 60000),
    ("FERRARI", "https://www.chavesnamao.com.br/carros/brasil/ferrari/2002/", 50000),
    ("Infiniti", "https://www.chavesnamao.com.br/carros/brasil/infiniti/2002/", 40000),
    ("ASIA", "https://www.chavesnamao.com.br/carros/brasil/asia/2002/", 20000),
    ("Cadillac", "https://www.chavesnamao.com.br/carros/brasil/cadillac/2002/", 20000),
    ("Mclaren", "https://www.chavesnamao.com.br/carros/brasil/mclaren/2002/", 20000),
    ("Agrale", "https://www.chavesnamao.com.br/carros/brasil/agrale/2002/", 10000),
    ("Bentley", "https://www.chavesnamao.com.br/carros/brasil/bentley/2002/", 10000),
    ("BRM", "https://www.chavesnamao.com.br/carros/brasil/brm/2002/", 10000),
]

#Onde sera salvo os links coletados
ARQUIVO_PICKLE = "links_chaves_na_mao_carros.pkl"
ARQUIVO_EXCEL = "links_chaves_na_mao_carros.xlsx"  
DOMINIO_BASE = "https://www.chavesnamao.com.br/carros-usados/brasil/"

#Salvando progresso em arquivos pkl
def salvar_progresso(dados):
    if os.path.exists(ARQUIVO_PICKLE):
        with open(ARQUIVO_PICKLE, "rb") as f:
            dados_existentes = pickle.load(f)
    else:
        dados_existentes = set()

    novos_dados = {d["Link"] for d in dados}
    dados_existentes.update(novos_dados)

    with open(ARQUIVO_PICKLE, "wb") as f:
        pickle.dump(dados_existentes, f)
    print(f"Progresso salvo em '{ARQUIVO_PICKLE}'")

def carregar_progresso():
    if os.path.exists(ARQUIVO_PICKLE):
        with open(ARQUIVO_PICKLE, "rb") as f:
            return pickle.load(f)
    return set()

async def rolar_e_coletar(pagina, limite_itens):
    ids_itens_carregados = set()
    tentativas_sem_novos_itens = 0
    altura_anterior = 0

    while tentativas_sem_novos_itens < 5 and len(ids_itens_carregados) < limite_itens:
        try:
            await pagina.wait_for_selector('//a[@href]', timeout=50000)
            links = await pagina.query_selector_all('//a[@href]')
            novos_itens = 0

            for link in links:
                if len(ids_itens_carregados) >= limite_itens:
                    break
                href = await link.get_attribute('href')
                if href:
                    href = urljoin(DOMINIO_BASE, href)
                    if "/carro/" in href and "/id-" in href and href not in ids_itens_carregados:
                        ids_itens_carregados.add(href)
                        novos_itens += 1

            print(f"Total de links coletados até agora: {len(ids_itens_carregados)}")

            altura_atual = await pagina.evaluate("document.body.scrollHeight")
            if altura_atual == altura_anterior and novos_itens == 0:
                tentativas_sem_novos_itens += 1
            else:
                tentativas_sem_novos_itens = 0
                altura_anterior = altura_atual

            await pagina.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            await asyncio.sleep(3)

        except Exception as e:
            print(f"Erro ao encontrar links: {e}")
            traceback.print_exc()
            tentativas_sem_novos_itens += 1
            await asyncio.sleep(3)

    print(f"Fim da rolagem para esta página. Total de itens carregados: {len(ids_itens_carregados)}")
    return [{"Link": link} for link in ids_itens_carregados]

async def processar_url(navegador, marca, url, limite, links_existentes):
    if url in links_existentes:
        print(f"{marca} já processado. Pulando...")
        return []

    print(f"Abrindo página para {marca} ({url})...")
    pagina = await navegador.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
    try:
        await pagina.route("**/*.{png,jpg,jpeg,gif,webp}", lambda route: route.abort())
        await pagina.goto(url, timeout=60000)
        await pagina.wait_for_load_state("networkidle")
        dados = await rolar_e_coletar(pagina, limite)
        return dados
    except Exception as e:
        print(f"Erro ao processar {marca}: {e}")
        traceback.print_exc()
        return []
    finally:
        await pagina.close()

async def main():
    links_existentes = carregar_progresso()
    async with async_playwright() as p:
        navegador = await p.chromium.launch()
        for i in range(0, len(links), 5):
            lote = links[i:i+5]
            tarefas = [processar_url(navegador, m, u, l, links_existentes) for m, u, l in lote]
            resultados = await asyncio.gather(*tarefas)
            dados_totais = [item for sublista in resultados for item in sublista if sublista]
            if dados_totais:
                salvar_progresso(dados_totais)
        await navegador.close()

    print("Extração finalizada!")

    print("\nConvertendo o arquivo pickle para Excel...")
    dados_finais = carregar_progresso()

    if dados_finais:
        df = pd.DataFrame([{"Link": link} for link in dados_finais])
        df.to_excel(ARQUIVO_EXCEL, index=False)
        print(f"Arquivo Excel '{ARQUIVO_EXCEL}' gerado com sucesso!")
    else:
        print("Nenhum dado foi encontrado no arquivo pickle para conversão.")

if __name__ == "__main__":
    asyncio.run(main())