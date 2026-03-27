import os
import time
import math
import shutil
import subprocess
from pathlib import Path


ARQUIVO_ORIGINAL = "imagem_aleatoria_1gb.ppm"   # mantenha o nome do arquivo gerado
ARQUIVO_SAIDA_FINAL = "imagem_aleatoria_1gb_cinza_parallel.ppm"
CONVERSOR_SCRIPT = "conversoremescalacinza.py"

PASTA_TEMP = Path("temp_parallel")
PASTA_ENTRADAS = PASTA_TEMP / "entradas"
PASTA_SAIDAS = PASTA_TEMP / "saidas"


def ler_header_ppm(caminho):
    with open(caminho, "rb") as f:
        tipo = f.readline().strip()
        if tipo != b"P6":
            raise ValueError("Formato não suportado. Esperado PPM P6.")

        linha = f.readline().strip()
        while linha.startswith(b"#"):
            linha = f.readline().strip()

        largura, altura = map(int, linha.split())

        linha = f.readline().strip()
        while linha.startswith(b"#"):
            linha = f.readline().strip()

        valor_maximo = int(linha)
        if valor_maximo != 255:
            raise ValueError("Somente PPM com max=255 suportado.")

        offset_dados = f.tell()

    return largura, altura, valor_maximo, offset_dados


def preparar_pastas():
    if PASTA_TEMP.exists():
        shutil.rmtree(PASTA_TEMP)

    PASTA_ENTRADAS.mkdir(parents=True, exist_ok=True)
    PASTA_SAIDAS.mkdir(parents=True, exist_ok=True)


def dividir_imagem_em_partes(caminho_arquivo, num_partes):
    largura, altura, valor_maximo, offset_dados = ler_header_ppm(caminho_arquivo)

    linhas_por_parte = math.ceil(altura / num_partes)
    bytes_por_linha = largura * 3

    partes = []

    with open(caminho_arquivo, "rb") as fin:
        for i in range(num_partes):
            linha_inicio = i * linhas_por_parte
            if linha_inicio >= altura:
                break

            linha_fim = min((i + 1) * linhas_por_parte, altura)
            altura_parte = linha_fim - linha_inicio

            caminho_parte = PASTA_ENTRADAS / f"parte_{i}.ppm"

            with open(caminho_parte, "wb") as fout:
                header = f"P6\n{largura} {altura_parte}\n{valor_maximo}\n".encode("ascii")
                fout.write(header)

                fin.seek(offset_dados + linha_inicio * bytes_por_linha)

                bytes_restantes = altura_parte * bytes_por_linha
                buffer_size = 1024 * 1024 * 64  # 64 MB

                while bytes_restantes > 0:
                    chunk = fin.read(min(buffer_size, bytes_restantes))
                    if not chunk:
                        raise IOError("Erro ao dividir a imagem.")
                    fout.write(chunk)
                    bytes_restantes -= len(chunk)

            partes.append({
                "indice": i,
                "entrada": str(caminho_parte),
                "saida": str(PASTA_SAIDAS / f"parte_{i}_cinza.ppm"),
                "altura": altura_parte
            })

    return largura, altura, valor_maximo, partes


def executar_conversor_em_subprocesso(arquivo_entrada, arquivo_saida):
    """
    Executa o conversor original como caixa-preta, sem alterar sua lógica.
    Em vez de rodar o __main__ dele, importamos e chamamos a função externamente.
    """
    comando = [
        "python",
        "-c",
        (
            "import conversoremescalacinza as c; "
            f'c.converter_para_cinza_serial(r"{arquivo_entrada}", r"{arquivo_saida}", linhas_por_bloco=256)'
        )
    ]
    return subprocess.Popen(comando)


def processar_partes_em_paralelo(partes, max_workers):
    processos = []

    # dispara todos
    for parte in partes[:max_workers]:
        p = executar_conversor_em_subprocesso(parte["entrada"], parte["saida"])
        processos.append((parte, p))

    # como aqui num_partes == max_workers, isso já resolve
    # se quisesse controlar fila maior que workers, faria um scheduler
    for parte, processo in processos:
        retorno = processo.wait()
        if retorno != 0:
            raise RuntimeError(f"Erro no processamento da parte {parte['indice']}.")


def juntar_partes(caminho_saida_final, largura, altura_total, valor_maximo, partes):
    with open(caminho_saida_final, "wb") as fout:
        header = f"P6\n{largura} {altura_total}\n{valor_maximo}\n".encode("ascii")
        fout.write(header)

        for parte in sorted(partes, key=lambda x: x["indice"]):
            with open(parte["saida"], "rb") as fin:
                # pula cabeçalho da parte
                _, _, _, offset_dados = ler_header_ppm(parte["saida"])
                fin.seek(offset_dados)

                while True:
                    chunk = fin.read(1024 * 1024 * 64)  # 64 MB
                    if not chunk:
                        break
                    fout.write(chunk)


def executar_experimento(num_threads):
    print(f"\n=== EXECUTANDO COM {num_threads} THREADS/WORKERS ===")

    preparar_pastas()

    inicio_total = time.time()

    largura, altura, valor_maximo, partes = dividir_imagem_em_partes(
        ARQUIVO_ORIGINAL,
        num_threads
    )

    inicio_proc = time.time()
    processar_partes_em_paralelo(partes, num_threads)
    fim_proc = time.time()

    juntar_partes(
        ARQUIVO_SAIDA_FINAL.replace(".ppm", f"_{num_threads}.ppm"),
        largura,
        altura,
        valor_maximo,
        partes
    )

    fim_total = time.time()

    tempo_proc = fim_proc - inicio_proc
    tempo_total = fim_total - inicio_total

    print(f"Tempo só do processamento paralelo: {tempo_proc:.2f} s")
    print(f"Tempo total (divisão + processamento + junção): {tempo_total:.2f} s")

    return tempo_total


if __name__ == "__main__":
    resultados = {}

    for n in [2, 4, 8, 12]:
        tempo = executar_experimento(n)
        resultados[n] = tempo

    print("\n=== RESULTADOS FINAIS ===")
    for n, t in resultados.items():
        print(f"{n} workers -> {t:.2f} s")