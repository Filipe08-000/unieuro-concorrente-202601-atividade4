# Relatório do concorrente-202601-atividade2

**Disciplina: PROGRAMAÇÃO CONCORRENTE E DISTRIBUÍDA** 

**Aluno(s): Arthur Dias, Filipe Ferreira, Nicolas Mateus**

**Turma:Manhã**

**Professor:Rafael**

**Data: 27/03/26**

---

# 1. Descrição do Problema

O problema computacional desta atividade consiste na conversão de uma imagem no formato PPM (P6) para escala de cinza. A imagem utilizada possui dimensões muito grandes (até dezenas de gigabytes), tornando o processamento intensivo tanto em CPU quanto em operações de entrada e saída (I/O).

O objetivo principal foi comparar o desempenho entre uma abordagem serial e uma abordagem paralela, onde a paralelização foi realizada externamente ao programa original, que deveria ser tratado como uma caixa-preta.

Problema implementado



## Orientações para preenchimento

Explique:

* Qual problema foi implementado

Conversão de uma imagem RGB para escala de cinza, onde cada pixel é transformado utilizando a fórmula:

cinza = 0.299R + 0.587G + 0.114B

* Qual algoritmo foi utilizado

O algoritmo percorre a imagem em blocos, lendo os pixels e aplicando a conversão para escala de cinza. A complexidade é aproximadamente O(N), onde N é o número total de pixels.

* Qual o tamanho da entrada utilizada nos testes

magem PPM com dimensões aproximadas de:

75672 x 75672 pixels
aproximadamente 16 GB

* Qual o objetivo da paralelização

Reduzir o tempo total de execução distribuindo o processamento entre múltiplos workers (2, 4, 8 e 12), respeitando a restrição de não modificar o código original do conversor.

**Questões que devem ser respondidas:**

* Qual é o objetivo do programa?

O programa tem como objetivo converter uma imagem no formato PPM (P6) para escala de cinza e, ao mesmo tempo, servir como ferramenta de benchmarking. Ele permite comparar o tempo de execução entre uma abordagem sequencial (serial) e uma abordagem paralela, variando o número de workers (2, 4, 8 e 12), analisando o desempenho da paralelização.

* Qual o volume de dados processado?

O volume de dados processado corresponde a uma imagem de grandes dimensões (aproximadamente 75672 x 75672 pixels), resultando em um arquivo de cerca de 16 GB. Cada pixel contém três valores (RGB), totalizando bilhões de dados processados.

* Qual algoritmo foi utilizado?

Foi utilizado o algoritmo de conversão de imagem RGB para escala de cinza, baseado em uma operação linear sobre cada pixel:

cinza = 0.299R + 0.587G + 0.114B

Na versão paralela, foi aplicada a estratégia de divisão e conquista, onde a imagem é dividida em faixas horizontais independentes, processadas separadamente e posteriormente reunidas.

* Qual a complexidade aproximada do algoritmo?

A complexidade do algoritmo é O(N), onde N representa o número total de pixels da imagem. Cada pixel é processado exatamente uma vez, tanto na versão serial quanto na paralela. Mesmo com a divisão em múltiplos workers, a complexidade global permanece linear, pois todo o conjunto de dados precisa ser percorrido integralmente.

---

# 2. Ambiente Experimental

Descreva o ambiente em que os experimentos foram realizados.

## Orientações

Informar as características do hardware e software utilizados na execução dos testes.

| Item                        | Descrição |
| --------------------------- | --------- |
| Processador                 |12th Gen Intel(R) Core(TM) i5-12500 3.00 GHz|
| Número de núcleos           |6 núcleos físicos (12 threads lógicas)|
| Memória RAM                 |16,0 GB|
| Sistema Operacional         |Windows 11 Pro|
| Linguagem utilizada         |Python 3.12.4|
| Biblioteca de paralelização |NumPy|
| Compilador / Versão         |subprocess (múltiplos processos)|

---

# 3. Metodologia de Testes

Utilizou-se a função time.time() da biblioteca padrão do Python para medir o tempo total de execução.
O cronômetro foi iniciado imediatamente antes do início do processamento e encerrado após a finalização completa da execução, incluindo todas as etapas da solução paralela.

## Orientações

Descrever:

* Como o tempo de execução foi medido

Utilizou-se time.time() para medir o tempo total de execução.

* Quantas execuções foram realizadas

Foi realizada 1 execução para cada configuração, conforme solicitado:

1 worker (versão serial)
2 workers
4 workers
8 workers
12 workers

* Se foi utilizada média dos tempos

Não foi utilizada média de múltiplas execuções.
Cada configuração foi executada apenas uma vez, pois o ambiente de testes estava relativamente controlado e os tempos obtidos foram considerados suficientes para análise comparativa.

* Qual tempo foi considerado

O tempo medido na versão paralela corresponde ao tempo total da solução, incluindo:

-divisão da imagem em partes
-execução paralela do processamento
-junção das partes no resultado final

Na versão serial, o tempo corresponde apenas ao processamento direto da imagem completa.

### Configurações testadas

Os experimentos devem ser realizados nas seguintes configurações:

* 1 thread/processo (versão serial)
* 2 threads/processos
* 4 threads/processos
* 8 threads/processos
* 12 threads/processos

### Procedimento experimental

Descrever:

* Número de execuções para cada configuração

Foi realizada uma única execução para cada configuração exigida (1, 2, 4, 8 e 12 workers), todas executadas sequencialmente por meio de um script automatizado.

* Forma de cálculo da média

Não foi utilizada média de múltiplas execuções.
Para cada configuração, foi considerado o tempo absoluto de execução obtido em uma única rodada de testes.

A escolha por não utilizar média se deve ao fato de que a atividade possui alto custo computacional e de I/O (leitura e escrita em disco), tornando a repetição dos testes muito demorada, sendo suficiente uma execução para análise comparativa.

* Condições de execução (ex: máquina dedicada, carga do sistema, etc.)

Os testes foram realizados em uma máquina de laboratório, com a carga do sistema reduzida ao mínimo possível (fechamento de aplicações em segundo plano).

Diferentemente de cenários puramente computacionais, não houve isolamento das operações de entrada e saída (I/O). Todo o processamento envolveu leitura e escrita em disco, incluindo:

-leitura da imagem original
-criação de arquivos temporários (partições)
-processamento das partes
-escrita das saídas
-junção da imagem final

Essas operações foram mantidas no experimento, pois fazem parte da estratégia de paralelização adotada e impactam diretamente no desempenho observado.

---

# 4. Resultados Experimentais

Preencha a tabela com os **tempos médios de execução** obtidos.

## Orientações

* O tempo deve ser informado em **segundos**
* Utilizar a **média das execuções**

| Nº Threads/Processos | Tempo de Execução (s) |
| -------------------- | --------------------- |
| 1                    | 123.53                |
| 2                    | 134.05                |
| 4                    | 126.73                |
| 8                    | 154.85                |
| 12                   | 163.98                |

---

# 5. Cálculo de Speedup e Eficiência

## Fórmulas Utilizadas

### Speedup

```
Speedup(p) = T(1) / T(p)
```

Onde:

* **T(1)** = tempo da execução serial
* **T(p)** = tempo com p threads/processos

### Eficiência

```
Eficiência(p) = Speedup(p) / p
```

Onde:

* **p** = número de threads ou processos

---

# 6. Tabela de Resultados

Preencha a tabela abaixo utilizando os tempos medidos.

| Threads/Processos | Tempo (s) | Speedup | Eficiência |
| ----------------- | --------- | ------- | ---------- |
| 1                 | 123.53    | 1.00    | 1.00       |
| 2                 | 134.05    | 0.92    | 0.46       |
| 4                 | 126.73    | 0.97    | 0.24       |
| 8                 | 154.85    | 0.80    | 0.10       |
| 12                | 163.98    | 0.75    | 0.06       |

---

# 7. Gráfico de Tempo de Execução

Construa um gráfico mostrando o **tempo de execução em função do número de threads/processos**.

## Orientações

* Eixo X: número de threads/processos
* Eixo Y: tempo de execução (segundos)

Inserir o gráfico abaixo:

![Gráfico Tempo Execução](graficos/tempo_execucao.png)

---

# 8. Gráfico de Speedup

Construa um gráfico mostrando o **speedup obtido**.

## Orientações

* Eixo X: número de threads/processos
* Eixo Y: speedup
* Incluir também a **linha de speedup ideal (linear)** para comparação

Inserir o gráfico abaixo:

![Gráfico Speedup](graficos/speedup.png)

---

# 9. Gráfico de Eficiência

Construa um gráfico mostrando a **eficiência da paralelização**.

## Orientações

* Eixo X: número de threads/processos
* Eixo Y: eficiência
* Valores entre 0 e 1

Inserir o gráfico abaixo:

![Gráfico Eficiência](graficos/eficiencia.png)

---

# 10. Análise dos Resultados

Realize uma análise crítica dos resultados obtidos.

## Questões a serem respondidas

* O speedup obtido foi próximo do ideal?

Não. O speedup ficou abaixo de 1 em todas as configurações testadas, indicando que a versão paralela foi mais lenta que a versão serial. O comportamento ideal seria um aumento progressivo do speedup conforme o número de workers aumenta, o que não ocorreu neste experimento.

* A aplicação apresentou escalabilidade?

A aplicação não apresentou boa escalabilidade. O melhor resultado paralelo foi obtido com 4 workers (126,73 s), porém ainda inferior ao tempo da execução serial (123,53 s). A partir desse ponto, o aumento do número de workers resultou em piora significativa do desempenho, especialmente com 8 e 12 workers.

* Em qual ponto a eficiência começou a cair?

A eficiência já começa a cair desde o uso de 2 workers (aproximadamente 46%). Apesar de um leve ganho relativo ao passar para 4 workers, a eficiência continua baixa e cai drasticamente a partir de 8 workers, chegando a cerca de 6% com 12 workers. Isso indica saturação do sistema e aumento do overhead.

* O número de threads ultrapassa o número de núcleos físicos da máquina?

Sim. A máquina utilizada possui aproximadamente 6 núcleos físicos. Ao utilizar 8 e 12 workers, o número de processos ultrapassa os núcleos disponíveis, fazendo com que múltiplos processos disputem os mesmos recursos de CPU, memória e disco, o que contribui para a queda de desempenho.

* Houve overhead de paralelização?

Sim. O overhead foi significativo e foi o principal fator responsável pela piora no desempenho da versão paralela. Esse overhead está relacionado principalmente a:

-divisão da imagem em várias partes
-criação de arquivos temporários
-múltiplas operações de leitura e escrita em disco
-junção final da imagem

Essas etapas adicionaram um custo elevado que superou os benefícios da execução concorrente.

Discutir possíveis causas para:

* perda de desempenho

A perda de desempenho está diretamente associada ao alto custo de operações de I/O (entrada e saída em disco). A necessidade de dividir a imagem em arquivos separados e posteriormente reuni-los gerou um volume significativo de leitura e escrita, tornando o disco o principal gargalo do sistema.

* gargalos no algoritmo

O algoritmo em si é eficiente (complexidade O(N)), porém o gargalo não está no cálculo da conversão, e sim na manipulação dos dados. O processo de particionamento da imagem e a recomposição final introduzem um custo adicional elevado.

* sincronização entre threads/processos

A execução paralela exige que o processo principal aguarde a conclusão de todos os subprocessos antes de realizar a junção final. Isso cria um ponto de sincronização onde o tempo total passa a depender do processo mais lento.

* comunicação entre processos

Embora não haja troca direta de dados complexos entre processos, a comunicação ocorre indiretamente através do sistema de arquivos. Cada processo lê e escreve arquivos distintos, o que gera competição por recursos de disco e aumenta o tempo total.

* contenção de memória ou cache

Com múltiplos processos acessando o disco simultaneamente, ocorre contenção de recursos, especialmente em sistemas com armazenamento limitado. Isso resulta em redução de desempenho devido à disputa por largura de banda de leitura/escrita e cache.


---

# 11. Conclusão

Apresente as conclusões do experimento.

## Sugestões de pontos a comentar

* O paralelismo trouxe ganho significativo de desempenho?

Não. A partir dos resultados obtidos, conclui-se que o paralelismo não trouxe ganho de desempenho neste cenário. A execução serial apresentou o melhor tempo (123,53 s), enquanto todas as versões paralelas apresentaram tempos superiores.

* Qual foi o melhor número de threads/processos?

O melhor cenário foi a execução serial (1 worker).
Entre as configurações paralelas, a de 4 workers apresentou o melhor desempenho (126,73 s), porém ainda inferior à versão serial.

* O programa escala bem com o aumento do paralelismo?

Não. O programa não escala bem com o aumento do paralelismo neste caso. À medida que o número de workers aumenta, o tempo total de execução também aumenta, especialmente a partir de 8 workers. Isso demonstra que o custo adicional da paralelização supera os benefícios da execução concorrente.

* Quais melhorias poderiam ser feitas na implementação?

Para melhorar o desempenho da solução, poderiam ser adotadas as seguintes estratégias:

-Evitar a criação de arquivos temporários, realizando o processamento diretamente em memória
-Utilizar técnicas de memória compartilhada, reduzindo a necessidade de leitura e escrita em disco
-Implementar paralelismo interno no algoritmo de conversão, em vez de paralelização externa
-Utilizar bibliotecas otimizadas para processamento de imagem que exploram paralelismo de forma mais eficiente
-Reduzir o número de acessos ao disco, minimizando o principal gargalo identificado no experimento

* Conclusão geral

A atividade demonstrou que, embora a paralelização seja uma técnica poderosa, sua eficácia depende diretamente da natureza do problema e da forma como é implementada. Neste caso, a necessidade de tratar o programa como caixa-preta levou à adoção de uma abordagem baseada em divisão da imagem em arquivos temporários, o que introduziu um alto custo de operações de entrada e saída.

Como consequência, o overhead da paralelização superou os ganhos esperados, tornando a versão paralela menos eficiente que a serial. Assim, conclui-se que, para este tipo de problema e nas condições testadas, a paralelização externa não foi vantajosa em termos de desempenho.

---
