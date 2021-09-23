# Querido Diário - Piloto Educação

Projeto piloto em parceria com a Aliança (Fundação Lemann + Imaginable Futures) para monitorar com o Querido Diário as aquisições de tecnologia para educação básica.

## Colaborando

Para saber como colaborar com o projeto, seja através de *Issues*, *Pull
Requests* ou interagindo com a comunidade, leia o
[documento de colaboração](CONTRIBUTING.md).

## Subindo com Docker

- é necessário ter um snapshot do índice do Elasticsearch dentro da pasta `./downloads` (baixe o índice [aqui](https://drive.google.com/drive/folders/1V1G5gkPuG0ehyTNzCiUfc02fi5nqDtfA?usp=sharing), descompactando o conteúdo da pasta `indices` de `indices.zip` para a pasta `./downloads/`, assim como os outros arquivos - disponível para download até Out/2021)
- execute o restore `make restore-es`
- crie um ambiente virtual, ative-o e execute `pip install -r requirements_dev.txt`
- inicialize o Jupyter `make run-jupyter-notebook`
- acesse o endereço com o token gerado
- se desejar reproduzir experimentos feitos pela OKBR, copie o conteúdo [desta pasta](https://drive.google.com/drive/folders/1OIHgJiEcCGSP8llYcZ8NCgXLBNV5bW_s?usp=sharing) (disponível até Out/2021) para `./data` e execute os scripts ou notebooks que desejar da pasta `./examples`
- para executar alguns scripts da pasta `./examples` é necessário uma credencial do Google que permita interação com o Google Docs, você pode gerá-la de acordo com [esse tutorial](https://developers.google.com/sheets/api/quickstart/python) e deve colocar o `.json` na pasta `./tests/data`
