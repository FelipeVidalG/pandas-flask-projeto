import zipfile
import pandas as pd
from flask import Flask, jsonify

app = Flask(__name__)

def descompactar_arquivo_zip():
  with zipfile.ZipFile("dados.zip", "r") as zip:
    zip.extractall()

def ler_arquivos_csv():
  df1 = pd.read_csv("tipos.csv")
  df2 = pd.read_csv("origem-dados.csv")
  return df1,df2

def manipula_origem_dados(arquivos):
  df_tipos = arquivos[0]
  df_origem_dados = arquivos[1]
  df_status_critico = df_origem_dados[df_origem_dados["status"] == "CRITICO"]
  df_ordenado = df_status_critico.sort_values(by="created_at")
  nome_tipo = []

  for tipo in df_ordenado["tipo"]:
    resultado = df_tipos.loc[df_tipos['id'] == tipo, 'nome'].values

    # Tratando possível erro se não existir o tipo
    if len(resultado) > 0:
      valor_nome_tipo = resultado[0]
    else:
      valor_nome_tipo = ""
    nome_tipo.append(valor_nome_tipo)
  
  df_ordenado["nome_tipo"] = nome_tipo
  gera_arquivo_sql(df_ordenado)

def gera_arquivo_sql(df_ordenado):
  with open("insert-dados.sql", "w") as arquivo:
    # Query para criar a tabela
    arquivo.write("CREATE TABLE IF NOT EXISTS dados_finais (\n id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,\n created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,\n product_code INT NOT NULL,\n customer_code INT NOT NULL,\n status TEXT NOT NULL,\n tipo INT NOT NULL,\n nome_tipo TEXT\n);\n\n")
    
    valores = []
    comando_sql = "INSERT INTO dados_finais (created_at, product_code, customer_code, status, tipo, nome_tipo) VALUES"

    # Monta e adiciona ao arquivo sql um insert com todos os dados.
    for index, linha in df_ordenado.iterrows():
      valores.append(f"('{linha['created_at']}', '{linha['product_code']}', '{linha['customer_code']}', '{linha['status']}', '{linha['tipo']}', '{linha['nome_tipo']}')")

    valores_str = ', '.join(valores)
    comando_sql_final = f"{comando_sql} {valores_str};"
    arquivo.write(comando_sql_final)

    # Query para retornar por dia a quantidade de itens agrupada pelo tipo
    arquivo.write("\n\nSELECT DATE(created_at) AS data, tipo, COUNT(*) AS quantidade \n FROM \n dados_finais \n GROUP BY \n DATE(created_at), tipo \n ORDER BY \n DATE(created_at);")
  
@app.route('/tipo/<tipo_id>', methods=['GET'])
def busca_tipo(tipo_id):
  df_tipos = ler_arquivos_csv()[0]
  try:
    nome_tipo = df_tipos.loc[df_tipos['id'] == int(tipo_id), 'nome'].values[0]
    resultado = {"id": tipo_id, "nome": nome_tipo }
    return jsonify({'mensagem': 'Tipo encontrado', 'data': resultado}), 200
  except:
    return jsonify({'message': "Tipo não encontrado", 'data': {}}), 404
  
def main():
  descompactar_arquivo_zip()
  arquivos = ler_arquivos_csv()
  manipula_origem_dados(arquivos)

  # Iniciar servidor Flask após gerar e manipular os arquivos
  app.run(debug=True)

if __name__ == "__main__":
  main()