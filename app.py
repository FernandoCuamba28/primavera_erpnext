from flask import Flask, jsonify
import requests
import pandas as pd
from io import BytesIO
import json
from datetime import datetime

app = Flask(__name__)

# Configurações da API do ERPNext
base_url = "http://192.168.60.13:8080"

# As suas credenciais de API
api_key = "528"
api_secret = "197"

# Cabeçalhos para autenticação
headers = {
    'Authorization': f'token {api_key}:{api_secret}',
    'Content-Type': 'application/json'
}


def fetch_facturas():
    url = f'{base_url}/api/resource/Sales Invoice'
    params = {
        'fields': json.dumps(["*"]),  # Adiciona um parâmetro para retornar todos os campos
        'limit_page_length': 100,  # Limitar o número de resultados para 20
        'order_by': 'posting_date desc'  # Ordenar por data de postagem de forma decrescente
    }
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        invoices = response.json()['data']
        print(json.dumps(invoices, indent=4))  # Adicionando uma instrução de impressão para debug
        return invoices
    else:
        print(f'Error {response.status_code}: {response.text}')
        return []


def fetch_invoice_details(invoice_name):
    url = f'{base_url}/api/resource/Sales Invoice/{invoice_name}'
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get('data', {})
    else:
        print(f'Error {response.status_code}: {response.text}')
        return {}


def fetch_customer_details(customer_name):
    url = f'{base_url}/api/resource/Customer/{customer_name}'
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get('data', {})
    else:
        print(f'Error {response.status_code}: {response.text}')
        return {}


def fetch_item_details(item_name):
    url = f'{base_url}/api/resource/Item/{item_name}'
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get('data', {})
    else:
        print(f'Error {response.status_code}: {response.text}')
        return {}


def generate_excel(data):
    df = pd.DataFrame(data)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Invoices')
    output.seek(0)
    return output


@app.route('/facturas', methods=['GET'])
def get_invoices():
    invoices = fetch_facturas()
    return jsonify(invoices)


@app.route('/facturas/excel', methods=['GET'])
def get_invoices_excel():
    invoices = fetch_facturas()

    # Prepare data for Excel export
    report_data = []

    for invoice in invoices:
        invoice_name = invoice.get('name')
        if not invoice_name:
            continue

        detailed_invoice = fetch_invoice_details(invoice_name)
        customer_name = detailed_invoice.get('customer')
        customer_details = fetch_customer_details(customer_name)

        # Adicionando campos de itens
        items = detailed_invoice.get('items', [])
        total_qty = sum(item.get('qty', 0) for item in items)  # Total de quantidade dos itens
        total_amount = detailed_invoice.get('grand_total', 0)  # Total da fatura
        total_taxes = detailed_invoice.get('base_total_taxes_and_charges', 0)  # Total dos impostos

        for item in items:
            item_name = item.get('item_code')
            item_details = fetch_item_details(item_name) if item_name else {}

            # Coletando dados da fatura
            invoice_data = {
                "Data": detailed_invoice.get('posting_date', ''),
                "Entidade": customer_details.get('custom_codigo', ''),  # Campo personalizado do cliente
                "TipoDoc": 'FA',
                "NumDoc": '',  # Campo "NumDoc" vazio
                "CondPag": 1,
                "DescPag": 0,
                "TotalMerc": total_amount,
                "TotalIva": total_taxes,
                "TotalDesc": 0,
                "ModoPag": '',
                "RegimeIva": 5,
                "moeda": 'MT',
                "Cambio": 1,
                "DataVencimento": detailed_invoice.get('due_date', ''),
                "Filial": 0,
                "Serie": detailed_invoice.get('posting_date', '').split('-')[0],  # Extrai o ano da data
                "MoedaDaUEM": 1,
                "DataUltimaActualizacao": detailed_invoice.get('posting_date', ''),
                "NumContribuinte": '.',
                "Nome": detailed_invoice.get('customer_name', ''),
                "CodPostalLocalidade": None,
                "Utilizador": "primavera",
                "Posto": None,
                "Observacoes": None,
                "PercentagemRetencao": 0,
                "TotalRetencao": 0,
                "Id": '',
                "IdCabecTesouraria": None,
                "TipoEntidade": 'C',
                "DescEntidade": 0,
                "CambioMAlt": '',
                "TipoEntidadeEntrega": 'c',
                "TipoEntidadeFac": 'C',
                "NumContribuinteFac": None,
                "TotalIEC": 0,
                "DataGravacao": detailed_invoice.get('posting_date', ''),
                "RegimeIvaReembolsos": 0,
                "PaisCarga": 'MZ',
                "TotalIS": 0,
                "TrataIvaCaixa": 0,
                "Documento": invoice_name,
                "TotalDocumento": total_amount,
                "MargemDoc": 0,
                "DataHoraCarga": detailed_invoice.get('posting_date', '') + 'T' + datetime.now().strftime('%H:%M:%S'),
                "ValorEntregue": 0,
                "ServContinuados": 0,
                "CriadoPor": "primavera",
                "NumLinha": '',
                "artigo": item_details.get('custom_codigo', ''),  # Campo personalizado do item
                "Desconto1": 0,
                "TaxaIva": 5,
                "CodIva": 5,
                "Quantidade": total_qty,
                "PCM": '',
                "Pre Unit": item.get('rate', 0),
                "RegimeIva": 5,
                "Data": detailed_invoice.get('posting_date', ''),
                "TipoLinha": '',
                "Seccao": '',
                "Armazen": item_details.get('warehouse', ''),
                "MovSTK": item_details.get('is_stock_item', ''),
                "FactorConv": 6,
                "NumLinhaSTKGerada": '',
                "Data de Saida": detailed_invoice.get('posting_date', ''),
                "DescontoComercial": 0,
                "QntFormula": 0,
                "Comissao": 0,
                "Lote": item_details.get('batch_no'),
                "Preco Liquido": total_amount - total_taxes,
                "IntrastatValorLiq": 0,
                "Descricao": item.get('item_name', ''),
                "VersaoUltAct": '',
                "IdCabecDoc": '',
                "Id": '',
                "Unidade": item_details.get('stock_uom', ''),
                "DataEntrega": None,
                "IdHistorico": None,
                "Devolucao": 0,
                "PCMDevolucao": 0,
                "DifPCMedio": 0,
                "PercIvaDedutivel": 100,
                "IvaNaoDedutivel": 0,
                "Armazen": item_details.get('warehouse', ''),
                "TaxaRecargo": 0,
                "TotalIliquido": total_amount,
                "EstadoOrigem": 'DISP',
                "CustoMercadoriasMBase": '',
                "CustoMercadoriasMAlt": '',
                "IdCabecDoc": '',
                "Estado": 'P',
                "DocImp": 0
            }

            report_data.append(invoice_data)

    output = generate_excel(report_data)

    # Salvar o arquivo na raiz do projeto
    file_path = 'invoices.xlsx'
    with open(file_path, 'wb') as f:
        f.write(output.getvalue())

    return jsonify({"message": f"Arquivo salvo como {file_path} na raiz do projeto"})


if __name__ == '__main__':
    app.run(debug=True)


@app.route('/item/update', methods=['GET'])
def update_item():
    # URL base do seu ERPNext
    base_url = "http://13.244.142.208"
    endpoint = "/api/resource/Item"

    # Suas credenciais de API
    api_key = "0789558443d9687"
    api_secret = "92501dbb7e7b5d8"

    # Headers de autenticação
    headers = {
        "Authorization": f"token {api_key}:{api_secret}"
    }

    # Carregar o último código atualizado
    try:
        with open('last_custom_code.txt', 'r') as file:
            last_code = int(file.read().strip())
    except FileNotFoundError:
        last_code = 0

    # Variável para manter o código sequencial
    code = last_code + 1

    # Contador para limitar o número de atualizações
    update_limit = 200
    updates_done = 0

    # Parâmetros da consulta
    params = {
        "fields": '["name", "custom_codigo"]',  # Buscar apenas os campos necessários
        "limit_page_length": update_limit,  # Limitar o número de resultados a 200
        "order_by": "creation asc"  # Ordenar por data de criação
    }

    try:
        # Fazendo a requisição GET para obter a lista de itens com timeout aumentado
        response = requests.get(base_url + endpoint, headers=headers, params=params, timeout=120)
        response.raise_for_status()  # Levanta um erro para status HTTP 4xx/5xx

        items = response.json().get('data', [])

        if not items:
            return jsonify({"message": "Todos os itens já foram atualizados"}), 200

        # Para cada item, atualizar o campo custom_codigo com o código sequencial, se não estiver presente ou for 0
        for item in items:
            if updates_done >= update_limit:
                break

            item_name = item.get('name')
            current_code = item.get('custom_codigo')

            if current_code in [None, '', 0]:  # Atualizar se custom_codigo estiver ausente, vazio ou igual a 0
                new_code = str(code).zfill(6)  # Gerar um código de 6 dígitos
                update_endpoint = f"{base_url}/api/resource/Item/{item_name}"
                update_data = {
                    "custom_codigo": new_code
                }

                try:
                    update_response = requests.put(update_endpoint, headers=headers, json=update_data, timeout=120)
                    update_response.raise_for_status()  # Levanta um erro para status HTTP 4xx/5xx

                    print(f"Código {new_code} atualizado para o item {item_name}")

                    # Persistir o código atualizado
                    with open('last_custom_code.txt', 'w') as file:
                        file.write(new_code)

                    # Incrementar o código para o próximo item
                    code += 1
                    updates_done += 1

                except requests.RequestException as e:
                    print(f"Erro ao atualizar código para o item {item_name}: {e}")

        return jsonify({"message": f"{updates_done} itens atualizados com sucesso"}), 200

    except requests.RequestException as e:
        return jsonify({"error": f"Erro ao buscar a lista de itens: {e}"}), 400


if __name__ == '__main__':
    app.run(debug=True)
