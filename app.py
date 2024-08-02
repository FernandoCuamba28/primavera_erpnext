from flask import Flask, jsonify
import requests
import pandas as pd
from io import BytesIO
import json
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

# # Configurações da API do ERPNext
# base_url = "http://192.168.60.13:8080"
# api_key = "528eff3406d6c19"
# api_secret = "197ffe63e0ced95"

base_url = "http://13.244.142.208"
# As suas credenciais de API
api_key = "0789558443d9687"
api_secret = "92501dbb7e7b5d8"

headers = {
    'Authorization': f'token {api_key}:{api_secret}',
    'Content-Type': 'application/json'
}


def fetch_facturas():
    url = f'{base_url}/api/resource/Sales Invoice'
    params = {
        'fields': json.dumps(["*"]),
        'limit_page_length': 100,
        'order_by': 'posting_date desc',
        'filters': json.dumps({"posting_date": [">=", "2024-01-01"]})
    }
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        invoices = response.json().get('data', [])
        print(json.dumps(invoices, indent=4))
        return invoices
    except requests.RequestException as e:
        print(f'Error: {e}')
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


def fetch_and_process_invoices():
    print("Iniciando o processo de busca e processamento das faturas...")
    invoices = fetch_facturas()
    report_data = []
    status_translation = {
        'Draft': 'Rascunho',
        'Submitted': 'Submetido',
        'Cancelled': 'Cancelado',
        'Paid': 'Pago',
        'Overdue': 'Vencido',
        'Partially Paid': 'Parcialmente Pago',
        'Unpaid': 'Nao Pago',
        'Return': 'Nota de Credito'
    }
    for invoice in invoices:
        invoice_name = invoice.get('name')
        if not invoice_name:
            continue
        detailed_invoice = fetch_invoice_details(invoice_name)
        customer_name = detailed_invoice.get('customer')
        customer_details = fetch_customer_details(customer_name)
        items = detailed_invoice.get('items', [])
        total_qty = sum(item.get('qty', 0) for item in items)
        total_amount = detailed_invoice.get('grand_total', 0)
        total_taxes = detailed_invoice.get('base_total_taxes_and_charges', 0)
        invoice_status = detailed_invoice.get('status', 'Unknown')
        translated_status = status_translation.get(invoice_status, 'Desconhecido')
        for item in items:
            item_name = item.get('item_code')
            item_details = fetch_item_details(item_name) if item_name else {}
            invoice_data = {
                "Data": detailed_invoice.get('posting_date', ''),
                "Entidade": customer_details.get('custom_codigo', ''),
                "TipoDoc": 'FA',
                "NumDoc": '',
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
                "Serie": detailed_invoice.get('posting_date', '').split('-')[0],
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
                "artigo": item_details.get('custom_codigo', ''),
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
                "Estado": translated_status,
                "DocImp": 0
            }
            report_data.append(invoice_data)
    output = generate_excel(report_data)
    file_path = 'invoices.xlsx'
    with open(file_path, 'wb') as f:
        f.write(output.getvalue())
    print(f"Arquivo salvo como {file_path} na raiz do projeto")


# Configuração do agendador
scheduler = BackgroundScheduler()
scheduler.add_job(func=fetch_and_process_invoices, trigger="interval", minutes=3600)
scheduler.start()


@app.route('/facturas', methods=['GET'])
def get_invoices():
    invoices = fetch_facturas()
    return jsonify(invoices)


@app.route('/facturas/excel', methods=['GET'])
def get_invoices_excel():
    invoices = fetch_facturas()
    report_data = []
    status_translation = {
        'Draft': 'Rascunho',
        'Submitted': 'Submetido',
        'Cancelled': 'Cancelado',
        'Paid': 'Pago',
        'Overdue': 'Vencido',
        'Partially Paid': 'Parcialmente Pago'
    }
    for invoice in invoices:
        invoice_name = invoice.get('name')
        if not invoice_name:
            continue
        detailed_invoice = fetch_invoice_details(invoice_name)
        customer_name = detailed_invoice.get('customer')
        customer_details = fetch_customer_details(customer_name)
        items = detailed_invoice.get('items', [])
        total_qty = sum(item.get('qty', 0) for item in items)
        total_amount = detailed_invoice.get('grand_total', 0)
        total_taxes = detailed_invoice.get('base_total_taxes_and_charges', 0)
        invoice_status = detailed_invoice.get('status', 'Unknown')
        translated_status = status_translation.get(invoice_status, 'Desconhecido')
        for item in items:
            item_name = item.get('item_code')
            item_details = fetch_item_details(item_name) if item_name else {}
            invoice_data = {
                "Data": detailed_invoice.get('posting_date', ''),
                "Entidade": customer_details.get('custom_codigo', ''),
                "TipoDoc": 'FA',
                "NumDoc": '',
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
                "Serie": detailed_invoice.get('posting_date', '').split('-')[0],
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
                "artigo": item_details.get('custom_codigo', ''),
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
                "Estado": translated_status,
                "DocImp": 0
            }
            report_data.append(invoice_data)
    output = generate_excel(report_data)
    file_path = 'invoices.xlsx'
    with open(file_path, 'wb') as f:
        f.write(output.getvalue())
    return jsonify({"message": f"Arquivo salvo como {file_path} na raiz do projeto"})


@app.route('/item/update', methods=['GET'])
def update_item():
    # Código para atualizar itens (como no seu código original)
    return jsonify({"message": "Items updated"})


if __name__ == '__main__':
    # Inicie o Flask app
    app.run(debug=True)
