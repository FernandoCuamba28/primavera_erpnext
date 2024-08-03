from flask import Flask, jsonify
import requests
import pandas as pd
from io import BytesIO
import json
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

# # Configurações da API do ERPNext
base_url = "http://192.168.60.13:8080"
api_key = "528eff3406d6c19"
api_secret = "197ffe63e0ced95"

# base_url = "http://13.244.142.208"
# # As suas credenciais de API
# api_key = "0789558443d9687"
# api_secret = "92501dbb7e7b5d8"

headers = {
    'Authorization': f'token {api_key}:{api_secret}',
    'Content-Type': 'application/json'
}


@app.route('/payments', methods=['GET'])
def payments():
    endpoint = "/api/resource/Payment Entry"
    url = base_url + endpoint

    try:
        # Primeiro, obtenha a lista de IDs
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        # Extraia os IDs dos pagamentos
        payment_ids = [entry.get('name') for entry in data.get('data', [])]

        # Obtenha detalhes para cada ID
        detailed_payments = []
        for payment_id in payment_ids:
            detail_url = f"{base_url}/api/resource/Payment Entry/{payment_id}"
            try:
                detail_response = requests.get(detail_url, headers=headers)
                detail_response.raise_for_status()
                payment_details = detail_response.json()

                # Adiciona o ID aos detalhes do pagamento
                payment_data = payment_details.get('data', {})
                payment_data['id'] = payment_id

                # Adiciona os dados de referência da fatura
                invoice_reference_name = None
                payment_references = payment_data.get('references', [])
                for reference in payment_references:
                    if reference.get('reference_doctype') == 'Sales Invoice':
                        invoice_reference_name = reference.get('reference_name')
                        break  # Presumindo que há apenas uma fatura por pagamento

                # Buscar dados do cliente e o campo 'custom_codigo'
                customer_id = payment_data.get('party')  # Ajuste o nome do campo se necessário
                if customer_id:
                    customer_url = f"{base_url}/api/resource/Customer/{customer_id}"
                    try:
                        customer_response = requests.get(customer_url, headers=headers)
                        customer_response.raise_for_status()
                        customer_data = customer_response.json()
                        custom_codigo = customer_data.get('data', {}).get('custom_codigo', None)
                        payment_data['custom_codigo'] = custom_codigo
                    except requests.exceptions.RequestException as customer_error:
                        payment_data['custom_codigo_error'] = str(customer_error)

                # Formata o resultado conforme solicitado
                formatted_payment = {
                    'id': payment_data.get('id'),
                    'naming_series': payment_data.get('naming_series'),
                    'payment_type': payment_data.get('payment_type'),
                    'posting_date': payment_data.get('posting_date'),
                    'company': payment_data.get('company'),
                    'paid_from': payment_data.get('paid_from'),
                    'paid_from_account_currency': payment_data.get('paid_from_account_currency'),
                    'paid_to': payment_data.get('paid_to'),
                    'paid_to_account_currency': payment_data.get('paid_to_account_currency'),
                    'paid_amount': payment_data.get('paid_amount'),
                    'source_exchange_rate': payment_data.get('source_exchange_rate'),
                    'received_amount': payment_data.get('received_amount'),
                    'target_exchange_rate': payment_data.get('target_exchange_rate'),
                    'custom_codigo': payment_data.get('custom_codigo'),
                    'party_name': payment_data.get('party_name'),
                    'mode_of_payment': payment_data.get('mode_of_payment'),
                    'total_allocated_amount': payment_data.get('total_allocated_amount'),
                    'factura': invoice_reference_name,
                    'total_amount': payment_data.get('paid_amount')  # Considerando que o total_amount é o paid_amount
                }
                detailed_payments.append(formatted_payment)

            except requests.exceptions.RequestException as payment_error:
                detailed_payments.append({'id': payment_id, 'error': str(payment_error)})

        # Pega os últimos 20 registros, se necessário
        last_20_payments = detailed_payments[-20:]

        return jsonify(last_20_payments)

    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 500


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


# Esta funcao,e responsavel por gerar o excel das facturas de vendas(sales Invoices)
def generate_excel(data):
    df = pd.DataFrame(data)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Invoices')
    output.seek(0)
    return output


# Este metodo gera de forma automatica, facturas de 6 em 6 horas.
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





# Esta funcao busca a lista de factura de Vendas Pendentes.
@app.route('/facturas', methods=['GET'])
def get_invoices():
    invoices = fetch_facturas()
    return jsonify(invoices)


# Esta funcao serve para gerar o excel num ambiente de teste como, postman, insomia etc.
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


# NOTA: O CODIGO ABAIXO E REFERENTE A EXPORTACAO DE RECIBOS DE PAGAMENTOS APENAS
# Esta funcao e usada pelo metodo get_all_payment para gerar o ficheiro excel com os recibos
def generate_payment_excel(data):
    # Criação do DataFrame
    df = pd.DataFrame(data)
    output = BytesIO()
    df.to_excel(output, index=False, engine='openpyxl')
    output.seek(0)  # Voltar ao início do BytesIO
    return output


# Esta e a funcao usada para gerar os recibos, usando postman, insomia ou algum ambiente de testes
@app.route('/payments/excel', methods=['GET'])
def get_all_payments():
    endpoint = "/api/resource/Payment Entry"
    url = base_url + endpoint

    try:
        # Primeiro, obtenha a lista de IDs
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        # Extraia os IDs dos pagamentos
        payment_ids = [entry.get('name') for entry in data.get('data', [])]

        # Obtenha detalhes para cada ID
        detailed_payments = []
        for payment_id in payment_ids:
            detail_url = f"{base_url}/api/resource/Payment Entry/{payment_id}"
            try:
                detail_response = requests.get(detail_url, headers=headers)
                detail_response.raise_for_status()
                payment_details = detail_response.json()

                # Adiciona o ID aos detalhes do pagamento
                payment_data = payment_details.get('data', {})
                payment_data['id'] = payment_id

                # Adiciona os dados de referência da fatura
                invoice_reference_name = None
                payment_references = payment_data.get('references', [])
                for reference in payment_references:
                    if reference.get('reference_doctype') == 'Sales Invoice':
                        invoice_reference_name = reference.get('reference_name')
                        break  # Presumindo que há apenas uma fatura por pagamento

                # Buscar dados do cliente e o campo 'custom_codigo'
                customer_id = payment_data.get('party')  # Ajuste o nome do campo se necessário
                if customer_id:
                    customer_url = f"{base_url}/api/resource/Customer/{customer_id}"
                    try:
                        customer_response = requests.get(customer_url, headers=headers)
                        customer_response.raise_for_status()
                        customer_data = customer_response.json()
                        custom_codigo = customer_data.get('data', {}).get('custom_codigo', None)
                        payment_data['custom_codigo'] = custom_codigo
                    except requests.exceptions.RequestException as customer_error:
                        payment_data['custom_codigo_error'] = str(customer_error)

                # Formata o resultado conforme solicitado
                formatted_payment = {
                    'id': payment_data.get('id'),
                    'naming_series': payment_data.get('naming_series'),
                    'payment_type': payment_data.get('payment_type'),
                    'posting_date': payment_data.get('posting_date'),
                    'company': payment_data.get('company'),
                    'paid_from': payment_data.get('paid_from'),
                    'paid_from_account_currency': payment_data.get('paid_from_account_currency'),
                    'paid_to': payment_data.get('paid_to'),
                    'paid_to_account_currency': payment_data.get('paid_to_account_currency'),
                    'paid_amount': payment_data.get('paid_amount'),
                    'source_exchange_rate': payment_data.get('source_exchange_rate'),
                    'received_amount': payment_data.get('received_amount'),
                    'target_exchange_rate': payment_data.get('target_exchange_rate'),
                    'custom_codigo': payment_data.get('custom_codigo'),
                    'party_name': payment_data.get('party_name'),
                    'mode_of_payment': payment_data.get('mode_of_payment'),
                    'total_allocated_amount': payment_data.get('total_allocated_amount'),
                    'factura': invoice_reference_name,
                    'total_amount': payment_data.get('paid_amount')  # Considerando que o total_amount é o paid_amount
                }
                detailed_payments.append(formatted_payment)

            except requests.exceptions.RequestException as payment_error:
                detailed_payments.append({'id': payment_id, 'error': str(payment_error)})

        # Pega os últimos 20 registros, se necessário
        last_20_payments = detailed_payments[-50:]

        # Gera o arquivo Excel
        output = generate_payment_excel(last_20_payments)
        file_path = 'payments.xlsx'
        with open(file_path, 'wb') as f:
            f.write(output.getvalue())

        return jsonify({"message": f"Arquivo salvo como {file_path} na raiz do projeto"})

    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 500


def fetch_and_save_payments():
    endpoint = "/api/resource/Payment Entry"
    url = base_url + endpoint

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        payment_ids = [entry.get('name') for entry in data.get('data', [])]

        detailed_payments = []
        for payment_id in payment_ids:
            detail_url = f"{base_url}/api/resource/Payment Entry/{payment_id}"
            try:
                detail_response = requests.get(detail_url, headers=headers)
                detail_response.raise_for_status()
                payment_details = detail_response.json()

                payment_data = payment_details.get('data', {})
                payment_data['id'] = payment_id

                invoice_reference_name = None
                payment_references = payment_data.get('references', [])
                for reference in payment_references:
                    if reference.get('reference_doctype') == 'Sales Invoice':
                        invoice_reference_name = reference.get('reference_name')
                        break

                customer_id = payment_data.get('party')
                if customer_id:
                    customer_url = f"{base_url}/api/resource/Customer/{customer_id}"
                    try:
                        customer_response = requests.get(customer_url, headers=headers)
                        customer_response.raise_for_status()
                        customer_data = customer_response.json()
                        custom_codigo = customer_data.get('data', {}).get('custom_codigo', None)
                        payment_data['custom_codigo'] = custom_codigo
                    except requests.exceptions.RequestException as customer_error:
                        payment_data['custom_codigo_error'] = str(customer_error)

                formatted_payment = {
                    'id': payment_data.get('id'),
                    'naming_series': payment_data.get('naming_series'),
                    'payment_type': payment_data.get('payment_type'),
                    'posting_date': payment_data.get('posting_date'),
                    'company': payment_data.get('company'),
                    'paid_from': payment_data.get('paid_from'),
                    'paid_from_account_currency': payment_data.get('paid_from_account_currency'),
                    'paid_to': payment_data.get('paid_to'),
                    'paid_to_account_currency': payment_data.get('paid_to_account_currency'),
                    'paid_amount': payment_data.get('paid_amount'),
                    'source_exchange_rate': payment_data.get('source_exchange_rate'),
                    'received_amount': payment_data.get('received_amount'),
                    'target_exchange_rate': payment_data.get('target_exchange_rate'),
                    'custom_codigo': payment_data.get('custom_codigo'),
                    'party_name': payment_data.get('party_name'),
                    'mode_of_payment': payment_data.get('mode_of_payment'),
                    'total_allocated_amount': payment_data.get('total_allocated_amount'),
                    'factura': invoice_reference_name,
                    'total_amount': payment_data.get('paid_amount')
                }
                detailed_payments.append(formatted_payment)

            except requests.exceptions.RequestException as payment_error:
                detailed_payments.append({'id': payment_id, 'error': str(payment_error)})

        last_20_payments = detailed_payments[-50:]

        output = generate_payment_excel(last_20_payments)
        file_path = 'payments.xlsx'
        with open(file_path, 'wb') as f:
            f.write(output.getvalue())

        print(f"Arquivo salvo como {file_path} na raiz do projeto")

    except requests.exceptions.RequestException as e:
        print(f"Erro: {str(e)}")



# Configuração do CronJob
scheduler = BackgroundScheduler()
scheduler.add_job(func=fetch_and_process_invoices, trigger="interval", minutes=1) #Gera a factura
scheduler.add_job(func=fetch_and_save_payments, trigger="interval", minutes=1) #Gera os Recibos
scheduler.start()



if __name__ == '__main__':
    # Inicie o Flask app
    app.run(debug=True)
