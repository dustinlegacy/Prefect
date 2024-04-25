import requests
import json
from datetime import datetime
import win32com.client
import pythoncom
from prefect import task, flow




# Task 1: Authentication
@task
def authenticate():
    GetTokenURL = "https://clmapi.landgorilla.com/api/token"
    body = {'api_name': 'clm'}
    headers = {
        'USER': 'dustinh@legacyg.com',
        'PASSWORD': 'Index@2034!'
    }
    response = requests.get(GetTokenURL, headers=headers, json=body)
    json_data = response.json()
    return json_data.get('token')

# Task 2: Get Template ID

@task
def get_template_id(api_token):
    GetTemplateURL = "https://clmapi.landgorilla.com/api/clm/pipelineReportTemplates"
    headers = {
    'Authorization': f'Bearer {api_token}'
    }
    response = requests.get(GetTemplateURL, headers=headers, json={})
    templatedata = response.json()
    
    for item in templatedata['data']['items']:
        if item['name'] == 'AccountingAllActiveLoans':
            template_id = item['id']

    return template_id
    
# Task 3: Get Specific Loan

@task
def get_loan(template_id, loan_id, api_token):
    GetLoanURL = f"https://clmapi.landgorilla.com/api/clm/pipelineReportTemplates/{template_id}/loans/{loan_id}"
    headers = {'Authorization': f'Bearer {api_token}'
    }
    GetLoanResponse = requests.get(GetLoanURL, headers=headers, json={})
    loan_data = GetLoanResponse.json().get('data', {}).get('fields', {})
    print(f"token: {api_token}, loan data: {loan_data}")
    return loan_data

# Task 4: Send Email Notification

@task
def send_email(loan_data, event=None, updated_status=None):
    risk_labels = loan_data.get('riskLabel', [])
    loan_number = loan_data.get('loanNumber', [])
    if 'Accounting' in risk_labels:
            pythoncom.CoInitialize()
            outlook = win32com.client.Dispatch("Outlook.Application")
            mail = outlook.CreateItem(0)
            mail.Subject = f"Draw Change for {loan_number}"
            mail.Body = f"Loan Number: {loan_number}\nRisk Label(s): {risk_labels}\nUpdated Status: {updated_status}"
            mail.To = "everetth@legacyg.com"
            mail.Send()
    else:
            print(f"Risk label does not contain 'Accounting'. No email sent. loan#:{loan_number}, risk label: {risk_labels}")

@flow
def Accounting_webhook(event: str, kwargs, loan_id: str, updated_status: str) -> None:
    api_token = authenticate()
    template_id = get_template_id(api_token)
    loan_data = get_loan(template_id, loan_id, api_token)
    send_email(loan_data, event, updated_status)

if __name__ == "__main__":
    Accounting_webhook()