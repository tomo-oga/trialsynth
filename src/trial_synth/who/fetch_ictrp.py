import requests
from bs4 import BeautifulSoup

# URL for the form
url = "https://trialsearch.who.int/AdvSearch.aspx"

# Headers
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "application/xml",
    "Sec-Fetch-Site": "same-origin",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Fetch-Mode": "navigate",
    "Host": "trialsearch.who.int",
    "Origin": "https://trialsearch.who.int",
    "Referer": "https://trialsearch.who.int/AdvSearch.aspx",
    "Connection": "keep-alive",
}

# Start a session
session = requests.Session()

# Initial GET request to set session cookies and fetch the form page
response = session.get(url, headers=headers)

# Check response status
print(f"GET request status: {response.status_code}")

# Parse the response to extract hidden form fields
soup = BeautifulSoup(response.text, 'html.parser')
viewstate = soup.find('input', {'name': '__VIEWSTATE'})['value']
event_validation = soup.find('input', {'name': '__EVENTVALIDATION'})['value']
viewstate_generator = soup.find('input', {'name': '__VIEWSTATEGENERATOR'})['value']

# Logging extracted values
print(f"__VIEWSTATE: {viewstate}")
print(f"__EVENTVALIDATION: {event_validation}")
print(f"__VIEWSTATEGENERATOR: {viewstate_generator}")

# Payload for POST request
payload = {
    "ctl00_ContentPlaceHolder1_ToolkitScriptManager_HiddenField": "%3B%3BAjaxControlToolkit%2C+Version%3D3.5.40412.0%2C+Culture%3Dneutral%2C+PublicKeyToken%3D28f01b0e84b6d53e%3Aen-US%3A1547e793-5b7e-48fe-8490-03a375b13a33%3Af2c8e708%3Ade1feab2%3A720a52bf%3Af9cec9bc%3A4a2c8239%3B",
    "ctl00$ContentPlaceHolder1$ucExportDefault$ddlPageSize": "100",
    "ctl00$ContentPlaceHolder1$ucExportDefault$GridViewExport$ctl02$CollapsiblePanelExtender1_ClientState": "true",
    "ctl00$ContentPlaceHolder1$ucExportDefault$GridViewExport$ctl03$CollapsiblePanelExtender1_ClientState": "true",
    "ctl00$ContentPlaceHolder1$ucExportDefault$GridViewExport$ctl04$CollapsiblePanelExtender1_ClientState": "true",
    "ctl00$ContentPlaceHolder1$ucExportDefault$GridViewExport$ctl05$CollapsiblePanelExtender1_ClientState": "true",
    "ctl00$ContentPlaceHolder1$ucExportDefault$GridViewExport$ctl06$CollapsiblePanelExtender1_ClientState": "true",
    "ctl00$ContentPlaceHolder1$ucExportDefault$GridViewExport$ctl07$CollapsiblePanelExtender1_ClientState": "true",
    "ctl00$ContentPlaceHolder1$ucExportDefault$GridViewExport$ctl08$CollapsiblePanelExtender1_ClientState": "true",
    "ctl00$ContentPlaceHolder1$ucExportDefault$GridViewExport$ctl09$CollapsiblePanelExtender1_ClientState": "true",
    "ctl00$ContentPlaceHolder1$ucExportDefault$GridViewExport$ctl10$CollapsiblePanelExtender1_ClientState": "true",
    "ctl00$ContentPlaceHolder1$ucExportDefault$GridViewExport$ctl11$CollapsiblePanelExtender1_ClientState": "true",
    "__EVENTTARGET": "ctl00$ContentPlaceHolder1$ucExportDefault$butExportAllTrials",
    "__EVENTARGUMENT": "",
    "__LASTFOCUS": "",
    "__VIEWSTATE": viewstate,
    "__VIEWSTATEGENERATOR": viewstate_generator,
    "__EVENTVALIDATION": event_validation,
    "__VIEWSTATEENCRYPTED": ""
}

# Send POST request to export trials
response = session.post(url, headers=headers, data=payload)

# Check response
print(f"POST request status: {response.status_code}")
print(response.headers.get('Content-Disposition'))  # To check if the response contains a file
print(response.text)  # Print response content for debugging
