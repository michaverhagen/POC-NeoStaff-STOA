import os.path
import base64
import csv
import time
import argparse
import pickle
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from email.mime.text import MIMEText

SERVICE_ACCOUNT_FILE = 'credentials.json'
SCOPES = ['https://www.googleapis.com/auth/gmail.send', 'https://www.googleapis.com/auth/gmail.readonly']
COUNTER = 0


def _initialize_service():
    service = None
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(SERVICE_ACCOUNT_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('gmail', 'v1', credentials=creds)
    return service


def _send_message(service, user_id, message):
    """Send an email message.
    Args:
      service: Authorized Gmail API service instance.
      user_id: User's email address. The special value "me"
      can be used to indicate the authenticated user.
      message: Message to be sent.
    """

    global COUNTER
    try:
        message = (service.users().messages().send(userId=user_id, body=message).execute())
        print('Message Id: ' + message['id'])
        if COUNTER <= 7:
            time.sleep(5)
            COUNTER = COUNTER + 1
        else:
            print('Pauze...')
            COUNTER = 0

    except Exception as e:
        print('An error occurred:', e)


def _create_message(subject, template, address):
    """Create a message """

    output = None
    with open(template, 'r') as file:
        content = file.read().replace('\n', '')
        messagetext = content.replace('{{ firstname }}', "Roland")

        message = MIMEText(messagetext, 'html')
        message['to'] = address
        message['from'] = 'Micha Verhagen <micha@semi.technology>'
        message['subject'] = subject
        output = {'raw': base64.b64encode(str.encode(message.as_string())).decode('utf-8')}

    return output


def send_mail(subject, template, address):
    service = _initialize_service()
    message = _create_message(subject, template, address)
    _send_message(service, 'me', message)
