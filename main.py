import re
from base64 import urlsafe_b64decode
from functools import lru_cache
from pathlib import Path
from time import sleep
from typing import List
from imap_tools import MailMessage
import json

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from bs4 import BeautifulSoup


def _get_creds(token_file, credentials_file, scopes, oauth2_port):
	creds = None

	if Path(token_file).exists():
		creds = Credentials.from_authorized_user_file(token_file, scopes)

	# If there are no (valid) credentials available, let the user log in.
	if not creds or not creds.valid:
		if creds and creds.expired and creds.refresh_token:
			from google.auth.exceptions import RefreshError
			try:
				creds.refresh(Request())
			except RefreshError:
				flow = InstalledAppFlow.from_client_secrets_file(credentials_file, scopes)
				creds = flow.run_local_server(open_browser=False, oauth2_port=oauth2_port)
		else:
			flow = InstalledAppFlow.from_client_secrets_file(credentials_file, scopes)
			creds = flow.run_local_server(open_browser=False, oauth2_port=oauth2_port)
		# Save the credentials for the next run
		with Path(token_file).open("w") as token:
			token.write(creds.to_json())
	return creds


class GmailConnection():
	def __init__(
			self,
			token_file: str,
			credentials_file: str,
			scopes: List[str],
			include_spam_trash: bool,
			reports_folder: str,
			oauth2_port: int,
			paginate_messages: bool,
	):
		creds = _get_creds(token_file, credentials_file, scopes, oauth2_port)
		self.service = build("gmail", "v1", credentials=creds)
		self.include_spam_trash = include_spam_trash
		self.reports_label_id = self._find_label_id_for_label(reports_folder)
		self.paginate_messages = paginate_messages

	def _fetch_all_message_ids(self, reports_label_id, page_token=None):
		results = (
			self.service.users()
			.messages()
			.list(
				userId="me",
				includeSpamTrash=self.include_spam_trash,
				labelIds=[reports_label_id],
				pageToken=page_token,
			)
			.execute()
		)
		messages = results.get("messages", [])
		for message in messages:
			yield message["id"]

		if "nextPageToken" in results and self.paginate_messages:
			yield from self._fetch_all_message_ids(
				reports_label_id, results["nextPageToken"]
			)

	def fetch_messages(self, reports_folder: str, **kwargs) -> List[str]:
		reports_label_id = self._find_label_id_for_label(reports_folder)
		return [id for id in self._fetch_all_message_ids(reports_label_id)]

	def fetch_message(self, message_id):
		msg = (
			self.service.users()
			.messages()
			.get(userId="me", id=message_id, format="raw")
			.execute()
		)
		return urlsafe_b64decode(msg["raw"])

	@lru_cache(maxsize=10)
	def _find_label_id_for_label(self, label_name: str) -> str:
		results = self.service.users().labels().list(userId="me").execute()
		labels = results.get("labels", [])
		for label in labels:
			if label_name == label["id"] or label_name == label["name"]:
				return label["id"]


if __name__ == "__main__":
	infoDict = []
	batch_size_increment = 200
	batch_min = 0
	batch_max = batch_size_increment
	batch_counter = 1
	client = GmailConnection("token.json", "tyler_cred.json",
	                         ["https://www.googleapis.com/auth/gmail.readonly"], False, "Gaby", 0,
	                         True)
	message_list = client.fetch_messages("Gaby")
	while batch_max < len(message_list):
		for message_id in message_list[batch_min:batch_max]:
			result = client.fetch_message(message_id)
			msg = MailMessage.from_bytes(result)
			msg_dict = {}
			for key, value in msg.obj.items():
				msg_dict[key] = value
			regex_pat = r'(?:\n>+(.*?)\r)|(\u200B{3,})'
			regex = re.compile(regex_pat)
			cleaned_text = regex.sub('',msg.text)
			msg_dict['email_content'] = cleaned_text
			infoDict.append(msg_dict)

		outputJSON = json.dumps(infoDict)
		with open(f"gaby-email-part-{batch_counter}.json", "w") as outfile:
			outfile.write(outputJSON)
		batch_min += batch_size_increment
		batch_max += batch_size_increment
		if batch_min + batch_max > len(message_list):
			batch_max = len(message_list)
		batch_counter += 1

