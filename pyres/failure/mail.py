import smtplib

from textwrap import dedent
from email.mime.text import MIMEText

from base import BaseBackend

class MailBackend(BaseBackend):
    subject = 'Pyres Failure on {queue}'

    recipients = []
    from_user = None
    smtp_host = None
    smtp_port = 25

    smtp_tls = False

    smtp_user = None
    smtp_password = None

    def save(self, resq=None):
        if not self.recipients or not self.smtp_host or not self.from_user:
            return

        message = self.create_message()
        subject = self.format_subject()

        message['Subject'] = subject
        message['From'] = self.from_user
        message['To'] = ", ".join(self.recipients)

        self.send_message(message)

    def format_subject(self):
        return self.subject.format(queue=self._queue,
                                   worker=self._worker,
                                   exception=self._exception)

    def create_message(self):
        """Returns a message body to send in this email."""

        body = dedent("""\
        Received exception {exception} on {queue} from worker {worker}:

        {traceback}

        Payload:
        {payload}

        """).format(exception=self._exception,
                   traceback=self._traceback,
                   queue=self._queue,
                   payload=self._payload,
                   worker=self._worker)

        return MIMEText(body)

    def send_message(self, message):
        smtp = smtplib.SMTP(self.smtp_host, self.smtp_port)

        try:
            smtp.ehlo()

            if self.smtp_tls:
                smtp.starttls()

            if self.smtp_user:
                smtp.login(self.smtp_user, self.smtp_password)

            smtp.sendmail(self.from_user, self.recipients, message.as_string())
        finally:
            smtp.close()
