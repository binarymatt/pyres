import smtplib

from textwrap import dedent
from email.mime.text import MIMEText

from base import BaseBackend

class MailBackend(BaseBackend):
    """Extends ``BaseBackend`` to provide support for emailing failures.
    Intended to be used with the MultipleBackend:

    from pyres import failure

    from pyres.failure.mail import MailBackend
    from pyres.failure.multiple import MultipleBackend
    from pyres.failure.redis import RedisBackend

    class EmailFailure(MailBackend):
        subject = 'Pyres Failure on {queue}'
        from_user = 'My Email User <mailuser@mydomain.tld>'
        recipients = ['Me <me@mydomain.tld>']

        smtp_host = 'mail.mydomain.tld'
        smtp_port = 25
        smtp_tls = True

        smtp_user = 'mailuser'
        smtp_password = 'm41lp455w0rd'

    failure.backend = MultipleBackend
    failure.backend.classes = [RedisBackend, EmailFailure]


    Additional notes:
        - The following tokens are available in subject: queue, worker, exception

        - Override the create_message method to provide an alternate body. It
        should return one of the message types from email.mime.*
    """
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
        """Returns a message body to send in this email. Should be from email.mime.*"""

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
